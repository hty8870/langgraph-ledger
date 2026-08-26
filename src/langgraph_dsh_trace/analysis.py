# -*- coding: utf-8 -*-
"""Failure analysis over a recorded log.

Because every tool call carries a content-addressed label, exact-repeat loops
(the agent calling the same tool with the same input over and over) fall out
for free. Combined with error events and node timings this gives a compact
first-stop failure report:

    python -m langgraph_dsh_trace analyze <log.jsonl>
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import events as ev
from .recorder import read_log

__all__ = ["analyze_log"]


def analyze_log(path: str | Path) -> dict[str, Any]:
    """Summarize one thread log: counts, errors, loops, timeline, checkpoints."""
    by_kind: dict[str, int] = {}
    errors: list[dict[str, Any]] = []
    tool_labels: dict[str, dict[str, Any]] = {}
    checkpoints = 0
    interrupted_runs = 0
    models: dict[str, int] = {}
    node_ms: dict[str, int] = {}
    first_ts: str | None = None
    last_ts: str | None = None
    unknown: dict[str, int] = {}

    prev_call_label: str | None = None
    streaks: list[dict[str, Any]] = []
    streak: dict[str, Any] | None = None

    for e in read_log(path):
        kind = str(e.get("kind") or "")
        payload = e.get("payload") or {}
        seq = e.get("seq")
        first_ts = first_ts or e.get("ts")
        last_ts = e.get("ts") or last_ts
        if kind in ev.EVENT_KINDS:
            by_kind[kind] = by_kind.get(kind, 0) + 1
        else:
            unknown[kind] = unknown.get(kind, 0) + 1

        if kind == ev.KIND_TOOL_CALL:
            label = str(payload.get("label") or "")
            name = str(payload.get("name") or "")
            slot = tool_labels.setdefault(label, {"name": name, "count": 0, "seqs": []})
            slot["count"] += 1
            slot["seqs"].append(seq)
            if label == prev_call_label:
                if streak is None:
                    streak = {"label": label, "name": name, "start_seq": seq, "length": 2}
                else:
                    streak["length"] += 1
            else:
                if streak is not None:
                    streaks.append(streak)
                    streak = None
            prev_call_label = label
        elif kind == ev.KIND_TOOL_RESULT:
            if not payload.get("ok"):
                errors.append({"seq": seq, "kind": kind,
                               "name": payload.get("name"),
                               "error": payload.get("error")})
        elif kind == ev.KIND_LLM_CALL and payload.get("error"):
            errors.append({"seq": seq, "kind": kind, "node": payload.get("node"),
                           "error": payload.get("error")})
        if kind == ev.KIND_LLM_CALL and payload.get("model"):
            m = str(payload["model"])
            models[m] = models.get(m, 0) + 1
        elif kind == ev.KIND_NODE_END and not payload.get("ok", True):
            errors.append({"seq": seq, "kind": kind, "run_id": payload.get("run_id"),
                           "error": payload.get("error")})
        if kind == ev.KIND_NODE_END and payload.get("node"):
            node = str(payload["node"])
            node_ms[node] = node_ms.get(node, 0) + int(payload.get("ms") or 0)
        elif kind == ev.KIND_RUN_END and payload.get("status") == "interrupted":
            interrupted_runs += 1
        elif kind == ev.KIND_ERROR:
            errors.append({"seq": seq, "kind": kind, "where": payload.get("where"),
                           "error": payload.get("error")})
        elif kind == ev.KIND_STATE_SNAPSHOT:
            checkpoints += 1
            prev_call_label = None

    if streak is not None:
        streaks.append(streak)

    loops = [{"label": k, "name": v["name"], "count": v["count"], "seqs": v["seqs"]}
             for k, v in tool_labels.items() if v["count"] > 1]

    return {
        "log": str(path),
        "events": sum(by_kind.values()) + sum(unknown.values()),
        "by_kind": by_kind,
        "unknown_kinds": unknown,
        "checkpoints": checkpoints,
        "interrupted_runs": interrupted_runs,
        "models": models,
        "node_time_ms": node_ms,
        "errors": errors,
        "error_count": len(errors),
        "repeated_tool_calls": loops,
        "consecutive_repeat_streaks": streaks,
        "first_ts": first_ts,
        "last_ts": last_ts,
    }
