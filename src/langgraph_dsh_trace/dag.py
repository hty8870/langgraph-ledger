# -*- coding: utf-8 -*-
"""Build the run DAG from a recorded event log.

Node = one event. Edges:
- ``chain``   — the hash chain itself (prev → id), the tamper-evident timeline;
- ``call``    — tool/call → its tool/result (paired by label/run id);
- ``writes``  — state/snapshot → the checkpoint/writes that followed it;
- ``parent``  — state/snapshot → its parent snapshot (the checkpoint DAG edge
  LangGraph gives us via the config's checkpoint_id);
- ``fork``    — fork event → the child thread it seeded (cross-log lineage).

When several edge kinds would connect the same pair, the most *semantic* one
wins (parent > call > writes > chain): the timeline is already readable from
seq order, so the chain edge yields to any richer relation.

Export as plain JSON or Mermaid (errors highlighted) for audit reports.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import events as ev
from .recorder import read_log

__all__ = ["RunDAG", "build_dag", "build_dag_from_file"]

#: edge-kind priority when several kinds hit the same (from, to) pair
_PRIORITY = {"parent": 4, "call": 3, "writes": 2, "fork": 2, "chain": 1}


class RunDAG:
    def __init__(self) -> None:
        self.nodes: dict[int, dict[str, Any]] = {}
        self._edges: dict[tuple[int, int], str] = {}

    def add_node(self, seq: int, kind: str, summary: str, label: str = "",
                 *, error: bool = False) -> None:
        self.nodes[seq] = {"seq": seq, "kind": kind, "summary": summary,
                           "label": label, "error": error}

    def add_edge(self, src: int, dst: int, kind: str) -> None:
        key = (src, dst)
        existing = self._edges.get(key)
        if existing is None or _PRIORITY.get(kind, 0) > _PRIORITY.get(existing, 0):
            self._edges[key] = kind

    @property
    def edges(self) -> list[dict[str, Any]]:
        return [{"from": s, "to": d, "kind": k}
                for (s, d), k in sorted(self._edges.items())]

    # -- exports ---------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {"nodes": [self.nodes[k] for k in sorted(self.nodes)],
                "edges": self.edges}

    def to_mermaid(self) -> str:
        """Mermaid flowchart; node text = seq + kind + summary; errors in red."""
        lines = ["flowchart LR"]
        for seq in sorted(self.nodes):
            n = self.nodes[seq]
            text = f"{seq} {n['kind']}"
            if n["summary"]:
                text += f" {n['summary']}"
            text = text.replace('"', "'")[:60]
            lines.append(f'  n{seq}["{text}"]')
            if n["error"]:
                lines.append(f"  class n{seq} err")
        style = {"chain": "-->", "call": "-.->", "writes": "-.->",
                 "parent": "==>", "fork": "==>"}
        for e in self.edges:
            arrow = style.get(e["kind"], "-->")
            lines.append(f"  n{e['from']} {arrow} n{e['to']}")
        if any(n["error"] for n in self.nodes.values()):
            lines.append("  classDef err fill:#f66,stroke:#900,color:#fff")
        return "\n".join(lines)


def _is_error(kind: str, payload: dict) -> bool:
    if kind == ev.KIND_ERROR:
        return True
    if kind == ev.KIND_TOOL_RESULT and not payload.get("ok"):
        return True
    if kind == ev.KIND_LLM_CALL and payload.get("error"):
        return True
    if kind == ev.KIND_NODE_END and not payload.get("ok", True):
        return True
    if kind == ev.KIND_RUN_END and payload.get("status") not in ("completed", ""):
        return True
    return False


def _summarize(kind: str, payload: dict) -> str:
    if kind == ev.KIND_TOOL_CALL:
        return str(payload.get("name") or "")
    if kind == ev.KIND_TOOL_RESULT:
        return "ok" if payload.get("ok") else "FAIL"
    if kind in (ev.KIND_NODE_START, ev.KIND_NODE_END):
        return str(payload.get("node") or "")
    if kind == ev.KIND_STATE_SNAPSHOT:
        return str(payload.get("label") or "")[:20]
    if kind == ev.KIND_LLM_CALL:
        return str(payload.get("model") or "")
    if kind == ev.KIND_RUN_END:
        return str(payload.get("status") or "")
    if kind == ev.KIND_FORK:
        return "→ " + str(payload.get("child_thread_id") or "")[:12]
    return ""


def build_dag(events: list[dict[str, Any]]) -> RunDAG:
    dag = RunDAG()
    by_id: dict[str, int] = {}
    call_by_run: dict[str, int] = {}
    call_by_label: dict[str, int] = {}
    snapshot_by_cp: dict[str, int] = {}
    last_snapshot_seq: int | None = None

    for e in events:
        seq = int(e.get("seq", 0))
        kind = str(e.get("kind") or "")
        payload = e.get("payload") or {}
        dag.add_node(seq, kind, _summarize(kind, payload),
                     label=str(payload.get("label") or ""),
                     error=_is_error(kind, payload))
        by_id[str(e.get("id") or "")] = seq

        prev = str(e.get("prev") or "")
        if prev in by_id:
            dag.add_edge(by_id[prev], seq, "chain")

        if kind == ev.KIND_TOOL_CALL:
            call_by_run[str(payload.get("run_id") or "")] = seq
            call_by_label.setdefault(str(payload.get("label") or ""), seq)
        elif kind == ev.KIND_TOOL_RESULT:
            src = call_by_run.get(str(payload.get("run_id") or ""))
            if src is None and payload.get("label"):
                src = call_by_label.get(str(payload["label"]))
            if src is not None:
                dag.add_edge(src, seq, "call")
        elif kind == ev.KIND_STATE_SNAPSHOT:
            parent_cp = payload.get("parent_checkpoint_id")
            if parent_cp and str(parent_cp) in snapshot_by_cp:
                dag.add_edge(snapshot_by_cp[str(parent_cp)], seq, "parent")
            snapshot_by_cp[str(payload.get("checkpoint_id") or "")] = seq
            last_snapshot_seq = seq
        elif kind == ev.KIND_CHECKPOINT_WRITES and last_snapshot_seq is not None:
            dag.add_edge(last_snapshot_seq, seq, "writes")
    return dag


def build_dag_from_file(path: str | Path) -> RunDAG:
    return build_dag(list(read_log(path)))
