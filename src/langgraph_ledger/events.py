# -*- coding: utf-8 -*-
"""Controlled event vocabulary and payload constructors.

Modeled on DeepSeek Harness's SessionEvent discipline (append-only log as the
single source of truth, format version, ignorable marker for forward
compatibility) adapted to a LangGraph run:

- run/start · run/end          — one graph invocation ("turn" in dsh terms)
- node/start · node/end        — one node execution ("step" in dsh terms)
- llm/call                     — one model call (prompt/response digests)
- tool/call · tool/result      — one tool invocation, hash-labeled
- state/snapshot               — one checkpoint write (content hash + parent)
- checkpoint/writes            — pending writes of one task
- fork                         — session lineage (parent thread + seed length)
- error                        — anything that failed

Payload builders are pure functions; the recorder owns persistence. Unknown
kinds on read are counted, not rejected (forward compatibility).
"""
from __future__ import annotations

from typing import Any

from .hashing import sha256_hex

__all__ = [
    "FORMAT_VERSION",
    "KIND_RUN_START", "KIND_RUN_END",
    "KIND_NODE_START", "KIND_NODE_END",
    "KIND_LLM_CALL",
    "KIND_TOOL_CALL", "KIND_TOOL_RESULT",
    "KIND_STATE_SNAPSHOT", "KIND_CHECKPOINT_WRITES",
    "KIND_FORK", "KIND_ERROR",
    "EVENT_KINDS",
    "digest_text",
    "run_start_payload", "run_end_payload",
    "node_boundary_payload", "llm_call_payload",
    "tool_call_payload", "tool_result_payload",
    "state_snapshot_payload", "checkpoint_writes_payload",
    "fork_payload", "error_payload",
]

#: On-disk log format version. Bump when an older runtime could no longer read
#: a new log with full semantic correctness (dsh rule: "parses" is not enough).
FORMAT_VERSION = 0

KIND_RUN_START = "run/start"
KIND_RUN_END = "run/end"
KIND_NODE_START = "node/start"
KIND_NODE_END = "node/end"
KIND_LLM_CALL = "llm/call"
KIND_TOOL_CALL = "tool/call"
KIND_TOOL_RESULT = "tool/result"
KIND_STATE_SNAPSHOT = "state/snapshot"
KIND_CHECKPOINT_WRITES = "checkpoint/writes"
KIND_FORK = "fork"
KIND_ERROR = "error"

EVENT_KINDS: frozenset[str] = frozenset({
    KIND_RUN_START, KIND_RUN_END, KIND_NODE_START, KIND_NODE_END,
    KIND_LLM_CALL, KIND_TOOL_CALL, KIND_TOOL_RESULT,
    KIND_STATE_SNAPSHOT, KIND_CHECKPOINT_WRITES, KIND_FORK, KIND_ERROR,
})


def digest_text(text: Any) -> dict[str, Any]:
    """Content digest without the content: sha256 + char count + 80-char head.

    Answers "what was this call about" for audit without persisting full
    prompts/responses (size + sensitivity). Full text is opt-in at the call
    site, never default.
    """
    s = str(text if text is not None else "")
    return {"sha256": sha256_hex(s), "chars": len(s), "head": s[:80]}


def run_start_payload(*, graph: str = "", thread_id: str = "",
                      checkpoint_id: str | None = None) -> dict[str, Any]:
    return {"graph": str(graph or ""), "thread_id": str(thread_id or ""),
            "checkpoint_id": checkpoint_id}


def run_end_payload(*, status: str, error: str = "", steps: int = 0,
                    ms: int = 0) -> dict[str, Any]:
    """status: completed | error | aborted (dsh turn/end reason, simplified)."""
    return {"status": str(status or ""), "error": str(error or ""),
            "steps": int(steps or 0), "ms": int(ms or 0)}


def node_boundary_payload(*, node: str, run_id: str = "",
                          checkpoint_id: str | None = None) -> dict[str, Any]:
    return {"node": str(node or ""), "run_id": str(run_id or ""),
            "checkpoint_id": checkpoint_id}


def llm_call_payload(*, node: str, model: str, prompt: Any, response: Any,
                     ms: int, usage: dict | None = None,
                     error: str = "", record_full: bool = False) -> dict[str, Any]:
    payload = {"node": str(node or ""), "model": str(model or ""),
               "prompt": digest_text(prompt), "response": digest_text(response),
               "ms": int(ms or 0), "usage": usage, "error": str(error or "")}
    if record_full:
        payload["prompt_text"] = str(prompt if prompt is not None else "")
        payload["response_text"] = str(response if response is not None else "")
    return payload


def tool_call_payload(*, label: str, node: str, name: str, tool_input: Any,
                      run_id: str = "", parent_run_id: str = "",
                      record_full: bool = False) -> dict[str, Any]:
    """One tool invocation. `label` is the content-addressed identity from
    hashing.tool_call_label — identical (name, input) pairs share a label."""
    payload = {"label": str(label or ""), "node": str(node or ""),
               "name": str(name or ""), "input_digest": digest_text(tool_input),
               "run_id": str(run_id or ""), "parent_run_id": str(parent_run_id or "")}
    if record_full:
        payload["input"] = tool_input
    return payload


def tool_result_payload(*, label: str, name: str, ok: bool, ms: int = 0,
                        output: Any = None, error: str = "",
                        run_id: str = "", record_full: bool = False) -> dict[str, Any]:
    payload = {"label": str(label or ""), "name": str(name or ""), "ok": bool(ok),
               "ms": int(ms or 0),
               "output_digest": digest_text(output) if output is not None else None,
               "error": str(error or ""), "run_id": str(run_id or "")}
    if record_full and output is not None:
        payload["output"] = output if isinstance(output, (dict, list)) else str(output)
    return payload


def state_snapshot_payload(*, checkpoint_id: str, checkpoint_sha256: str,
                           label: str, parent_checkpoint_id: str | None,
                           step: int | None, source: str = "") -> dict[str, Any]:
    return {"checkpoint_id": str(checkpoint_id or ""),
            "checkpoint_sha256": str(checkpoint_sha256 or ""),
            "label": str(label or ""),
            "parent_checkpoint_id": parent_checkpoint_id,
            "step": step, "source": str(source or "")}


def checkpoint_writes_payload(*, checkpoint_id: str | None, task_id: str,
                              writes: list[dict]) -> dict[str, Any]:
    """Pending writes of one task; each write carries channel + value digest."""
    return {"checkpoint_id": checkpoint_id, "task_id": str(task_id or ""),
            "writes": list(writes or [])}


def fork_payload(*, parent_thread_id: str, seed_upto_checkpoint_id: str | None,
                 seed_events: int, child_thread_id: str) -> dict[str, Any]:
    """dsh SessionHeader lineage, adapted: parent session + seed boundary."""
    return {"parent_thread_id": str(parent_thread_id or ""),
            "seed_upto_checkpoint_id": seed_upto_checkpoint_id,
            "seed_events": int(seed_events or 0),
            "child_thread_id": str(child_thread_id or "")}


def error_payload(*, where: str, error: str, detail: dict | None = None) -> dict[str, Any]:
    return {"where": str(where or ""), "error": str(error or ""),
            "detail": dict(detail or {})}
