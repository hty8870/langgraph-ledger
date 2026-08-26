# -*- coding: utf-8 -*-
"""Replay — rebuild the message timeline from a log (dsh's deriveMessages analog).

Faithful to the dsh rule with an honest constraint: the log is the source of
truth, and reconstruction yields exactly what was recorded. With the default
digest-only payloads you get the *shape* of the conversation (roles, order,
labels, digests); with ``record_full=True`` payloads you get the text.

The function never invents content: a digest-only event renders as its digest,
marked ``content_full: False``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import events as ev
from .recorder import read_log

__all__ = ["replay_messages", "ReplayError"]


class ReplayError(ValueError):
    """The log cannot support reconstruction (corrupt envelope, bad order)."""


def _message(role: str, content: Any, *, full: bool, **extra: Any) -> dict[str, Any]:
    return {"role": role, "content": content, "content_full": full, **extra}


def replay_messages(path_or_events: str | Path | list[dict]) -> list[dict[str, Any]]:
    """Rebuild the ordered message list from a log.

    Returns a list of messages:
    - ``user``      — from the run start (input digests; full text if recorded);
    - ``assistant`` — from llm/call (response digest or full text);
    - ``tool``      — from tool/result paired to its call label.
    """
    if isinstance(path_or_events, (str, Path)):
        events = list(read_log(path_or_events))
    else:
        events = list(path_or_events)

    messages: list[dict[str, Any]] = []
    pending_calls: dict[str, dict[str, Any]] = {}  # label -> call payload

    for e in events:
        kind = e.get("kind")
        payload = e.get("payload") or {}

        if kind == ev.KIND_RUN_START:
            pass  # run inputs are not messages in our vocabulary
        elif kind == ev.KIND_LLM_CALL:
            full = "response_text" in payload
            messages.append(_message(
                "assistant",
                payload.get("response_text") if full else payload.get("response"),
                full=full, node=payload.get("node", ""), model=payload.get("model", ""),
                ms=payload.get("ms", 0), error=payload.get("error", "")))
        elif kind == ev.KIND_TOOL_CALL:
            pending_calls[str(payload.get("label") or "")] = payload
            full = "input" in payload
            messages.append(_message(
                "tool_call",
                payload.get("input") if full else payload.get("input_digest"),
                full=full, label=payload.get("label", ""), name=payload.get("name", "")))
        elif kind == ev.KIND_TOOL_RESULT:
            label = str(payload.get("label") or "")
            call = pending_calls.pop(label, {})
            full = "output" in payload
            messages.append(_message(
                "tool",
                payload.get("output") if full else payload.get("output_digest"),
                full=full, label=label, name=payload.get("name") or call.get("name", ""),
                ok=payload.get("ok"), ms=payload.get("ms", 0),
                error=payload.get("error", "")))

    # a call whose result never arrived (crash mid-tool) is surfaced, not dropped
    for label, call in pending_calls.items():
        messages.append(_message("tool", None, full=False, label=label,
                                 name=call.get("name", ""), ok=None,
                                 error="interrupted: no result recorded"))
    return messages
