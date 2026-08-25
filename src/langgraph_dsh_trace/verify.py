# -*- coding: utf-8 -*-
"""Integrity verification — prove the record was not tampered with.

Two levels:

- ``verify_log``: recompute the hash chain of one event log. Catches deletion,
  reordering, editing and truncation attempts (seq gaps, broken prev links,
  id mismatches).
- ``verify_thread``: additionally re-read every recorded checkpoint from the
  saver and re-hash it, proving the *stored state* still matches the *logged
  claim*. A checkpoint whose content changed after the fact fails here.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver

from . import events as ev
from .hashing import GENESIS_PREV, event_id, sha256_hex
from .recorder import read_log


def _checkpoint_sha(serde, checkpoint) -> str:
    """Roundtrip-normalized checkpoint hash (same口径 as the write side)."""
    tag1, blob1 = serde.dumps_typed(checkpoint)
    restored = serde.loads_typed((tag1, blob1))
    tag2, blob2 = serde.dumps_typed(restored)
    if isinstance(blob2, str):
        blob2 = blob2.encode("utf-8")
    return sha256_hex(tag2.encode() + b"\x00" + blob2)

__all__ = ["verify_log", "verify_thread", "VerifyReport"]


class VerifyReport(dict):
    """dict report with a boolean truth value: truthy ⇔ verified ok."""

    def __bool__(self) -> bool:
        return bool(self.get("ok"))


def verify_log(path: str | Path) -> VerifyReport:
    """Re-verify one JSONL log file. Returns a report dict (truthy when ok)."""
    errors: list[dict[str, Any]] = []
    events = 0
    unknown_kinds: dict[str, int] = {}
    expected_seq = 0
    expected_prev = GENESIS_PREV
    version: int | None = None

    try:
        lines = list(read_log(path))
    except (OSError, ValueError) as exc:
        return VerifyReport(ok=False, events=0,
                            errors=[{"seq": None, "error": f"log unreadable: {exc}"}])

    for e in lines:
        seq = e.get("seq")
        kind = str(e.get("kind") or "")
        if kind and kind not in ev.EVENT_KINDS:
            unknown_kinds[kind] = unknown_kinds.get(kind, 0) + 1  # forward-compat: count, don't reject
        if version is None:
            version = e.get("v")
        if seq != expected_seq:
            errors.append({"seq": seq, "error": f"seq gap: expected {expected_seq}"})
        if e.get("prev") != expected_prev:
            errors.append({"seq": seq, "error": "prev link broken (deletion/reorder/edit?)"})
        recomputed = event_id(version=e.get("v", 0), seq=e.get("seq", -1),
                              ts=e.get("ts", ""), kind=kind,
                              payload=e.get("payload"), prev=e.get("prev", ""))
        if recomputed != e.get("id"):
            errors.append({"seq": seq, "error": "id mismatch: content was modified"})
        expected_seq = (seq or 0) + 1
        expected_prev = str(e.get("id") or "")
        events += 1

    return VerifyReport(ok=not errors, events=events, version=version,
                        errors=errors, unknown_kinds=unknown_kinds)


def verify_thread(saver: BaseCheckpointSaver, path: str | Path,
                  *, checkpoint_ns: str = "") -> VerifyReport:
    """verify_log + re-hash every logged checkpoint against the saver's bytes."""
    report = verify_log(path)
    if not report.get("ok"):
        return report

    thread_id = Path(path).stem
    extra_errors: list[dict[str, Any]] = list(report.get("errors") or [])

    def cfg(cp_id: str) -> dict:
        return {"configurable": {"thread_id": thread_id,
                                 "checkpoint_ns": checkpoint_ns,
                                 "checkpoint_id": cp_id}}

    for e in read_log(path):
        if e.get("kind") != ev.KIND_STATE_SNAPSHOT:
            continue
        payload = e.get("payload") or {}
        cp_id = payload.get("checkpoint_id")
        claimed = payload.get("checkpoint_sha256")
        if not cp_id or not claimed:
            continue
        tup = saver.get_tuple(cfg(cp_id))
        if tup is None:
            extra_errors.append({"seq": e.get("seq"),
                                 "error": f"checkpoint missing from saver: {cp_id}"})
            continue
        try:
            actual = _checkpoint_sha(saver.serde, tup.checkpoint)
        except Exception as exc:  # noqa: BLE001
            extra_errors.append({"seq": e.get("seq"),
                                 "error": f"checkpoint unhashable: {type(exc).__name__}"})
            continue
        if actual != claimed:
            extra_errors.append({"seq": e.get("seq"),
                                 "error": f"checkpoint content drifted: {cp_id}"})

    ok = not extra_errors
    out = VerifyReport(report)
    out["errors"] = extra_errors
    out["ok"] = ok
    out["checkpoints_checked"] = sum(
        1 for e in read_log(path) if e.get("kind") == ev.KIND_STATE_SNAPSHOT)
    return out
