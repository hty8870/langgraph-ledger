# -*- coding: utf-8 -*-
"""Append-only, hash-chained JSONL recorder.

Design rules (inherited from DeepSeek Harness's session log and hardened for
audit use):

- The log is the source of truth: append-only, one file per thread.
- Every line is a self-contained envelope ``{v, seq, ts, kind, payload, id, prev}``
  where ``id`` hash-chains the event to its predecessor — deleting, reordering
  or editing any line breaks the chain and is caught by :func:`verify_log`.
- Payloads are validated as strict JSON at *append* time: bad events fail at
  the entrance, not at read time.
- The observer must never take down the observed: :meth:`TraceRecorder.emit`
  is fail-soft (warn-once on stderr, event dropped); :meth:`TraceRecorder.append`
  is strict for tests/CLI.
- Recorder is propagated via contextvars so integration points need no
  signature changes.
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import re
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .events import FORMAT_VERSION
from .hashing import GENESIS_PREV, event_id

__all__ = [
    "TracePayloadError",
    "TraceRecorder",
    "active_recorder",
    "bind_recorder",
    "current_recorder",
    "emit_event",
    "read_log",
    "recorder_for",
]

_SEGMENT_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")
_PATH_LOCKS: dict[str, threading.Lock] = {}
_PATH_LOCKS_GUARD = threading.RLock()  # RLock: recorder_for → __init__ → _lock_for nests
_WARNED: set[str] = set()


def _warn_once(key: str, message: str) -> None:
    if key not in _WARNED:
        _WARNED.add(key)
        print(f"[langgraph_dsh_trace] {message}", file=sys.stderr)


def _safe_segment(value: Any) -> str:
    """One path segment, whitelist-sanitized; ``..`` sequences are collapsed."""
    seg = _SEGMENT_UNSAFE.sub("_", str(value or ""))
    while ".." in seg:
        seg = seg.replace("..", "__")
    seg = seg.strip(".")
    return seg or "x"


def _lock_for(path: Path) -> threading.Lock:
    key = str(path)
    with _PATH_LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PATH_LOCKS[key] = lock
        return lock


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class TracePayloadError(ValueError):
    """Payload is not a strictly JSON-serializable dict (append-time rejection)."""


def _checked_envelope(version: int, seq: int, ts: str, kind: str,
                      payload: Any, prev: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TracePayloadError(f"payload must be a dict, got {type(payload).__name__}")
    if not kind or not str(kind).strip():
        raise TracePayloadError("kind must be non-empty")
    try:
        eid = event_id(version=version, seq=seq, ts=ts, kind=kind,
                       payload=payload, prev=prev)
        envelope = {"v": int(version), "seq": int(seq), "ts": ts, "kind": str(kind),
                    "payload": payload, "id": eid, "prev": str(prev)}
        json.dumps(envelope, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TracePayloadError(f"payload is not JSON-serializable: {exc}") from exc
    return envelope


class TraceRecorder:
    """Append-only recorder for one thread's event log.

    ``seq`` and the hash chain resume from the existing file when reopening,
    so a continued thread never renumbers and never forks the chain.
    """

    def __init__(self, root: str | Path, thread_id: str,
                 *, enabled: bool = True, version: int = FORMAT_VERSION) -> None:
        self.root = Path(root)
        self.thread_id = str(thread_id or "anonymous")
        self.enabled = bool(enabled)
        self.version = int(version)
        self.path = self.root / f"{_safe_segment(self.thread_id)}.jsonl"
        self._lock = _lock_for(self.path)
        self.dropped = 0
        self._next_seq = 0
        self._prev = GENESIS_PREV
        if self.enabled and self.path.exists():
            try:
                for line in read_log(self.path):
                    self._next_seq = line["seq"] + 1
                    self._prev = line["id"]
            except (OSError, KeyError, json.JSONDecodeError) as exc:
                _warn_once(f"resume::{type(exc).__name__}",
                           f"existing log unreadable ({type(exc).__name__}); "
                           "starting a fresh chain — investigate manually.")

    # -- writing ------------------------------------------------------------

    def append(self, kind: str, payload: dict) -> dict[str, Any]:
        """Strict append: bad payload or write failure raises."""
        with self._lock:
            envelope = _checked_envelope(self.version, self._next_seq,
                                         _now_iso(), kind, payload, self._prev)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(envelope, ensure_ascii=False, allow_nan=False) + "\n")
                fh.flush()
            self._prev = envelope["id"]
            self._next_seq += 1
        return envelope

    def emit(self, kind: str, payload: Any) -> bool:
        """Fail-soft append for the hot path. Never raises; returns persisted?."""
        if not self.enabled:
            return False
        try:
            self.append(kind, payload)
            return True
        except Exception as exc:  # noqa: BLE001 — observer faults are dropped, logged once
            self.dropped += 1
            _warn_once(f"emit::{type(exc).__name__}",
                       f"trace event dropped ({type(exc).__name__}); log may be incomplete.")
            return False

    # -- introspection --------------------------------------------------------

    @property
    def next_seq(self) -> int:
        return self._next_seq


def read_log(path: str | Path) -> Iterator[dict[str, Any]]:
    """Iterate a JSONL log, yielding envelopes. Blank lines are skipped."""
    with Path(path).open(encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                yield json.loads(raw)


# ---- contextvars propagation (zero-signature-change integration) -------------

_CURRENT: contextvars.ContextVar["TraceRecorder | None"] = contextvars.ContextVar(
    "langgraph_dsh_trace_recorder", default=None)


def current_recorder() -> "TraceRecorder | None":
    return _CURRENT.get()


def active_recorder() -> "TraceRecorder | None":
    """Bound *and* enabled — the uniform short-circuit for hook points."""
    rec = _CURRENT.get()
    return rec if (rec is not None and rec.enabled) else None


@contextlib.contextmanager
def bind_recorder(recorder: "TraceRecorder | None") -> "Iterator[TraceRecorder | None]":
    token = _CURRENT.set(recorder)
    try:
        yield recorder
    finally:
        _CURRENT.reset(token)


def emit_event(kind: str, payload: Any) -> bool:
    """One-line integration point: silently no-ops when no recorder is bound."""
    rec = current_recorder()
    return rec.emit(kind, payload) if rec is not None else False


# ---- per-path recorder cache (one writer per log) ----------------------------

#: Two live recorders on one path would diverge (each caches next seq/prev) —
#: the fork flow hits this when the checkpointer and the fork logger target the
#: same thread. All recorder construction goes through recorder_for().
_RECORDERS: dict[str, "TraceRecorder"] = {}


def recorder_for(root: str | Path, thread_id: str, *,
                 enabled: bool = True) -> "TraceRecorder":
    """The one recorder per (root, thread) — cached by path. The first call's
    `enabled` wins; later calls get the same instance regardless."""
    key = str(Path(root) / f"{_safe_segment(thread_id)}.jsonl")
    with _PATH_LOCKS_GUARD:
        rec = _RECORDERS.get(key)
        if rec is None:
            rec = TraceRecorder(root, thread_id, enabled=enabled)
            _RECORDERS[key] = rec
        return rec


def new_thread_id() -> str:
    return uuid.uuid4().hex
