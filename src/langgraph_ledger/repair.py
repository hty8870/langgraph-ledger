# -*- coding: utf-8 -*-
"""Crash recovery — dsh's `interrupted` semantics.

If the process dies mid-run, the log ends with an open ``run/start`` bracket
and no ``run/end``. On the next open, a reader cannot tell "still running"
from "died". dsh solves this by closing crash-orphaned turns on reload with an
``interrupted`` marker; we do the same:

- :func:`find_orphaned_runs` — logs whose last run bracket is unclosed;
- :func:`close_orphaned_run` — append ``run/end {status: interrupted}`` to one
  log (appended through the recorder, so the hash chain stays intact);
- :func:`repair_all` — sweep a trace root; idempotent.

The appended event is honest: it says the run *was interrupted*, not that it
completed. Pre-crash events are never touched.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import events as ev
from .recorder import TraceRecorder, read_log, recorder_for

__all__ = ["find_orphaned_runs", "close_orphaned_run", "repair_all"]


def _last_bracket(path: Path) -> str:
    """'open' if the log ends inside a run bracket, 'closed'/'empty' else."""
    state = "empty"
    for e in read_log(path):
        kind = e.get("kind")
        if kind == ev.KIND_RUN_START:
            state = "open"
        elif kind == ev.KIND_RUN_END:
            state = "closed"
    return state


def find_orphaned_runs(trace_root: str | Path) -> list[Path]:
    """All logs under a trace root whose last run was never closed."""
    root = Path(trace_root)
    if not root.is_dir():
        return []
    return [p for p in sorted(root.glob("*.jsonl")) if _last_bracket(p) == "open"]


def close_orphaned_run(path: str | Path, *, strict: bool = False) -> dict[str, Any]:
    """Append ``run/end {status: interrupted}`` if the log ends open.

    Idempotent: an already-closed log is a no-op. The event goes through the
    recorder so it joins the hash chain like any other entry.
    """
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(p)
    if _last_bracket(p) != "open":
        return {"log": str(p), "closed": False, "reason": "not orphaned"}
    rec = recorder_for(p.parent, p.stem, strict=strict)
    envelope = rec.append(ev.KIND_RUN_END, ev.run_end_payload(status="interrupted"))
    return {"log": str(p), "closed": True, "seq": envelope["seq"]}


def repair_all(trace_root: str | Path, *, strict: bool = False) -> dict[str, Any]:
    """Close every orphaned run under a trace root. Returns a summary."""
    repaired = []
    for p in find_orphaned_runs(trace_root):
        repaired.append(close_orphaned_run(p, strict=strict))
    return {"scanned": str(trace_root), "repaired": repaired,
            "count": len(repaired)}
