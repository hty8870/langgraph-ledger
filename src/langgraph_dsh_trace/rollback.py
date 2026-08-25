# -*- coding: utf-8 -*-
"""Rollback and fork — the payoff of the whole design.

LangGraph gives per-thread checkpoints with parent links; this module turns
them into dsh-style operations:

- ``time_travel_config`` — resume/fork a thread from any recorded checkpoint
  (LangGraph-native: streaming with this config forks from that checkpoint);
- ``fork_thread`` — create a NEW thread seeded with the source thread's
  checkpoint ancestry up to a chosen checkpoint, and log a ``fork`` event with
  the parent lineage (dsh SessionHeader's parentSession + seedLength).

Because every checkpoint carries a content hash, a fork can be verified after
the fact with :func:`langgraph_dsh_trace.verify.verify_thread`.
"""
from __future__ import annotations

import uuid
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver, RunnableConfig

from . import events as ev
from .recorder import TraceRecorder

__all__ = ["time_travel_config", "fork_thread", "find_checkpoint_by_label"]


def time_travel_config(thread_id: str, checkpoint_id: str,
                       *, checkpoint_ns: str = "") -> RunnableConfig:
    """Config that makes the next invocation fork from a past checkpoint."""
    return {"configurable": {"thread_id": str(thread_id),
                             "checkpoint_ns": str(checkpoint_ns),
                             "checkpoint_id": str(checkpoint_id)}}


def _cfg(thread_id: str, checkpoint_id: str | None = None,
         checkpoint_ns: str = "") -> RunnableConfig:
    conf: dict[str, Any] = {"thread_id": str(thread_id),
                            "checkpoint_ns": str(checkpoint_ns)}
    if checkpoint_id:
        conf["checkpoint_id"] = str(checkpoint_id)
    return {"configurable": conf}


def _ancestry(saver: BaseCheckpointSaver, thread_id: str,
              checkpoint_id: str, checkpoint_ns: str = "") -> list:
    """Oldest→newest checkpoint tuples from the thread root to `checkpoint_id`."""
    chain = []
    current: str | None = checkpoint_id
    while current:
        tup = saver.get_tuple(_cfg(thread_id, current, checkpoint_ns))
        if tup is None:
            break
        chain.append(tup)
        md = dict(tup.metadata or {})
        parents = md.get("parents") or {}
        current = parents.get(checkpoint_ns) or (tup.parent_config or {}).get(
            "configurable", {}).get("checkpoint_id")
    chain.reverse()
    return chain


def fork_thread(saver: BaseCheckpointSaver, source_thread_id: str, *,
                at_checkpoint_id: str | None = None,
                new_thread_id: str | None = None,
                checkpoint_ns: str = "",
                recorder: TraceRecorder | None = None) -> str:
    """Fork `source_thread_id` into a new thread seeded up to a checkpoint.

    Copies the ancestry chain (checkpoints + their pending writes) into the new
    thread preserving checkpoint ids, then logs a ``fork`` event with the
    lineage. Returns the new thread id.
    """
    new_id = str(new_thread_id or uuid.uuid4().hex)

    if at_checkpoint_id is None:
        latest = saver.get_tuple(_cfg(source_thread_id, None, checkpoint_ns))
        if latest is None:
            raise ValueError(f"thread has no checkpoints: {source_thread_id}")
        at_checkpoint_id = latest.checkpoint["id"]

    chain = _ancestry(saver, source_thread_id, at_checkpoint_id, checkpoint_ns)
    if not chain:
        raise ValueError(f"checkpoint not found: {at_checkpoint_id}")

    parent_id: str | None = None
    for tup in chain:
        # put(): config carries the PARENT checkpoint id; put_writes(): config
        # must carry the OWNING checkpoint id (InMemorySaver hard-requires it).
        saver.put(_cfg(new_id, parent_id, checkpoint_ns),
                  tup.checkpoint, tup.metadata or {},
                  tup.checkpoint.get("channel_versions") or {})
        own_cfg = _cfg(new_id, tup.checkpoint["id"], checkpoint_ns)
        for write in (tup.pending_writes or []):
            task_id, channel, value = write[0], write[1], write[2]
            saver.put_writes(own_cfg, [(channel, value)], str(task_id))
        parent_id = tup.checkpoint["id"]

    if recorder is not None and recorder.enabled:
        recorder.emit(ev.KIND_FORK, ev.fork_payload(
            parent_thread_id=source_thread_id,
            seed_upto_checkpoint_id=at_checkpoint_id,
            seed_events=len(chain), child_thread_id=new_id))
    return new_id


def find_checkpoint_by_label(recorder: TraceRecorder, label: str) -> str | None:
    """Resolve a recorded ``cp_*`` label to its checkpoint id via the trace log.

    Labels live in the log (state/snapshot events), not in the saver, so the
    log is the lookup source.
    """
    from .recorder import read_log

    if not recorder.path.exists():
        return None
    for e in read_log(recorder.path):
        if e.get("kind") != ev.KIND_STATE_SNAPSHOT:
            continue
        payload = e.get("payload") or {}
        if payload.get("label") == label:
            return payload.get("checkpoint_id")
    return None
