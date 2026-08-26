# -*- coding: utf-8 -*-
"""A drop-in wrapper that adds dsh-grade traceability to ANY LangGraph checkpointer.

Wraps an existing ``BaseCheckpointSaver`` (in-memory, SQLite, Postgres, Redis,
…) and, while passing every call through unchanged:

- hashes every checkpoint on write (content-addressed ``cp_*`` label + full
  sha256) and records its parent — this is the checkpoint DAG edge;
- records pending writes per task with digested values;
- emits all of it into the hash-chained append-only event log.

Behavioral contract of the wrapped saver is preserved exactly: return values,
exceptions and ordering all pass through. Trace failures are fail-soft and
never reach the caller.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Iterator, Sequence

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelProtocol,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    RunnableConfig,
    SerializerProtocol,
)

from . import events as ev
from .hashing import checkpoint_label, sha256_hex
from .recorder import TraceRecorder, current_recorder, recorder_for

__all__ = ["TracingCheckpointSaver", "normalized_checkpoint_digest"]


def normalized_checkpoint_digest(serde: SerializerProtocol,
                                 checkpoint: Checkpoint) -> tuple[str, str]:
    """(full sha256 hex, cp_* label) over the *roundtrip-normalized* checkpoint.

    The object handed to ``put`` may contain fields that do not survive
    serialization. Hashing ``dumps(loads(dumps(cp)))`` makes the write-time
    digest equal to the read-time digest — the property ``verify_thread``
    relies on to prove no post-hoc drift.
    """
    try:
        tag1, blob1 = serde.dumps_typed(checkpoint)
        restored = serde.loads_typed((tag1, blob1))
        tag2, blob2 = serde.dumps_typed(restored)
        if isinstance(blob2, str):
            blob2 = blob2.encode("utf-8")
        return sha256_hex(tag2.encode() + b"\x00" + blob2), checkpoint_label(blob2)
    except Exception:  # noqa: BLE001 — digest must never break a write
        return "", ""


def _cfg_ids(config: RunnableConfig) -> tuple[str, str, str | None]:
    conf = (config or {}).get("configurable") or {}
    return (str(conf.get("thread_id") or "anonymous"),
            str(conf.get("checkpoint_ns") or ""),
            conf.get("checkpoint_id"))


class TracingCheckpointSaver(BaseCheckpointSaver):
    """Wrap any BaseCheckpointSaver with hash-labeled, hash-chained tracing."""

    def __init__(self, inner: BaseCheckpointSaver, *,
                 trace_root: str | Path,
                 recorder: TraceRecorder | None = None,
                 enabled: bool = True) -> None:
        super().__init__(serde=inner.serde)
        self.inner = inner
        self.trace_root = Path(trace_root)
        self._default_recorder = recorder
        self.enabled = bool(enabled)

    # -- recorder resolution ------------------------------------------------------

    def recorder_for(self, thread_id: str) -> TraceRecorder:
        return recorder_for(self.trace_root, thread_id, enabled=self.enabled)

    def _rec_for(self, thread_id: str) -> TraceRecorder | None:
        bound = self._default_recorder or current_recorder()
        if bound is not None:
            return bound if bound.enabled else None
        if not self.enabled:
            return None
        return self.recorder_for(thread_id)

    # -- checkpoint hashing ---------------------------------------------------------

    def _checkpoint_digest(self, checkpoint: Checkpoint) -> tuple[str, str]:
        """(full sha256 hex, short cp_* label) — roundtrip-normalized."""
        return normalized_checkpoint_digest(self.serde, checkpoint)

    def _emit_snapshot(self, out_config: RunnableConfig,
                       metadata: CheckpointMetadata) -> None:
        """Hash the *stored* form: re-read the just-written checkpoint and digest
        its reconstruction — the same object verify_thread will see later."""
        thread_id, _ns, _parent = _cfg_ids(out_config)
        rec = self._rec_for(thread_id)
        if rec is None:
            return
        tup = self.inner.get_tuple(out_config)
        if tup is None:
            return
        full, label = self._checkpoint_digest(tup.checkpoint)
        md = dict(metadata or {})
        parent_id = (tup.parent_config or {}).get("configurable", {}).get("checkpoint_id")
        rec.emit(ev.KIND_STATE_SNAPSHOT, ev.state_snapshot_payload(
            checkpoint_id=str(tup.checkpoint.get("id") or ""),
            checkpoint_sha256=full, label=label,
            parent_checkpoint_id=str(parent_id) if parent_id else None,
            step=md.get("step"), source=str(md.get("source") or "")))

    # -- pass-through with tracing ---------------------------------------------------

    def put(self, config: RunnableConfig, checkpoint: Checkpoint,
            metadata: CheckpointMetadata, new_versions: Any) -> RunnableConfig:
        out = self.inner.put(config, checkpoint, metadata, new_versions)
        try:
            self._emit_snapshot(out, metadata)
        except Exception:  # noqa: BLE001 — observation never breaks the run
            pass
        return out

    def _trace_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]],
                      task_id: str) -> None:
        try:
            thread_id, _ns, checkpoint_id = _cfg_ids(config)
            rec = self._rec_for(thread_id)
            if rec is not None:
                digested = []
                for channel, value in writes:
                    try:
                        _t, blob = self.serde.dumps_typed(value)
                        digest = sha256_hex(blob if isinstance(blob, bytes)
                                            else str(blob).encode("utf-8"))
                        size = len(blob)
                    except Exception:  # noqa: BLE001
                        digest, size = "", 0
                    digested.append({"channel": str(channel), "sha256": digest,
                                     "size": int(size)})
                rec.emit(ev.KIND_CHECKPOINT_WRITES, ev.checkpoint_writes_payload(
                    checkpoint_id=str(checkpoint_id) if checkpoint_id else None,
                    task_id=task_id, writes=digested))
        except Exception:  # noqa: BLE001
            pass

    def put_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]],
                   task_id: str, task_path: str = "") -> None:
        self.inner.put_writes(config, writes, task_id, task_path)
        self._trace_writes(config, writes, task_id)

    # -- pure delegation ---------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self.inner.get_tuple(config)

    def list(self, config: RunnableConfig | None, *,
             filter: dict | None = None, before: RunnableConfig | None = None,
             limit: int | None = None) -> Iterator[CheckpointTuple]:
        yield from self.inner.list(config, filter=filter, before=before, limit=limit)

    def delete_thread(self, thread_id: str) -> None:
        self.inner.delete_thread(thread_id)

    # -- async: delegate to the inner saver's async path --------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await self.inner.aget_tuple(config)

    async def alist(self, config: RunnableConfig | None, *,
                    filter: dict | None = None, before: RunnableConfig | None = None,
                    limit: int | None = None) -> AsyncIterator[CheckpointTuple]:
        async for tup in self.inner.alist(config, filter=filter, before=before, limit=limit):
            yield tup

    async def aput(self, config: RunnableConfig, checkpoint: Checkpoint,
                   metadata: CheckpointMetadata, new_versions: Any) -> RunnableConfig:
        out = await self.inner.aput(config, checkpoint, metadata, new_versions)
        try:
            tup = await self.inner.aget_tuple(out)
            if tup is not None:
                thread_id, _ns, _p = _cfg_ids(out)
                rec = self._rec_for(thread_id)
                if rec is not None:
                    full, label = self._checkpoint_digest(tup.checkpoint)
                    md = dict(metadata or {})
                    parent_id = (tup.parent_config or {}).get(
                        "configurable", {}).get("checkpoint_id")
                    rec.emit(ev.KIND_STATE_SNAPSHOT, ev.state_snapshot_payload(
                        checkpoint_id=str(tup.checkpoint.get("id") or ""),
                        checkpoint_sha256=full, label=label,
                        parent_checkpoint_id=str(parent_id) if parent_id else None,
                        step=md.get("step"), source=str(md.get("source") or "")))
        except Exception:  # noqa: BLE001
            pass
        return out

    async def aput_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]],
                          task_id: str, task_path: str = "") -> None:
        await self.inner.aput_writes(config, writes, task_id, task_path)
        self._trace_writes(config, writes, task_id)

    async def adelete_thread(self, thread_id: str) -> None:
        await self.inner.adelete_thread(thread_id)

    # -- convenience -----------------------------------------------------------------------

    def register_channel(self, channel: ChannelProtocol, key: str) -> None:
        self.inner.register_channel(channel, key)
