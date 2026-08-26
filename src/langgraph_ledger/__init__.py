# -*- coding: utf-8 -*-
"""Public API of langgraph-ledger."""
from __future__ import annotations

from .analysis import analyze_log
from .callbacks import DshTraceCallbackHandler
from .checkpointer import TracingCheckpointSaver
from .dag import RunDAG, build_dag, build_dag_from_file
from .events import EVENT_KINDS, FORMAT_VERSION
from .hashing import (canonical_json, checkpoint_label, event_id, sha256_hex,
                      tool_call_label)
from .recorder import (TracePayloadError, TraceRecorder, active_recorder,
                       bind_recorder, current_recorder, emit_event, read_log,
                       recorder_for)
from .repair import close_orphaned_run, find_orphaned_runs, repair_all
from .replay import ReplayError, replay_messages
from .rollback import find_checkpoint_by_label, fork_thread, time_travel_config
from .verify import VerifyReport, chain_head, verify_log, verify_thread

__version__ = "0.2.1"

__all__ = [
    "__version__",
    # recording
    "TraceRecorder", "TracePayloadError", "bind_recorder", "current_recorder",
    "active_recorder", "emit_event", "read_log", "recorder_for",
    # integration
    "TracingCheckpointSaver", "DshTraceCallbackHandler",
    # labels / hashing
    "canonical_json", "sha256_hex", "event_id", "tool_call_label",
    "checkpoint_label", "EVENT_KINDS", "FORMAT_VERSION",
    # dag / rollback / audit
    "RunDAG", "build_dag", "build_dag_from_file",
    "fork_thread", "time_travel_config", "find_checkpoint_by_label",
    "verify_log", "verify_thread", "VerifyReport", "analyze_log",
    "chain_head",
    # crash recovery / replay
    "find_orphaned_runs", "close_orphaned_run", "repair_all",
    "replay_messages", "ReplayError",
]
