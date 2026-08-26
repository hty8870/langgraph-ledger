# -*- coding: utf-8 -*-
"""LangChain/LangGraph callback handler that feeds the trace recorder.

Attach to any LangGraph run:

    graph.invoke(input, config={**config, "callbacks": [DshTraceCallbackHandler()]})

LangChain propagates callbacks into every node, LLM call and tool call, so a
single handler sees the whole run. Node boundaries are recognized through
LangGraph's ``langgraph_node`` metadata; LLM/tool activity is paired by run id
— the tool *result* carries the same content label as its call, not just a
run-id echo.

``record_full=True`` opts into persisting full prompt/response/input/output
text alongside the digests (required for replay). Default off: digests only.

All emit calls are fail-soft unless the recorder is strict: tracing never
breaks the run.
"""
from __future__ import annotations

import time
from typing import Any
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler

from . import events as ev
from .hashing import tool_call_label
from .recorder import current_recorder

__all__ = ["DshTraceCallbackHandler"]


class DshTraceCallbackHandler(BaseCallbackHandler):
    """Emit dsh-style trace events for node / LLM / tool activity."""

    def __init__(self, recorder: Any = None, *, record_full: bool = False,
                 head_chars: int = 80) -> None:
        super().__init__()
        #: Optional explicit recorder; falls back to the context-bound one.
        self.recorder = recorder
        #: Opt-in: persist full text next to digests (replay prerequisite).
        self.record_full = bool(record_full)
        #: Plaintext preview length in digests; 0 = pure digest (no leak).
        self.head_chars = int(head_chars)
        self._starts: dict[str, float] = {}
        self._llm_open: dict[str, dict[str, Any]] = {}
        #: run_id -> {label, name, node} so the result can echo the call's label
        self._tool_open: dict[str, dict[str, Any]] = {}
        #: run_id -> node name, so node/end keeps its identity
        self._node_open: dict[str, str] = {}
        #: run_id -> {"steps": int} for the top-level graph run (run brackets —
        #: the crash-recovery anchor: an unclosed run/start means "died here")
        self._run_open: dict[str, dict[str, Any]] = {}

    # -- plumbing --------------------------------------------------------------

    def _rec(self) -> Any:
        return self.recorder if self.recorder is not None else current_recorder()

    def _emit(self, kind: str, payload: Any) -> bool:
        rec = self._rec()
        return rec.emit(kind, payload) if rec is not None else False

    @staticmethod
    def _node(metadata: dict | None) -> str:
        return str((metadata or {}).get("langgraph_node") or "")

    @staticmethod
    def _model_of(serialized: dict | None, metadata: dict | None,
                  llm_output: dict | None = None) -> str:
        """Best-effort model identity: langsmith metadata → serialized → llm_output."""
        for source in ((metadata or {}).get("ls_model_name"),
                       (serialized or {}).get("name"),
                       (serialized or {}).get("id", [None])[-1]
                       if isinstance((serialized or {}).get("id"), list) else None,
                       (llm_output or {}).get("model_name")):
            if source:
                return str(source)
        return ""

    def _mark(self, run_id: UUID) -> None:
        self._starts[str(run_id)] = time.monotonic()

    def _ms_since(self, run_id: UUID) -> int:
        start = self._starts.pop(str(run_id), None)
        return int((time.monotonic() - start) * 1000) if start is not None else 0

    # -- node boundaries (LangGraph chain runs carrying langgraph_node) ---------

    def on_chain_start(self, serialized: dict | None, inputs: Any, *,
                       run_id: UUID, parent_run_id: UUID | None = None,
                       metadata: dict | None = None, **kwargs: Any) -> None:
        node = self._node(metadata)
        if not node:
            if parent_run_id is None:
                # the top-level graph invocation — open a run bracket
                self._mark(run_id)
                self._run_open[str(run_id)] = {"steps": 0}
                self._emit(ev.KIND_RUN_START, ev.run_start_payload(
                    graph=str(kwargs.get("name") or "")))
            return
        self._mark(run_id)
        self._node_open[str(run_id)] = node
        self._emit(ev.KIND_NODE_START, ev.node_boundary_payload(node=node, run_id=str(run_id)))

    def _close_run(self, run_id: UUID, *, status: str, error: str = "") -> None:
        opened = self._run_open.pop(str(run_id), None)
        if opened is None:
            return
        ms = self._ms_since(run_id)
        self._emit(ev.KIND_RUN_END, ev.run_end_payload(
            status=status, error=error, steps=opened["steps"], ms=ms))

    def _close_node(self, run_id: UUID, *, ok: bool, error: str = "") -> None:
        node = self._node_open.pop(str(run_id), None)
        if node is None:
            return
        ms = self._ms_since(run_id)
        payload = {**ev.node_boundary_payload(node=node, run_id=str(run_id)),
                   "ms": ms, "ok": ok}
        if error:
            payload["error"] = error
        self._emit(ev.KIND_NODE_END, payload)
        for opened in self._run_open.values():
            opened["steps"] += 1

    def on_chain_end(self, outputs: Any, *, run_id: UUID, **kwargs: Any) -> None:
        if str(run_id) in self._run_open:
            self._close_run(run_id, status="completed")
            return
        self._close_node(run_id, ok=True)

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        if str(run_id) in self._run_open:
            self._close_run(run_id, status="error",
                            error=f"{type(error).__name__}: {error}")
            return
        self._close_node(run_id, ok=False, error=f"{type(error).__name__}: {error}")

    # -- LLM calls ----------------------------------------------------------------

    def on_llm_start(self, serialized: dict | None, prompts: list[str], *,
                     run_id: UUID, metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        self._llm_open[str(run_id)] = {
            "text": "\n".join(prompts),
            "model": self._model_of(serialized, metadata),
            "node": self._node(metadata),
        }

    def on_chat_model_start(self, serialized: dict | None, messages: Any, *,
                            run_id: UUID, metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        flat = "\n".join(f"{getattr(m, 'type', '?')}: {getattr(m, 'content', m)}"
                         for batch in (messages or []) for m in (batch or []))
        self._llm_open[str(run_id)] = {
            "text": flat,
            "model": self._model_of(serialized, metadata),
            "node": self._node(metadata),
        }

    def on_llm_end(self, response: Any, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        info = self._llm_open.pop(str(run_id), {})
        text = ""
        usage = None
        try:
            text = "\n".join(getattr(g, "text", "") or str(getattr(g, "message", ""))
                             for gen in (response.generations or [])
                             for g in (gen or []))
            usage = (response.llm_output or {}).get("token_usage")
            if not info.get("model"):
                info["model"] = self._model_of(None, None, response.llm_output)
        except AttributeError:
            pass
        self._emit(ev.KIND_LLM_CALL, ev.llm_call_payload(
            node=info.get("node", ""), model=info.get("model", ""),
            prompt=info.get("text", ""), response=text, ms=ms, usage=usage,
            record_full=self.record_full, head_chars=self.head_chars))

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        info = self._llm_open.pop(str(run_id), {})
        self._emit(ev.KIND_LLM_CALL, ev.llm_call_payload(
            node=info.get("node", ""), model=info.get("model", ""),
            prompt=info.get("text", ""), response="", ms=ms,
            error=f"{type(error).__name__}: {error}",
            record_full=self.record_full, head_chars=self.head_chars))

    # -- tool calls (hash-labeled; result echoes the call's label) -----------------

    def on_tool_start(self, serialized: dict | None, input_str: str, *,
                      run_id: UUID, parent_run_id: UUID | None = None,
                      metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        name = str((serialized or {}).get("name") or "")
        tool_input = kwargs.get("inputs", input_str)
        canonical_input = tool_input if isinstance(tool_input, (dict, list)) else input_str
        label = tool_call_label(name, canonical_input)
        self._tool_open[str(run_id)] = {"label": label, "name": name,
                                        "node": self._node(metadata)}
        self._emit(ev.KIND_TOOL_CALL, ev.tool_call_payload(
            label=label, node=self._node(metadata), name=name,
            tool_input=canonical_input, run_id=str(run_id),
            parent_run_id=str(parent_run_id) if parent_run_id else "",
            record_full=self.record_full, head_chars=self.head_chars))

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        opened = self._tool_open.pop(str(run_id), {})
        self._emit(ev.KIND_TOOL_RESULT, ev.tool_result_payload(
            label=opened.get("label", ""), name=opened.get("name", ""),
            ok=True, ms=ms, output=output, run_id=str(run_id),
            record_full=self.record_full, head_chars=self.head_chars))

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        opened = self._tool_open.pop(str(run_id), {})
        self._emit(ev.KIND_TOOL_RESULT, ev.tool_result_payload(
            label=opened.get("label", ""), name=opened.get("name", ""),
            ok=False, ms=ms, error=f"{type(error).__name__}: {error}",
            run_id=str(run_id), record_full=self.record_full, head_chars=self.head_chars))
