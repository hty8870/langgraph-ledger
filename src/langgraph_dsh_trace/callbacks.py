# -*- coding: utf-8 -*-
"""LangChain/LangGraph callback handler that feeds the trace recorder.

Attach to any LangGraph run:

    graph.invoke(input, config={**config, "callbacks": [DshTraceCallbackHandler()]})

LangChain propagates callbacks into every node, LLM call and tool call, so a
single handler sees the whole run. Node boundaries are recognized through
LangGraph's ``langgraph_node`` metadata; LLM/tool timing is paired by run id.

All emit calls are fail-soft: tracing never breaks the run.
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

    def __init__(self, recorder: Any = None) -> None:
        super().__init__()
        #: Optional explicit recorder; falls back to the context-bound one.
        self.recorder = recorder
        self._starts: dict[str, float] = {}
        self._llm_prompts: dict[str, Any] = {}
        self._node_runs: set[str] = set()

    # -- plumbing --------------------------------------------------------------

    def _rec(self) -> Any:
        return self.recorder if self.recorder is not None else current_recorder()

    def _emit(self, kind: str, payload: Any) -> bool:
        rec = self._rec()
        return rec.emit(kind, payload) if rec is not None else False

    @staticmethod
    def _node(metadata: dict | None) -> str:
        return str((metadata or {}).get("langgraph_node") or "")

    def _mark(self, run_id: UUID) -> float:
        start = time.monotonic()
        self._starts[str(run_id)] = start
        return start

    def _ms_since(self, run_id: UUID) -> int:
        start = self._starts.pop(str(run_id), None)
        return int((time.monotonic() - start) * 1000) if start is not None else 0

    # -- node boundaries (LangGraph chain runs carrying langgraph_node) ---------

    def on_chain_start(self, serialized: dict | None, inputs: Any, *,
                       run_id: UUID, parent_run_id: UUID | None = None,
                       metadata: dict | None = None, **kwargs: Any) -> None:
        node = self._node(metadata)
        if not node:
            return  # graph-level and internal chains stay silent
        self._mark(run_id)
        self._node_runs.add(str(run_id))
        self._emit(ev.KIND_NODE_START, ev.node_boundary_payload(node=node, run_id=str(run_id)))

    def on_chain_end(self, outputs: Any, *, run_id: UUID, **kwargs: Any) -> None:
        if str(run_id) not in self._node_runs:
            return
        self._node_runs.discard(str(run_id))
        ms = self._ms_since(run_id)
        self._emit(ev.KIND_NODE_END, {**ev.node_boundary_payload(node="", run_id=str(run_id)),
                                      "ms": ms, "ok": True})

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        if str(run_id) not in self._node_runs:
            return
        self._node_runs.discard(str(run_id))
        ms = self._ms_since(run_id)
        self._emit(ev.KIND_NODE_END, {**ev.node_boundary_payload(node="", run_id=str(run_id)),
                                      "ms": ms, "ok": False,
                                      "error": f"{type(error).__name__}: {error}"})

    # -- LLM calls ----------------------------------------------------------------

    def on_llm_start(self, serialized: dict | None, prompts: list[str], *,
                     run_id: UUID, metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        self._llm_prompts[str(run_id)] = {"text": "\n".join(prompts),
                                          "model": str((serialized or {}).get("name") or ""),
                                          "node": self._node(metadata)}

    def on_chat_model_start(self, serialized: dict | None, messages: Any, *,
                            run_id: UUID, metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        flat = "\n".join(f"{getattr(m, 'type', '?')}: {getattr(m, 'content', m)}"
                         for batch in (messages or []) for m in (batch or []))
        self._llm_prompts[str(run_id)] = {"text": flat,
                                          "model": str((serialized or {}).get("name") or ""),
                                          "node": self._node(metadata)}

    def on_llm_end(self, response: Any, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        info = self._llm_prompts.pop(str(run_id), {})
        text = ""
        usage = None
        try:
            text = "\n".join(getattr(g, "text", "") for gen in (response.generations or [])
                             for g in (gen or []))
            usage = (response.llm_output or {}).get("token_usage")
        except AttributeError:
            pass
        self._emit(ev.KIND_LLM_CALL, ev.llm_call_payload(
            node=info.get("node", ""), model=info.get("model", ""),
            prompt=info.get("text", ""), response=text, ms=ms, usage=usage))

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        info = self._llm_prompts.pop(str(run_id), {})
        self._emit(ev.KIND_LLM_CALL, ev.llm_call_payload(
            node=info.get("node", ""), model=info.get("model", ""),
            prompt=info.get("text", ""), response="", ms=ms,
            error=f"{type(error).__name__}: {error}"))

    # -- tool calls (hash-labeled) -------------------------------------------------

    def on_tool_start(self, serialized: dict | None, input_str: str, *,
                      run_id: UUID, parent_run_id: UUID | None = None,
                      metadata: dict | None = None, **kwargs: Any) -> None:
        self._mark(run_id)
        name = str((serialized or {}).get("name") or "")
        tool_input = kwargs.get("inputs", input_str)
        label = tool_call_label(name, tool_input if isinstance(tool_input, (dict, list)) else input_str)
        self._emit(ev.KIND_TOOL_CALL, ev.tool_call_payload(
            label=label, node=self._node(metadata), name=name,
            tool_input=tool_input if isinstance(tool_input, (dict, list)) else input_str,
            run_id=str(run_id),
            parent_run_id=str(parent_run_id) if parent_run_id else ""))

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        self._emit(ev.KIND_TOOL_RESULT, ev.tool_result_payload(
            label="", name="", ok=True, ms=ms, output=output, run_id=str(run_id)))

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        ms = self._ms_since(run_id)
        self._emit(ev.KIND_TOOL_RESULT, ev.tool_result_payload(
            label="", name="", ok=False, ms=ms,
            error=f"{type(error).__name__}: {error}", run_id=str(run_id)))
