# -*- coding: utf-8 -*-
"""v0.2: strict mode, crash recovery (repair), replay, concurrency, async."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from typing_extensions import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_dsh_trace import (DshTraceCallbackHandler, TracePayloadError,
                                 TraceRecorder, TracingCheckpointSaver,
                                 analyze_log, close_orphaned_run,
                                 find_orphaned_runs, read_log, recorder_for,
                                 repair_all, replay_messages, verify_log)
from langgraph_dsh_trace import events as ev


# -- strict mode ---------------------------------------------------------------

def test_strict_emit_raises_when_disabled(tmp_path):
    rec = TraceRecorder(tmp_path, "t-strict", enabled=False, strict=True)
    with pytest.raises(TracePayloadError):
        rec.emit("run/start", {})
    # non-strict stays fail-soft
    soft = TraceRecorder(tmp_path, "t-soft", enabled=False)
    assert soft.emit("run/start", {}) is False


def test_strict_emit_raises_on_bad_payload(tmp_path):
    rec = TraceRecorder(tmp_path, "t-strict2", strict=True)
    with pytest.raises(TracePayloadError):
        rec.emit("run/start", {"bad": float("nan")})
    # fail-soft drops it instead
    soft = TraceRecorder(tmp_path, "t-soft2")
    assert soft.emit("run/start", {"bad": float("nan")}) is False
    assert soft.dropped == 1


# -- crash recovery -------------------------------------------------------------

def test_orphaned_run_is_flagged_then_repaired(tmp_path):
    rec = TraceRecorder(tmp_path, "t-crash")
    rec.append("run/start", {"graph": "g"})
    rec.append("node/start", {"node": "n1", "run_id": "r"})

    # an unclosed run is not "ok" — it is honestly incomplete
    report = verify_log(rec.path)
    assert not report
    assert report["open_run"] is True

    assert find_orphaned_runs(tmp_path) == [rec.path]
    outcome = close_orphaned_run(rec.path)
    assert outcome["closed"] is True

    # repaired: bracket closed with an honest interrupted marker, chain intact
    assert verify_log(rec.path)
    last = list(read_log(rec.path))[-1]
    assert last["kind"] == ev.KIND_RUN_END
    assert last["payload"]["status"] == "interrupted"
    assert analyze_log(rec.path)["interrupted_runs"] == 1

    # idempotent
    assert find_orphaned_runs(tmp_path) == []
    assert close_orphaned_run(rec.path)["closed"] is False


def test_repair_all_sweeps_a_trace_root(tmp_path):
    for name in ("a", "b"):
        r = TraceRecorder(tmp_path, name)
        r.append("run/start", {})
    ok = TraceRecorder(tmp_path, "c")
    ok.append("run/start", {})
    ok.append("run/end", {"status": "completed"})

    summary = repair_all(tmp_path)
    assert summary["count"] == 2
    assert all(verify_log(tmp_path / f"{n}.jsonl") for n in ("a", "b", "c"))


def test_verify_rejects_unknown_format_version(tmp_path):
    rec = TraceRecorder(tmp_path, "t-ver", version=99)
    rec.append("run/start", {})
    rec.append("run/end", {"status": "completed"})
    report = verify_log(rec.path)
    assert not report
    assert any("format version" in e["error"] for e in report["errors"])


# -- replay ----------------------------------------------------------------------

def test_replay_roundtrip_digest_and_full(tmp_path):
    for record_full, expect_full in ((False, False), (True, True)):
        tid = f"t-replay-{record_full}"
        rec = TraceRecorder(tmp_path, tid)
        rec.append("run/start", {"graph": "g"})
        rec.append(ev.KIND_LLM_CALL, ev.llm_call_payload(
            node="agent", model="m", prompt="hello", response="world",
            ms=5, record_full=record_full))
        rec.append("run/end", {"status": "completed"})

        msgs = replay_messages(rec.path)
        assistant = [m for m in msgs if m["role"] == "assistant"]
        assert assistant and assistant[0]["content_full"] is expect_full
        if expect_full:
            assert assistant[0]["content"] == "world"
        else:
            assert isinstance(assistant[0]["content"], dict)  # digest, honestly marked


def test_replay_surfaces_unfinished_tool_call(tmp_path):
    rec = TraceRecorder(tmp_path, "t-unfin")
    rec.append("run/start", {})
    rec.append(ev.KIND_TOOL_CALL, ev.tool_call_payload(
        label="L1", node="n", name="search", tool_input={"q": "x"}))
    msgs = replay_messages(rec.path)
    tails = [m for m in msgs if m["role"] == "tool" and m["label"] == "L1"]
    assert tails and tails[0]["ok"] is None
    assert "interrupted" in tails[0]["error"]


# -- concurrency ------------------------------------------------------------------

def test_concurrent_appends_keep_chain_intact(tmp_path):
    rec = TraceRecorder(tmp_path, "t-conc")

    def work(i: int) -> None:
        for j in range(25):
            rec.append("checkpoint/writes",
                       {"checkpoint_id": None, "task_id": f"w{i}-{j}", "writes": []})

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(8)))

    events = list(read_log(rec.path))
    assert len(events) == 200
    assert verify_log(rec.path)


# -- async checkpointer path -------------------------------------------------------

class _State(TypedDict):
    count: int


def _graph(saver):
    def inc(state: _State) -> dict:
        return {"count": state["count"] + 1}

    g = StateGraph(_State)
    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_edge("inc", END)
    return g.compile(checkpointer=saver)


def test_async_run_records_and_verifies(tmp_path):
    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=tmp_path)
    graph = _graph(saver)
    handler = DshTraceCallbackHandler()
    cfg = {"configurable": {"thread_id": "t-async"}, "callbacks": [handler]}

    out = asyncio.run(graph.ainvoke({"count": 1}, cfg))
    assert out["count"] == 2

    log = tmp_path / "t-async.jsonl"
    assert verify_log(log)
    snaps = [e for e in read_log(log) if e["kind"] == ev.KIND_STATE_SNAPSHOT]
    assert snaps


# -- soak: multi-round run with a failing tool --------------------------------------

def test_soak_multi_round_with_retries(tmp_path):
    from langchain_core.tools import tool

    calls = {"n": 0}

    @tool
    def flaky(query: str) -> str:
        """Fails on the first attempt, succeeds after."""
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return f"ok {query}"

    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=tmp_path)
    graph = _graph(saver)
    rec = recorder_for(tmp_path, "t-soak")
    handler = DshTraceCallbackHandler(recorder=rec)
    cfg = {"configurable": {"thread_id": "t-soak"}, "callbacks": [handler]}

    for i in range(5):
        out = graph.invoke({"count": i}, cfg)
        assert out["count"] == i + 1
        for attempt in range(3):
            try:
                flaky.invoke({"query": f"q{i}"}, config={"callbacks": [handler]})
                break
            except RuntimeError:
                continue

    log = tmp_path / "t-soak.jsonl"
    assert verify_log(log)
    report = analyze_log(log)
    assert report["checkpoints"] >= 5
    assert report["interrupted_runs"] == 0
    # repeated identical calls across rounds are detected via labels
    assert report["by_kind"].get(ev.KIND_TOOL_CALL, 0) >= 5
