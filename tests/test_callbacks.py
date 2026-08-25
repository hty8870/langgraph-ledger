# -*- coding: utf-8 -*-
"""Callback handler: tool-call hash labels, error capture, loop detection."""
from langchain_core.tools import tool

from langgraph_dsh_trace import (DshTraceCallbackHandler, TraceRecorder,
                                 analyze_log, read_log, verify_log)
from langgraph_dsh_trace import events as ev
from langgraph_dsh_trace.hashing import tool_call_label


@tool
def search_db(query: str) -> str:
    """Search a database for a query string."""
    return f"results for {query}"


@tool
def explode(query: str) -> str:
    """Always fails."""
    raise RuntimeError("boom")


def test_tool_calls_get_content_labels_and_pair_with_results(tmp_path):
    rec = TraceRecorder(tmp_path, "tools")
    handler = DshTraceCallbackHandler(recorder=rec)

    search_db.invoke({"query": "lung"}, config={"callbacks": [handler]})
    search_db.invoke({"query": "lung"}, config={"callbacks": [handler]})
    search_db.invoke({"query": "brain"}, config={"callbacks": [handler]})

    events = list(read_log(rec.path))
    calls = [e for e in events if e["kind"] == ev.KIND_TOOL_CALL]
    results = [e for e in events if e["kind"] == ev.KIND_TOOL_RESULT]
    assert len(calls) == 3 and len(results) == 3
    assert all(r["payload"]["ok"] for r in results)

    labels = [c["payload"]["label"] for c in calls]
    assert labels[0] == labels[1] == tool_call_label("search_db", {"query": "lung"})
    assert labels[2] != labels[0]
    assert verify_log(rec.path)

    report = analyze_log(rec.path)
    loops = {l["label"]: l for l in report["repeated_tool_calls"]}
    assert labels[0] in loops and loops[labels[0]]["count"] == 2
    streaks = report["consecutive_repeat_streaks"]
    assert any(s["label"] == labels[0] and s["length"] == 2 for s in streaks)


def test_tool_error_is_recorded_as_failed_result(tmp_path):
    rec = TraceRecorder(tmp_path, "tools-err")
    handler = DshTraceCallbackHandler(recorder=rec)

    try:
        explode.invoke({"query": "x"}, config={"callbacks": [handler]})
    except RuntimeError:
        pass

    results = [e for e in read_log(rec.path) if e["kind"] == ev.KIND_TOOL_RESULT]
    assert len(results) == 1 and results[0]["payload"]["ok"] is False
    assert "boom" in results[0]["payload"]["error"]
    report = analyze_log(rec.path)
    assert report["error_count"] == 1
