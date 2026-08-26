# -*- coding: utf-8 -*-
"""Minimal end-to-end demo: traced run → verify → DAG → time travel → fork.

    python examples/basic_usage.py
"""
import tempfile
from typing_extensions import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_ledger import (DshTraceCallbackHandler, TraceRecorder,
                                 TracingCheckpointSaver, analyze_log,
                                 build_dag, fork_thread, read_log,
                                 time_travel_config, verify_log, verify_thread)


class State(TypedDict):
    count: int


def inc(state: State) -> dict:
    return {"count": state["count"] + 1}


def main() -> None:
    trace_dir = tempfile.mkdtemp(prefix="dsh-trace-demo-")
    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=trace_dir)

    graph = StateGraph(State)
    graph.add_node("inc", inc)
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    app = graph.compile(checkpointer=saver)

    handler = DshTraceCallbackHandler()
    cfg = {"configurable": {"thread_id": "demo"}, "callbacks": [handler]}
    app.invoke({"count": 1}, cfg)
    app.invoke({"count": 10}, cfg)

    log = TraceRecorder(trace_dir, "demo").path
    print("log:", log)
    print("verify_log   :", verify_log(log)["ok"])
    print("verify_thread:", verify_thread(saver, log)["ok"])

    report = analyze_log(log)
    print("checkpoints  :", report["checkpoints"], "| errors:", report["error_count"])

    dag = build_dag(list(read_log(log)))
    print("\n--- DAG (mermaid) ---\n" + dag.to_mermaid())

    # time travel: fork the thread's future from the first checkpoint
    first_cp = next(e["payload"]["checkpoint_id"]
                    for e in read_log(log)
                    if e["kind"] == "state/snapshot")
    tt = time_travel_config("demo", first_cp)
    new_cfg = app.update_state(tt, {"count": 999})
    print("\ntime travel  : forked at", new_cfg["configurable"]["checkpoint_id"])

    # dsh-style fork: a brand-new thread seeded with the ancestry chain
    new_tid = fork_thread(saver, "demo", at_checkpoint_id=first_cp)
    print("fork         : new thread", new_tid,
          "| fork event logged:", (TraceRecorder(trace_dir, new_tid).path).exists())


if __name__ == "__main__":
    main()
