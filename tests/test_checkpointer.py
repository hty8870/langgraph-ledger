# -*- coding: utf-8 -*-
"""End-to-end: a real LangGraph run through TracingCheckpointSaver.

Covers: snapshot events with parent DAG edges, hash-chain integrity,
checkpoint re-hash verification, label resolution, time travel, fork.
"""
from typing_extensions import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from langgraph_dsh_trace import (DshTraceCallbackHandler, TracingCheckpointSaver,
                                 analyze_log, build_dag,
                                 find_checkpoint_by_label, fork_thread,
                                 read_log, recorder_for, time_travel_config,
                                 verify_log, verify_thread)
from langgraph_dsh_trace import events as ev


class State(TypedDict):
    count: int


def _build(saver):
    def inc(state: State) -> dict:
        return {"count": state["count"] + 1}

    def double(state: State) -> dict:
        return {"count": state["count"] * 2}

    g = StateGraph(State)
    g.add_node("inc", inc)
    g.add_node("double", double)
    g.add_edge(START, "inc")
    g.add_edge("inc", "double")
    g.add_edge("double", END)
    return g.compile(checkpointer=saver)


def _run_graph(tmp_path):
    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=tmp_path)
    graph = _build(saver)
    handler = DshTraceCallbackHandler()
    cfg = {"configurable": {"thread_id": "t-demo"}, "callbacks": [handler]}
    out1 = graph.invoke({"count": 1}, cfg)      # (1+1)*2 = 4
    out2 = graph.invoke({"count": out1["count"]}, cfg)  # (4+1)*2 = 10
    return saver, graph, cfg, out2


def _snapshots(log_path):
    return [e for e in read_log(log_path) if e["kind"] == ev.KIND_STATE_SNAPSHOT]


def test_run_records_snapshot_chain_and_verifies(tmp_path):
    saver, _graph, _cfg, out = _run_graph(tmp_path)
    assert out["count"] == 10

    log = tmp_path / "t-demo.jsonl"
    assert log.exists()
    assert verify_log(log)
    assert verify_thread(saver, log)

    snaps = _snapshots(log)
    assert len(snaps) >= 2
    # parent edges form a chain: later snapshots point at earlier checkpoint ids
    ids = [s["payload"]["checkpoint_id"] for s in snaps]
    parents = [s["payload"]["parent_checkpoint_id"] for s in snaps]
    later_with_parent = [p for p in parents[1:] if p]
    assert later_with_parent and all(p in ids for p in later_with_parent)
    # every snapshot is content-addressed
    assert all(s["payload"]["label"].startswith("cp_") for s in snaps)
    assert all(len(s["payload"]["checkpoint_sha256"]) == 64 for s in snaps)


def test_dag_has_parent_and_chain_edges(tmp_path):
    _saver, _g, _cfg, _out = _run_graph(tmp_path)
    dag = build_dag(list(read_log(tmp_path / "t-demo.jsonl")))
    kinds = {e["kind"] for e in dag.edges}
    assert "chain" in kinds
    assert "parent" in kinds
    assert dag.nodes  # non-empty


def test_time_travel_forks_from_past_checkpoint(tmp_path):
    saver, graph, cfg, _out = _run_graph(tmp_path)
    snaps = _snapshots(tmp_path / "t-demo.jsonl")
    first_cp = snaps[0]["payload"]["checkpoint_id"]

    # rewind to the first checkpoint and update state there → a fork
    tt = time_travel_config("t-demo", first_cp)
    new_cfg = graph.update_state(tt, {"count": 100})
    new_cp = new_cfg["configurable"]["checkpoint_id"]
    assert new_cp != first_cp

    # the forked checkpoint's parent is the old one; trace log records it
    tup = saver.get_tuple(new_cfg)
    assert tup is not None
    parent_of_new = (tup.parent_config or {}).get("configurable", {}).get("checkpoint_id")
    assert parent_of_new == first_cp


def test_fork_thread_copies_ancestry_with_lineage_event(tmp_path):
    saver, _graph, _cfg, _out = _run_graph(tmp_path)
    log = tmp_path / "t-demo.jsonl"
    snaps = _snapshots(log)
    target = snaps[-1]["payload"]["checkpoint_id"]

    fork_recorder = recorder_for(tmp_path, "t-demo-forked")
    new_tid = fork_thread(saver, "t-demo", at_checkpoint_id=target,
                          new_thread_id="t-demo-forked", recorder=fork_recorder)
    assert new_tid == "t-demo-forked"

    tup = saver.get_tuple({"configurable": {"thread_id": new_tid,
                                            "checkpoint_ns": "",
                                            "checkpoint_id": target}})
    assert tup is not None and tup.checkpoint["id"] == target

    forks = [e for e in read_log(tmp_path / "t-demo-forked.jsonl")
             if e["kind"] == ev.KIND_FORK]
    assert forks and forks[0]["payload"]["parent_thread_id"] == "t-demo"
    assert forks[0]["payload"]["seed_upto_checkpoint_id"] == target
    assert verify_log(tmp_path / "t-demo-forked.jsonl")


def test_find_checkpoint_by_label_roundtrip(tmp_path):
    _saver, _graph, _cfg, _out = _run_graph(tmp_path)
    rec = recorder_for(tmp_path, "t-demo")
    snaps = _snapshots(rec.path)
    label = snaps[0]["payload"]["label"]
    assert find_checkpoint_by_label(rec, label) == snaps[0]["payload"]["checkpoint_id"]
    assert find_checkpoint_by_label(rec, "cp_nonexistent") is None


def test_analysis_counts_and_structure(tmp_path):
    _saver, _graph, _cfg, _out = _run_graph(tmp_path)
    report = analyze_log(tmp_path / "t-demo.jsonl")
    assert report["checkpoints"] >= 2
    assert report["by_kind"].get(ev.KIND_STATE_SNAPSHOT, 0) >= 2
    assert report["error_count"] == 0
