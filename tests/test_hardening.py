# -*- coding: utf-8 -*-
"""v0.2.1 adversarial-review fixes: torn tail, quarantine, strict passthrough,
head_chars privacy knob, chain head anchoring."""
from __future__ import annotations

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from langgraph_ledger import (DshTraceCallbackHandler, TracePayloadError,
                              TraceRecorder, TracingCheckpointSaver,
                              chain_head, read_log, verify_log)
from langgraph_ledger import events as ev
from langgraph_ledger.hashing import GENESIS_PREV


# -- torn tail / quarantine (R2) --------------------------------------------------

def test_torn_tail_is_truncated_and_chain_continues(tmp_path):
    rec = TraceRecorder(tmp_path, "t-torn")
    rec.append("run/start", {})
    rec.append("run/end", {"status": "completed"})
    # simulate a crash mid-write: half a JSON line glued to the tail
    with rec.path.open("a", encoding="utf-8") as fh:
        fh.write('{"v":0,"seq":2,"ts":"zzz","kind":"node/st')

    again = TraceRecorder(tmp_path, "t-torn")  # reopen: torn tail truncated
    e = again.append("run/start", {})
    assert e["seq"] == 2                      # resumes after last VALID event
    again.append("run/end", {"status": "completed"})
    assert verify_log(again.path)
    assert len(list(read_log(again.path))) == 4


def test_unreadable_log_is_quarantined_never_regenesis(tmp_path):
    bad = tmp_path / "t-corrupt.jsonl"
    bad.write_bytes(b"\xff\xfe not json at all \x00\x01")

    rec = TraceRecorder(tmp_path, "t-corrupt")
    e = rec.append("run/start", {})
    assert e["seq"] == 0 and e["prev"] == GENESIS_PREV
    rec.append("run/end", {"status": "completed"})

    # original bytes preserved in a quarantined file, NOT appended to
    quarantined = list(tmp_path.glob("t-corrupt.corrupt-*.jsonl"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"\xff\xfe not json at all \x00\x01"
    assert verify_log(rec.path)  # the new clean chain verifies on its own


def test_midfile_corruption_still_fails_verification(tmp_path):
    """Torn-tail tolerance must not become a hole: a corrupt MIDDLE line is
    tampering, and read_log/verify must still refuse it."""
    rec = TraceRecorder(tmp_path, "t-mid")
    for i in range(3):
        rec.append("node/start", {"node": f"n{i}"})
    lines = rec.path.read_text(encoding="utf-8").splitlines()
    lines[1] = "{corrupted"
    rec.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(ValueError):
        list(read_log(rec.path))


# -- strict passthrough at the checkpointer (R3) ------------------------------------

def _cp(cp_id: str = "c1") -> dict:
    return {"id": cp_id, "v": 1, "ts": "2026-01-01T00:00:00+00:00",
            "channel_values": {}, "channel_versions": {},
            "versions_seen": {}, "pending_sends": []}


def test_strict_recorder_failure_propagates_through_put(tmp_path, monkeypatch):
    # a strict recorder whose write genuinely fails must break the run, even
    # behind the checkpointer's fail-soft wrapper
    rec = TraceRecorder(tmp_path, "t-strict-put", strict=True)
    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=tmp_path,
                                   recorder=rec)
    cfg = {"configurable": {"thread_id": "t-strict-put", "checkpoint_ns": ""}}

    def boom(self, kind, payload):
        raise OSError("disk full")

    monkeypatch.setattr(TraceRecorder, "append", boom)
    with pytest.raises(OSError, match="disk full"):
        saver.put(cfg, _cp(), {}, {})


def test_failsoft_recorder_failure_still_swallowed(tmp_path, monkeypatch):
    rec = TraceRecorder(tmp_path, "t-soft-put")  # strict=False
    saver = TracingCheckpointSaver(InMemorySaver(), trace_root=tmp_path,
                                   recorder=rec)
    cfg = {"configurable": {"thread_id": "t-soft-put", "checkpoint_ns": ""}}

    def boom(self, kind, payload):
        raise OSError("disk full")

    monkeypatch.setattr(TraceRecorder, "append", boom)
    out = saver.put(cfg, _cp(), {}, {})  # must NOT raise
    assert out["configurable"]["checkpoint_id"]
    assert rec.dropped >= 1


# -- head_chars privacy knob (R7) ---------------------------------------------------

def test_head_chars_zero_yields_pure_digest(tmp_path):
    rec = TraceRecorder(tmp_path, "t-heads")
    handler = DshTraceCallbackHandler(recorder=rec, head_chars=0)
    handler._emit(ev.KIND_LLM_CALL, ev.llm_call_payload(
        node="n", model="m", prompt="secret system prompt",
        response="secret answer", ms=1, head_chars=handler.head_chars))
    e = list(read_log(rec.path))[0]
    assert e["payload"]["prompt"]["head"] == ""
    assert e["payload"]["prompt"]["sha256"]
    assert e["payload"]["prompt"]["chars"] == len("secret system prompt")


def test_head_chars_default_keeps_80_char_preview():
    d = ev.digest_text("x" * 200)
    assert len(d["head"]) == 80
    d0 = ev.digest_text("x" * 200, head_chars=0)
    assert d0["head"] == "" and d0["chars"] == 200


# -- chain head anchoring (R1) -------------------------------------------------------

def test_chain_head_matches_last_event(tmp_path):
    rec = TraceRecorder(tmp_path, "t-head")
    assert chain_head(rec.path) == {"seq": -1, "id": GENESIS_PREV} if rec.path.exists() else True
    e0 = rec.append("run/start", {})
    assert chain_head(rec.path) == {"seq": 0, "id": e0["id"]}
    e1 = rec.append("run/end", {"status": "completed"})
    assert chain_head(rec.path) == {"seq": 1, "id": e1["id"]}
