# -*- coding: utf-8 -*-
"""Recorder: append-only discipline, chain continuity, fail-soft emit."""
import json

import pytest

from langgraph_dsh_trace.recorder import (TracePayloadError, TraceRecorder,
                                          read_log)
from langgraph_dsh_trace.verify import verify_log


def test_append_writes_hash_chained_envelopes(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    e0 = rec.append("run/start", {"graph": "g"})
    e1 = rec.append("node/start", {"node": "n1", "run_id": "r"})
    assert e0["seq"] == 0 and e1["seq"] == 1
    assert e1["prev"] == e0["id"]
    assert verify_log(rec.path)


def test_reopen_resumes_seq_and_chain(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    rec.append("run/start", {})
    again = TraceRecorder(tmp_path, "t1")
    e = again.append("run/end", {"status": "completed"})
    assert e["seq"] == 1
    assert verify_log(rec.path)


def test_append_rejects_non_dict_and_nan(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    with pytest.raises(TracePayloadError):
        rec.append("x", ["not-a-dict"])
    with pytest.raises(TracePayloadError):
        rec.append("x", {"bad": float("inf")})


def test_emit_is_fail_soft(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    assert rec.emit("x", {"bad": object()}) is False
    assert rec.dropped == 1
    assert rec.emit("x", {"good": 1}) is True


def test_disabled_recorder_writes_nothing(tmp_path):
    rec = TraceRecorder(tmp_path, "t1", enabled=False)
    assert rec.emit("x", {"a": 1}) is False
    assert not rec.path.exists()


def test_thread_id_is_path_sanitized(tmp_path):
    rec = TraceRecorder(tmp_path, "../../etc/evil")
    assert rec.path.parent == tmp_path
    assert ".." not in rec.path.name


def test_tamper_edit_is_detected(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    rec.append("run/start", {"graph": "g"})
    rec.append("run/end", {"status": "completed"})
    lines = rec.path.read_text(encoding="utf-8").splitlines()
    middle = json.loads(lines[0])
    middle["payload"]["graph"] = "forged"
    lines[0] = json.dumps(middle, ensure_ascii=False)
    rec.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = verify_log(rec.path)
    assert not report
    assert any("modified" in e["error"] or "broken" in e["error"]
               for e in report["errors"])


def test_tamper_deletion_is_detected(tmp_path):
    rec = TraceRecorder(tmp_path, "t1")
    for i in range(4):
        rec.append("node/start", {"i": i})
    lines = rec.path.read_text(encoding="utf-8").splitlines()
    del lines[2]  # delete one event
    rec.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = verify_log(rec.path)
    assert not report


def test_read_log_skips_blank_lines(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text('{"seq": 0}\n\n{"seq": 1}\n', encoding="utf-8")
    assert len(list(read_log(p))) == 2
