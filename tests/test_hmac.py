# -*- coding: utf-8 -*-
"""Keyed (HMAC-SHA256) hash chain tests — including the full-rewrite attack.

The attack these tests pin down: an attacker with write access to a *keyless*
log can edit any line, then recompute the whole chain from genesis and the
log verifies clean. With a keyed chain the same rewrite fails verification,
because the attacker cannot produce valid HMACs without the key.
"""
from __future__ import annotations

import json

import pytest

from langgraph_ledger import TraceRecorder, verify_log
from langgraph_ledger.hashing import GENESIS_PREV, event_id, sha256_hex

KEY = b"test-secret-key"
WRONG = b"attacker-does-not-have-this"


def _write_keyed(tmp_path, events=3):
    rec = TraceRecorder(tmp_path, "t", hmac_key=KEY)
    for i in range(events):
        rec.append("node/start", {"node": f"n{i}"})
    return rec


def _rechain_keyless(path, tamper=None):
    """Simulate the full-rewrite attack: edit one line, then recompute the
    whole chain with PLAIN sha256 (no key) so the file looks self-consistent."""
    lines = [json.loads(raw) for raw in path.read_text(encoding="utf-8").splitlines() if raw.strip()]
    if tamper:
        tamper(lines)
    prev = GENESIS_PREV
    for i, e in enumerate(lines):
        e["seq"] = i
        e["prev"] = prev
        e["id"] = event_id(version=e["v"], seq=e["seq"], ts=e["ts"],
                           kind=e["kind"], payload=e["payload"], prev=prev)
        prev = e["id"]
    path.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in lines),
                    encoding="utf-8")


# -- happy path ---------------------------------------------------------------

def test_keyed_log_verifies_with_key(tmp_path):
    rec = _write_keyed(tmp_path)
    report = verify_log(rec.path, hmac_key=KEY)
    assert report["ok"], report["errors"]
    assert report["events"] == 3


def test_keyed_chain_differs_from_keyless(tmp_path):
    keyed = TraceRecorder(tmp_path, "keyed", hmac_key=KEY)
    plain = TraceRecorder(tmp_path, "plain")
    e_keyed = keyed.append("node/start", {"node": "n"})
    e_plain = plain.append("node/start", {"node": "n"})
    assert e_keyed["id"] != e_plain["id"]  # same content, different chain id


def test_hmac_key_str_is_utf8_encoded(tmp_path):
    rec = TraceRecorder(tmp_path, "t", hmac_key="秘密")  # str accepted
    rec.append("node/start", {"node": "n"})
    assert verify_log(rec.path, hmac_key="秘密")["ok"]
    assert verify_log(rec.path, hmac_key="秘密".encode())["ok"]


def test_empty_key_rejected(tmp_path):
    with pytest.raises(ValueError, match="non-empty"):
        TraceRecorder(tmp_path, "t", hmac_key=b"").append("node/start", {"node": "n"})


# -- fail closed --------------------------------------------------------------

def test_keyed_log_fails_without_key(tmp_path):
    rec = _write_keyed(tmp_path)
    report = verify_log(rec.path)  # no key
    assert not report["ok"]
    assert any("HMAC key" in e["error"] for e in report["errors"])  # helpful hint


def test_keyed_log_fails_with_wrong_key(tmp_path):
    rec = _write_keyed(tmp_path)
    assert not verify_log(rec.path, hmac_key=WRONG)["ok"]


def test_keyless_log_fails_when_key_supplied(tmp_path):
    rec = TraceRecorder(tmp_path, "t")  # keyless log
    rec.append("node/start", {"node": "n"})
    assert not verify_log(rec.path, hmac_key=KEY)["ok"]


# -- the attack ----------------------------------------------------------------

def test_surgical_tamper_detected_with_key(tmp_path):
    rec = _write_keyed(tmp_path)
    lines = rec.path.read_text(encoding="utf-8").splitlines()
    e = json.loads(lines[1])
    e["payload"]["node"] = "EVIL"
    lines[1] = json.dumps(e, ensure_ascii=False)
    rec.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert not verify_log(rec.path, hmac_key=KEY)["ok"]


def test_full_rechain_attack_defeats_keyless_but_not_keyed(tmp_path):
    """The adversarial core: attacker rewrites the file and re-chains it with
    plain sha256. A keyless verify accepts the forgery; the keyed verify
    rejects it — the attacker cannot forge HMACs without the key."""
    rec = _write_keyed(tmp_path)
    _rechain_keyless(rec.path,
                     tamper=lambda ls: ls[1]["payload"].update(node="EVIL"))
    # The forged file is a self-consistent KEYLESS chain — keyless verify passes:
    assert verify_log(rec.path)["ok"]
    # ...but it is not the chain the key holder wrote — keyed verify catches it:
    assert not verify_log(rec.path, hmac_key=KEY)["ok"]


# -- resume & compatibility -----------------------------------------------------

def test_keyed_resume_continues_the_same_chain(tmp_path):
    rec = TraceRecorder(tmp_path, "t", hmac_key=KEY)
    first = rec.append("node/start", {"node": "n0"})
    rec2 = TraceRecorder(tmp_path, "t", hmac_key=KEY)  # reopen
    second = rec2.append("node/start", {"node": "n1"})
    assert second["seq"] == 1
    assert second["prev"] == first["id"]
    assert verify_log(rec.path, hmac_key=KEY)["ok"]


def test_hmac_does_not_touch_content_labels():
    """Content digests (dedup/loop detection) stay keyless by design; only the
    chain id accepts a key."""
    assert sha256_hex("hello") == sha256_hex("hello", key=None)
    assert sha256_hex("hello", key=KEY) != sha256_hex("hello")
