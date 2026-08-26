# -*- coding: utf-8 -*-
"""Hashing primitives: canonicalization, labels, hash chain."""
import pytest

from langgraph_ledger.hashing import (GENESIS_PREV, canonical_json,
                                         checkpoint_label, event_id,
                                         sha256_hex, tool_call_label)


def test_canonical_json_is_order_independent():
    a = canonical_json({"b": 1, "a": {"y": 2, "x": 3}})
    b = canonical_json({"a": {"x": 3, "y": 2}, "b": 1})
    assert a == b


def test_canonical_json_rejects_nan():
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_tool_call_label_is_content_addressed():
    l1 = tool_call_label("search", {"q": "lung"})
    l2 = tool_call_label("search", {"q": "lung"})
    l3 = tool_call_label("search", {"q": "brain"})
    l4 = tool_call_label("fetch", {"q": "lung"})
    assert l1 == l2 and l1.startswith("tl_")
    assert l1 != l3 and l1 != l4


def test_event_id_binds_position_and_history():
    kw = dict(version=0, seq=0, ts="t", kind="k", payload={"a": 1}, prev=GENESIS_PREV)
    base = event_id(**kw)
    assert base != event_id(**{**kw, "seq": 1})        # position matters
    assert base != event_id(**{**kw, "payload": {"a": 2}})  # content matters
    assert base != event_id(**{**kw, "prev": "x"})     # history matters


def test_checkpoint_label():
    assert checkpoint_label(b"abc") == checkpoint_label(b"abc")
    assert checkpoint_label(b"abc") != checkpoint_label(b"abd")
    assert checkpoint_label(b"abc").startswith("cp_")
