# -*- coding: utf-8 -*-
"""Canonicalization and content hashing — the labeling primitives.

Every event and every tool call gets a content-addressed label derived from a
*canonical* JSON rendering (sorted keys, tight separators, no NaN/Infinity).
The same logical content always yields the same label, which is what makes
loop detection, dedup and tamper evidence possible.

Label formats:
- Event id (hash chain): ``sha256`` over the full canonical envelope including
  the previous event id — tamper-evident ordering (a merkle chain). With an
  ``hmac_key`` the chain becomes *keyed* (HMAC-SHA256): an attacker who can
  rewrite the whole file can no longer re-chain it without the key.
- Tool-call label: ``tl_<hex16>`` over ``{name, input}`` only — identical
  calls share a label regardless of when they happen (loop/dedup signal).
- Checkpoint label: ``cp_<hex16>`` over the serialized checkpoint bytes.

Only the *chain id* accepts a key. Content labels stay keyless on purpose:
they power cross-log dedup and loop detection, which must work without any
secret (and carry no integrity claim — the chain is the integrity layer).
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

__all__ = [
    "canonical_json",
    "sha256_hex",
    "event_id",
    "tool_call_label",
    "checkpoint_label",
    "GENESIS_PREV",
]

#: The `prev` value of the first event in a log (64 zeros — unmistakably genesis).
GENESIS_PREV = "0" * 64


def canonical_json(value: Any) -> str:
    """Canonical JSON: sorted keys, tight separators, UTF-8, NaN/Infinity rejected.

    NaN/Infinity are pseudo-legal in JSON; accepting them would make hashes
    depend on parser quirks, so they fail here at the *entry* of the pipeline.
    """
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False, default=_fallback,
    )


def _fallback(value: Any) -> Any:
    """Last-resort rendering for non-JSON-native objects (repr, marked).

    Note: ``repr`` may embed memory addresses, so labels over non-serializable
    inputs are not guaranteed stable across processes — the callback handler
    mitigates this by falling back to the plain input string for such calls.
    """
    return {"__repr__": repr(value)}


def _key_bytes(key: bytes | str | None) -> bytes | None:
    """Normalize an HMAC key: str is UTF-8 encoded; None stays None."""
    if key is None:
        return None
    if isinstance(key, str):
        key = key.encode("utf-8")
    if not key:
        raise ValueError("hmac_key must be non-empty")
    return key


def sha256_hex(data: str | bytes, *, key: bytes | str | None = None) -> str:
    """Content hash. With ``key``, HMAC-SHA256 instead of plain SHA-256."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    k = _key_bytes(key)
    if k is not None:
        return hmac.new(k, data, hashlib.sha256).hexdigest()
    return hashlib.sha256(data).hexdigest()


def event_id(*, version: int, seq: int, ts: str, kind: str,
             payload: dict, prev: str, key: bytes | str | None = None) -> str:
    """Hash-chain identity of one event: binds content AND position AND history.

    ``key`` switches the chain from keyless (detect edits given a trusted
    head) to keyed (an attacker cannot re-chain even a full rewrite without
    the key). Both directions of a mismatch fail closed: a keyed log verified
    without the key fails, and vice versa.
    """
    return sha256_hex(canonical_json({
        "v": int(version), "seq": int(seq), "ts": str(ts),
        "kind": str(kind), "payload": payload, "prev": str(prev),
    }), key=key)


def tool_call_label(name: Any, tool_input: Any) -> str:
    """Content-addressed label of a tool call: same (name, input) → same label."""
    return "tl_" + sha256_hex(canonical_json({
        "name": str(name or ""), "input": tool_input,
    }))[:16]


def checkpoint_label(blob: bytes) -> str:
    """Content-addressed label of a serialized checkpoint."""
    return "cp_" + sha256_hex(blob)[:16]
