# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.3.0] - 2026-09-01

Keyed chains: the full-rewrite attack from the threat model now has a
cryptographic countermeasure.

### Added
- **Optional HMAC-SHA256 keyed hash chain** — `TraceRecorder` / `recorder_for`
  accept `hmac_key=`, `verify_log(path, hmac_key=...)` verifies keyed logs, and
  the CLI reads the key from an env var (`LANGGRAPH_LEDGER_HMAC_KEY` by default;
  `--hmac-key-env NAME` to choose another). Without the key, an attacker can no
  longer rewrite the whole file and re-chain it — the attack the keyless threat
  model had to delegate to external anchoring.
- Key-mismatch hint in verify reports: when every id mismatches from genesis,
  the report says "wrong or missing HMAC key?" instead of looking like wholesale
  tampering.

### Notes
- Fully backward compatible: keyless logs verify exactly as before; content
  labels (`tl_*`, `cp_*`, payload digests) stay keyless so dedup and loop
  detection keep working without any secret.
- New adversarial test (`test_full_rechain_attack_defeats_keyless_but_not_keyed`)
  demonstrates the attack succeeding against a keyless chain and failing
  against a keyed one.

## [0.2.1] - 2026-08-26

Hardening release driven by an adversarial self-review.

### Fixed
- **Crash resume no longer glues a new chain onto a corrupt log.** A torn tail
  (half-written last line after a crash) is truncated to the longest valid
  prefix and the chain resumes; a log with zero valid events is quarantined to
  `*.corrupt-<ts>.jsonl` instead of receiving a second genesis. Mid-file
  corruption still fails loudly — tolerance is tail-only.
- **`strict=True` now actually fails closed through the checkpointer** —
  tracing exceptions are re-raised instead of swallowed when the recorder is
  strict. (Callback path caveat documented: LangChain's callback manager may
  swallow handler exceptions.)
- dag module docstring promised `fork` edges that were never emitted; corrected
  (forks render as nodes, the cross-log edge is documented as out of scope).

### Added
- **Threat model section** in both READMEs: what a keyless hash chain proves
  (any edit detectable *given a trusted head*) and what it cannot prove
  (whole-file rewrite) — plus `chain_head()` and `langgraph-ledger head` CLI
  to export the head for external anchoring.
- **`head_chars` knob** (handler + payload builders): the digest's plaintext
  head preview is now configurable; `head_chars=0` gives a pure digest.
- CI now also tests on Python 3.10 (the declared minimum).

## [0.2.0] - 2026-08-26

> **Renamed**: the project was renamed from `langgraph-dsh-trace` to
> **`langgraph-ledger`** before its first PyPI publication. The Python package
> is now `langgraph_ledger`. The dsh lineage lives in the README design-mapping
> table and keywords, not in the name.

### Added
- **Run brackets**: the callback handler now opens `run/start` on the top-level
  graph invocation and closes it with `run/end` (status, step count, wall
  time) — the anchor for crash detection.
- `python -m langgraph_ledger` entry point and a `langgraph-ledger` console
  script (the documented CLI invocation previously lacked `__main__.py`).
- **Crash recovery** (`repair.py`): `find_orphaned_runs` /
  `close_orphaned_run` / `repair_all` close crash-orphaned runs by appending an
  honest `run/end {status: "interrupted"}` through the hash chain. Idempotent;
  history is never rewritten. CLI: `python -m langgraph_ledger repair`.
- **Replay** (`replay.py`): `replay_messages` rebuilds the message timeline
  from a log (dsh `deriveMessages` analog) — digests by default, full text
  with `record_full=True`; unfinished tool calls surface as interrupted, never
  silently dropped. CLI: `python -m langgraph_ledger replay`.
- **Strict mode**: `TraceRecorder(..., strict=True)` flips tracing from
  fail-soft to fail-closed — a run that cannot record must not proceed
  (dsh's Model-Visible ⟺ Logged invariant, enforced).
- **`record_full=True`** on `DshTraceCallbackHandler` persists full
  prompt/response/input/output text alongside digests.
- **Verify**: unknown format versions are rejected; unclosed run brackets are
  reported as `open_run` with a pointer to `repair`.
- **Analysis**: interrupted-run count, per-model call counts, per-node time.

### Fixed
- Tool *results* now echo the content label of their call (paired by run id),
  so call↔result edges and loop detection no longer rely on position.
- `node/end` events keep their node name; LLM events resolve model identity
  from langsmith metadata → serialized name → `llm_output`.
- DAG: redundant parallel edges between the same node pair are deduped
  (priority parent > call > writes > fork > chain); error nodes are
  highlighted in Mermaid output; fork events appear in summaries.

## [0.1.0] - 2026-08-25

Initial release: hash-chained append-only JSONL trace log, content-addressed
tool-call and checkpoint labels, `TracingCheckpointSaver` wrapper, callback
handler, DAG builder with Mermaid export, time travel & dsh-style fork,
log/thread verification, failure analysis, CLI.
