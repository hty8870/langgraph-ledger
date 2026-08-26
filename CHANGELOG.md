# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/).

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
