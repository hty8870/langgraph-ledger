# langgraph-ledger

[![PyPI](https://img.shields.io/pypi/v/langgraph-ledger)](https://pypi.org/project/langgraph-ledger/)
[![CI](https://github.com/hty8870/langgraph-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/hty8870/langgraph-ledger/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/langgraph-ledger)](https://pypi.org/project/langgraph-ledger/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Tamper-evident audit ledger for LangGraph agents** — hash-chained (optionally HMAC-keyed) event log, *state-level* checkpoint fingerprinting, replay, DAG lineage, and rollback. Inspired by DeepSeek Harness's append-only session discipline; adds the cryptographic integrity layer dsh itself does not have.

[English](README.md) · [中文](README.zh.md)

```bash
pip install langgraph-ledger
```

## Why

**Your agent's logs are not evidence.** Plain transcripts (a Claude Code / Codex session file, a LangSmith trace) are mutable text: edit a line, delete a line, and nothing downstream can tell. Fine for debugging — worthless in an audit, an incident review, or a dispute.

LangGraph already checkpoints state and can time-travel. What it does *not* give you is an **audit-grade record**: were these events edited after the fact? Which tool call exactly — same name, or same *content*? Does the stored checkpoint still match what the run actually produced? What did the run look like as a graph, and can I fork from any point of it?

This plugin carries the append-only session discipline of [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (dsh) onto LangGraph's checkpointer contract — and adds the integrity layer **neither LangGraph nor dsh has**: cryptographic fingerprinting of the event stream *and* the stored state.

The combination is the point:

| Capability | Mechanism |
|---|---|
| Tool-call identity | content-addressed label `tl_<hash>` — same (name, input) ⇒ same label |
| Tamper evidence | every event hash-chained to its predecessor (merkle-style); optional **HMAC-keyed** chain (0.3.0) so even a whole-file rewrite fails |
| State accountability | `verify_thread` re-hashes every stored checkpoint against its logged claim — *state* drift is caught, not just log edits |
| Execution structure | DAG over chain + call/result + checkpoint-parent edges |
| Rollback | `time_travel_config` (native fork) and `fork_thread` (dsh-style seeded fork with lineage event) |
| Failure analysis | error timeline, exact-repeat loop detection (falls out of the labels) |
| Verification | `verify_log` re-checks the chain; `verify_thread` re-hashes stored checkpoints against logged claims |

**A correctness detail most tools miss:** serializers normalize (msgpack turns `tuple` into `list`), so hashing the in-memory object and re-hashing the reloaded object can disagree on an untouched log — false "tampered" alarms. We hash the **roundtrip-normalized** form (the exact bytes a future verifier can reproduce), so verification compares like with like. This is what makes the fingerprints usable as evidence instead of noise.

## Differentiation (2026-09 refresh)

Honest snapshot of the neighborhood — see [POSITIONING](docs/positioning.md) for details:

| Project | Hash chain | Keyed chain | State fingerprinting | Rollback/fork | Form |
|---|---|---|---|---|---|
| **langgraph-ledger** | ✅ | ✅ HMAC (0.3.0) | ✅ re-hashes stored checkpoints | ✅ | LangGraph-native library |
| LangSmith / Langfuse / AgentOps | ✗ (hosted observability) | ✗ | ✗ | ✗ | SaaS |
| agent-flight-recorder | ✅ | ✅ HMAC | ✗ (tool-call level) | ✗ | Claude Code skill pack |
| Promptise | ✅ | ✅ HMAC | ✗ | ✗ | MCP middleware |
| Probity / Zyvra | ✅ | ✅ signed | ✗ | ✗ | commercial compliance |
| DeepSeek Harness (inspiration) | ✗ (zstd frame checksums) | ✗ | attachments only | fork lineage | TS/Node harness |
| langgraph checkpointers | ✗ | ✗ | ✗ | time-travel only | ✅ |

The 2026 audit-tooling wave validates the need; the open slot this project occupies is narrower and deeper: **state-level fingerprinting + keyed chains + rollback, as one pip-installable LangGraph library** — no SaaS, no sidecar, your logs stay files you own.

## Install

```bash
pip install langgraph-ledger
```

## Quickstart

```python
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_ledger import TracingCheckpointSaver, DshTraceCallbackHandler

saver = TracingCheckpointSaver(InMemorySaver(), trace_root="./traces")
graph = builder.compile(checkpointer=saver)          # your graph, unchanged

graph.invoke(input, config={
    "configurable": {"thread_id": "run-42"},
    "callbacks": [DshTraceCallbackHandler()],         # LLM/tool/node events
})
```

Every run now leaves `./traces/run-42.jsonl` — one hash-chained JSON event per line:

```json
{"v":0,"seq":7,"ts":"…","kind":"tool/call","payload":{"label":"tl_9f2e…","name":"search",…},"id":"…","prev":"…"}
{"v":0,"seq":8,"ts":"…","kind":"state/snapshot","payload":{"checkpoint_id":"…","label":"cp_2c01…","parent_checkpoint_id":"…","checkpoint_sha256":"…"},"id":"…","prev":"…"}
```

### Rollback & fork

```python
from langgraph_ledger import time_travel_config, fork_thread

# resume/fork from any recorded checkpoint (LangGraph-native time travel)
cfg = time_travel_config("run-42", checkpoint_id="<past-id>")
graph.update_state(cfg, {"count": 100})              # forks from that point

# dsh-style: a new thread seeded with the ancestry up to a checkpoint
new_tid = fork_thread(saver, "run-42", at_checkpoint_id="<past-id>")
```

### Audit

```bash
python -m langgraph_ledger verify  traces/run-42.jsonl   # hash chain intact?
python -m langgraph_ledger analyze traces/run-42.jsonl   # errors, loops, timeline
python -m langgraph_ledger dag     traces/run-42.jsonl --mermaid
python -m langgraph_ledger repair  traces/               # close crash-orphaned runs
python -m langgraph_ledger replay  traces/run-42.jsonl   # rebuild the message timeline
```

```python
from langgraph_ledger import verify_thread
report = verify_thread(saver, "traces/run-42.jsonl")  # stored state == logged claim?
assert report["ok"]
```

### Crash recovery & replay

If the process dies mid-run, the log ends with an unclosed `run/start`.
`verify` flags it as an *open run*; `repair` appends an honest
`run/end {status: "interrupted"}` through the hash chain (idempotent, never
rewrites history). `replay_messages()` rebuilds the conversation timeline from
the log — digests by default, full text when the handler was created with
`record_full=True`. For fail-closed operation (a run that cannot record must
not proceed), use `TraceRecorder(..., strict=True)` — enforced at the recorder
and the checkpointer. Caveat: LangChain's callback manager may swallow handler
exceptions, so for hard guarantees prefer the checkpointer path.

## Design mapping from DeepSeek Harness

A design study, not a port of code (dsh is TypeScript/Node; this is Python/LangGraph):

| dsh concept | here |
|---|---|
| `SessionEvent` append-only log, single source of truth | one hash-chained JSONL per thread |
| `SESSION_FORMAT_VERSION` + `ignorable` marker | `v` field; unknown kinds counted, not rejected |
| turn / step hierarchy | `run/*` / `node/*` events |
| `parentSession` + `seedLength` fork lineage | `fork` event payload |
| `sourceEventSeqs` provenance | DAG edges: chain, call→result, snapshot→parent |
| Model-Visible ⟺ Logged invariant | payload digests (sha256 + head); `strict=True` enforces fail-closed recording |
| crash-orphaned turns closed as `interrupted` on reload | `repair` — appends `run/end {status: interrupted}` through the hash chain |
| `deriveMessages` (log → conversation) | `replay_messages` — digests or full text (`record_full=True`) |
| fail-closed invariants | append-time strict JSON validation; `verify_*` refuse on mismatch |

Deliberate deviations: prompt/response full text is **not** stored by default (digest only, opt-in via `record_full=True`); dsh's byte-level replay and compaction are out of scope.

## Proven in production

Before extraction, this design was battle-tested inside a production LangGraph agent product (private, Aug 2026) — a multi-round, tool-calling, real-model workload:

- **Tracing overhead measured <0.1% of a turn** — microsecond-level append+flush per event vs. hundreds of ms per LLM round-trip. "ON by default" was justified by measurement, not hope.
- **10 hook points wired with zero signature changes** to existing code (contextvars propagation); with tracing OFF, behavior is bit-identical — pinned by tests.
- **Snapshot/rollback drill passed end-to-end**: file-level preimages, fail-closed on missing preimage (refuses rather than destroys), idempotent re-application.
- **3,150-test suite green** after wiring the full hook surface.
- The trace layer was how failures were *audited*: one reviewed failure class went from **17/17 of cases to 0** after the trace made its mechanism visible; routing dead-ends went **2 → 0**. (The latency cost of that refactor came from extra model rounds, not from tracing — the ledger itself stayed under 0.1%.)

## Threat model (read before you rely on "tamper-evident")
The hash chain is **keyless by default** — and since 0.3.0, optionally **keyed**:

- ✅ **Proven (keyless)**: given a trusted chain head, ANY deletion, reordering, or edit of ANY logged line is detectable (`verify` recomputes the full chain).
- ❌ **Not proven (keyless)**: an attacker with write access to the log file who rewrites the *entire* file and re-chains it from genesis. No secret is involved, so nothing stops a full rewrite.
- 🔑 **Keyed mode (HMAC-SHA256)**: pass `hmac_key=` to `TraceRecorder` / `recorder_for`, and the chain cannot be re-chained without the key — the full-rewrite attack fails even against an attacker who controls the file. Keep the key out of the log's trust domain (env var, secrets manager); losing the key means losing the ability to verify.

```python
TraceRecorder("traces", thread_id, hmac_key=os.environ["LEDGER_HMAC_KEY"])
```

```bash
export LANGGRAPH_LEDGER_HMAC_KEY=...          # never pass keys as CLI args
python -m langgraph_ledger verify traces/run-42.jsonl
```

Keyed mode is fully backward compatible: keyless logs verify exactly as before (just don't pass a key), and content labels (`tl_*`, `cp_*`, payload digests) stay keyless so cross-log dedup and loop detection keep working without any secret.

**Whether keyed or not: anchor the head outside the log's trust domain.** Export it after each run and store it somewhere the process cannot write:

```bash
python -m langgraph_ledger head traces/run-42.jsonl   # {"seq": N, "id": "<hex>"}
```

Later, `verify` + a head comparison gives you end-to-end integrity. A daily cron appending heads to an append-only store (or an email to yourself) is enough.

Two more honest limits:

- **One writer process per log.** The append lock is in-process (`threading.Lock`); two processes appending to the same `<thread>.jsonl` will fork the chain. Use one trace root per process, or serialize externally.
- **`verify_thread` needs the live saver.** Checkpoint drift detection re-reads the saver, so it works across restarts only with a persistent backend (SQLite/Postgres). The log itself (`verify_log`) verifies anywhere.

## What it is NOT

- Not a hosted observability platform — the log is a local file you own.
- Not byte-level replay of model streams — it is for error localization, audit and state rollback.
- Rollback restores *agent state*; side effects your tools made in the world are yours to undo (pair tool calls with your own preimages if you need that).

## Development

```bash
pip install -e ".[test]"
pytest tests/
```

## License

MIT — see [LICENSE](LICENSE).
