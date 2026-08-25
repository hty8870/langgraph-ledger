# langgraph-dsh-trace

**DSH-grade traceability for LangGraph** — content-addressed labels on every tool call, a tamper-evident hash-chained event log, the execution DAG, and first-class fork/rollback. For full rollback, high auditability, and failure analysis of agent runs.

[English](README.md) · [中文](README.zh.md)

## Why

LangGraph already checkpoints state and can time-travel. What it does *not* give you is an **audit-grade record**: were these events edited after the fact? Which tool call exactly — same name, or same *content*? What did the run look like as a graph, and can I fork from any point of it?

This plugin ports the traceability design of [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (dsh) — append-only session log as the single source of truth, format versioning, fork lineage — onto LangGraph's checkpointer contract, and adds what neither has out of the box: **content hashing**.

The combination is the point:

| Capability | Mechanism |
|---|---|
| Tool-call identity | content-addressed label `tl_<hash>` — same (name, input) ⇒ same label |
| Tamper evidence | every event hash-chained to its predecessor (merkle-style) |
| Execution structure | DAG over chain + call/result + checkpoint-parent edges |
| Rollback | `time_travel_config` (native fork) and `fork_thread` (dsh-style seeded fork with lineage event) |
| Failure analysis | error timeline, exact-repeat loop detection (falls out of the labels) |
| Verification | `verify_log` re-checks the chain; `verify_thread` re-hashes stored checkpoints against logged claims |

## Differentiation (as of 2026-08)

Honest snapshot of the neighborhood — see [POSITIONING](docs/positioning.md) for details:

| Project | Hash chain | Execution DAG | State rollback/fork | LangGraph-native |
|---|---|---|---|---|
| **langgraph-dsh-trace** | ✅ | ✅ | ✅ | ✅ (checkpointer drop-in) |
| LangSmith / Langfuse / AgentOps | ✗ (hosted observability) | trace view | ✗ | SDK |
| CONTINUUM | ✅ | ✗ | crash recovery focus | ✗ (MCP server) |
| burnout / VeritasAgent / memtrail | ✅ | ✗ | ✗ | partial |
| langgraph checkpointers (redis/mysql/…) | ✗ | parent links only | time-travel only | ✅ |

Nobody else ships hash-labels + DAG + rollback as one LangGraph-native unit. That is the slot this project occupies.

## Install

```bash
pip install langgraph-dsh-trace
```

## Quickstart

```python
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_dsh_trace import TracingCheckpointSaver, DshTraceCallbackHandler

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
from langgraph_dsh_trace import time_travel_config, fork_thread

# resume/fork from any recorded checkpoint (LangGraph-native time travel)
cfg = time_travel_config("run-42", checkpoint_id="<past-id>")
graph.update_state(cfg, {"count": 100})              # forks from that point

# dsh-style: a new thread seeded with the ancestry up to a checkpoint
new_tid = fork_thread(saver, "run-42", at_checkpoint_id="<past-id>")
```

### Audit

```bash
python -m langgraph_dsh_trace verify  traces/run-42.jsonl   # hash chain intact?
python -m langgraph_dsh_trace analyze traces/run-42.jsonl   # errors, loops, timeline
python -m langgraph_dsh_trace dag     traces/run-42.jsonl --mermaid
```

```python
from langgraph_dsh_trace import verify_thread
report = verify_thread(saver, "traces/run-42.jsonl")  # stored state == logged claim?
assert report["ok"]
```

## Design mapping from DeepSeek Harness

A design study, not a port of code (dsh is TypeScript/Node; this is Python/LangGraph):

| dsh concept | here |
|---|---|
| `SessionEvent` append-only log, single source of truth | one hash-chained JSONL per thread |
| `SESSION_FORMAT_VERSION` + `ignorable` marker | `v` field; unknown kinds counted, not rejected |
| turn / step hierarchy | `run/*` / `node/*` events |
| `parentSession` + `seedLength` fork lineage | `fork` event payload |
| `sourceEventSeqs` provenance | DAG edges: chain, call→result, snapshot→parent |
| Model-Visible ⟺ Logged invariant | payload digests (sha256 + head) — auditable without storing full text |
| fail-closed invariants | append-time strict JSON validation; `verify_*` refuse on mismatch |

Deliberate deviations: prompt/response full text is **not** stored by default (digest only, opt-in at call site); dsh's byte-level replay and compaction are out of scope.

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
