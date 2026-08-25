# Positioning — langgraph-dsh-trace vs. the neighborhood

Survey date: 2026-08-26 (GitHub search; stars move fast, re-check before quoting).

## The slot

Traceability for LLM agents splits into three traditions that don't talk to
each other:

1. **Hosted observability** — LangSmith, Langfuse, AgentOps: dashboards, spans,
   costs. Great for "what happened", weak for "prove it wasn't edited" and no
   state rollback.
2. **Checkpointing** — LangGraph's own savers (memory/SQLite/Postgres/Redis/
   MySQL/DynamoDB): state snapshots with parent links and time-travel. No
   tamper evidence, no audit log, no content labels.
3. **Tamper-evident logs** — CONTINUUM, burnout, VeritasAgent, memtrail:
   hash-chained receipts. But they are not LangGraph checkpointers: no
   execution DAG over real checkpoints, no fork-from-any-node rollback.

**langgraph-dsh-trace = 1 ∩ 2 ∩ 3**, as a drop-in `BaseCheckpointSaver`
wrapper plus a callback handler. The design discipline (append-only source of
truth, format versioning, fork lineage, fail-closed invariants) is ported from
DeepSeek Harness; the content-addressed labeling (`tl_*` / `cp_*`) and the
checkpoint-DAG verification are the differentiators on top.

## Closest neighbors (checked 2026-08-26)

| Project | What it does | Gap vs. this |
|---|---|---|
| CONTINUUM (Cyrax321) | hash-chained event log + semantic checkpoints + idempotency ledger, MCP server | framework-agnostic MCP, crash-recovery focus; no LangGraph checkpoint DAG, no fork-from-node |
| burnout (mahidhar96) | hash-chained audit log + human approval gate | approval workflow, not state rollback |
| VeritasAgent (baggie11) | signed receipts, offline verification | receipts only; no DAG, no resume |
| memtrail (aayushcodebook) | tamper-evident memory audit (Mem0/Zep/LangGraph) | memory scope, not execution |
| langgraph-vew (razr001) | browser visual debugger, checkpoint editing | debugging UI; no integrity proof |
| fast-langgraph (neul-labs) | Rust accelerators for checkpoint ops | performance, orthogonal |

## Claim we make — and the one we don't

We claim: *as of 2026-08, no public project combines content-addressed
tool-call labels + execution DAG + rollback as a LangGraph-native plugin.*

We do not claim novelty of hash chains, event sourcing, or time travel — each
is well known. The contribution is the disciplined combination, packaged for
one framework's real contract.
