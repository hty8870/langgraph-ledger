# langgraph-dsh-trace

**LangGraph 的 DSH 级可回溯性插件** —— 每个 tool call 的内容寻址哈希标签、防篡改的哈希链事件日志、执行 DAG，以及一等公民的 fork/回退。服务于操作的完整可回退、高度可审计与故障分析。

[English](README.md) · [中文](README.zh.md)

## 为什么

LangGraph 自带 checkpoint 和时间旅行，但它不给你的的是一份**审计级记录**：这些事件事后被改过吗？这次 tool call 到底是"同名"还是"同内容"？整次运行长成什么样的图，能不能从任意节点分叉？

本插件把 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)（dsh）的可回溯性设计——仅追加会话日志作为唯一真相源、格式版本、fork 谱系——移植到 LangGraph 的 checkpointer 契约上，并补上双方都没有原生提供的东西：**内容哈希**。

价值在组合里：

| 能力 | 机制 |
|---|---|
| tool call 身份 | 内容寻址标签 `tl_<hash>`：同（工具名, 入参）⇒ 同标签 |
| 防篡改 | 每个事件与前驱哈希链锁（merkle 式） |
| 执行结构 | 链边 + 调用/结果配对 + checkpoint 父子边构成的 DAG |
| 回退 | `time_travel_config`（原生分叉）与 `fork_thread`（dsh 式带谱系事件的播种分叉） |
| 故障分析 | 错误时间线、精确重复调用环检测（标签的免费副产品） |
| 验证 | `verify_log` 重算哈希链；`verify_thread` 重哈希存储态对照日志声明 |

## 差异化定位（2026-08 现状）

| 项目 | 哈希链 | 执行 DAG | 状态回退/分叉 | LangGraph 原生 |
|---|---|---|---|---|
| **langgraph-dsh-trace** | ✅ | ✅ | ✅ | ✅（checkpointer 即插即用） |
| LangSmith / Langfuse / AgentOps | ✗（托管观测） | trace 视图 | ✗ | SDK |
| CONTINUUM | ✅ | ✗ | 聚焦崩溃恢复 | ✗（MCP server） |
| burnout / VeritasAgent / memtrail | ✅ | ✗ | ✗ | 部分 |
| langgraph 各 checkpointer | ✗ | 仅父链 | 仅时间旅行 | ✅ |

"哈希标签 + DAG + 可用于回退"三合一、LangGraph 原生——目前没人占这个位置。

## 安装

```bash
pip install langgraph-dsh-trace
```

## 快速上手

```python
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_dsh_trace import TracingCheckpointSaver, DshTraceCallbackHandler

saver = TracingCheckpointSaver(InMemorySaver(), trace_root="./traces")
graph = builder.compile(checkpointer=saver)          # 你的图，不用改

graph.invoke(input, config={
    "configurable": {"thread_id": "run-42"},
    "callbacks": [DshTraceCallbackHandler()],         # LLM/tool/node 事件
})
```

每次运行都会留下 `./traces/run-42.jsonl`（每行一个哈希链锁的 JSON 事件）：

```json
{"v":0,"seq":7,"ts":"…","kind":"tool/call","payload":{"label":"tl_9f2e…","name":"search",…},"id":"…","prev":"…"}
{"v":0,"seq":8,"ts":"…","kind":"state/snapshot","payload":{"checkpoint_id":"…","label":"cp_2c01…","parent_checkpoint_id":"…","checkpoint_sha256":"…"},"id":"…","prev":"…"}
```

### 回退与分叉

```python
from langgraph_dsh_trace import time_travel_config, fork_thread

# 从任意已记录 checkpoint 恢复/分叉（LangGraph 原生时间旅行）
cfg = time_travel_config("run-42", checkpoint_id="<past-id>")
graph.update_state(cfg, {"count": 100})              # 从该点开叉

# dsh 式：播种一条全新 thread，继承到指定 checkpoint 的祖先链
new_tid = fork_thread(saver, "run-42", at_checkpoint_id="<past-id>")
```

### 审计

```bash
python -m langgraph_dsh_trace verify  traces/run-42.jsonl   # 哈希链完好？
python -m langgraph_dsh_trace analyze traces/run-42.jsonl   # 错误、循环、时间线
python -m langgraph_dsh_trace dag     traces/run-42.jsonl --mermaid
python -m langgraph_dsh_trace repair  traces/               # 闭合崩溃遗留的未完结 run
python -m langgraph_dsh_trace replay  traces/run-42.jsonl   # 从日志重建消息时间线
```

```python
from langgraph_dsh_trace import verify_thread
report = verify_thread(saver, "traces/run-42.jsonl")  # 存储态 == 日志声明？
assert report["ok"]
```

### 崩溃恢复与回放

进程在 run 中途死掉时，日志末尾留下未闭合的 `run/start`。`verify` 会如实报
*open run*；`repair` 通过哈希链追加一条诚实的 `run/end {status: "interrupted"}`
（幂等、不改写历史）。`replay_messages()` 从日志重建对话时间线——默认只给
digest，handler 以 `record_full=True` 创建时给全文。若需要 fail-closed（记录
不下来就不允许 run 继续），用 `TraceRecorder(..., strict=True)`。

## 与 DeepSeek Harness 的设计映射

设计思想的学习迁移，不是代码搬运（dsh 是 TypeScript/Node；本项目是 Python/LangGraph）：

| dsh 概念 | 本项目 |
|---|---|
| `SessionEvent` 仅追加日志、唯一真相源 | 每 thread 一份哈希链 JSONL |
| `SESSION_FORMAT_VERSION` + `ignorable` 标记 | `v` 字段；未知 kind 如实计数不拒读 |
| turn / step 层级 | `run/*` / `node/*` 事件 |
| `parentSession` + `seedLength` 分叉谱系 | `fork` 事件载荷 |
| `sourceEventSeqs` 溯源 | DAG 边：链、调用→结果、快照→父快照 |
| Model-Visible ⟺ Logged 不变量 | 载荷 digest（sha256 + 首 80 字）；`strict=True` 强制 fail-closed |
| 崩溃孤儿 turn 在重载时闭合为 `interrupted` | `repair`——经哈希链追加 `run/end {status: interrupted}` |
| `deriveMessages`（日志 → 对话） | `replay_messages`——digest 或全文（`record_full=True`） |
| fail-closed 不变量 | 追加处严格 JSON 校验；`verify_*` 发现不符即拒绝 |

有意偏离：prompt/response 全文默认**不落盘**（只存 digest，`record_full=True` 可 opt-in）；dsh 的字节级流回放与上下文压缩不在范围内。

## 它不是什么

- 不是托管观测平台——日志是你自己手里的本地文件。
- 不是模型流的字节级回放——它服务错误定位、审计与状态回退。
- 回退恢复的是 *agent 状态*；工具对外部世界造成的副作用需要你自己配 preimage 才能退。

## 开发

```bash
pip install -e ".[test]"
pytest tests/
```

## License

MIT — 见 [LICENSE](LICENSE)。
