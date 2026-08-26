# langgraph-ledger

[![PyPI](https://img.shields.io/pypi/v/langgraph-ledger)](https://pypi.org/project/langgraph-ledger/)
[![CI](https://github.com/hty8870/langgraph-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/hty8870/langgraph-ledger/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/langgraph-ledger)](https://pypi.org/project/langgraph-ledger/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**面向 LangGraph agent 的全量、防篡改可回溯性** —— 移植 DeepSeek Harness（dsh）的可回溯性设计：每个 tool call 的内容寻址哈希标签、仅追加的哈希链事件账本、执行 DAG、崩溃恢复与回放，以及一等公民的 fork/回退。服务于操作的完整可回退、高度可审计与故障分析。

```bash
pip install langgraph-ledger
```

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
| **langgraph-ledger** | ✅ | ✅ | ✅ | ✅（checkpointer 即插即用） |
| LangSmith / Langfuse / AgentOps | ✗（托管观测） | trace 视图 | ✗ | SDK |
| CONTINUUM | ✅ | ✗ | 聚焦崩溃恢复 | ✗（MCP server） |
| burnout / VeritasAgent / memtrail | ✅ | ✗ | ✗ | 部分 |
| langgraph 各 checkpointer | ✗ | 仅父链 | 仅时间旅行 | ✅ |

"哈希标签 + DAG + 可用于回退"三合一、LangGraph 原生——目前没人占这个位置。

## 安装

```bash
pip install langgraph-ledger
```

## 快速上手

```python
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_ledger import TracingCheckpointSaver, DshTraceCallbackHandler

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
from langgraph_ledger import time_travel_config, fork_thread

# 从任意已记录 checkpoint 恢复/分叉（LangGraph 原生时间旅行）
cfg = time_travel_config("run-42", checkpoint_id="<past-id>")
graph.update_state(cfg, {"count": 100})              # 从该点开叉

# dsh 式：播种一条全新 thread，继承到指定 checkpoint 的祖先链
new_tid = fork_thread(saver, "run-42", at_checkpoint_id="<past-id>")
```

### 审计

```bash
python -m langgraph_ledger verify  traces/run-42.jsonl   # 哈希链完好？
python -m langgraph_ledger analyze traces/run-42.jsonl   # 错误、循环、时间线
python -m langgraph_ledger dag     traces/run-42.jsonl --mermaid
python -m langgraph_ledger repair  traces/               # 闭合崩溃遗留的未完结 run
python -m langgraph_ledger replay  traces/run-42.jsonl   # 从日志重建消息时间线
```

```python
from langgraph_ledger import verify_thread
report = verify_thread(saver, "traces/run-42.jsonl")  # 存储态 == 日志声明？
assert report["ok"]
```

### 崩溃恢复与回放

进程在 run 中途死掉时，日志末尾留下未闭合的 `run/start`。`verify` 会如实报
*open run*；`repair` 通过哈希链追加一条诚实的 `run/end {status: "interrupted"}`
（幂等、不改写历史）。`replay_messages()` 从日志重建对话时间线——默认只给
digest，handler 以 `record_full=True` 创建时给全文。若需要 fail-closed（记录
不下来就不允许 run 继续），用 `TraceRecorder(..., strict=True)`——在 recorder 和
checkpointer 两处强制执行。注意：LangChain 的 callback manager 可能吞掉 handler
异常，要硬保证请走 checkpointer 通道。

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

## 生产验证

这套设计在抽取成本项目之前，先在一个生产级 LangGraph agent 产品（私有，2026-08；多轮、多工具、真实模型负载）里打过仗：

- **tracing 开销实测 <0.1%**——每事件微秒级 append+flush，对比每次数百毫秒的 LLM 往返。"默认开启"是实测支撑的决策，不是拍脑袋。
- **10 类挂钩点、零函数签名变更**（contextvars 传播）；tracing 关闭时行为逐位不变，有测试钉死。
- **快照/回退端到端演练通过**：文件级 preimage、无 preimage 时 fail-closed 拒动（宁少退不毁数据）、重复执行幂等。
- 接线完成后**全量 3,150 条测试全绿**。
- 这套 trace 正是故障**复盘**的工具：一类曾占复盘样本 **17/17** 的失败模式，在 trace 让机制现形后降为 **0**；误判路由死胡同 **2 → 0**。（那轮重构的延迟代价来自更多模型轮次，不是 tracing——账本自身开销始终在 0.1% 以下。）

## 威胁模型（依赖"防篡改"之前必读）

哈希链是**无密钥**的——这是设计选择，含义很精确：

- ✅ **能证明**：给定一个可信链头，对任何一行的删除、乱序、篡改都能被检出（`verify` 重算整条链）。
- ❌ **不能证明**：对日志文件有写权限的攻击者把**整个文件**重写并重新接链。没有秘密参与，就无法阻止整体重写。

**所以：把链头锚定到日志信任域之外。** 每次 run 后导出链头，存到进程写不到的地方：

```bash
python -m langgraph_ledger head traces/run-42.jsonl   # {"seq": N, "id": "<hex>"}
```

之后 `verify` + 链头比对即得端到端完整性。一个每天把链头追加到仅追加存储（或发邮件给自己）的 cron 就够了。

另外两条如实边界：

- **单进程单写者**：追加锁是进程内的（`threading.Lock`）；两个进程写同一个 `<thread>.jsonl` 会把链写劈叉。多进程请各用各的 trace root。
- **`verify_thread` 需要活着的 saver**：checkpoint 漂移检测要重读 saver，跨进程重启验证需配持久化后端（SQLite/Postgres）。日志本身（`verify_log`）在哪都能验。

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
