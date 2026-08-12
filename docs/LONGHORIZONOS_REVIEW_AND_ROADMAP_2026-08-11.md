# LongHorizonOS 技术审查、定位与实施路线图

> 审查日期：2026-08-11  
> 审查对象：LongHorizonOS 本地工作区及 GitHub 当前版本  
> 文档目的：明确项目差异化、当前实现边界、发布阻塞项，以及从开源验证走向系统论文的实施顺序。

## 0. 2026-08-11 并行修复进展

本轮已经把审查中最危险的一批“执行闭环”问题直接落到了代码和回归测试中。
下面的状态以当前本地工作区为准；第 8 节保留了问题的原始分析，便于理解为什么要改。

### 已解决

- **Claim/dispatch 数量对齐**：`AgentOS.run()` 将剩余 dispatch budget 传给
  Scheduler 的 `max_claims`，不会再 Claim 全部 READY Task 却只执行一部分。
- **真实接通 `Agent.executor`**：支持 `executor(task_id)` 和零参数 executor；
  有独立 `Task.verify` 时先执行、后验证；无独立 verifier 时允许 executor
  直接返回 `VerificationOutcome`。
- **Attempt 进入 `RUNNING`**：用户执行代码运行前，Scheduler Attempt 会产生
  明确的 RUNNING 状态转换。
- **SDK 执行结果 fencing**：执行前和用户代码返回后都会检查精确 Claim、Agent
  owner 及当前有效 Kernel Lease；旧 owner、过期 Lease 或已重分配 Claim 的结果
  不会通过当前 SDK 路径进入 Facts/VPG。
- **Kernel Action 终态提交 fencing**：每个资源都有持久化单调 fencing token；
  同一批并发 shared holders 共享 cohort token，新 cohort 或 exclusive reacquire
  推进 token。Action 持久化原始 Lease IDs 与 token map，只有仍处于 `RUNNING`
  且原始 Lease/token 仍有效时，才能在同一事务内完成
  `RUNNING -> COMMITTED`；cancel、fail、uncertain 与 completion 的竞态不会
  覆盖已有终态。
- **失败清理只作用于当前 Claim**：executor、verifier 或 Evidence 写入失败时，
  不会误删后来重分配给其他 owner 的 Claim。
- **Evidence Graph patch 原子化**：ArtifactRef、Verification、Evidence 及三条关系边
  在一个 VPG patch 中提交，不再产生“半张 Evidence 子图”。
- **主 SDK Lease-to-VPG fenced commit**：Evidence 提交在共享 SQLite writer
  transaction 中校验精确 Lease generation；release、reassignment、renew
  与 Graph patch 具有确定的持久化顺序，旧 generation 不会写入新的 VPG
  projection。Lease expiry 是 liveness 字段，不是 generation。
- **预留资源后才能 Admission**：Action 使用
  `SUBMITTED -> atomic acquire -> ADMITTED -> INTENT_DURABLE`；资源竞争不会遗留
  无 Lease 的可调度 ADMITTED Action。
- **Dispatch 前验证完整资源契约**：资源、mode、owner PID、Lease 数量/唯一性及
  TTL 任一不符合，Action 都会失败并释放 Lease，driver 不会被调用。
- **Lease waiter 生命周期清理**：retry、成功 acquire、cancel、release-all、
  process exit 和 deadlock recovery 都会清理失效 waiter，避免历史请求制造虚假环。
- **Lease loss 后可重新调度**：Claim 新进入 LOST 时清除对应 idempotency key；
  STALE Claim 则按 semantic epoch 判断旧 owner 是否应该被回收。
- **普通资源竞争不再击穿 scheduling pass**：Kernel 的
  `LeaseAcquisitionFailed` 在 Scheduler provider 边界被规范化为正常 contention。
- **重复资源声明 fail-closed**：同一个 atomic acquire bundle 不能重复声明同一
  resource，避免同一 owner 获得两条 exclusive Lease。
- **Admission 中途异常有补偿**：`admit` 或 `mark_intent_durable` 失败时，Action
  会进入终态并释放已取得的 Lease，不再留下 orphan ADMITTED Action。
- **同步 SDK 明确拒绝 async executor**：async function、async callable object，
  以及同步 wrapper 返回的 awaitable 都会被明确拒绝并清理 coroutine，避免
  “看似执行、实际从未 await”的假成功。
- **Exact-version Evidence fail-closed**：verifier 返回的 Artifact 版本早于 Facts
  当前版本时，不再被静默提升为 latest；旧版本 PASS 不能伪造成新版本 Evidence。
- **Repair 保留调用者指定的版本身份**：requested version 等于当前版本时复用，
  小于当前版本时拒绝，大于当前版本时精确登记，不再静默 `cur + 1`。
- **Repair cause 可审计且 fail-closed**：`cause_details` 记录 exact
  `old_version -> new_version`；多轮修复选择严格小于新版本的最近历史绑定。
  拼错或未被 Graph 引用的 Artifact 会在写 Facts 前被拒绝，不再任意选择第一个
  Task 伪造失效原因。
- **VPG 每版本 durable projection snapshot**：Graph v0 和每个成功 patch 都有
  不可变 projection snapshot；patch、GraphVersion、events、materialized
  projection 与历史 snapshot 在同一事务提交。加载和恢复会校验 Graph 归属、
  node identity/type/payload、edge endpoints 以及完整 projection hash。
- **VPG verified atomic recovery**：恢复会先验证所有版本的 snapshot header 和
  目标 snapshot，再以版本条件检查原子替换 materialized projection；损坏或缺失
  的历史会 fail-closed，不会先删除当前可用 projection。
- **VPG 增量 entity-revision history**：每个 GraphVersion 仍有完整 projection
  hash，但历史表只追加发生变化的 node/edge revision；旧 full-history 格式仍可读。
  连续单节点小 patch 的 durable history 已从二次增长降为线性。

### 当前验证（更新于 2026-08-12）

当前发布候选已经通过多组定向门禁，包括 Multi-Agent Scheduler、公开
SDK/CLI/Demo、VPG 与发布回归测试。最后一次公开声明、CLI 和 Demo 聚焦复核为
`76 passed`；更完整的定向计数与复现边界记录在
[`docs/releases/v0.1.0.md`](releases/v0.1.0.md)。

静态与发布门禁已通过：

```text
ruff check / format: passed
mypy src/lhos: passed
compileall src: passed
wheel + sdist build: passed
twine check: passed
fresh-venv wheel install: passed
installed CLI help + recovery-repair JSON demo: passed
```

完整 non-slow 仓库测试在本机 Windows 的 600 秒窗口内没有跑完，并曾在组合运行中
出现一次 SIGKILL journal offset 间歇失败；对应测试文件单独重跑通过。因此本版本
**不宣称全仓测试已完整全绿**，也不把定向测试数写成整个仓库总数。

### 仍未解决，不能宣传为已具备

> **2026-08-12 状态勘误：** 本节最初记录的三个缺口中，当前版本已经实现
> AgentOS.run_async 的有界异步执行、CPU/RAM/GPU/VRAM/model-slot 的**逻辑**
> 原子准入，以及可选的单 writer Scheduler durable replay。它们仍不是物理主机资源
> enforcement、RPM/TPM/browser/sandbox/workspace-lock 调度，也不是分布式多 writer
> replay。以下列表按当前 `v0.1.0` 边界更新。

- 已有真正的 executor 并发，但 Evidence/VPG commit 在单次 `run_async` 中仍串行；
  不同 runtime 实例也不共享该锁，尚不是分布式 worker runtime；
- 当前只消费 CPU/RAM/GPU/VRAM/model-slot 的**每 Agent 逻辑资源向量**；尚无共享
  host/device inventory、动态 telemetry、物理隔离、RPM/TPM、browser、sandbox、
  workspace lock、preemption、fairness 或 starvation guarantee；
- Scheduler Claim/Attempt/idempotency 已支持可选 durable replay 与 hash-chain 校验，
  但假设单 Scheduler writer；尚无 leader election、分布式 CAS 或 multi-writer fencing；
- **Kernel fencing 的边界只到 Action 终态。** 旧 Action 返回后已经不能把 Kernel
  状态写成 `COMMITTED`，但 driver/resource sink 尚未消费 fencing token 或执行
  自身 CAS；driver 内部已经发生的外部不可逆副作用仍不能宣传为 exactly-once；
- **VPG/Claim 的完整 fenced semantic commit 尚未完成。** 主 SDK 的
  Lease-generation 校验与 Evidence VPG patch 已在同一个 SQLite writer
  transaction 中，关闭了 Lease release/reassignment 到 patch 的 TOCTOU；
  但内存 Claim/semantic epoch、Facts、Action、Claim completion、Lease
  release 及 driver side effect 仍未纳入同一跨平面事务；
- **通用 checkpoint/restore 不是完整执行状态恢复。** 现有能力覆盖持久化元数据、
  journal/projection 和可选 workspace checkpoint；不保存任意 Python 进程内存、
  调用栈或正在运行的执行上下文。Context snapshot 目前也不是进程重启后自动
  rehydrate 的 durable full-state snapshot；这与已经完成的 VPG durable
  projection snapshot 是两种不同能力；
- 任意 Python executor/verifier callback 仍运行在调用进程，尚未强制经过
  Kernel capability/tool gateway；
- 超过单 patch 上限的大 Goal 仍采用多批提交，批次中途失败可能留下部分 Graph。

因此下一阶段优先级不是再增加概念模块，而是把已有单机逻辑闭环扩展为：

```text
shared host/device inventory + telemetry
-> cross-process worker delivery
-> provider quota / lock resources / fairness / preemption
-> driver-consumed side-effect fencing
-> VPG/Claim conditional semantic commit
-> cross-plane transaction protocol
-> capability-enforced tool gateway
```

---

## 1. 执行摘要

LongHorizonOS 的方向是成立的，但对外定位必须足够准确。

现有 Agent framework、workflow engine、分布式调度器并不是没有状态、Graph、恢复或资源调度，而是分别解决不同层面的问题：

- LangGraph、Microsoft Agent Framework、CrewAI 等擅长 Agent/workflow 编排、checkpoint、resume 和并行分支；
- Temporal 擅长 durable execution、队列、重试和 Worker 容量控制；
- Ray、Kubernetes 擅长 CPU、GPU、内存及自定义资源的放置与隔离；
- Bazel、Dagster 等已经具备依赖图、版本变化、stale propagation 和增量重算思想；
- AIOS 已经探索了 Agent syscall、LLM/tool/memory 调度和 Agent Kernel；
- LongHorizon-Harness、SemIso 等新工作已经开始触及长程 Agent 状态管理和语义隔离。

因此，LongHorizonOS 不应宣传为：

> 第一个拥有 Graph、checkpoint、Kernel 或资源调度的 Agent OS。

更稳固的定位是：

> **LongHorizonOS 是 Stateful Agent 的语义控制平面，将 provenance-aware recovery、evidence-backed validity 与 resource-aware scheduling 连接成统一闭环。**

推荐首屏文案：

> **Most agent frameworks answer what should run next. LongHorizonOS also answers what remains valid after the world changes.**

进一步可以表述为：

> **Ray knows where work can run. Temporal knows how it can resume. LongHorizonOS decides whether previous work is still valid—and schedules the minimum verified repair.**

真正应该构建的完整闭环是：

```text
外部世界或 Artifact 变化
→ 自动捕获 provenance
→ 判断历史 Evidence 是否仍然适用
→ 传播 causal invalidation
→ 计算 minimal repair frontier
→ 根据资源、成本和关键路径进行 admission/scheduling
→ 阻止 stale execution 或失效 Lease 的结果提交
→ 生成新 Evidence
→ Goal verified reclosure
```

---

## 2. 现有系统已经做到什么

| 系统 | 强项 | LongHorizonOS 应补充的部分 |
|---|---|---|
| LangGraph | Graph execution、checkpoint、durable execution、interrupt/resume、并行分支 | 外部 Artifact 变化后的 Evidence validity、causal invalidation、异构资源调度 |
| Microsoft Agent Framework / AutoGen | 多 Agent workflow、并发、fan-out/fan-in、状态保存和恢复 | provenance-driven repair 与底层资源 admission 的统一闭环 |
| CrewAI | Flow persistence、resume/fork、异步任务和团队编排 | Artifact/Evidence 版本失效、Lease 和资源正确性协议 |
| OpenHands | Coding Agent、Sandbox、会话和 workspace 状态恢复 | Graph-based semantic reclosure 和跨 Agent 资源优化 |
| Temporal | Durable workflow、Task Queue、retry、heartbeat、throttling、Worker slots | 不理解某个历史结论是否仍然为真 |
| Ray | CPU/GPU/custom resource scheduling、placement group、Task/Actor 并行 | 不理解 VERIFIED、Evidence、Artifact version 和 Goal closure |
| Bazel | Action graph、声明式 inputs/outputs、缓存与增量重建 | Agent 的隐式依赖、非确定执行、外部副作用和独立验证 |
| Dagster | Asset lineage、data version、stale propagation | 通用 Agent 对话、工具调用、副作用和 Evidence authority |
| AIOS | Agent syscall、Kernel、memory/tool/model 调度 | Evidence-backed validity、causal invalidation、minimal repair |
| LongHorizon-Harness | 长程任务状态、Manager/Executor/Auditor、真实任务 benchmark | Graph-based 并行 frontier 与异构资源调度 |
| SemIso | Prompt/model/index/tool 版本变化下的语义隔离 | 增量 repair、共享 Artifact 写入、资源调度和 Goal reclosure |

结论不是“别人都做不到”，而是：

> **能力已经分别存在，但 semantic control plane 与 resource execution plane 仍然缺少统一的一致性协议。**

---

## 3. LongHorizonOS 的核心研究问题

### 3.1 Checkpoint 不等于语义有效

最重要的动机可以表述为：

> Checkpoint 能告诉 Agent 上次执行停在哪里，但不能告诉它：当需求、文件、API、模型、工具或外部事实变化后，之前完成的工作是否仍然成立。

例如：

```text
Agent 已完成 80 个步骤
→ 用户改变需求
→ 一个依赖文件被修改
→ API schema 更新
```

普通恢复系统通常只能：

- 从 checkpoint 恢复；
- 从失败节点重试；
- 或从头重新执行。

LongHorizonOS 应回答：

1. 哪些历史结果仍然有效？
2. 哪些 Evidence 已经过期？
3. 失效范围是否只限于 causal cone？
4. 最小需要修复哪些节点？
5. 当前资源条件下，哪些修复任务应该并行？
6. 如何阻止旧 epoch 的执行结果重新污染新状态？

### 3.2 Agent workload 破坏了传统增量系统的假设

传统 build/data system 往往依赖以下假设：

- inputs/outputs 可以完整声明；
- Action 相对确定；
- Action 是纯函数或至少幂等；
- 资源需求事先已知；
- 输出正确性可以由内容哈希或 cache key 判断。

Agent workload 中这些假设经常不成立：

- Agent 可能隐式读取文件、网页、对话和环境变量；
- LLM 输出非确定；
- 工具调用可能产生不可逆副作用；
- Agent 执行过程中可能动态发现新依赖；
- “结果存在”不代表“结果正确”，仍需要 Verifier 和 Evidence。

这正是 LongHorizonOS 的机制创新空间。

---

## 4. 推荐系统架构

Graph 应该为调度器提供状态和约束，但 Graph 不应包办所有调度策略。

建议至少区分三个逻辑投影。

### 4.1 Verified Progress Graph

负责回答：

- Artifact 和 Evidence 当前版本；
- Task 的 `UNVERIFIED / VERIFIED / STALE`；
- 依赖是否满足；
- Ready Frontier；
- Repair Frontier；
- Goal 是否能够闭合；
- 哪些任务位于 reclosure critical path；
- 哪些节点能够解锁更多 downstream obligations。

### 4.2 Execution / Attempt Graph

负责记录：

- 哪个 Agent 执行哪个 Task；
- attempt 状态；
- graph version；
- semantic epoch；
- Action IDs；
- operational success；
- semantic verification；
- crash/retry/timeout；
- side effect 是 committed、failed 还是 uncertain。

### 4.3 Resource Allocation / Wait-for Graph

负责记录：

- Agent/Action 当前持有哪些资源；
- 正在等待哪些资源；
- CPU/GPU/内存/LLM quota/Sandbox 是否足够；
- exclusive workspace/artifact 是否冲突；
- Lease 和 fencing token；
- 是否存在循环等待；
- 哪个进程应当被抢占、取消或回滚。

这三个 Graph 可以建立在同一 append-only event log 上，作为不同 projection，而不是维护三个互不一致的事实来源。

### 4.4 统一的运行条件

一个 Action 只有同时满足以下条件，才允许进入 `RUNNING`：

```text
semantic_ready(action)
AND dependencies_are_current(action)
AND complete_resource_bundle_granted(action)
AND capability_authorized(action)
AND graph_version_is_current(action)
AND semantic_epoch_is_current(action)
AND lease_fencing_token_is_current(action)
```

最终提交还需要再次执行 fenced validation：

```text
commit_allowed(action)
IFF
  claim is still owned
  AND lease is still live
  AND fencing token is current
  AND graph version/semantic epoch is current
  AND declared dependency versions are current
```

---

## 5. 推荐资源模型

Task/Action 应声明完整的资源请求，而不只是 Agent specialization。

示例：

```python
ResourceRequest(
    cpu=2,
    memory_gb=8,
    gpu=1,
    gpu_memory_gb=16,
    sandbox_slots=1,
    browser_slots=1,
    llm_concurrency={"provider/model": 1},
    token_rate=20_000,
    api_quotas={"github": 1, "browser": 2},
    required_tools={"shell", "python"},
    exclusive_resources={"workspace://repo/main"},
    locality_preferences={"artifact-cache://node-3"},
)
```

Agent workload 的稀缺资源不仅包括 CPU/GPU，还包括：

- Provider RPM/TPM；
- 模型并发 slot；
- Token budget；
- Sandbox/container；
- Browser；
- 数据库连接；
- Git worktree；
- Workspace write lock；
- Artifact exclusive ownership；
- 外部 API quota；
- 人工审批。

建议明确区分：

```text
STALE
READY
WAITING_RESOURCE
ADMITTED
RUNNING
VERIFYING
VERIFIED
FAILED
EXHAUSTED
CANCELLED
UNCERTAIN
```

其中 `WAITING_RESOURCE` 不能被模糊地表示成 `FAILED`、`BLOCKED` 或无原因的 scheduler skip。

---

## 6. Deadlock、资源不足与饥饿

### 6.1 资源不足不是死锁

资源暂时不足属于 admission failure：

```text
READY
→ WAITING_RESOURCE
→ ADMITTED
→ RUNNING
```

此时任务：

- 不应进入 RUNNING；
- 不应占有部分资源；
- 不应永久持有 Task Claim；
- 不应被算作执行失败；
- 资源释放后应该被公平唤醒。

### 6.2 Task DAG 无环不代表没有资源死锁

例如：

```text
Agent A 持有 GPU，等待 Repo write lease
Agent B 持有 Repo write lease，等待 GPU
```

任务依赖 DAG 即使完全无环，也可能产生 Resource Wait-for Graph 环。

### 6.3 推荐机制

1. 原子资源包分配：全部获得或全部不获得；
2. 禁止持有部分资源后等待第二组资源；
3. 动态发现新资源时，释放旧资源后重新 admission；
4. Lease TTL、heartbeat 和 crash reclamation；
5. fencing token，阻止过期 owner 提交；
6. wait-for graph cycle detection；
7. deterministic victim selection；
8. bounded retry 和 exponential backoff；
9. priority aging 或 DRF，避免 starvation；
10. backpressure，避免无限生成 runnable work。

只有在资源需求完整声明、资源有限、Lease 有界、旧 owner 被 fencing 等前提下，才适合声明某类 deadlock freedom。当前不应宣传无条件的 `deadlock-free`。

---

## 7. 当前代码已经具备的基础

### 7.1 VPG 与 operational scheduling 已有分层意识

`src/lhos/runtimes/verified_progress/models.py` 中已经明确区分：

- logical READY；
- operational RUNNABLE。

这是正确的系统边界。

### 7.2 D2 Scheduler 使用 VPG 作为语义权威

`src/lhos/runtimes/multi_agent/scheduler.py` 当前会消费：

- `ready_frontier(graph_id)`；
- `current_graph_version(graph_id)`；
- Task payload；
- Task validity；
- semantic epoch。

它还会在 claim 线性化前重新检查 GraphVersion 和 Ready Frontier。

### 7.3 Kernel Lease 机制是真实实现

`src/lhos/agent_os/services/lease_service.py` 已经具有：

- SQLite 事务下的 atomic acquire；
- shared/exclusive lease；
- TTL 和过期回收；
- process exit 后的资源释放；
- wait-for graph；
- cycle detection；
- deterministic deadlock victim；
- durable per-resource monotonic fencing token；
- 同一并发 shared cohort 共享 token，新 cohort/exclusive reacquire 推进 token；
- Action 对 Lease IDs 和 token map 的持久化；
- 从 `RUNNING` 出发、重新验证 durable Lease/token 的 conditional
  `RUNNING -> COMMITTED` transition。

因此，Kernel 已能阻止失去资源权威的旧 Action 把自身状态提交为
`COMMITTED`，也能避免 completion、cancel、fail 和 uncertain 的并发终态覆盖。
这一保证不延伸到 driver 内部已经发生的外部副作用；资源 sink 仍需显式消费
fencing token/CAS。

### 7.4 VPG durable version snapshots 与原子恢复已经实现

`src/lhos/runtimes/verified_progress/graph_store.py`、`sdk.py` 和
`recovery.py` 已经具有：

- v0 及每个 committed GraphVersion 的 durable projection snapshot；
- patch/version/events/materialized projection/history 同事务提交；
- snapshot header、GraphVersion hash、node identity/type/payload、Graph 归属和
  edge endpoint 校验；
- 完整 node/edge projection hash 校验；
- 恢复前先验证 immutable history，再按 expected GraphVersion 原子替换
  materialized projection；
- 非空 legacy 库缺少可信历史时 fail-closed。

这里的 snapshot 是 **VPG projection 的 durable version history**，不是任意
Agent 进程内存、Python 调用栈或执行中 Context 的完整 checkpoint。

2026-08-12 已将物理存储改为 append-only entity revisions：每次 commit 仍记录
完整 projection hash，但只为发生变化的 node/edge 写 history row；加载版本 V 时，
为每个实体选择 `version <= V` 的最新 revision，再验证完整 projection hash。
旧 full-history 数据库与新旧混合 history 均可读取。

最终可复现命令：

```bash
python scripts/benchmark_vpg_incremental_history.py \
  --sizes 100 200 400 --check \
  --output artifacts/benchmark_results/vpg-incremental-history-2026-08-12-final.json
```

| 连续单节点 patch 数 | 总耗时 | 总数据库 | History rows | History payload |
|---:|---:|---:|---:|---:|
| 100 | 0.685s | 516,096 B | 100 | 35,274 B |
| 200 | 2.281s | 1,171,456 B | 200 | 70,874 B |
| 400 | 8.859s | 2,772,992 B | 400 | 142,074 B |

旧 full-copy 布局在 N=400 时需要 80,200 行；当前为 400 行，减少 99.50%。
之前记录的约 37.9 MB 空间回归已经降至当前门禁中的 2.77 MB。这里能严谨证明的是：

- 对连续小 patch，durable entity-history 行数与 payload 已线性增长；
- 任意历史版本仍可重建并通过 GraphVersion/full projection hash；
- 继承 revision 被篡改或删除时 fail-closed；
- history 写失败会回滚 patch/version/event/cache/idempotency；
- materialized cache 中已有 durable entity 被合法 JSON 篡改后，下一 commit
  会 fail-closed，不能把篡改内容固化为新 revision。

这里**不能**宣称完整 commit pipeline 已线性。当前 SDK 每次仍进行全图读取、
deepcopy、patch validation、derived-state 计算和全量 hash，因此 N=200 到 N=400
的端到端耗时仍明显超线性。下一步性能工作应是增量 candidate/diff、增量 derived
计算与 hash，以及周期 checkpoint/compaction，而不是继续复制完整历史。

兼容性必须分两类说明：具有可信 full-history snapshot 的旧数据库可以直接读取；
更早版本创建、完全没有可信 snapshot 的非空 Graph 仍只能读取 materialized
projection，rebuild 与后续 commit 会 fail-closed。正式发布前仍需要基于可信
export/backup 的迁移工具，不能从 mutable cache 自动伪造历史。当前 Patch API
也没有删除语义；未来支持删除前必须引入显式 tombstone。

### 7.5 Checkpoint/restore 当前是有范围的恢复能力

当前 workspace checkpoint 可以记录/恢复 Git 或 filesystem 状态，Journal 与
projection 可以恢复部分 durable metadata。Context VM 也支持同一服务实例内按
Artifact version 重新物化 snapshot，但其 snapshot registry 尚未成为重启后自动
rehydrate 的完整持久化执行状态。

因此对外应写：

> durable metadata/projection recovery and optional workspace restore

而不是：

> full process-memory or execution-stack checkpoint/resume

### 7.6 多 Agent matching 已经存在

当前已经具备：

- specialization；
- supported tools；
- capability eligibility；
- max concurrency；
- load penalty；
- cost weight；
- preferred agent；
- deterministic matching。

当前更准确的名称是：

> 单机 bounded async multi-Agent execution + task ownership +
> 每 Agent 逻辑资源准入原型。

它还不能称为物理 Host/Device 资源调度器、跨进程 Worker Runtime 或分布式
Multi-Agent 集群系统。

---

## 8. 发布阻塞问题的原始分析与当前状态

本节保留最初发现问题时的故障机制和复现路径。每一项都增加当前状态，避免把
已经修复的历史分析误读成当前实现。标为“已修复”的项目仍应保留回归测试。

### P0-1：Resource acquisition 失败后，Action 仍可能无 Lease 执行

> **当前状态：已修复。** Admission 现在采用
> `SUBMITTED -> atomic acquire -> ADMITTED -> INTENT_DURABLE`，资源竞争不会留下
> 无 Lease 的可调度 Action；dispatch 前还会验证完整资源契约。

原始问题中的主要顺序是：

```text
submit
→ ADMITTED
→ acquire resources
```

若资源申请失败，Action 可能遗留为 `ADMITTED`，而 Kernel 会调度所有 `ADMITTED` Action，dispatch 前没有再次验证资源 Lease。

最初的最小复现实验曾观察到：

```text
resource busy
→ LeaseAcquisitionFailed
→ action remains ADMITTED with no lease
→ later dispatch executes and commits the action
```

已按以下方向修复：

```text
SUBMITTED
→ WAITING_RESOURCE
→ atomic reserve
→ ADMITTED / INTENT_DURABLE
→ DISPATCHED
```

并在 driver dispatch 前验证 Lease contract；Kernel Action 终态还会在同一事务内
重新验证 durable fencing token。

### P0-1A：当前 waiter 记录可能制造虚假死锁

> **当前状态：已修复。** retry、成功 acquire、cancel、release-all、process
> exit 和 deadlock recovery 都会清理无效 waiter；普通 contention 也已在
> Scheduler provider 边界规范化。

原始问题是：Lease acquire 失败时会写入 `lease_waiters`，但这个记录不是一个
具有生命周期的 durable pending request：

- 资源释放时不会自动清除或唤醒 waiter；
- owner 改变后，旧 waiter 仍然存在；
- 只有同一 pid 后续成功取得同一资源时才会删除；
- wait-for graph 因而可能把历史失败请求误认为当前正在等待。

最初曾可以构造出如下情况：

```text
A 曾经申请 R 失败
→ R 后来释放并由 C 获得
→ A 实际已经没有正在执行的 acquire request
→ wait-for graph 仍把 A 视为等待 C
→ 再结合 C 的一次等待，产生虚假的 cycle
→ Kernel 可能错误选择并终止 victim
```

长期仍建议将 resource wait 建模为正式对象：

```text
ResourceWaitRequest(
    request_id,
    pid,
    resource_bundle,
    created_at,
    deadline,
    priority,
    state=PENDING|GRANTED|CANCELLED|EXPIRED,
)
```

Wait-for Graph 只能消费仍为 `PENDING` 且未超时的请求；release、cancel、process exit、retry 和 timeout 都必须显式更新其生命周期。

### P0-2：Scheduler claim 全部 READY Task，但 SDK 只执行 max_dispatches 个

> **当前状态：已修复。** `AgentOS.run()` 会把剩余 dispatch budget 传给
> Scheduler 的 `max_claims`。

原始问题中：

- Scheduler 一次会 claim 当前全部 Ready Frontier；
- SDK 只执行前 `max_dispatches` 个；
- 剩余 Task 保留 ACTIVE claim；
- 后续 scheduling pass 因为已有 active claim 而跳过；
- Task 永久无法执行。

复现结果：

```text
10 个独立 Task
max_dispatches = 8
→ T0-T7 VERIFIED
→ T8/T9 UNVERIFIED + ACTIVE
→ 第二次 run 仍然不会执行 T8/T9
```

短期修复：

- 把剩余 dispatch budget 作为 `max_claims` 传给 Scheduler。

正确长期修复：

- Scheduler 只 claim 能够真正进入 worker queue 的任务；
- 或者真正异步执行全部已经成功 claim 的任务。

### P0-3：Agent.executor 已接通；Agent.model provider wiring 尚未完成

> **当前状态：部分修复。** `Agent.executor` 已进入主执行路径，并支持 executor
> 与 verifier 分离；`Agent.model` 目前仍是配置元数据，不会自动创建 provider
> client 或自动完成模型调用接线。

原始公开 API 保存了：

- `Agent.executor`；
- `Agent.model`。

早期 SDK 曾直接同步调用 `Task.verify()`，没有消费 Agent executor/model；当前
`Agent.executor` 已接线，下面的原始描述仅用于说明历史问题。

原始执行路径还表现为：

- 早期 Scheduler 只生成 `EXECUTION_DISPATCHED` event；
- 早期没有真实 AgentDispatcher/work delivery；
- 早期 Agent process 没有真正运行 Task；
- 同步 `run()` 路径中的 verifier callback 仍然是串行执行。

其中 executor 接线已经修复；当前仍未完成的是跨进程独立 worker delivery、
`Agent.model` provider wiring，以及由此带来的分布式 worker runtime。

应建立正式接口：

```text
TaskContext
→ AgentExecutor.execute()
→ ExecutionResult + produced Artifact
→ Verifier.verify()
→ VerificationOutcome + Evidence
```

### P0-4：同步 `run()` 串行；`run_async()` 已提供有界并发

> **当前状态：部分修复。** 同步 `run()` 仍按顺序执行，这是有意保留的
> 同步语义；公开 `run_async()` 已提供同进程 bounded executor overlap、
> 全局/per-Agent 并发限制、取消清理和结果 fencing。跨进程独立 worker
> delivery、timeout、preemption 和分布式并发仍未完成。

SDK 的同步 `run()` 使用顺序循环调用 verifier；因此两个独立的 0.2 秒任务在
同步路径上约需要 0.4 秒，即使 Agent 的 `max_concurrency` 大于 1。需要并发时
应使用 `run_async()`；当前 checked-in async AgentOS benchmark 已通过（24 个
任务、峰值并发 4、1.921x 受控 I/O speedup，详见 benchmark 文档）。

仍需：

- 跨进程 worker pool / work delivery；
- timeout；
- preemption/fairness/starvation 机制；
- 跨进程 crash recovery 与多 writer 协调。

### P0-5：Lease loss 后的重新调度存在状态泄漏

> **当前状态：部分修复。** 新进入 LOST 的 Claim 会清理对应 idempotency key，
> STALE Claim 会按 semantic epoch 回收，普通 Lease contention 不再击穿整个
> scheduling pass；Claim/Attempt/idempotency 已支持可选的**单 Scheduler writer**
> durable replay 与 hash-chain 校验，但尚无 leader election、分布式 CAS 或
> multi-writer fencing。

原始问题中，scheduler 的 claims、attempts 和 idempotency key 主要保存在内存中。

最初观察到：

- Lease loss/reconcile 后 claim 可能变成 LOST；
- idempotency key 没有同步清除；
- Ready Task 后续被 `idempotent replay` 跳过；
- TTL expiry/process death 后不一定能够重新分配；
- 跨进程重启时，旧 Kernel Lease 与新 scheduler 内存状态脱节。

需要 durable scheduler projection，并把：

```text
claim
attempt
idempotency
lease owner
semantic epoch
```

放入可 replay 的统一状态机。

最初 Kernel Lease 被其他 owner 占用时，底层 provider 会抛出
`LeaseAcquisitionFailed`，而 Scheduler claim path 主要按“返回 None”设计。
当时结果可能是：

- 整个 scheduling pass 异常退出；
- claim 暂留在 `ACQUIRING`；
- 必须依赖之后的 reconcile 才能清理；
- 正常的 claim race 被升级成用户可见的 `SchedulingError`。

当前已把底层异常规范化为正常 contention；长期仍应把资源竞争显式建模为状态转换：

```text
PROPOSED
→ ACQUIRING
→ WAITING_RESOURCE / REJECTED
```

而不是让可预期的容量竞争穿透为 runtime exception。

### P0-6：STALE Task 的 ACTIVE claim 没有可靠撤销

> **当前状态：已修复当前 SDK 路径；durable replay 受单 writer 边界限制。**
> Claim/Lease/semantic epoch 会在执行前后重新检查，失败清理只影响精确 Claim；
> 可选 Scheduler durable replay 假设单 writer，不能替代分布式 multi-writer
> recovery。

原始问题中，reconciliation 虽接收 `vpg_task_stale`，但 active claim 的
reconciliation 未完整消费该条件。

需要：

```text
Artifact invalidated
→ semantic epoch increment
→ active attempts from older epoch fenced
→ old claims cancelled/released
→ Task returns to Repair Frontier
```

### P0-7：Evidence attachment 与 Lease authority 的原子边界

> **当前状态：已修复 VPG 子图内部原子性，并关闭主 SDK 的
> Lease-to-VPG TOCTOU。** ArtifactRef、Verification、Evidence 及其关系边
> 通过单个 patch 提交；Evidence 路径在同一 SQLite writer transaction 中
> 重新校验 Lease generation。它尚不等于 Facts、Action、Claim completion、
> VPG patch 与 Lease release 的跨平面统一事务。

原始 Evidence attach 被拆分为多个 VPG patch。

若中间失败，可能留下：

- ArtifactRef/produces 已提交；
- Verification/Evidence 未提交；
- Task claim 仍 ACTIVE；
- Lease 仍被持有；
- 后续 run 永久卡住。

需要：

- 单事务提交；
- staging graph + atomic publish；
- 或完整补偿式 Saga。

单个 Evidence 子图和主 SDK 的 Lease-to-VPG 条件提交已经落地；跨平面事务
以及 driver side-effect fencing 仍需独立设计。

### P0-8：fenced commit 的已修边界与剩余缺口

> **当前状态：Lease-to-VPG 部分完成。** Kernel 已有 durable monotonic token，
> 并以同一事务校验 Lease/token 后提交 Action 终态，所以旧 Action 不能再把
> Kernel 状态写成 `COMMITTED`。主 SDK Evidence path 也会在共享 SQLite
> writer transaction 中校验精确 Lease generation 后再写 VPG patch。
> 内存 Claim/semantic epoch、Facts、Action、Claim completion、Lease release
> 与 driver 外部副作用仍未纳入同一跨平面 conditional transaction。

正确提交条件至少包括：

```text
claim still active
AND lease still live
AND fencing token matches
AND graph version is current
AND semantic epoch is current
```

剩余工作是把 Claim/semantic epoch 与这些 durable 条件变成统一的跨平面原子
CAS，而不是只依赖内存提交前检查。此外，side-effect sink 必须消费相同 token，
才能阻止旧 owner 在 Kernel 之外产生不可逆效果。

### P0-9：Verifier 可以绕过 Kernel capability/resource boundary

> **当前状态：未修复。** 这是当前任意 Python callback 模型的重要安全边界。

当前 verifier 在调用方 Python 进程中直接执行，而不是在 Agent process/Kernel driver 下运行。

因此即使 Agent 没有 shell、workspace 或其他 capability，callback/CommandVerifier 仍可能直接访问对应资源。

需要：

- Task execution 和 verifier 都获得明确的 `ExecutionContext`；
- 所有 tool access 必须绑定 pid、capability 和 resource lease；
- Shell、workspace、browser 等工具不能绕过 Kernel；
- Verifier 最好使用独立的、权限更小的 verifier identity。

### P1：旧 Runtime 与新 AgentOS/D2 主路径存在分叉

> **当前状态：未完全收敛。** 所有论文级收益实验仍应明确输出所使用的 runtime
> implementation ID。

仓库中还保留了一套 single-worker sequential runtime。部分 controlled/advanced benchmark 使用的是这套旧 RuntimeStack，而不是公开 SDK 的 AgentOS + D2 + Kernel execution path。

因此，即使旧 benchmark 数字本身有效，它也不能直接证明：

- 新 AgentOS 的真实并行收益；
- D2 的资源调度收益；
- Kernel Lease 与 VPG 已形成完整闭环；
- multi-agent execution 的端到端收益。

建议尽快：

1. 明确标注 legacy runtime；
2. 所有公开 benchmark 输出 runtime implementation ID；
3. 新实验统一经过正式 SDK/Kernel 主路径；
4. 最终删除或收敛两套重复状态机。

---

## 9. README 与开源发布检查

本节保留审查开始时发现的问题，并标注当前处理状态；它不是对当前 README
的重复判定。

### 9.1 原始公开指标问题（已收敛）

审查开始时，GitHub README 曾存在以下不可复现声明：

- advanced benchmark 命令尚未真正存在；
- README 链接的两篇 advanced benchmark 文档不存在；
- README 宣称 2,521 tests，但当前收集约为 2,482；
- 一个未经协议和原始结果支持的 wall-clock/token headline。

当时必须二选一：

1. 提交对应 benchmark 实现、命令、文档、raw artifacts 和复现实验；
2. 暂时撤下这些 headline claims。

本轮选择撤下无法复现的 headline，并将当前公开说法收敛为：

> 在 deterministic synthetic task-DAG benchmark 中，LongHorizonOS 相对 full restart 减少约 48.64% 的模型化加权工作量；该指标不是实际 wall-clock、token 或 API cost 节省。

还需要明确：

- 相对 oracle-informed task-DAG checkpoint 的额外节省目前为 0%；
- 真正优势需要 Artifact/Evidence 级细粒度 provenance 来证明。

### 9.2 发行渠道剩余项

本轮已完成：

- `pyproject.toml` project URLs 与 keywords；
- `0.1.0` 版本名和“实验版本”状态统一；
- release checklist；
- 可执行并经过 smoke 验证的 Quickstart。

仓库外仍需要：

- 发布 PyPI package；
- GitHub Release/tag；
- Repository description；
- Topics；
- Social preview；
- branch protection。

### 9.3 Quickstart 闭环（已完成）

审查开始时，`:memory:` 示例和 `save_run()`/CLI 的持久化流程不一致。
当前 README 已同时提供零配置恢复 Demo、可持久化 SDK 示例、CLI
观测命令和 benchmark 命令，并通过 fresh-wheel smoke。

当前持久化主路径为：

```text
创建持久 DB
→ 注册 Agent/Goal
→ run
→ save
→ lhos status
→ 修改 Artifact
→ lhos repair
→ 查看 reclosure
```

仍可继续改进的非阻断 UX：

- 单 Goal 自动推断；
- `lhos goals`；
- 友好的无 manifest/无 goal 错误提示。

---

## 10. 实施路线图

## Phase 0：停止扩功能，修复执行闭环

建议时间：立即开始。

### 目标

让一条最小主路径满足：

```text
Graph Ready
→ Scheduler Admission
→ Resource Reservation
→ Real Agent Execution
→ Artifact
→ Independent Verification
→ Evidence
→ Verified Closure
```

### 必做事项

- [x] 修复 claim-all / dispatch-limit 泄漏；
- [ ] 增加面向 Scheduler 的显式 `WAITING_RESOURCE`；
- [x] resource acquire 成功之前不得进入 `ADMITTED`；
- [x] dispatch 前二次检查 Lease；Kernel Action terminal commit 已实现 durable
  token fencing，主 SDK Lease-to-VPG Evidence commit 也已在 writer transaction
  内 fenced，但 driver sink 与完整 VPG/Claim cross-plane commit 仍未完成；
- [x] 接通 `Agent.executor`；
- [x] Executor 与 Verifier 分离，并保留旧 API 兼容；
- [x] Evidence 子图 attachment 单 patch 原子化；
- [x] Lease loss / stale epoch 后可重新调度；
- [x] scheduler state 持久化和 replay（单 Scheduler writer；不含 leader
  election、分布式 CAS 或 multi-writer fencing）；
- [ ] verifier/tool 调用接入 capability enforcement。

### 验收测试

- [x] 10 个 Ready Task、`max_dispatches=8`，第二次运行能够完成剩余 2 个；
- [x] 资源被占用时 Action 绝不执行；
- [x] Agent executor 调用次数与成功 dispatch 数一致；
- [x] `run_async()` 两个独立 0.2 秒任务总耗时明显低于 0.4 秒（checked-in
  async AgentOS benchmark 已通过）；同步 `run()` 保持串行语义；
- [x] Lease 过期后 Task 可被另一 Agent 接管；
- [x] 旧 Lease owner 不能提交 Kernel Action 终态，主 SDK 路径也拒绝 late
  Evidence；driver 外部 side effect 与完整 VPG/Claim cross-plane fencing
  仍需后续测试；
- [x] Evidence Graph patch 中途故障不会留下半提交图状态；
- [ ] 无 capability 的 Agent 无法调用 shell/workspace。

---

## Phase 1：扩展为 Host/Cluster Resource-Aware Runtime

### 必做事项

- [x] `ResourceRequest` / `ResourceVector` typed model（当前为每 Agent
  逻辑容量）；
- [x] Agent capacity/capability matching model；
- [x] atomic multi-resource bundle reservation（逻辑资源）；
- [x] bounded same-process executor concurrency；
- [x] async execution（公开 `AgentOS.run_async()`）；
- [x] completion/verification cleanup path；
- [x] cancellation cleanup；
- [ ] shared host/device inventory and physical enforcement；
- [ ] cross-process/cluster worker delivery and queue；
- [ ] task-level timeout/preemption；
- [ ] Lease heartbeat；
- [ ] backoff 和 retry budget；
- [ ] fairness/aging；
- [ ] wait-for graph 可视化；
- [ ] resource utilization telemetry。

### 第一批 Scheduler baseline

- FIFO；
- Random；
- Work stealing；
- Critical-path-first；
- HEFT；
- Cost-aware；
- Locality-aware；
- Semantic-reclosure-aware。

---

## Phase 2：做开源用户真正会使用的 Demo

最强 Demo 应一次展示：

1. 多个 Agent 接入真实 Executor；
2. Graph 找出两个独立任务并行执行；
3. 两个任务竞争 GPU、Sandbox 或 workspace write lock；
4. 资源不足的任务进入 `WAITING_RESOURCE`；
5. 一个 Agent crash，Lease 自动回收；
6. 一个 Artifact 更新；
7. 旧 semantic epoch 的结果提交被拒绝；
8. 只重跑 causal cone；
9. 新 Evidence 产生；
10. Goal verified reclosure。

建议提供：

- 10–15 秒 GIF；
- 一条安装命令；
- 一条 Demo 命令；
- before/after Graph；
- JSON audit log；
- 资源时间线；
- baseline 对比。

README 首屏问题可以写成：

> Your agent completed 80 steps. Then a requirement, file, API, or external fact changed. Which results are still valid, and what is the minimum work needed to recover?

---

## Phase 3：真实收益验证

当前最重要的不是继续增加测试数量，而是证明真实收益。

### Baselines

- Full restart；
- State-only resume；
- Oracle task-DAG checkpoint；
- LangGraph/Microsoft Agent Framework；
- Temporal；
- Ray；
- Temporal + Ray；
- LongHorizon-Harness；
- LongHorizonOS 不同模块的 ablation。

### Workloads

- 长程 coding task；
- 多文件 repository repair；
- research/data analysis；
- 浏览器和外部 API；
- 需求中途变化；
- 文件/API/model/tool version 变化；
- 多 Agent 共享 workspace；
- GPU/LLM quota 竞争；
- worker crash；
- non-idempotent side effects。

### 指标

语义正确性：

- false-verified count；
- false closure rate；
- under-invalidation；
- over-invalidation；
- stale-result commit rate；
- repair frontier precision/recall；
- successful reclosure rate。

系统效率：

- time-to-verified-reclosure；
- wall-clock makespan；
- token/API cost；
- tool CPU time；
- GPU utilization；
- queue wait；
- context compilation cost；
- wasted stale work；
- lease conflicts；
- crash recovery latency；
- deadlock rate；
- starvation/fairness。

---

## Phase 4：论文机制与形式化

推荐论文问题：

> 在 dependency、semantic validity、capability、resource capacity、Lease 和 repair constraints 下，如何最小化 Time/Cost to Verified Reclosure，同时保证 ownership safety、semantic safety、deadlock freedom 和 liveness？

### 建议优化目标

\[
\min \mathbb{E}[T_{\text{verified reclosure}}]
+ \lambda C_{\text{token/API}}
+ \mu W_{\text{stale}}
+ \nu P_{\text{fairness}}
\]

满足：

\[
\sum_{i \in S} d_{i,r} \le C_r
\]

以及：

- dependency/current-evidence constraints；
- placement/locality constraints；
- exclusive resource constraints；
- capability constraints；
- Lease/fencing constraints；
- semantic epoch constraints；
- fairness constraints。

### 可以形式化的安全性与活性性质

#### Closure soundness

```text
CLOSED
⇒ 每个 obligation 都存在对当前 exact version 有效的 PASS Evidence
```

#### Precise invalidation

```text
依赖完整时：
causal cone 内不能保留 VERIFIED
causal cone 外保持有效
```

#### Frontier minimality

```text
Repair Frontier 是 stale subgraph 中依赖已经满足的最小立即执行集合
```

#### Ownership safety

```text
任一 task/resource 同时最多一个有效 exclusive owner
```

#### Fenced semantic commit

```text
旧 epoch 或旧 fencing token 的执行结果不能改变当前语义状态
```

#### Resource safety

```text
任何时刻都不超过资源容量，且无完整资源包不得执行
```

#### Deadlock freedom

在 atomic bundle admission 和禁止 hold-and-wait 的条件下，排除相应资源循环等待。

#### Liveness

在输入稳定、资源最终可用、调度公平、Lease 有界、Verifier 最终成功等条件下，Goal 最终重新闭合。

---

## 11. 推荐的论文与项目定位

### 项目定位

> **LongHorizonOS does more than choose the next agent step: after the world changes, it preserves progress still justified by exact-version Evidence and schedules the graph-derived Repair Frontier to safely reclose the Goal.**

### 论文标题方向

- `LongHorizonOS: An Evidence-Backed Semantic Control Plane for Stateful Agents`
- `LongHorizonOS: Safe Incremental Recovery for Stateful Agent Workflows`
- `LongHorizonOS: Resource-Aware Verified Reclosure for Long-Horizon Agents`

### 不应使用的 claim

- 第一个 Agent OS；
- 第一个有 Graph 的 Agent framework；
- 第一个有 checkpoint/resume；
- 第一个有 CPU/GPU scheduling；
- 已经实现完整 deadlock-free runtime；
- 已证明真实 78% wall-clock/token savings。

### 更稳固的 claim

> Existing systems separately provide workflow state, durable execution, resource scheduling, or incremental recomputation. LongHorizonOS connects semantic validity with operational scheduling, allowing agents to preserve still-valid progress and safely advance the graph-derived Repair Frontier to verified reclosure.

---

## 12. 现在应该怎么做

如果目标是先成为好用的开源项目，再形成顶会论文，推荐严格按以下顺序推进。

### 第一优先级：把主路径做真

暂时不要继续增加新模块。先确保：

```text
Agent 真执行
资源真申请
任务真并行
Verifier 真独立
Evidence 真原子
Kernel Action 终态真 fencing
 主 SDK Lease-to-VPG 真 fenced；VPG/Claim cross-plane 与 driver side effect
 真 fenced
VPG history 真可验证恢复
Checkpoint/restore 边界真诚可复现
```

这是开源可信度和论文可信度共同的地基。

### 第二优先级：做一个不可替代的 Demo

Demo 不应只是展示 Graph，而应该展示：

```text
普通 checkpoint：
恢复了，但错误复用了 stale result

LongHorizonOS：
识别 Artifact 变化
→ 精确失效
→ 资源感知并行 repair
→ 拒绝 late stale result
→ Verified reclosure
```

### 第三优先级：发布一个诚实可复现的 0.1.0

- 修复 README claims；
- PyPI 发布；
- GitHub Release；
- Quickstart 一次跑通；
- 提供 raw benchmark artifacts；
- 明确当前 limitations；
- 不用测试数量代替真实收益。

### 第四优先级：收集真实工作负载数据

先验证用户是否真正需要：

- 修改需求后的局部修复；
- Agent crash 后的安全恢复；
- 多 Agent 共享 workspace；
- LLM/API/GPU quota 调度；
- Evidence 审计和可解释失效。

### 第五优先级：再收敛论文贡献

论文最好只有两条核心贡献：

1. **Evidence/provenance-backed semantic recovery**；
2. **Semantic-reclosure-aware resource scheduling**。

其他功能作为系统支撑，不要把论文写成功能清单。

---

## 13. 最终判断

LongHorizonOS 已经从普通 Graph runtime 演化成具有：

- VPG；
- Evidence；
- causal invalidation；
- repair frontier；
- Kernel Lease；
- multi-agent matching；
- deadlock detection；
- recovery；
- Artifact/Context runtime；

的系统原型，工程进步明显。

但当前最关键的问题不是继续扩展模块，而是将已经存在的 Kernel、VPG、Scheduler 和 SDK 真正接成一条一致的执行路径。

项目最值得坚持的核心不是：

> “我也实现了 Graph、checkpoint 和 scheduler。”

而是：

> **LongHorizonOS uses verified semantic state to decide what remains valid and what becomes runnable, while explicit ownership and capacity controls drive the Goal back to verified closure.**

只要先修复执行闭环，再用真实任务证明 `verified reclosure` 的正确性和收益，这个项目既具备开源传播潜力，也存在形成系统论文的合理空间。
