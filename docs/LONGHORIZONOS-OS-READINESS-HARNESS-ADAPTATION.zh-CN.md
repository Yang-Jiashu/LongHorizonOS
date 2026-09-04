# LongHorizonOS OS 就绪度、Harness 适配与加速边界

> 审查对象：`C:\Users\yangjiashu\Downloads\LongHorizonOS-main\LongHorizonOS-main`  
> 审查日期：2026-08-19  
> 相关总报告：`docs/LONGHORIZONOS-ALGORITHM-OS-HARNESS-EVALUATION-AUDIT.zh-CN.md`

---

## 1. 直接结论

当前 LongHorizonOS 已经具备：

```text
Semantic Progress Graph
+ versioned Evidence
+ READY/VERIFIED/STALE
+ causal invalidation
+ repair frontier
+ Claim/Attempt
+ Kernel Lease/fencing
+ logical resource admission
+ bounded interrupt/preemption primitives
```

因此它已经是：

> **一个语义计算控制面与执行权限运行时原型。**

但它还不是完整的：

> **持续在线、事件驱动、跨 Harness、联合管理 Task/Agent/Context/模型/资源的 Agent Operating System。**

最关键的缺口不在于再增加一个 policy，而在于把已有 policy、VPG、Scheduler、Kernel、Context 和 Harness control 接成一个权威闭环。

---

## 2. 距离 OS 还有多远

下面的百分比是工程成熟度估计，不是代码覆盖率。判断依据是：

1. 机制是否存在；
2. 是否进入默认主路径；
3. 是否有端到端、同资源、真实 Harness 和故障恢复证据。

| 能力层 | 当前估计 | 现状 |
|---|---:|---|
| Semantic Progress Graph、Evidence、版本有效性 | 80–85% | 当前最强部分 |
| 选择性失效传播与 task-level repair | 75–85% | D3 cone/frontier 基本可用 |
| Claim/Attempt/Lease/fencing | 65–75% | 主 SDK commit path 较强 |
| cognition/context/resource 状态观测 | 45–55% | DTO 和投影存在，但不是原子全局状态 |
| critical path/conflict/resource/budget policy | 50–60% | 原语较多，联合度不足 |
| 默认路径的 online co-scheduler | 25–35% | batch barrier、placement 断裂、controller 非自治 |
| Harness 统一执行 substrate | 20–30% | opt-in bridge，默认 executor 仍绕过 |
| Context paging/residency/真实 eviction | 20–30% | 选择模型有，真实内存管理不足 |
| 物理资源、quota、公平性、分布式 | 0–15% | 目前主要是逻辑资源和单机原型 |

### 2.1 按不同定义看

如果论文声称：

> 单机、显式/mediated provenance、版本化 Evidence 的语义修复和 fenced execution runtime。

当前约有 **60–70%** 的基础。

如果论文声称：

> 本文完整描述的持续自治、联合调度、跨 Harness LongHorizonOS。

当前约有 **35–40%**。

如果声称：

> 任意真实 Harness 的十小时任务普遍压缩到三小时。

当前实现和证据不足 **20%**，不能使用这个 claim。

---

## 3. 当前已经像 OS 的地方

## 3.1 语义内核

当前 VPG 已经表达：

```text
Goal
Task
Artifact
Verification
Evidence
DEPENDS_ON
PRODUCES
VERIFIES
GraphVersion
ArtifactVersionBinding
READY
VERIFIED
STALE
Goal closure
```

主要代码：

- `src/lhos/runtimes/verified_progress/models.py`
- `src/lhos/runtimes/verified_progress/graph_store.py`
- `src/lhos/runtimes/verified_progress/verification.py`
- `src/lhos/runtimes/verified_progress/readiness.py`
- `src/lhos/runtimes/verified_progress/sdk.py`

这使系统可以区分：

```text
OperationalComplete
  Harness/executor 返回了结果

Verified
  当前 ArtifactVersion 上存在有效 Evidence

Stale
  曾经 Verified，但版本/依赖已变化

Committed
  在有效 Claim/Lease/epoch/read-set fence 下发布
```

这部分已经超过了简单的 task list 或 session transcript。

## 3.2 语义失效和选择性修复

当前闭环：

```text
Artifact version change
-> Evidence applicability loss
-> STALE cone
-> Goal reopen
-> minimal Repair Frontier
-> fresh Attempt
-> new Evidence
-> Goal reclosure
```

主要代码：

- `src/lhos/runtimes/invalidation/cone.py`
- `src/lhos/runtimes/invalidation/frontier.py`
- `src/lhos/runtimes/invalidation/engine.py`
- `src/lhos/sdk/os.py::repair`
- `src/lhos/sdk/watchers.py`

现有 benchmark 已经支持：

- 相对 full restart 节省约 48.64% weighted work；
- under/over invalidation 为 0；
- false VERIFIED 为 0；
- 与 task-DAG oracle checkpoint 持平。

## 3.3 执行所有权

当前主路径的身份链是：

```text
Process
-> Agent
-> Task
-> Attempt
-> Claim
-> Kernel Lease
-> fencing token
```

主要代码：

- `src/lhos/runtimes/multi_agent/claims.py`
- `src/lhos/runtimes/multi_agent/attempts.py`
- `src/lhos/runtimes/multi_agent/scheduler.py`
- `src/lhos/agent_os/services/lease_service.py`
- `src/lhos/sdk/os.py`

主 SDK Evidence path 已经能拒绝一部分：

- stale worker；
- stale epoch；
- superseded Lease；
- invalid read set；
- 不匹配 Claim/Attempt；
- late completion。

---

## 4. 还不像完整 OS 的地方

## 4.1 没有统一执行 substrate

默认 `AgentOS.run()`/`run_async()` 仍直接调用 `Agent.executor` 和 provider registry。

代码入口：

- `src/lhos/sdk/os.py::_SDKExecutorDispatcher`
- `src/lhos/sdk/os.py::_invoke_executor`
- `src/lhos/sdk/os.py::_invoke_executor_async`

Harness registry/control 是显式 opt-in：

- `src/lhos/sdk/os.py::register_harness`
- `src/lhos/sdk/os.py::control_harness`

因此当前真实结构是：

```text
默认路径:
  Scheduler/Kernel -> Agent.executor -> verifier -> VPG

可选路径:
  Scheduler Claim -> register Harness -> bounded control
```

不是：

```text
所有执行 -> HarnessAdapter -> OS lifecycle
```

## 4.2 没有 continuous work-conserving scheduler

当前 `run_async` 大致是：

```text
plan batch
-> start WorkerPool
-> await entire batch
-> replan
```

代码：

- `src/lhos/sdk/os.py` 的 async execution loop；
- `src/lhos/runtimes/multi_agent/worker_pool.py::run`。

如果一个短任务完成并解锁关键路径，而同批另一个任务是长尾，空闲 slot 不能立即补位。

真正 OS-like 的行为应该是：

```text
completion/failure/stale/lease release/observation
-> update semantic state
-> replan
-> refill free capacity
```

## 4.3 policy assignment 不是执行合同

`UnifiedAdaptivePolicy` 和 `ResourceAwareParallelismPolicy` 可以产生：

```text
task -> pool/resource assignment
```

但真实 Scheduler 仍可能重新做 Agent best-fit。

因此当前 policy 的 placement 主要是：

```text
advisory/audit
```

而不是：

```text
authoritative placement admission
```

需要 `PlacementAdmissionContract`，至少绑定：

```text
graph_id
graph_version
projection_hash
policy_decision_hash
task_id
agent_id
pool_id
resource_vector
model/verifier tier
context generation
```

## 4.4 Context 还不是完整虚拟内存系统

当前已有：

- ContextManifest；
- ContextPage；
- WorkingSet；
- pin/unpin；
- snapshot；
- token/byte budget；
- lifecycle selection。

但仍缺：

- 真实 eviction；
- close/cleanup 释放 resident bytes；
- residency lease；
- shared page/COW；
- durable content bytes；
- 跨 Attempt page reuse；
- provider KV cache control。

注意：当前一次 `load()` 已有 `_VersionContentCache`，所以“每个 selected page 都重复整文件读取”已不是准确的当前事实。剩余热点主要在：

- `restore_snapshot()`；
- 跨 load/Attempt 共享；
- range-read；
- hash `.hex()` 放大；
- snapshot page lookup；
- eviction/residency。

## 4.5 Checkpoint 还不是可执行进程恢复

Kernel checkpoint 会保存 PCB metadata，但 `restore` 主要查询记录并写事件，不会完整恢复：

- PCB；
- program state；
- wait condition；
- mailbox cursor；
- event cursor；
- old ownership。

Subprocess Harness 的 checkpoint 明确只是 progress/usage marker，不是 process memory checkpoint。

所以必须区分：

```text
event durability marker
logical session checkpoint
Context snapshot
workspace snapshot
executable process checkpoint
```

## 4.6 跨平面不是原子 GlobalEpoch

`GlobalRuntimeState` 会分别读取：

```text
VPG
Scheduler
Context
Resource
Facts
```

当前主要只对 Graph projection 做 optimistic double-read，不能保证所有 plane 来自同一时刻。

可能出现：

```text
Graph v10
Claims v9
Resources v11
Context v8
```

需要：

```text
RuntimeEpochToken {
  graph_version
  projection_hash
  scheduler_generation
  resource_generation
  context_generation
  facts_generation
}
```

policy 和 admission 必须绑定该 token；无法一致读取时重试或 fail closed。

---

## 5. 能直接适配 DeepSeek Harness 或 Claude Code 吗

## 5.1 粗粒度适配：现在可以

当前可以使用：

- `CallableHarnessAdapter`
- `SubprocessHarnessAdapter`

包装任意 command/session，做到：

```text
start
poll
collect final output
coarse terminate
final verifier
```

这足够做：

- 黑盒 baseline；
- 粗粒度并行；
- workspace 隔离；
- 超时/kill；
- final result verification。

但它不能提供：

- precise read/write set；
- mid-turn remaining work；
- session cognition state；
- tool-level fencing；
- precise rebase；
- portable process checkpoint；
- staged external side-effect commit。

## 5.2 DeepSeek Harness：推荐 native plugin/SDK adapter

DeepSeek Harness（`dsh`）官方定位是插件化 Agent Harness：模型、工具、Session log、Agent loop、sandbox、subagent、workflow 等均可组合为插件。它已有 session persistence、tool parallelism、subagent/workflow/Ralph 和 sandbox，但这些主要是 session/local runtime 能力。不要把“DSH 没有 loop/并发/恢复”作为 LongHorizonOS novelty。

推荐接入：

```text
LongHorizonOS controller
-> DSH plugin/SDK event seam
-> DSH Agent loop/tools/session
-> staged workspace/CAS
-> verifier
-> fenced publish
-> Evidence/VPG
```

需要把以下状态接出来：

```text
task_id
attempt_id
claim_id
lease_id
fencing_token
graph_version
artifact_versions
read/write observations
session/workspace identity
usage/progress
```

DSH 的 one-shot external provider 或只返回 final text 的接口不适合作为论文主路径，因为无法支持精细 interrupt/rebase/usage/diff。

## 5.3 Claude Code：优先 Agent SDK，其次 hooks/headless

Claude Code 已有 agent loop、subagents、Agent Teams、background sessions、checkpoint/rewind、hooks、permissions、sandbox、MCP 和 usage observability。LongHorizonOS 不应重新实现这些局部能力。citeturn0search1turn0search2turn0search3

推荐接入顺序：

1. **Claude Agent SDK**：程序化控制 session、tools、loop、context 和 usage；
2. **Hooks + staged worktree**：快速兼容原型；
3. **Headless CLI**：黑盒 baseline；
4. **MCP**：数据面，不作为唯一 authority。

Hooks 适合：

```text
SessionStart
PreToolUse
PostToolUse
SubagentStart/Stop
TaskCompleted
FileChanged
```

但 hooks 不是完整内核边界；timeout、async hook 和未经过 tool hook 的输入路径都可能削弱控制。

## 5.4 适配就绪度

| 适配层 | 当前能否做 | 可支持的 claim |
|---|---|---|
| 黑盒 subprocess | 可以 | 粗粒度启动、等待、kill、最终验证 |
| Hooks/MCP/ACP | 可以做原型 | lifecycle observation、基础控制 |
| Native DSH plugin | 尚无现成实现 | 需要新增 adapter |
| Claude Agent SDK | 尚无现成实现 | 需要新增 adapter |
| 全面 staged/fenced commit | 尚未完成 | 论文主闭环的必要条件 |
| 跨 Harness 统一 policy | 尚未完成 | 论文 generality claim 的必要条件 |

所以答案是：

> **可以适配，但不是直接 plug-and-play；现在能做黑盒适配，距离论文级 native adapter 还差一个明确的事件、身份、读写集和提交协议。**

---

## 6. OS 怎么体现

OS 不是目录名，也不是在 Python 外面加一层 wrapper。它至少要在以下八个方面体现。

### 6.1 Authority boundary

每个执行必须有：

```text
Process
-> Attempt
-> Claim
-> Lease
-> fencing token
```

所有权威写入前都检查：

```text
graph version
semantic epoch
Claim
Lease
read set
Evidence
```

### 6.2 Scheduler

OS 必须真正决定：

```text
Task
Agent/Harness
pool/provider
resource vector
parallel degree
continue/defer/preempt/rebase
```

而不是只产生排序建议。

### 6.3 Semantic memory

OS 必须管理：

```text
ArtifactVersion
Evidence
Context pages
read/write sets
residency generations
stale status
repair frontier
```

### 6.4 Execution lifecycle

需要拥有或强制管理：

```text
spawn
attach
heartbeat
checkpoint
interrupt
rebase
terminate
reap
orphan recovery
replacement
close
```

### 6.5 Persistent state and recovery

必须能恢复或 fail closed 处理：

```text
effect happened but ACK lost
Harness control succeeded but journal missing
Lease expiry
worker crash
late completion
stale checkpoint
duplicate watcher event
```

### 6.6 Resource accounting

至少有：

- token/time/dollar budget；
- model/provider quota；
- CPU/RAM/GPU/model slot logical admission；
- reservation/release；
- fairness/starvation；
- preemption payoff。

当前 logical pool 还不是 physical enforcement。

### 6.7 Observability and invariants

每个 OS 决策应能解释：

```text
为什么选这个 Task？
为什么没有选另一个？
为什么放这个 Agent？
为什么等待？
为什么抢占？
哪一个 Evidence 使 Goal closure 成立？
```

并可检查：

```text
false VERIFIED = 0
stale commit = 0
duplicate Claim = 0
over-capacity = 0
replay divergence = 0
```

### 6.8 Measurable acceleration

OS 的最终证据不是“Agent 看起来更忙”，而是：

```text
same final VERIFIED Goal
less doomed/repeated compute
less repair work
lower Time-to-Verified-Goal
lower token/time/$ cost
```

必须同时展示 stable/null workload 的控制面负开销。

---

## 7. 距离真正 LongHorizonOS 的最短路线

推荐顺序：

```text
P0. PlacementAdmissionContract
    让 policy assignment 变成实际 Scheduler admission

P1. Event-driven refill
    消除 run_async batch barrier

P2. Weighted critical path + unified planner
    联合 critical path/conflict/resource/budget/risk

P3. Incremental semantic coherence
    ArtifactVersion 变化只更新 affected cone

P4. Native Harness adapter
    先 DSH plugin/SDK，或 Claude Agent SDK

P5. Staged fenced commit
    isolated workspace -> verifier -> lease/version check -> publish

P6. Context residency lease + real eviction

P7. Durable control intent/ack

P8. Semantic OCC 或 churn-aware checkpoint
```

第一个可执行里程碑：

```text
PlacementAdmissionContract
+ event-driven completion/refill
+ weighted critical-path score
+ current greedy safety guards
```

先证明两件事：

1. policy 计划和实际执行 placement 一致；
2. batch barrier 确实是长尾 workload 的主要损失。

---

## 8. 论文应如何定位

不建议：

> LongHorizonOS is an operating system for running multiple Claude Code or DeepSeek agents.

建议：

> **LongHorizonOS is a harness-agnostic semantic control plane that minimizes invalid computation in long-running, dynamically changing agent workflows through evidence-backed validity, incremental invalidation, fenced execution, and verified-progress-aware scheduling.**

中文：

> **LongHorizonOS 是运行在 DeepSeek Harness、Claude Code 等 Agent Harness 之上的语义一致性与资源控制平面；它通过版本化 Evidence、增量失效传播、Claim/Lease fencing、跨 Harness placement 和事件驱动调度，减少动态长时程任务中的 doomed computation 和不必要返工。**

这比“另一个 Agent Harness”更准确，也能避开已有 Harness 的重叠能力。

---

## 9. 最终判断

### 当前已经实现

```text
Semantic Progress Kernel
Versioned Evidence
Selective repair
Claim/Lease/fencing
Logical resource admission
部分 interrupt/preemption
```

### 当前尚未闭合

```text
Always-on co-scheduler
Work-conserving refill
Authoritative placement
统一 Harness substrate
Cross-plane atomic epoch
Real Context eviction/residency
Executable checkpoint/restore
Durable control intent/ack
Physical resource enforcement
```

最终一句话：

> **LongHorizonOS 已经有“语义内核”，但还缺“持续在线、联合决策、可强制执行的 compute scheduler/controller”。**
