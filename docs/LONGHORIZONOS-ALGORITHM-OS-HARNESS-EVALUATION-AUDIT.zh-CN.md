# LongHorizonOS 数据结构、操作系统算法、Harness 边界与测评设计审查报告

> 审查日期：2026-08-18  
> 审查对象：`C:\Users\yangjiashu\Downloads\LongHorizonOS-main\LongHorizonOS-main`  
> 审查方式：源码、文档、测试与已有 benchmark artifacts 的只读审查；DeepSeek Harness 与 Claude Code 部分只使用各自公开官方资料  
> 状态标签：文中明确区分“当前已实现/已验证基础”“已发现但未接入的机制”“建议实现”“论文目标”；任何未在当前主执行路径闭合的能力不得视为当前保证  
> 修改说明：原审查阶段未修改仓库，也未运行会写入缓存、SQLite 或 artifacts 的测试/benchmark；本报告文件是根据后续明确请求新增并扩展的文档。

---

## 目录

1. [执行摘要](#1-执行摘要)
2. [审查范围与代码结构](#2-审查范围与代码结构)
3. [宏观架构判断](#3-宏观架构判断)
4. [数据结构与经典算法优化](#4-数据结构与经典算法优化)
5. [操作系统机制与实现问题](#5-操作系统机制与实现问题)
6. [可以形成创新的操作系统算法](#6-可以形成创新的操作系统算法)
   - [第6章 Claude Code 实施卡](#69-第6章-claude-code-实施卡)
7. [Harness 与 LongHorizonOS 的边界](#7-harness-与-longhorizonos-的边界)
8. [能否定义为基于 Harness 的 Long-Horizon OS](#8-能否定义为基于-harness-的-long-horizon-os)
9. [测评设计](#9-测评设计)
10. [对现有 benchmark 的评价](#10-对现有-benchmark-的评价)
11. [建议实施优先级](#11-建议实施优先级)
12. [最终研究定位](#12-最终研究定位)
13. [全仓库第二轮微观到宏观审查](#13-全仓库第二轮微观到宏观审查)
14. [顶会论文问题定义](#14-顶会论文问题定义)
15. [DeepSeek Harness 对比](#15-deepseek-harness-对比)
16. [Claude Code 对比](#16-claude-code-对比)
17. [外置套接能否加速](#17-外置套接能否加速)
18. [距离真正 LongHorizonOS 还差什么](#18-距离真正-longhorizonos-还差什么)
19. [论文实验矩阵与决策门](#19-论文实验矩阵与决策门)
20. [建议论文摘要骨架](#20-建议论文摘要骨架)
21. [核心理念实现度评估](#21-核心理念实现度评估)
22. [Threats to Validity 与假设边界](#22-threats-to-validity-与假设边界)

---

# 1. 执行摘要

## 1.1 总体结论

LongHorizonOS 已经具备一组可以合理视为“Agent 操作系统机制”的组件：

- 基于 VPG 的语义有效性、READY、STALE 与 Goal closure；
- 基于 Claim、Attempt、Kernel Lease 和 fencing token 的执行所有权；
- 多 Agent eligibility、matching 与逻辑资源准入；
- Artifact/Namespace、Context VM、Journal、Signal、Checkpoint、Outbox；
- 版本感知的失效传播与最小 Repair Frontier；
- 可选的 Harness session 控制、协作式 interrupt、rebase 和子进程终止边界。

但是，当前版本更准确的定义仍然是：

> **面向长时程 Agent 的单机状态化语义计算控制面与执行权限运行时原型。**

不宜直接宣传为：

- 完整的通用 Agent 操作系统；
- 默认以 Harness 为执行基底的操作系统；
- 物理 CPU/GPU/RAM/VRAM 调度器；
- 分布式多主 Scheduler；
- 支持透明进程 checkpoint/migration 的系统；
- 能对任意 Python、浏览器、网络或工具副作用提供 exactly-once 的系统。

## 1.2 最大的统一性能问题

当前最主要的可扩展性瓶颈不是某个局部 Python 循环，而是：

> **局部状态变化经常触发全图、全日志或全 projection 的重新读取、复制、推导、序列化、校验和哈希。**

这一模式同时存在于：

- VPG commit；
- VPG derived-state refresh；
- Scheduler durable state；
- Provenance journal；
- Context materialization；
- legacy graph/runtime；
- 部分 benchmark 和状态投影。

最值得统一优化的方向是：

```text
版本化增量状态
+ dirty set
+ 局部 work queue
+ 持久化倒排索引
+ authenticated incremental digest
+ 周期性全量 scrub
```

## 1.3 最值得形成论文级贡献的主线

建议将创新点收敛为：

> **Verified-Progress Semantic Co-Scheduler：一种基于已验证进度、输入失效风险、冲突、多维资源和 Context residency 的事件驱动在线联合调度、放置与抢占算法。**

它联合决定：

- 哪些 READY/repair Task 现在运行；
- 分配给哪个 Agent、pool、provider 或 Harness；
- 允许多少并行度；
- 哪些正在运行的 session 应继续、checkpoint、rebase 或 preempt；
- 哪些 Context page 应保留、迁移、加载或淘汰。

与普通 DAG scheduler 的关键区别是：

> Scheduler 不只知道“任务是否完成”，还知道“任务结果在当前语义版本下是否仍然可提交”。

---

# 2. 审查范围与代码结构

## 2.1 规模

`src/lhos` 下约有 294 个 Python 源文件，主要目录包括：

```text
src/lhos/
  agent_os/
  runtimes/
    verified_progress/
    multi_agent/
    invalidation/
  sdk/
  provenance/
  integrations/
  benchmarks/

  graph/             # legacy
  runtime/           # legacy
  infrastructure/    # 部分 legacy，部分仍被产品层使用
  agents/            # canonical spec 中整体被标作 legacy，但实际有混用
  verification/
  cli/
```

## 2.2 大型核心文件

当前多个关键模块已形成巨型编排文件：

- `src/lhos/sdk/os.py`：约 9,000 行；
- `src/lhos/runtimes/multi_agent/scheduler.py`：约 3,600 行；
- `src/lhos/runtimes/verified_progress/graph_store.py`：约 3,100 行。

这不只是代码风格问题。它使以下问题更容易发生：

- policy 已实现但没有接入主路径；
- policy 输出在跨层传递时丢失；
- placement 计划和实际 Scheduler placement 不一致；
- Harness 与 Claim/Lease 生命周期不能形成一个事务；
- 同一事实在多个 facade 中被重复扫描和重新投影。

## 2.3 Core 与 legacy 边界不一致

Canonical Core spec 将以下目录整体列为 legacy/out-of-scope：

```text
graph
runtime
agents
domain
ports
infrastructure
verification
benchmarks
cli
```

证据：

- `docs/architecture/LONGHORIZONOS-CORE-V1.md:106-110`

但其他架构文档和实际入口又将 SDK、CLI、benchmark 作为 Core 之上的产品层：

- `docs/architecture/README.md:3-5,83-91`
- `pyproject.toml:53-56`

建议尽快将术语统一：

- `Core`：Kernel、VPG、D2 Scheduler、D3 invalidation；
- `Product layer`：SDK、CLI、Harness adapter、benchmark；
- `Legacy`：旧 `graph/runtime` 路径；
- 不再把“Core 之外”与“legacy”当成同义词。

---

# 3. 宏观架构判断

## 3.1 两平面结构

```text
语义控制平面
  VPG
    - validity
    - READY
    - VERIFIED
    - STALE
    - Goal closure

  D3
    - Evidence applicability
    - causal invalidation cone
    - repair frontier

  Policy
    - critical path
    - downstream unlock
    - conflict
    - resource
    - budget
    - interrupt

执行平面
  D2 Scheduler
    - eligibility
    - matching
    - Claim
    - Attempt
    - logical resource admission

  Kernel
    - Process
    - Action
    - Lease
    - fencing
    - Journal

  Agent executor / Harness
    - 一次实际执行
    - model/tool/session loop
```

规范证据：

- `docs/architecture/LONGHORIZONOS-CORE-V1.md:20-29`
- `docs/CONCEPTS.md:9-18`
- `docs/ARCHITECTURE-PATHS.md:7-20`

## 3.2 权威边界

| 层 | 权威事实 | 不应决定 |
|---|---|---|
| VPG | validity、READY、VERIFIED、STALE、Goal closure | Agent placement、物理执行 |
| Scheduler | eligibility、matching、Claim、Attempt、逻辑资源准入 | 语义真值 |
| Kernel | Process/Action、Lease、fencing、Journal | Evidence 是否充分 |
| Harness/Agent/Tool | 一次 operational execution 与输出 | Goal 是否最终有效 |
| Host OS/Container | 真实进程、CPU、RAM、GPU、网络、文件系统隔离 | VPG 语义 |

对应文档：

- `README.zh-CN.md:307-330`
- `docs/architecture/LONGHORIZONOS-CORE-V1.md:76-79`

---

# 4. 数据结构与经典算法优化

## 4.1 P0：VPG 小 patch 仍走全图提交

### 当前路径

一次 patch 会执行：

1. 读取全部节点和边；
2. 复制完整 candidate projection；
3. 全图推导 validity、closure 和 READY；
4. 遍历全部节点进行 JSON diff；
5. 将完整 projection 传给 GraphStore；
6. 对所有节点和边排序并计算 projection hash；
7. 写 delta 后重新读取完整 materialized projection；
8. 再次计算完整 hash；
9. 重建完整内存 cache。

证据：

- `src/lhos/runtimes/verified_progress/sdk.py:154-158`
- `src/lhos/runtimes/verified_progress/sdk.py:210-275`
- `src/lhos/runtimes/verified_progress/patch_validator.py:345-354`
- `src/lhos/runtimes/verified_progress/graph_store.py:2330-2671`
- `src/lhos/runtimes/verified_progress/graph_store.py:2936-3058`
- `src/lhos/runtimes/verified_progress/graph_store.py:3209-3239`

### 复杂度

即使只改变一个节点，单次也至少为：

```text
O(V + E)
```

连续提交 N 个单节点 patch 时，累计至少接近：

```text
O(N²)
```

仓库现有 artifact：

`artifacts/benchmark_results/vpg-audit-current-800-20260814.json`

记录了：

| Patch 数 | 平均 commit | 总时长 |
|---:|---:|---:|
| 100 | 约 8.22 ms | 约 0.82 s |
| 800 | 约 55.53 ms | 约 44.42 s |

800 是 100 的 8 倍，但总时长增长约 54 倍。

### 建议：Incremental Authenticated VPG

持久化以下索引：

```text
depends_out
depends_in
produces_out
verifications_by_task
evidence_by_verification
artifact_pins_by_task
goals_by_direct_task
unresolved_dependency_count
```

每个 patch 生成：

```text
dirty_node_ids
dirty_edge_ids
changed_fact_ids
affected_task_ids
```

只对 affected 区域执行 work queue。

projection hash 从完整排序 JSON hash 改为：

- keyed Merkle B-tree；
- authenticated persistent map；
- 或 SQLite page/subtree hash。

目标复杂度：

```text
O((|delta| + |affected|) log(V+E))
```

### 风险

当前 projection hash 是恢复和篡改检测契约。建议：

1. 引入 `projection-hash.v2`；
2. 一段时间双写旧 hash 与 Merkle root；
3. 使用 differential oracle 证明 projection、READY、event 顺序一致；
4. 周期性完整 scrub；
5. 不使用可抵消的 XOR hash。

---

## 4.2 P0：VPG derived state 使用全图 fixed point

目前只对 verification/produces 建了部分索引：

- `src/lhos/runtimes/verified_progress/sdk.py:494-500`

但 Task-local invalidation、dependency loss、artifact pins 和 fresh evidence 仍反复扫描全部边：

- `src/lhos/runtimes/verified_progress/sdk.py:522-564`
- `src/lhos/runtimes/verified_progress/sdk.py:567-647`
- `src/lhos/runtimes/verified_progress/sdk.py:720-810`

链式或逆序图中最坏可接近：

```text
O(V²E)
```

### 建议

改为确定性增量 work queue：

```text
Artifact/Evidence change
  -> seed tasks
  -> reverse dependency queue
  -> update unresolved count
  -> re-evaluate direct consumers
  -> update goal direct-dependency count
  -> update READY heap/set
```

确定性队列顺序建议：

```text
(topological_level, node_id)
```

---

## 4.3 P0：Durable Scheduler 每次写入重新验证完整历史和状态

### 当前行为

每次 append：

- 完整读取 Scheduler event history；
- JSON decode；
- Pydantic validate；
- 从 genesis 开始验证 hash chain；
- 完整反序列化 projection；
- 完整校验 Claim/Attempt invariants；
- 重新序列化所有 Claims、Attempts、Match log 和 idempotency keys；
- 重算完整 projection hash。

证据：

- `src/lhos/runtimes/multi_agent/durable_state.py:391-436`
- `src/lhos/runtimes/multi_agent/durable_state.py:583-667`
- `src/lhos/runtimes/multi_agent/durable_state.py:702-704`
- `src/lhos/runtimes/multi_agent/durable_state.py:772-824`
- `src/lhos/runtimes/multi_agent/durable_state.py:831-1021`

累计容易成为：

```text
O(events² + state²)
```

### 建议

维护：

```text
claim_by_id
active_claim_by_(graph, task)
claims_by_agent
attempt_by_id
latest_attempt_by_claim
attempt_count_by_(graph, task, semantic_epoch)
```

持久化 API 应返回 dirty set，而不是每次接收全状态。

日志改成：

- segmented append-only log；
- segment hash；
- tail hash；
- Merkle Mountain Range 或 authenticated root；
- 周期性完整 scrub；
- batch/group commit。

同一逻辑 Claim acquisition 应只 publish 一次，避免 Claim、Attempt、idempotency 分别触发全状态写。

---

## 4.4 P0：Context VM 对同一 ArtifactVersion 重复整文件读取

读取链：

1. `_resolve_and_verify_ref()` 读取完整内容；
2. pager 再次读取完整内容；
3. materialize 对每个 selected page 再次读取完整内容并切片；
4. omitted ref 还可能重新分页估算。

证据：

- `src/lhos/agent_os/context/service.py:168-204`
- `src/lhos/agent_os/context/pager.py:68-164`
- `src/lhos/agent_os/context/service.py:207-281`

若 Artifact 大小为 B，切成 P 页，复杂度接近：

```text
O(PB)
```

### 建议

引入：

```text
VerifiedArtifactBuffer {
  artifact_id
  version
  canonical_uri
  content_hash
  bytes/memoryview/mmap
}
```

hash、paging 和 materialize 共享同一 buffer。

进一步支持：

- `read_range()`；
- 按 `(artifact_id, version)` 分组批量读取；
- content-addressed shared ContextPage；
- refcount/COW；
- 流式 materialized hash，避免 `.hex()` 的约 2 倍内容扩张。

---

## 4.5 P1：D3 为每个 affected task 单独 BFS

Cone 本身是一轮 BFS：

- `src/lhos/runtimes/invalidation/cone.py:157-183`

但 proof 构造对每个 affected task 重新 BFS：

- `src/lhos/runtimes/invalidation/cone.py:211-263`

复杂度接近：

```text
O(A(V+E)) + path copying
```

### 建议

一次 multi-source BFS 记录：

```text
nearest_root
predecessor
distance
lexical_rank
```

每个 proof 只沿 predecessor 重建路径：

```text
O(V+E+proof_output_size)
```

---

## 4.6 P1：ConflictGraph 查询退化为线性扫描

构建阶段已有 resource inverted index：

- `src/lhos/sdk/conflict_graph.py:545-586`

但查询阶段：

- `access_for()` 扫描 access sets；
- `conflicts_with()` 扫描 conflict pairs；
- 每个 candidate 对每个 selected task 重复扫描。

证据：

- `src/lhos/sdk/conflict_graph.py:184-206`
- `src/lhos/sdk/conflict_graph.py:275-383`

### 建议

派生不进入公开序列化/hash 的索引：

```text
access_by_task
conflict_neighbors
resource_readers
resource_writers
```

大 frontier 使用 bitset。

---

## 4.7 P1：Conflict、resource、budget 仍是固定顺序 greedy

当前相当于：

- greedy maximal independent set；
- first-fit multidimensional packing；
- ratio-sort 后逐项装入的 multidimensional knapsack。

代码：

- `src/lhos/sdk/conflict_graph.py:275-383`
- `src/lhos/sdk/resource_policy.py:356-464,671-682`
- `src/lhos/sdk/compute_budget.py:458-509,681-727`

### 典型反例

```text
一个 lexical-first 中心 Task 与 k 个叶子冲突
叶子之间互不冲突
```

当前算法可能只选中心一个，而最优解可选 k 个叶子。

### 建议

把 frontier 选择正式定义为：

```text
Maximum-Weight Independent Set
+ multidimensional knapsack
+ generalized assignment
```

双层实现：

- 小 frontier：bitset branch-and-bound、ILP、CP-SAT；
- 大 frontier：确定性 weighted greedy + local search；
- benchmark 使用 ILP 作为 oracle。

---

## 4.8 P1：AtomicResourceManager 每次查询扫描全部 reservations

`used/available/shortages/can_reserve` 最终都遍历 reservation：

- `src/lhos/runtimes/multi_agent/resources.py:60-125`
- `src/lhos/runtimes/multi_agent/resources.py:155-233`

Scheduler 对每个 task、每个 Agent 都查询 shortages：

- `src/lhos/runtimes/multi_agent/scheduler.py:3677-3715`

### 建议

维护：

```text
_used_by_pool
_reservation_ids_by_pool
_owner_index
```

reserve/release 在同一锁内增减 aggregate。

---

## 4.9 P1：AttemptManager 和 ClaimManager 索引不足

当前多个查询仍扫描全部 Attempt/Claim：

- `src/lhos/runtimes/multi_agent/attempts.py:35-53`
- `src/lhos/runtimes/multi_agent/attempts.py:292-307`
- `src/lhos/runtimes/multi_agent/claims.py:50-62`

建议维护：

```text
attempt_ids_by_task
attempt_ids_by_agent
latest_attempt_by_task
latest_attempt_by_claim
attempt_count_by_epoch
active_claim_by_task
active_claim_ids_by_agent
```

---

## 4.10 P1：Provenance append 累计 O(N²)

InMemory：

- 每次重建 idempotency index；
- 每次验证完整 chain。

JSONL：

- 每次 append 前重读完整文件；
- 完整 Pydantic decode；
- 完整 chain 验证；
- 写入后又验证完整内存链。

证据：

- `src/lhos/provenance/store.py:93-149`
- `src/lhos/provenance/store.py:205-290`

### 建议

- tail metadata；
- event-id/idempotency/graph/task/attempt 倒排索引；
- segmented JSONL；
- sparse sequence offset；
- cross-process file lock；
- group commit；
- 周期 scrub。

---

## 4.11 P2：Artifact、watch、mount、signal 和 SQL 全扫描/N+1

问题：

- Artifact 变化扫描所有 active watches：  
  `src/lhos/agent_os/artifacts/service.py:721-740`
- namespace usage 对每个 Artifact 单独查询 versions：  
  `src/lhos/agent_os/artifacts/service.py:749-775`
- mount longest prefix 每次线性扫描：  
  `src/lhos/agent_os/artifacts/service.py:882-899`
- signal delivery 先取全部 signal，再逐条查询 PCB：  
  `src/lhos/agent_os/services/signal_service.py:67-96`

建议：

- watch/mount：radix trie；
- quota：单条 aggregate SQL；
- signal：与 blocked processes 做 join；
- 推荐索引：

```text
processes(state, priority, created_at)
leases(expires_at)
signals(consumed, created_at)
signals(target_pid, consumed, created_at)
artifacts(namespace_id, deleted, canonical_uri)
handles(pid) WHERE closed_at IS NULL
transactions(artifact_id, pid, idempotency_key)
transactions(pid, state)
```

---

## 4.12 Legacy CostAwareScheduler 存在指数级风险

旧实现：

- 每个 READY node 重建 remaining DAG；
- `longest_path_from()` 无 memo；
- 分层 diamond DAG 会重复求相同子问题；
- `ProgressGraph.in_edges/out_edges` 每次扫描全部 edge list。

证据：

- `src/lhos/graph/critical_path.py:18-54`
- `src/lhos/graph/queries.py:30-99`
- `src/lhos/runtime/cost_aware_scheduler.py:80-139`

建议：

- adjacency indexes；
- 一次逆拓扑 DP 计算全部 weighted longest path；
- descendant bitset；
- `max(key=...)`，不为只取第一名做全排序。

但该路径属于 legacy，应首先决定保留、迁移还是冻结。

---

# 5. 操作系统机制与实现问题

## 5.1 `run_async` 是 batch barrier，不是 work-conserving

当前模式：

```text
规划 batch
  -> 创建 WorkerPool
  -> await 整批 jobs 全部完成
  -> 下一轮才重新观察和规划
```

证据：

- `src/lhos/sdk/os.py:3240-3255`
- `src/lhos/sdk/os.py:3913-3935`
- `src/lhos/sdk/os.py:4017-4024`

### 后果

若一个短任务先完成并解锁关键路径，而同 batch 有一个长尾任务：

- 已释放的 slot 无法立即填充；
- 新关键路径 Task 要等整个 batch；
- 动态 DAG 和增量 READY 的价值被 barrier 抵消。

### 建议

实现事件驱动连续调度：

```text
任一 Task 完成/失败/失效
或资源释放
或新 observation 到达
  -> 局部更新 VPG
  -> 更新 READY/repair frontier
  -> 立即填充空 slot
```

---

## 5.2 Graph utility、ConflictGraph、resource policy 没有联合

普通 `adaptive=True` 会自动构造 ConflictGraph：

- `src/lhos/sdk/os.py:2154-2198`
- `src/lhos/sdk/os.py:2654-2657`
- `src/lhos/sdk/os.py:3221-3224`

但 `_plan_adaptive_epoch()`：

- `conflict_graph is None` 才走 graph utility；
- 有 conflict graph 时走 DynamicParallelismPolicy。

- `src/lhos/sdk/os.py:2233-2275`

ConflictGraph policy 只按：

```text
repair
access-known
task_id lexical
```

- `src/lhos/sdk/conflict_graph.py:275-287`

Resource policy 也采用类似 lexical order：

- `src/lhos/sdk/resource_policy.py:289-300`

因此当前多个机制“分别存在”，但没有真正联合。

---

## 5.3 当前 critical path 没有按执行重量计算

`graph_analysis.py` 的 critical path 主要按未完成 Task 节点数最长：

- `src/lhos/sdk/graph_analysis.py:200-268`

真实 makespan 应使用：

- 预计 wall time；
- token/model latency；
- verifier latency；
- success probability；
- tail variance；
- context reload；
- resource queue delay。

需要 weighted critical path，不能只数节点。

---

## 5.4 Resource placement 计划没有绑定真实 Scheduler 执行

Resource/Unified policy 会输出：

```text
task -> pool assignment
```

证据：

- `src/lhos/sdk/resource_policy.py:356-464`
- `src/lhos/sdk/unified_policy.py:424-459`

但真实执行只向 Scheduler 传：

- selected task ids；
- dispatch order。

证据：

- `src/lhos/sdk/os.py:3332-3341`
- `src/lhos/sdk/os.py:3393-3401`
- `src/lhos/runtimes/multi_agent/sdk.py:264-275`
- `src/lhos/runtimes/multi_agent/scheduler.py:1232-1284`

### 后果

多 pool 下：

- policy 证明的 packing 与实际 placement 可能不同；
- Scheduler 可能重新选择 Agent/pool；
- 产生额外拒绝、碎片化或次优放置。

### 建议

引入：

```text
PlacementAdmissionContract {
  graph_id
  graph_version
  policy_decision_hash
  task_id
  agent_id
  pool_id
  resource_vector
}
```

Scheduler 必须原子验证并执行 assignment，或者拒绝整个 batch。

---

## 5.5 Kernel-loop adaptive parallelism 可能退化为串行

`drive_goal_to_closure()` 调用 adaptive parallelism 时没有传 task resources：

- `src/lhos/sdk/kernel_loop.py:244-270`

而 policy 在 task resources 缺失时明确将：

```text
resource_headroom = 1
```

- `src/lhos/sdk/parallelism_policy.py:268-294`

chosen degree 为 0 时，入口又使用：

```python
max(1, chosen_degree)
```

- `src/lhos/sdk/kernel_loop.py:192-196`

这与“无稳定可运行工作时 degree=0”的语义不一致。

---

## 5.6 Context eviction 不是实际内存管理

详见 [4.4](#44-p0context-vm-对同一-artifactversion-重复整文件读取) 和 [4.5](#45-p1d3-为每个-affected-task-单独-bfs) 前后的 Context 分析。

核心结论：

- 当前能够做 deterministic selection；
- 能记录 pin/unpin 和 snapshot；
- 但没有真实从 resident state 释放 page bytes；
- 还不是完整 page replacement system。

---

## 5.7 Context residency 是“历史读过”，不是真正驻留

Scheduler 将所有 durable AgentSnapshot 的 read set 做单调并集：

- `src/lhos/runtimes/multi_agent/scheduler.py:1569-1610`

该集合没有在以下事件发生时删除：

- Context handle close；
- eviction；
- process cleanup；
- rebase；
- memory pressure；
- Context generation 更新。

Context close/cleanup 也保留 `_HandleRec.loaded`：

- `src/lhos/agent_os/context/service.py:825-888`

因此 locality bonus 实际更接近：

> 这个 Agent 历史上读过该资源。

而不是：

> 这个资源当前仍驻留在该 Agent 的 Context/KV cache 中。

建议将 residency 建模为带 generation、TTL、bytes 和 refcount 的 lease。

---

## 5.8 Kernel checkpoint restore 目前只是事件记录

Checkpoint 创建会存储 PCB snapshot：

- `src/lhos/agent_os/kernel/dispatcher.py:389-441`

但 restore 只：

- 查询 checkpoint；
- 写 `CHECKPOINT_RESTORED` event。

没有真正恢复：

- PCB projection；
- program state；
- wait condition；
- event cursor；
- mailbox cursor。

证据：

- `src/lhos/agent_os/kernel/dispatcher.py:445-468`

Subprocess Harness checkpoint 也只是 progress/usage marker：

- `src/lhos/sdk/subprocess_harness.py:561-574`

因此不能宣传为透明 process migration 或 executable checkpoint。

---

## 5.9 Kernel tick 是同步串行轮询

每个 tick 依次：

- reclaim lease；
- deliver signals；
- recover incomplete actions；
- detect deadlocks；
- await driver dispatch；
- await process step。

证据：

- `src/lhos/agent_os/kernel/kernel.py:106-133`
- `src/lhos/agent_os/kernel/kernel.py:497-559`
- `src/lhos/agent_os/kernel/kernel.py:999-1052`

任意慢 driver/process 都会阻塞：

- lease reclaim；
- signal delivery；
- 其他 action；
- 其他进程；
- deadlock handling。

### 建议

改成事件驱动 microkernel：

```text
ready process heap
per-device action queue
per-PID signal mailbox
lease expiry min-heap/timer wheel
driver future completion queue
budgeted maintenance classes
```

---

## 5.10 `effective_priority` 存在但未用于调度

字段存在：

- `src/lhos/agent_os/kernel/models.py`
- `src/lhos/agent_os/services/process_service.py`

但 Kernel FIFO 和 ready SQL 都使用 `priority`：

- `src/lhos/agent_os/kernel/kernel.py:45-49`
- `src/lhos/agent_os/services/process_service.py:157-160`

这为 priority inheritance 留出了直接接口。

---

## 5.11 Deadlock 检测算法可以改为 SCC

当前每个 tick：

- 从数据库重建 wait-for graph；
- recursive DFS；
- `stack.index()`；
- 邻接使用未排序 set；
- 尝试枚举 cycle。

证据：

- `src/lhos/agent_os/services/lease_service.py:603-667`

对于 deadlock recovery，通常不需要枚举所有 simple cycle，只需找到：

```text
包含环的 strongly connected components
```

建议：

- Tarjan/Kosaraju SCC；
- 或 wait-edge 插入时增量 cycle detection；
- 只在 wait graph 变化时运行；
- 每个 SCC 选择一个 victim。

---

## 5.12 Worker limiter 有 head-of-line blocking

`_UnitLimiter` 是严格 FIFO：

- `src/lhos/runtimes/multi_agent/worker_pool.py:465-536`

如果队首需要 4 units，而当前只有 3：

- 后面只需要 1 unit 的任务也不能运行。

它有利于公平和避免 partial-acquisition deadlock，但不是 work-conserving。

可选算法：

- bounded bypass；
- aging；
- deficit round-robin；
- size-aware queue；
- gang reservation。

---

# 6. 可以形成创新的操作系统算法

## 6.1 首选：Verified-Progress Semantic Co-Scheduler

> **当前状态校正：** 下面的 `WHO/WHERE/CONTEXT` 是论文目标，不是当前默认执行路径已经执行的联合决策。当前 `compute_routing.py`、`resource_policy.py` 和部分 unified policy 主要产生 advisory/audit；`src/lhos/sdk/os.py:2373-2387,2436-2516` 明确 compute routing 不改变 batch、Agent、provider/model、verifier 或 Context VM。要把本节算法变成真实系统，必须先完成 `PlacementAdmissionContract` 和 event-driven refill。

### 问题定义

每次 semantic/runtime event 到来后，联合决定：

```text
WHAT:
  run / reuse / repair / verify / gather evidence

WHEN:
  now / defer / checkpoint / rebase / preempt

WHO:
  agent / model / verifier / harness

WHERE:
  logical pool / provider / process / device

CONTEXT:
  keep / load / migrate / evict
```

### 目标函数

一个可审计的目标可以写成：

```text
maximize Σ_i x_i [
    P(success_i)
  × P(inputs remain valid_i)
  × (critical_path_value_i + unlock_value_i)
  - λ1 × token_cost_i
  - λ2 × wall_time_i
  - λ3 × dollar_cost_i
  - λ4 × expected_rework_i
  - λ5 × context_reload_i
  - λ6 × checkpoint/preemption_overhead_i
]
```

约束：

```text
VPG readiness
ConflictGraph
多维 resource vector
budget
Claim/Lease fencing
Harness capabilities
provider RPM/TPM
fairness/starvation
side-effect class
```

### 算法结构

小 frontier：

- ILP；
- CP-SAT；
- bitset branch-and-bound。

大 frontier：

- online primal-dual；
- Lagrangian relaxation；
- MWIS approximation；
- generalized assignment heuristic；
- local search。

### 系统要求

- work-conserving；
- 任一完成/失效后立即重新规划；
- placement contract 被 Scheduler 原子消费；
- decision hash 和 safety proof 可持久化；
- policy 失败时 fail closed。

---

## 6.2 Adaptive Semantic OCC

> **当前状态校正：** read recorder、undeclared-read report、`AccessCorrection` 和 commit-time validation 已存在，但 `correct_access_set(s)` 当前主要由自身函数/测试调用，尚未形成“Attempt 完成 -> 持久化 correction -> 下一 epoch 重建 ConflictGraph”的默认闭环。当前 `preempt_superseded=True` 也是 opt-in、批次内的受限路径，不是通用 OCC。

当前冲突控制主要依赖显式 read/write 声明，属于悲观策略。

系统已经具备乐观执行所需的大部分组件：

- read recorder；
- undeclared-read report；
- access correction；
- commit-time read validation；
- invalidation cone；
- repair frontier；
- superseded peer preemption。

README 也提出了这一交叉点：

- `README.zh-CN.md:1448-1467`

### 决策条件

若：

```text
预计并行收益
>
冲突概率 × (重做成本 + repair 成本 + 副作用风险)
+ read-set validation 成本
```

则允许 speculative execution；否则继续悲观串行。

### 创新点

OCC 本身不是新算法。可能的新贡献是：

> 将实际 read set、语义失效锥、预计剩余计算、Harness checkpoint/rebase/preempt 能力联合起来，在线选择悲观或乐观执行。

---

## 6.3 Incremental Semantic Coherence Engine

将以下链路统一为一个 delta transaction：

```text
ArtifactVersion change
  -> Evidence applicability delta
  -> Task validity delta
  -> Goal closure delta
  -> running Attempt interrupt
  -> Context delta
  -> repair frontier delta
  -> critical path delta
```

目标复杂度与 affected cone 成正比，而不是与全图成正比。

可同时整合：

- VPG incremental index；
- D3 multi-source proof tree；
- running Attempt read-set index；
- watcher observation batch；
- repair admission。

---

## 6.4 Verified-progress-aware Context Pager

> **当前状态校正：** `src/lhos/agent_os/context/service.py:69-105` 的 `_VersionContentCache` 已把一次 `load()` 内的 verification、分页和 materialization 共享到同一版本 bytes；因此不能再把“正常 load 每个 selected page 都整文件重读”作为当前事实。剩余问题集中在 `restore_snapshot()` 的重复读取（当前约 `:769-799`）、跨 load/Attempt 共享、range-read、`.hex()` hash 放大、真实 eviction 和 residency lease。

Context page value：

```text
future reuse probability × reload/token cost
+ KV-prefix preservation benefit
+ critical-path benefit
+ nonrecoverable benefit
- resident bytes cost
- staleness probability
```

联合决定：

- Task 放到已有 context 的 Agent；
- Context 迁移还是重建；
- 淘汰哪些页；
- 是否保持 prefix 稳定；
- 是否做 checkpoint/rebase。

需要引入：

- shared content-addressed pages；
- residency lease；
- generation；
- TTL；
- refcount；
- COW；
- multi-tier context/KV/disk cache。

---

## 6.5 Semantic Priority Inheritance

传统 priority inheritance 解决资源持有者阻塞高优先级任务的问题。

LongHorizonOS 可以扩展为：

```text
Goal closure priority
  -> critical-path Task
  -> 其依赖 Task
  -> 持有其所需 Lease/Context/model slot 的进程
```

动态传播 priority donation：

- 沿 VPG dependency；
- 沿 wait-for resource edge；
- 沿 Harness handoff dependency。

这样可以减少：

- semantic priority inversion；
- 低价值 holder 阻塞关键路径；
- resource release 延迟。

现有未使用的 `effective_priority` 可以作为实现入口。

---

## 6.6 Semantic Congestion Control

当前 parallelism backoff 主要按最近 rework task 数减少 degree：

- `src/lhos/sdk/parallelism_policy.py:296-312`
- `src/lhos/sdk/parallelism_policy.py:502-522`

可改成真正的闭环控制器，观测：

- queue latency；
- verified throughput；
- rework rate；
- preemption payoff；
- resource utilization；
- p95 task latency；
- stale commit rejection；
- verifier backlog。

算法候选：

- AIMD；
- PID；
- model-predictive control；
- contextual bandit。

Safety constraints必须独立于控制器，不能因为模型估计而放宽。

---

## 6.7 Failure/Churn-aware Checkpoint Placement

传统 checkpoint interval 可扩展为：

```text
checkpoint 当且仅当：

预计失败/失效后的重做损失
>
checkpoint 成本
+ restore 成本
+ consistency/fencing 成本
```

输入：

- Task remaining work；
- mutation/churn hazard；
- affected cone size；
- Harness checkpoint capability；
- context reload cost；
- side-effect class。

配合 CAS/Merkle workspace snapshot，可把全量 tar checkpoint 改为 dirty-chunk snapshot。

---

## 6.8 新颖性边界

不应单独声称以下机制是新的：

- critical-path scheduling；
- FIFO/work stealing；
- MVCC/OCC；
- Merkle tree；
- lease/fencing；
- LRU/LFU/ARC；
- DAG invalidation；
- checkpoint interval。

邻近工作已经包括：

- AIOS：LLM Agent 操作系统、Agent scheduling、context/memory/tool/access management，arXiv:2403.16971；
- MemGPT：将长上下文管理类比为操作系统虚拟内存，arXiv:2310.08560；
- 多 Agent LLM inference scheduling；
- Agent workflow semantic atomicity、recovery 与 rollback。

因此最有区别度的主张应是：

> **Evidence-backed semantic validity 直接驱动资源放置、Harness 生命周期和选择性重计算。**

## 6.9 第6章 Claude Code 实施卡

本节将 6.1–6.8 转换为可以直接交给 Claude Code 实施的工程卡。定位时优先使用**文件路径 + 类/函数名**，行号只表示 2026 年 8 月 19 日当前 checkout 的附近位置，后续代码变化时不得只依赖行号。

统一执行规则：

1. 每张卡先加 measurement/differential test，再切换主路径；
2. 新路径必须有 feature flag 或新 policy/schema id；
3. full/reference path 在迁移期保留，作为 correctness oracle；
4. safety gate 与 performance policy 分离；
5. 未知状态必须 `Unavailable`/fail closed，不能伪造为 0 或安全；
6. 每张卡都必须报告 stable/null workload 的负开销；
7. 不得在一张 PR 同时实现全部 6.1–6.7。

### 6.9.1 实施卡 S6-1：Verified-Progress Semantic Co-Scheduler

#### 当前基础

- `GlobalRuntimeState`：`src/lhos/sdk/runtime_state.py`；
- structural critical path/unlock：`src/lhos/sdk/graph_analysis.py`；
- frontier ranking：`src/lhos/sdk/frontier_policy.py`；
- conflict：`src/lhos/sdk/conflict_graph.py`；
- logical resource packing：`src/lhos/sdk/resource_policy.py`；
- compute budget：`src/lhos/sdk/compute_budget.py`；
- unified policy：`src/lhos/sdk/unified_policy.py`；
- semantic interrupt：`src/lhos/sdk/semantic_interrupt.py`；
- Scheduler/Claim/Lease：`src/lhos/runtimes/multi_agent/scheduler.py`；
- async execution：`src/lhos/runtimes/multi_agent/worker_pool.py`。

#### 具体问题

1. `AgentOS._plan_adaptive_epoch()`（`src/lhos/sdk/os.py`，搜索该符号）在 `conflict_graph is None` 时才使用 graph utility；正常 `adaptive=True` 会自动构造 ConflictGraph，因此通常进入 lexical conflict greedy。
2. `DynamicParallelismPolicy.suggest()` 当前主要按 repair、access-known、task id 排序，不包含 weighted critical path、budget、placement。
3. `UnifiedAdaptivePolicy` 虽输出 logical pool assignment，但 `run_async()` 最终主要将 selected task ids/order 交给 Scheduler。
4. Scheduler 会再次做 Agent best-fit，policy 的 assignment 不是执行合同。
5. `run_async()` 创建一批 WorkerPool 后等待整批完成，是 batch barrier，不是 work-conserving。
6. ComputeRouting 主要进入 RunResult audit，尚未决定真实 model/verifier/context/provider。
7. `GlobalRuntimeState` 不是 VPG/Scheduler/Context/Resource 的原子统一 epoch。

#### 最小复现

构造两个测试：

```text
Case A: barrier
  slots=2
  short task A=100ms, completion unlocks C
  straggler B=2s
  current: C waits for B
  target: C starts immediately after A

Case B: joint placement
  critical task lexical-last
  star conflict center lexical-first
  two heterogeneous pools
  current: lexical/first-fit chooses low-utility or wrong placement
  target: joint plan approaches oracle
```

#### 目标行为

在任一以下事件后立即重新规划和 refill：

```text
Task operational completion
verification/Evidence commit
failure/cancellation
Lease/resource release
Artifact/Graph mutation
semantic interrupt
Context generation change
provider quota update
```

一次决策必须同时输出：

```text
selected tasks
task -> Agent
task -> logical pool
task -> model/provider tier
task -> verifier tier
task -> Context plan
active Attempt -> continue/checkpoint/rebase/preempt
```

#### 硬不变量

- Task 必须在权威 READY/repair frontier；
- 一个 Task/semantic epoch 至多一个 authoritative active Claim；
- GraphVersion、projection hash 和 policy snapshot 必须匹配；
- selected batch 无声明冲突；
- resource/budget 不超限；
- unknown access/resource/capability fail closed；
- Scheduler 不得 silent rematch 到合同外 Agent/pool；
- late/stale completion 不得提交；
- safety gate 不读取 learned score 来决定是否放宽。

#### 新增 DTO/API

建议新增 `src/lhos/sdk/co_scheduler.py`：

```python
class RuntimeEpochToken(BaseModel):
    graph_id: str
    graph_version: int
    projection_hash: str
    scheduler_generation: int
    scheduler_event_tail: str
    context_generation: int
    resource_generation: int
    facts_generation: int

class TaskPlacementOption(BaseModel):
    task_id: str
    agent_id: str
    pool_id: str
    resources: ResourceVector
    model_tier: str
    verifier_tier: str
    context_plan_id: str | None
    access_set_hash: str
    harness_capabilities_hash: str
    utility_numerator: int
    utility_denominator: int

class PlacementAdmissionContract(BaseModel):
    epoch: RuntimeEpochToken
    policy_id: str
    decision_hash: str
    assignments: tuple[TaskPlacementOption, ...]

class CoScheduleDecision(BaseModel):
    contract: PlacementAdmissionContract
    deferred_task_ids: tuple[str, ...]
    active_attempt_actions: tuple[...]
    unavailable: tuple[UnavailableField, ...]
```

Scheduler API：

```python
SchedulerSession.run_pass(
    graph_id,
    placement_contract=contract,
    require_placement=True,
)
```

Worker API：

```python
AsyncWorkerPool.start(job) -> asyncio.Task[WorkerOutcome]
AsyncWorkerPool.completions() -> AsyncIterator[WorkerOutcome]
```

或新增 `run_stream(jobs)`，但必须支持增量 add jobs，不能只是一次性列表的 streaming wrapper。

#### 修改文件

第一阶段至少涉及：

- `src/lhos/sdk/co_scheduler.py`（新增）
- `src/lhos/sdk/unified_policy.py`
- `src/lhos/sdk/os.py`
- `src/lhos/sdk/runtime_state.py`
- `src/lhos/runtimes/multi_agent/sdk.py`
- `src/lhos/runtimes/multi_agent/scheduler.py`
- `src/lhos/runtimes/multi_agent/models.py`
- `src/lhos/runtimes/multi_agent/worker_pool.py`
- `src/lhos/sdk/online_epoch.py`

#### 分步实现

**P0：Placement contract**

1. 不改变算法，只把当前 `UnifiedAdaptivePlan.assignments` 转换成合同；
2. Scheduler 仅在合同指定 Agent/pool 上检查 eligibility/resource；
3. mismatch/stale contract 明确拒绝，不 fallback；
4. ScheduleResult 写 planned/actual assignment；
5. 增加 contract decision hash。

**P1：Event-driven refill**

1. 抽出当前单 Job executor/verify/commit 生命周期；
2. 用 `inflight_by_claim` 管理长期 WorkerPool；
3. `asyncio.wait(..., FIRST_COMPLETED)` 取任一 completion；
4. completion 先完成 verify/commit/release；
5. 重新 observe、plan、填满 free slots；
6. goal close 后停止 refill；
7. cancellation 对每个 exact Claim cleanup。

**P2：统一 heuristic**

1. 将 graph utility scorer 提取为公共函数；
2. 加入 weighted critical path；
3. conflict/resource/budget/risk 在同一 candidate loop 中处理；
4. 一个候选因资源/冲突失败后继续扫描 backfill；
5. model/verifier/context recommendation 写入 placement contract。

**P3：Oracle 与近似算法**

1. frontier `<=32` 使用 benchmark-only CP-SAT/ILP oracle；
2. runtime 使用 deterministic weighted greedy；
3. 加 1-swap/2-swap local improvement；
4. 报告 oracle gap，而不是声称全局最优。

**P4：在线校准**

只有 P0–P3 safety 稳定后，才接 success/stability/cost probability calibration。未知概率不得默认 1。

#### 测试

新增：

- `tests/sdk/test_co_scheduler_contract.py`
- `tests/sdk/test_event_driven_run.py`
- `tests/sdk/test_co_scheduler_oracle.py`
- `tests/runtimes/multi_agent/test_placement_admission.py`
- `tests/runtimes/multi_agent/test_continuous_refill.py`

必须覆盖：

- graph-version race；
- wrong agent/pool；
- partial resource admission；
- stale decision hash；
- long-tail unlock；
- failure refill；
- cancellation cleanup；
- same input insertion-order determinism；
- unknown access fail closed。

#### Benchmark

新增：

- `src/lhos/benchmarks/co_scheduler_oracle.py`
- `src/lhos/benchmarks/event_driven_refill.py`

复用并扩展：

- `scheduling_regimes.py`
- `unified_control.py`
- `provider_routing.py`

对比：

```text
serial FIFO
same-resource static parallel
current conflict greedy
current unified greedy
work-conserving current greedy
proposed joint heuristic
CP-SAT oracle
```

指标：

- Time-to-Verified-Goal；
- verified-progress AUC；
- critical-path stretch；
- eligible idle-slot fraction；
- policy p50/p95/p99；
- placement divergence；
- resource rejection/fragmentation；
- oracle gap；
- null-case overhead。

#### 验收门

- safety violations 全为 0；
- `require_placement=True` 时 actual assignment 100% 等于合同；
- 有 READY 且有安全容量时 eligible idle fraction 接近 0；
- long-tail unlock 场景显著优于 batch barrier；
- null case median overhead 预注册，例如不超过 5–10%；
- 同一 snapshot 决策 hash 完全一致；
- 任一 plane generation 变化导致合同拒绝。

#### 风险与依赖

优先依赖：

```text
PlacementAdmissionContract
-> event-driven refill
-> incremental coherence
-> joint optimizer
```

不能先引入 learned policy。跨平面 epoch 仍不原子时，合同必须带 generation 并由 Scheduler 重新校验。

---

### 6.9.2 实施卡 S6-2：Adaptive Semantic OCC

#### 当前基础

- 声明式 access sets：`src/lhos/sdk/conflict_graph.py`
- Goal inputs/outputs 到 ConflictGraph：`AgentOS._coerce_adaptive_conflict_graph`
- Python read recorder：`src/lhos/sdk/read_recorder.py`
- undeclared read diff：`src/lhos/sdk/undeclared_reads.py`
- widening correction：`src/lhos/sdk/access_correction.py`
- commit-time read guards：`src/lhos/sdk/os.py` 中 `_prepare_read_guards`、`_preflight_read_guards`、`_preflight_mediated_read_sets`
- stale cognition quarantine；
- D3 repair；
- `preempt_superseded`。

#### 具体问题

1. `AccessCorrection` 没有默认 runtime 调用点；
2. correction 不跨运行持久化，也不会自动进入下一 epoch ConflictGraph；
3. read recorder 仅覆盖部分 Python open 路径，不覆盖 `os.open`、mmap、C extension、grandchild 和任意网络；
4. 当前 pessimistic conflict policy 与 preemption path没有一个在线 mode selector；
5. speculative write 没有统一 staged workspace/CAS；
6. 任意外部副作用不能乐观并行后安全回滚；
7. 没有 conflict probability 与 wasted-work feedback store。

#### 最小复现

```text
Task R 声明不完整，实际读取 x
Task W 写 x
Epoch 1: 两者被错误并行，commit guard发现 stale
Epoch 2 target:
  correction 已持久化
  ConflictGraph 已 widening
  不再重复错误共调度
```

另构造 conflict probability 从 0 到 1 的 workload，画 pessimistic 与 optimistic 的交叉点。

#### 形式化输出

```python
class SemanticConcurrencyMode(StrEnum):
    PESSIMISTIC_SERIALIZE = "pessimistic_serialize"
    OPTIMISTIC_READONLY = "optimistic_readonly"
    OPTIMISTIC_STAGE = "optimistic_stage"

class SemanticOCCDecision(BaseModel):
    task_ids: tuple[str, ...]
    mode: SemanticConcurrencyMode
    graph_version: int
    snapshot_bindings: tuple[ArtifactVersionBinding, ...]
    stage_id: str | None
    estimated_parallel_gain_ms: int | None
    expected_conflict_loss_ms: int | None
    max_retries: int
    fallback_mode: SemanticConcurrencyMode
    decision_hash: str
```

决策条件：

```text
parallel time saved
>
p_conflict × (
    lost compute
  + repair cost
  + compensation risk
)
+ validation cost
+ staging cost
+ preemption cost
```

#### 硬不变量

- unknown/unobserved access 不得降低冲突风险；
- correction 只 widening，不 shrink；
- Task definition/access schema 变化必须开启新 correction epoch；
- `NON_REVERSIBLE/UNKNOWN` side effect 只能悲观执行，除非 sink 有明确 idempotency/fencing protocol；
- speculative output 只能写 stage，不得写权威 workspace；
- commit 时必须重新校验 read set、GraphVersion、Claim、Lease；
- guard 失败的结果不能进入 Evidence/VERIFIED。

#### 新增模块/API

新增：

- `src/lhos/sdk/semantic_occ.py`
- `src/lhos/sdk/access_observation_store.py`

DTO：

```python
class TaskAccessHistory(BaseModel):
    graph_id: str
    task_id: str
    task_definition_hash: str
    observed_read_union: tuple[str, ...]
    observed_write_union: tuple[str, ...]
    complete_samples: int
    partial_samples: int
    unknown_samples: int
    conflict_count: int
    attempt_count: int

class OCCValidationResult(BaseModel):
    stage_id: str
    committed: bool
    conflicting_resources: tuple[str, ...]
    victim_attempt_id: str | None
    lost_usage: UsageVector | None
```

#### 修改文件

- `src/lhos/sdk/access_correction.py`
- `src/lhos/sdk/undeclared_reads.py`
- `src/lhos/sdk/os.py`
- `src/lhos/sdk/conflict_graph.py`
- `src/lhos/sdk/semantic_occ.py`（新增）
- `src/lhos/sdk/access_observation_store.py`（新增）
- workspace/Artifact gateway；
- Scheduler event models。

#### 分步实现

**P0：闭合 correction**

1. Attempt terminal 后生成 UndeclaredReadReport；
2. 调 `correct_access_set()`；
3. 以 `(graph,task,definition_hash)` 持久化 widening set；
4. 记录 `ACCESS_SET_WIDENED`；
5. 下次 `_coerce_adaptive_conflict_graph()` 合并声明和 correction；
6. replay/reopen 恢复；
7. 此阶段仍全部 pessimistic。

**P1：Readonly OCC**

1. 仅对 PURE、read-only、fully observed subprocess 开启；
2. 绑定 snapshot/read guards；
3. guard 失败则丢弃 operational result并 repair；
4. 不允许权威写。

**P2：Staged write OCC**

1. 为 workspace/Artifact 写创建 `stage_id`；
2. stage 内输出不可见于权威 workspace；
3. verify 后校验 read set/fence；
4. 成功才 publish；
5. external API 仍禁止。

**P3：在线 selector**

1. 记录 conflict、lost usage、validation/staging overhead；
2. 估计 per-task/pair conflict risk；
3. 输出 pessimistic/optimistic；
4. unknown 始终悲观；
5. 加 fixed fallback 和 bounded retry。

#### 测试

新增：

- `tests/sdk/test_access_correction_runtime.py`
- `tests/sdk/test_access_observation_replay.py`
- `tests/sdk/test_semantic_occ.py`
- `tests/sdk/test_staged_occ_commit.py`

必须包含：

- 首轮发现、次轮 widening；
- process restart；
- task definition hash变化；
- mmap/C extension/grandchild 为 unknown；
- stage 成功/冲突/删除；
- graph race；
- stale Claim；
- irreversible effect拒绝。

#### Benchmark

参数：

- conflict probability；
- task duration；
- conflict timing；
- affected cone；
- recorder coverage；
- validation/staging/preempt cost；
- side-effect class。

Baseline：

```text
always pessimistic
always optimistic + restart
current declared ConflictGraph
proposed selector
clairvoyant trace oracle
```

指标：

- makespan；
- wasted compute/token；
- abort/repair；
- selector regret；
- unsafe effect count；
- crossover surface；
- stable overhead。

#### 验收门

- corrected set 单调；
- restart 后保留 correction；
- 第二 epoch 不重复已发现的错误共调度；
- stale speculative result 进入 VERIFIED 次数为 0；
- irreversible duplicate effect 为 0；
- selector 相对最佳固定 mode 在 phase-changing workload 上降低 regret。

#### 论文边界

可主张：

> 基于 actual access observation、semantic rework cost 和 Harness control 能力的自适应并发模式选择。

不可主张：

- 发明 OCC/MVCC；
- 完整 syscall provenance；
- 对 arbitrary Python/network effect sound。

---

### 6.9.3 实施卡 S6-3：Incremental Semantic Coherence Engine

#### 当前基础

- full VPG derived-state reference；
- partial verification indexes；
- D3 causal cone/frontier；
- GraphVersion CAS；
- Workspace observation batch；
- semantic interrupt；
- Context delta/rebase planner；
- Scheduler handoff/recovery witness。

#### 具体问题

当前一次局部变化仍分散经过：

```text
watcher/facts
-> VPG full snapshot/derived refresh
-> D3
-> Scheduler observe
-> interrupt proposal
-> Context rebase plan
-> Harness delivery/handoff
```

问题：

- 每个阶段可能重新扫全图；
- 没有共享 dirty set/affected index；
- D3 proof对每个 affected node重复 BFS；
- active Attempt read-set 没有 resource -> Attempt 倒排索引；
- VPG、Scheduler、Context、Harness 不共享一个 coherence intent；
- “跨平面原子”目前只能是 saga，而非 ACID。

#### 形式化输入输出

```python
class SemanticDelta(BaseModel):
    base_epoch: RuntimeEpochToken
    changed_node_ids: tuple[str, ...]
    changed_edge_ids: tuple[str, ...]
    artifact_changes: tuple[ObservationToken, ...]
    evidence_changes: tuple[str, ...]
    lease_changes: tuple[str, ...]
    process_changes: tuple[str, ...]

class CoherenceDeltaResult(BaseModel):
    node_validity_delta: tuple[...]
    goal_lifecycle_delta: tuple[...]
    ready_added: tuple[str, ...]
    ready_removed: tuple[str, ...]
    repair_frontier: tuple[str, ...]
    proof_roots: tuple[...]
    interrupt_intents: tuple[...]
    context_delta_intents: tuple[...]
    critical_path_delta: tuple[...]
    new_epoch: RuntimeEpochToken
    result_hash: str
```

目标：

```text
语义输出与 full recomputation 完全等价
工作量 O(|delta| + |affected cone| + |output|)
```

#### 硬不变量

- base epoch 不匹配则 abort/retry；
- 不将旧 delta merge 到新 GraphVersion；
- independent verified branch 不变化；
- READY、Goal closure、proof 和 Evidence predicate 与 full reference 一致；
- VPG 内部事务原子；
- 跨 plane 使用 durable intent/ack，不虚称 ACID；
- recovery 可确定哪些 intent 已 ACK、哪些 unknown。

#### 新增数据结构

新增 `src/lhos/runtimes/verified_progress/incremental_index.py`：

```python
class SemanticGraphIndex:
    depends_out
    dependents_in
    produces_by_task
    verifications_by_task
    evidence_by_verification
    artifact_consumers
    unresolved_dependency_count
    goal_unverified_count
    topo_level
    weighted_remaining_path
```

新增：

- `src/lhos/sdk/coherence_engine.py`
- `AttemptReadIndex`: resource identity -> live Attempt identities
- `CoherenceIntentStore` 或 Outbox event。

#### 修改文件

- `src/lhos/runtimes/verified_progress/sdk.py`
- `src/lhos/runtimes/verified_progress/patch_validator.py`
- `src/lhos/runtimes/verified_progress/graph_store.py`
- `src/lhos/runtimes/invalidation/cone.py`
- `src/lhos/runtimes/invalidation/engine.py`
- `src/lhos/sdk/watchers.py`
- `src/lhos/sdk/runtime_state.py`
- `src/lhos/sdk/graph_analysis.py`
- `src/lhos/runtimes/multi_agent/scheduler.py`
- `src/lhos/sdk/coherence_engine.py`（新增）

#### 分步实现

**P0：Differential harness**

1. 保留 `_recompute_derived_state_full`；
2. 随机 DAG/delta 同时运行 full/incremental；
3. 比较 nodes、events、READY、Goal、proof、hash；
4. 先不切主路径。

**P1：Dependency/READY counters**

1. patch validator 输出 dirty nodes/edges；
2. 构建 depends/reverse index；
3. 维护 unresolved dependency count；
4. 只对 affected dependents 入队；
5. 周期 full scrub。

**P2：Evidence/artifact/Goal/D3**

1. artifact -> Task consumer index；
2. verification/evidence index；
3. Goal direct-task counters；
4. multi-source BFS proof forest；
5. incremental repair frontier。

**P3：Active Attempt 与 Context**

1. Attempt bind snapshot 时注册 exact reads；
2. terminal/release 时删除；
3. observation change直接查询 live affected Attempts；
4. 生成 interrupt/context intents；
5. critical-path analyzer消费 validity delta。

**P4：Durable cross-plane saga**

```text
COHERENCE_INTENT
-> VPG commit
-> Scheduler ACK
-> Context ACK
-> Harness interrupt/handoff ACK
-> COHERENCE_COMPLETED
```

crash 后恢复未 ACK 项；任何 unknown fail closed。

#### 测试

新增：

- `tests/runtimes/verified_progress/test_incremental_coherence.py`
- `tests/runtimes/invalidation/test_proof_forest.py`
- `tests/sdk/test_coherence_intent_recovery.py`
- `tests/sdk/test_attempt_read_index.py`

覆盖：

- chain/fanout/diamond/multi-root；
- delta 乱序/重复；
- graph race；
- intent 任一步 crash；
- live Attempt lookup；
- unaffected preservation；
- full/incremental parity。

#### Benchmark

规模：

```text
N=100,1k,10k,100k
delta=1,10,1%
affected=0.01%,1%,10%,100%
```

指标：

- visited nodes/edges；
- SQL rows；
- commit/repair p50/p95/p99；
- CPU/RSS；
- intent/ACK latency；
- full scrub；
- event/hash parity。

#### 验收门

- 1000+ random seeds zero semantic difference；
- 小 cone 下访问量随 cone 而非全图增长；
- cone=100% 允许退化 full；
- crash recovery 无漏 interrupt、无错误 VERIFIED；
- periodic scrub 可发现 index/root drift。

#### 论文边界

可主张：

> single-host、event-sourced、affected-cone-sensitive semantic coherence。

不可主张：

- distributed cache coherence；
- 跨服务 ACID；
- 任意 hidden dependency。

---

### 6.9.4 实施卡 S6-4：Verified-progress-aware Context Pager

#### 当前基础

- ContextManifest/Page/WorkingSet/LoadedContext/Snapshot；
- priority-stable 与 lifecycle selection；
- pin/unpin；
- prefix-stability analysis；
- Context budget/delta/routing recommendation；
- 单次 load `_VersionContentCache`。

#### 具体问题

1. `_VersionContentCache` 只覆盖一次 `load()`，跨 Attempt/restore 不共享；
2. `restore_snapshot()` 对相同 Artifact 的多个 page binding 重复完整读取与 hash；
3. provider 没有通用 range-read capability；
4. `evict()` 只计算/记录 page IDs，不改变 LoadedContext/WorkingSet；
5. close/cleanup 不释放 LoadedContext bytes；
6. Scheduler locality 使用历史 read-set 单调并集；
7. 没有 page refcount、generation、TTL、last access；
8. Context routing是 advisory；
9. prefix stability 只报告，不控制真实 provider KV cache；
10. Facts 持久化 hash，默认不持久化内容 bytes。

#### 目标模型

Page identity：

```text
(artifact_id, version, content_hash, byte_start, byte_end, page_hash)
```

Resident state：

```text
tier
bytes/tokens
refcount
pin count
generation
last access
expires_at
reload cost
reuse probability
prefix position
recoverability
stale status
```

决策：

```text
keep/load/prefetch/migrate/evict/COW
Task -> Agent placement
```

替换目标：

```text
minimize sum(evicted page loss)
subject to freed capacity >= target
```

其中：

```text
loss =
  reuse_probability × reload_cost
+ prefix_invalidation_cost
+ critical_path_delay
+ nonrecoverable_penalty
- stale_risk_avoidance
```

#### 硬不变量

- required/pinned page 不可 evict；
- exact version/hash 才能 reuse；
- counters 等于 resident pages 总和；
- close/cleanup/refcount 不能负；
- page bytes 只有最后一个引用释放后回收；
- cache hit不能跳过 caller capability/freshness；
- stale generation 不获得 locality credit；
- KV cache 未接入时不得声称管理真实 KV。

#### 新增模块/API

新增 `src/lhos/agent_os/context/cache.py`：

```python
class PageKey(BaseModel): ...
class ResidentPage(BaseModel): ...
class ResidencyLease(BaseModel): ...
class SharedPageStore: ...
class PageReplacementDecision(BaseModel): ...
```

扩展 provider：

```python
read_version_range(..., start, end)  # optional
```

ContextService：

```python
evict(...) -> EvictionResult
page_fault(...)
acquire_residency(...)
release_residency(...)
snapshot_residency(...)
```

Context placement 必须进入 S6-1 的 PlacementAdmissionContract。

#### 修改文件

- `src/lhos/agent_os/context/service.py`
- `src/lhos/agent_os/context/models.py`
- `src/lhos/agent_os/context/policies.py`
- `src/lhos/agent_os/context/pager.py`
- `src/lhos/agent_os/context/cache.py`（新增）
- `src/lhos/runtimes/multi_agent/scheduler.py`
- `src/lhos/runtimes/multi_agent/matching.py`
- `src/lhos/sdk/runtime_state.py`
- `src/lhos/sdk/compute_routing.py`
- `src/lhos/sdk/providers.py`

#### 分步实现

**P0：真实 eviction/close**

1. 建 page table；
2. `evict()` 实际从 resident state 删除；
3. 更新 WorkingSet/LoadedContext counters/state；
4. required/pinned guard；
5. close/cleanup 释放引用；
6. 重复 eviction 幂等。

**P1：Verified content/page store**

1. `restore_snapshot()` 按 artifact/version/hash 分组，只读/验一次；
2. `PageKey` content-addressed；
3. 多 handle共享 immutable bytes；
4. refcount；
5. byte-budget LRU；
6. optional range-read/coalescing；
7. materialized hash v2 使用 length-prefixed streaming bytes，旧 snapshot双读。

**P2：Residency lease**

1. Context load/evict/close/rebase 发 residency events；
2. Scheduler locality只读取 live generation；
3. TTL/renew/replay；
4. historical reads 与 live residency 分离；
5. RuntimeState 暴露真实 bytes/pages。

**P3：Semantic replacement/placement**

1. 记录 page hit/miss/reload cost；
2. 估计 reuse；
3. critical-path value进入 replacement；
4. task placement与 context plan联合；
5. 先不控制真实 KV，仅记录 adapter prefix hit。

**P4：Multi-tier**

RAM、disk CAS、provider cache；支持 prefetch/migrate。

#### 测试

新增/修改：

- `tests/agent_os/context/test_eviction.py`
- `tests/agent_os/context/test_content_cache.py`
- `tests/agent_os/context/test_residency_lease.py`
- `tests/agent_os/context/test_snapshot_restore_io.py`
- `tests/runtimes/multi_agent/test_context_residency_dispatch.py`
- `tests/sdk/test_context_vm_integration.py`

必须断言：

- evict 后 page/counters 变化；
- 第二次 eviction为空；
- close释放；
- same version共享；
- different version不共享；
- restore 每 artifact full read最多一次；
- stale/expired residency locality=0；
- restart/replay一致。

#### Benchmark

```text
Artifact=1MB,10MB,100MB,1GB
page=4KB,32KB,256KB
selected=1%,10%,100%
context overlap=0–100%
mutation=0/低/高
```

Baseline：

```text
no cache
current selector
LRU
LFU
ARC
semantic pager
Belady offline oracle
```

指标：

- underlying bytes read；
- page/context hit；
- reload tokens；
- RSS；
- eviction latency；
- prefix hit（可观测时）；
- stale reuse；
- TTVG；
- metadata overhead。

#### 验收门

- eviction 后 accounting 精确；
- stale reuse=0；
- required/pinned violation=0；
- restore read量不随同 Artifact page 数线性放大；
- live locality precision 接近 100%；
- 相同 memory budget 下优于 LRU/当前策略；
- null trace 报告 metadata overhead。

#### 论文边界

可主张：

> version-pinned、verified-progress-aware page replacement/placement。

不可主张：

- 发明虚拟内存/LRU/ARC；
- 未接入时声称控制 Claude/DeepSeek provider 内部 KV；
- 物理 RAM enforcement。

---

### 6.9.5 实施卡 S6-5：Semantic Priority Inheritance

#### 当前基础与问题定位

- PCB 已有 `priority` 和 `effective_priority`：`src/lhos/agent_os/kernel/models.py::ProcessControlBlock`；
- spawn 时两者初始化相同：`src/lhos/agent_os/services/process_service.py::spawn`；
- Kernel `FIFOScheduler.select()` 仍按 `(priority, created_at)`；
- `ProcessService.list_ready()` SQL 仍按 `priority`；
- LeaseService 有 `lease_waiters`/wait-for graph，但只用于 deadlock；
- 全仓没有 donation/revoke API。

当前数值语义是**越小越优先**，因此 donation 应满足：

```text
effective_priority = min(base_priority, active_donations)
```

同时要先审计 `AgentKernel._select_victim()`：其注释称选择“低优先级” victim，但当前升序取最小 `priority`，可能选择最紧急进程。

#### 最小复现

```text
H priority=1 waits for resource X
L priority=10 owns X
M1..Mk priority=5 are unrelated

current:
  M runs before L, H is inverted

target:
  H donates priority 1 to L
  L releases X
  donation is revoked
```

再覆盖传递链 `H -> M -> L`。

#### 目标与硬不变量

- donation 可沿 resource wait、critical dependency、repair dependency、Harness handoff 传播；
- priority 只影响顺序，不绕过 READY/capability/resource/Claim/Lease；
- donation 绑定 graph/claim/lease/handoff identity 和 expiry；
- wait edge、GraphVersion、Claim 消失时撤销；
- 环中计算有界；
- 有 aging/max boost 防止 starvation；
- replay 后 effective priority 可重建。

#### 新增模块/API

新增 `src/lhos/agent_os/kernel/priority.py`：

```python
class DonationReason(StrEnum):
    RESOURCE_WAIT = "resource_wait"
    CRITICAL_DEPENDENCY = "critical_dependency"
    REPAIR_DEPENDENCY = "repair_dependency"
    HARNESS_HANDOFF = "harness_handoff"

class PriorityDonation(BaseModel):
    donation_id: str
    donor_id: str
    recipient_pid: str
    donated_priority: int
    reason: DonationReason
    graph_id: str | None
    graph_version: int | None
    claim_id: str | None
    lease_id: str | None
    expires_at: datetime | None

class PriorityInheritanceEngine:
    def add(self, donation: PriorityDonation) -> None: ...
    def revoke(self, donation_id: str) -> None: ...
    def recompute(self, affected_pids: Iterable[str]) -> tuple[...]: ...
```

`ProcessService.set_effective_priority(...)` 在同一事务更新 projection，并写：

```text
PROCESS_PRIORITY_DONATED
PROCESS_PRIORITY_REVOKED
PROCESS_EFFECTIVE_PRIORITY_CHANGED
```

数据结构：

```text
donations_by_donor
donations_by_recipient
recipient min-heap/multiset
wait graph adjacency/reverse adjacency
donation expiry heap
```

#### 分步实现

1. 锁定优先级数值方向；ready SQL/FIFO 使用 effective priority；无 donation 时保持旧行为；
2. 实现 Lease waiter -> holder inheritance，release/expiry/cancel 撤销；
3. 从 waiter/Lease projection 支持 restart rebuild；
4. Co-Scheduler 将 critical/repair urgency 映射到 Task/Attempt process donation；
5. 增加 handoff donation、TTL、aging 和 SCC 处理。

#### 修改文件

- `src/lhos/agent_os/kernel/priority.py`（新增）
- `src/lhos/agent_os/kernel/kernel.py`
- `src/lhos/agent_os/kernel/models.py`
- `src/lhos/agent_os/services/process_service.py`
- `src/lhos/agent_os/services/lease_service.py`
- `src/lhos/agent_os/storage/schema.py`
- `src/lhos/sdk/semantic_priority.py`（新增）
- `src/lhos/runtimes/multi_agent/scheduler.py`

#### 测试与 benchmark

新增：

- `tests/agent_os/test_priority_inheritance.py`
- `tests/agent_os/test_semantic_priority.py`

场景：

- 单级/多级 inversion；
- release/expiry/cancel；
- cycle；
- GraphVersion 变化；
- crash/restart；
- starvation。

指标：

- inversion duration；
- critical blocked time；
- TTVG；
- unrelated slowdown；
- donation latency；
- fairness。

#### 验收门

- waiter 出现后的下一调度点 holder 获得 boost；
- wait 消失后恢复；
- transitive donation 正确；
- 不绕过 admission；
- bounded workload 无 starvation；
- replay 一致；
- 无等待时原排序保持。

#### 论文边界

可主张 semantic urgency 跨 dependency/resource/handoff 的 donation；不可主张发明 priority inheritance 或 hard real-time guarantee。

---

### 6.9.6 实施卡 S6-6：Semantic Congestion Control

#### 当前基础与问题定位

`AdaptiveParallelismPolicy.decide()` 当前用：

```text
raw cap = min(ceiling, conflict headroom, resource headroom)
chosen degree = raw cap - recent distinct reworked tasks
```

代码入口：

- `src/lhos/sdk/parallelism_policy.py::AdaptiveParallelismPolicy.decide`
- `src/lhos/sdk/parallelism_policy.py::_contention_backoff`

缺口：

- 没有 increase law；
- rework 不是时间归一化 rate；
- 不区分高/低成本 rework；
- recent event DTO 不足以计算 queue/verifier/preempt 信号；
- 没有 EWMA、hysteresis、cooldown；
- 没有反馈延迟和稳定性分析；
- 没有 provider quota/verifier backlog；
- controller 尚未拥有长期 event-driven loop。

#### 控制模型

MVP 只控制：

```text
u_t = next global parallel degree
```

观测：

```text
verified progress/sec/token
ready queue depth
queue/admission latency
resource utilization
stale/rework ratio
verifier backlog
preemption net payoff
p95 task latency
provider throttling
```

Safety envelope：

```text
safe_cap = min(
  conflict cap,
  resource cap,
  provider quota,
  caller ceiling
)
```

控制器只能在 `[0, safe_cap]` 内行动。

#### MVP：Semantic AIMD

```text
healthy window + queued work:
  d = min(safe_cap, d + alpha)

semantic congestion:
  d = max(min_degree, floor(beta * d))

no admissible work:
  d = 0
```

加入 integer EWMA、连续窗口、cooldown、warm-up，保持确定性。

#### 新增模块/API

新增 `src/lhos/sdk/congestion_control.py`：

```python
class CongestionWindow(BaseModel):
    window_id: int
    duration_ms: int
    verified_progress: int
    verified_count: int
    stale_count: int
    rework_usage: UsageVector
    queue_wait_p95_ms: int | None
    verifier_backlog: int | None
    utilization_bp: int | None
    preemption_net_ms: int | None

class SafeParallelismEnvelope(BaseModel):
    ceiling: int
    conflict_cap: int
    resource_cap: int | None
    provider_cap: int | None
    safe_cap: int

class ParallelismControlDecision(BaseModel):
    previous_degree: int
    chosen_degree: int
    envelope: SafeParallelismEnvelope
    signals: tuple[str, ...]
    decision_hash: str

class SemanticAIMDController: ...
```

新增时间事件：

```text
TASK_ENQUEUED
ATTEMPT_STARTED
OPERATIONAL_COMPLETED
VERIFIER_STARTED/COMPLETED
EVIDENCE_COMMITTED
TASK_STALE
PREEMPT_REQUESTED/COMPLETED
```

#### 分步实现

1. **Telemetry/shadow**：只记录 controller would-choose，不影响执行；
2. **全局 AIMD**：Event-driven coordinator 每 K completion 更新 degree；
3. **稳定性**：EWMA、hysteresis、cooldown、delay-aware 窗口；
4. **对照算法**：PID/MPC/bandit 仅做实验，永不放宽 safety cap。

degree 下降时默认不杀已运行任务，只限制后续 refill；主动 preempt 交给 S6-1/SemanticInterrupt。

#### 修改文件

- `src/lhos/sdk/congestion_control.py`（新增）
- `src/lhos/sdk/parallelism_policy.py`
- `src/lhos/sdk/runtime_state.py`
- `src/lhos/sdk/event_supervisor.py`
- `src/lhos/sdk/os.py`
- `src/lhos/runtimes/multi_agent/worker_pool.py`
- telemetry/observability models。

#### 测试与 benchmark

新增：

- `tests/sdk/test_congestion_control.py`
- `tests/sdk/test_congestion_shadow_mode.py`
- `tests/sdk/test_dynamic_degree_execution.py`

Workload：

- stable；
- mutation burst；
- sustained churn；
- verifier bottleneck；
- provider quota shock；
- heavy tail；
- no work；
- missing telemetry。

Baseline：

```text
fixed degree 1/2/4/8/16
current rework backoff
Semantic AIMD
PID/MPC optional
offline trace oracle
```

指标：

- verified throughput；
- TTVG；
- rework；
- queue p95/p99；
- utilization；
- overshoot/settling；
- oscillation；
- overhead；
- safety cap violation。

#### 验收门

- degree 永不超过 safe cap；
- burst 后有界窗口内下降；
- congestion 消失后恢复；
- stable workload 不长期 serial；
- 同 trace 可重放；
- safety violations=0；
- null workload 报告 overhead。

#### 论文边界

可主张 verified-progress/rework/backlog 驱动的闭环并行度控制；不可主张发明 AIMD/PID，或没有物理 telemetry 时声称 GPU congestion control。

---

### 6.9.7 实施卡 S6-7：Failure/Churn-aware Checkpoint Placement

#### 当前基础与问题定位

- Kernel 有 `ProcessCheckpoint` 和 checkpoint table；
- ContextSnapshot 已有；
- Harness 有 CHECKPOINT vocabulary；
- legacy 有 filesystem/git checkpoint；
- Attempt 有 progress/cost/read set。

但：

- `SyscallDispatcher._handle_restore()` 只查询并写 `CHECKPOINT_RESTORED`，不恢复 PCB/program state；
- checkpoint 只存 `program_state_ref`；
- mailbox cursor 固定 0；
- SubprocessHarness checkpoint 明确只是 marker；
- ContextSnapshot 不是 cognition/process checkpoint；
- filesystem checkpoint 每次全 hash + tar；
- 没有 cost/hazard measurement；
- checkpoint 不统一绑定 Graph/read/Claim/Lease；
- restore 后没有强制 fresh revalidation。

#### 决策模型

```text
checkpoint iff

P(failure) × lost work
+ P(input invalidation) × recoverable work
+ expected context reload
+ effect recovery risk
>
checkpoint write
+ consistency barrier
+ restore
+ future revalidation
```

动作：

```text
NOOP
MARKER
CONTEXT_SNAPSHOT
WORKSPACE_SNAPSHOT
HARNESS_SESSION_CHECKPOINT
EXECUTABLE_PROCESS_CHECKPOINT
PREEMPT_AFTER_CHECKPOINT
```

#### 硬不变量

- scope 明确，marker 不冒充 process checkpoint；
- descriptor 绑定 graph/epoch/read set/Claim/Lease；
- restore 后重新校验；
- old checkpoint 不能直接生成 VERIFIED；
- irreversible effect 必须 known/uncertain；
- descriptor immutable/hash-bound；
- partial snapshot 不可见。

#### 新增 DTO/API

新增 `src/lhos/sdk/checkpoint_policy.py`：

```python
class CheckpointScope(StrEnum):
    MARKER = "marker"
    CONTEXT = "context"
    WORKSPACE = "workspace"
    SESSION = "session"
    PROCESS = "process"

class CheckpointDescriptorV2(BaseModel):
    checkpoint_id: str
    scope: CheckpointScope
    graph_id: str
    graph_version: int
    semantic_epoch: int
    task_id: str
    claim_id: str
    attempt_id: str
    lease_fencing_token: int
    context_snapshot_id: str | None
    workspace_root_hash: str | None
    program_state_hash: str | None
    read_bindings: tuple[ArtifactVersionBinding, ...]
    harness_revision: int | None

class CheckpointPlacementDecision(BaseModel): ...
```

需要：

- `CheckpointStore`
- versioned immutable program state；
- CAS/Merkle workspace chunks；
- `ProcessService.restore_checkpoint_projection()`；
- Harness capability 中区分 scope。

#### 分步实现

1. **修语义**：marker 命名；保存 immutable program state；真实 cursor；事务恢复 PCB/program/wait/cursor；release/revalidate Lease/actions；
2. **统一 descriptor**：Context/workspace/Harness metadata；fresh Attempt/continuation 语义；crash/reopen；
3. **测成本**：write/restore latency、dirty bytes、lost work；
4. **Hazard-aware policy**：failure/input survival/remaining work/cone；
5. **Incremental CAS**：Merkle directory、content-defined chunks、dirty journal。

#### 修改文件

- `src/lhos/sdk/checkpoint_policy.py`（新增）
- `src/lhos/agent_os/kernel/models.py`
- `src/lhos/agent_os/kernel/dispatcher.py`
- `src/lhos/agent_os/services/process_service.py`
- `src/lhos/agent_os/storage/schema.py`
- `src/lhos/sdk/harness.py`
- `src/lhos/sdk/subprocess_harness.py`
- Context/Workspace checkpoint store。

#### 测试与 benchmark

新增：

- `tests/agent_os/test_checkpoint_restore_state.py`
- `tests/sdk/test_checkpoint_descriptor.py`
- `tests/sdk/test_checkpoint_freshness.py`
- `tests/sdk/test_checkpoint_policy.py`
- `tests/sdk/test_checkpoint_crash_recovery.py`

Baseline：

```text
no checkpoint
task boundary
fixed interval/progress
Young/Daly failure-only
full tar/git
failure+semantic churn
offline oracle
```

指标：

- TTVG；
- checkpoint overhead；
- lost work/tokens；
- bytes/dedup；
- useful checkpoint precision；
- stale rejection；
- unsafe duplicate effect；
- oracle regret。

#### 验收门

- executable restore 后 PCB/program/wait/cursor 一致；
- stale checkpoint 被拒绝或 rebase；
- marker 不报告为 process checkpoint；
- partial snapshot 不可见；
- CAS 只写 dirty chunks；
- duplicate irreversible effect=0；
- phase-changing workload 优于最佳固定 interval。

#### 论文边界

可主张 failure hazard 与 semantic churn/repair blast radius 联合驱动的 checkpoint placement；不可主张发明 checkpoint/Merkle/CAS，或 arbitrary external effect rollback。

---

### 6.9.8 实施卡 S6-8：论文裁剪、依赖和总验收门

#### 中心 claim

建议主论文只保留：

> **Evidence-backed semantic validity drives an event-driven, fenced co-scheduler that reduces stale and redundant long-horizon Agent computation.**

角色：

- S6-1：主算法与系统；
- S6-3：增量状态底座；
- S6-2、S6-4、S6-6、S6-7 选 1–2 个做深；
- S6-5 更适合作为扩展或后续工作。

#### 推荐论文组合

优先方案：

```text
S6-1 Co-Scheduler
+ S6-3 Incremental Coherence
+ S6-2 Adaptive Semantic OCC
```

它与 mutable shared state、doomed compute、selective repair 最统一，也不依赖 provider KV。

备选：

```text
S6-1 + S6-3 + S6-7
```

但必须先完成真正 checkpoint/restore。

Context 方案只有在真实 Context/KV 可观测时适合成为主 claim。

#### Claim-evidence 矩阵

| 模块 | 可以说 | 不可以说 | 必须实验 |
|---|---|---|---|
| Co-Scheduler | semantic validity/conflict/resource/Harness lifecycle 联合 | 首个 Agent scheduler、全局最优 | 同资源 Harness baseline、oracle gap、TTVG |
| OCC | 根据 semantic rework 选择悲观/乐观 | 发明 OCC、完整 syscall provenance | crossover、correction replay、unsafe=0 |
| Coherence | affected-cone incremental maintenance | distributed coherence、跨服务 ACID | full differential、scale、crash saga |
| Context | version-pinned semantic replacement/placement | 发明 VM、真实 KV 管理 | 同 budget LRU/ARC/Belady |
| Priority | semantic urgency donation | 发明 inheritance、hard real-time | inversion/fairness/replay |
| Congestion | verified progress/rework feedback | 发明 AIMD/PID、真实 GPU control | phase/stability/safe cap |
| Checkpoint | failure+semantic churn placement | 发明 checkpoint、marker=process cp | fault/oracle/freshness |

#### Claude Code 实施依赖

```text
0. measurement + differential safety harness

1. correctness contracts
   - Context real eviction/close
   - checkpoint scope
   - AccessCorrection persistence
   - priority numeric semantics

2. PlacementAdmissionContract

3. Event-driven refill

4. Incremental semantic coherence

5. Joint optimizer + oracle

6. OCC 或 checkpoint 副主线

7. Context semantic placement

8. Congestion control

9. Semantic priority inheritance
```

#### 第一个里程碑

```text
Milestone 6A:
  PlacementAdmissionContract
  + event-driven completion/refill
  + weighted critical-path score
  + current greedy safety guards
```

先隔离验证：

1. policy assignment 是否真实执行；
2. batch barrier 是否是主要 wall-clock 瓶颈。

随后：

```text
Milestone 6B:
  incremental coherence + proof forest

Milestone 6C:
  semantic OCC 或 churn-aware checkpoint
```

#### 总验收门

Safety：

```text
false VERIFIED = 0
stale commit = 0
duplicate active Claim = 0
over-capacity = 0
contract divergence = 0
unsafe irreversible effect = 0
```

Correctness：

- full/incremental state/event/hash parity；
- actual placement=planned；
- decision replay deterministic。

Performance：

- event-driven long-tail 降低 TTVG/idle；
- joint optimizer 报告 oracle gap；
- mutable workload 减少 doomed/rework；
- null case 报告并限制 overhead。

Generality：

- 至少一个真实 DeepSeek Harness adapter；
- 最好再有 Claude Agent SDK 或 generic adapter；
- policy 不绑定特定模型。

---

# 7. Harness 与 LongHorizonOS 的边界

## 7.1 严格职责划分

### Harness 负责

- 单个 Agent session 内部执行循环；
- 模型调用；
- 工具调用；
- session history；
- local retry；
- checkpoint/resume mechanics；
- local/operational verification；
- prompt/model/tool 私有状态。

### LongHorizonOS 负责

- 全局语义有效性；
- READY/repair frontier；
- Claim、Attempt、Lease；
- 逻辑资源准入；
- 跨 Agent scheduling；
- 是否 START、CONTINUE、DEFER、CHECKPOINT、REBASE、PREEMPT；
- 独立 verifier/Evidence；
- Goal closure；
- 失效传播和选择性修复。

### Host OS/Container 负责

- CPU time；
- RAM；
- GPU/VRAM；
- process tree；
- network；
- filesystem；
- namespace/cgroup/seccomp/Job Object；
- 真正的物理隔离和 enforcement。

---

## 7.2 当前 Harness 协议

协议对象：

- `HarnessSessionIdentity`
- `HarnessCapabilities`
- `HarnessSessionSnapshot`
- `HarnessControlRequest`
- `HarnessControlResult`

操作：

```text
START
CONTINUE
CHECKPOINT
REBASE
PREEMPT
```

证据：

- `src/lhos/sdk/harness.py:49-95`
- `src/lhos/sdk/harness.py:121-160`
- `src/lhos/sdk/harness.py:190-312`

Harness completion 不是 Evidence：

- `src/lhos/sdk/harness.py:262-299`
- `docs/HARNESS-SESSION-PROTOCOL.md:34-36,65-67`

---

## 7.3 当前默认执行不经过 Harness

默认 `AgentOS.run()` / `run_async()` 直接调用 `Agent.executor`：

- `src/lhos/sdk/os.py:976-1085`
- `src/lhos/sdk/os.py:5124-5270`

Harness 必须显式注册：

- `src/lhos/sdk/os.py:4702-4775`

`schedule_online_epoch()` 只做到 policy + Scheduler admission，不运行 Harness：

- `src/lhos/sdk/os.py:6688-6722`

`handoff_online_epoch_to_harness()` 只绑定，不执行、不验证、不提交 Evidence：

- `src/lhos/sdk/os.py:7056-7074`

---

## 7.4 Harness control 不是完整 live-Lease fencing

`HarnessSessionIdentity` 包含：

```text
graph/version/epoch
task/agent
claim/attempt
```

但不包含：

```text
process_id
lease_id
lease_fencing_token
```

证据：

- `src/lhos/sdk/harness.py:78-95`

`register_harness()` 和 `control_harness()` 主要检查：

- Scheduler Claim 为 active；
- Claim/Attempt/session identity 相等。

证据：

- `src/lhos/sdk/os.py:4724-4758`
- `src/lhos/sdk/os.py:4838-4857`

真正检查 Kernel Lease 活性、owner、resource、fencing token 和 Attempt state 的是：

- `src/lhos/sdk/os.py:5848-5907`

前两个 Harness 方法没有调用完整 `_live_claim()`。

因此更准确的描述是：

> Harness bridge 当前是 Claim/Attempt identity-fenced，但不是完整 authoritative live-Lease-fenced。

---

## 7.5 Harness effect 与 journal 之间不是原子操作

顺序：

```text
await harness.control(request)
  -> Harness hook/child effect

随后

record HARNESS_CONTROL event
```

证据：

- Harness control：`src/lhos/sdk/os.py:4951`
- journal：`src/lhos/sdk/os.py:4991-5005`

若在二者之间 crash：

- Harness 操作可能已经生效；
- durable Scheduler history 没有记录；
- 跨进程恢复不能还原 callback/model state。

这是带 uncertain window 的 saga，不是原子事务或 exactly-once。

---

## 7.6 AgentOS 不拥有完整 Harness 生命周期

`AgentOS.close()` 只清空 Harness registry：

- `src/lhos/sdk/os.py:1564-1588`

`unregister_harness()` 只解除映射：

- `src/lhos/sdk/os.py:4777-4793`

而 `SubprocessHarnessAdapter` 需要显式 `close()` 才会 terminate/reap：

- `src/lhos/sdk/subprocess_harness.py:512-519`

所以当前 Harness lifecycle 主要仍是 caller-owned。

---

## 7.7 PREEMPT 能力语义需要拆分

当前 capability 只有：

```text
preemption_mode = none | cooperative
```

但 SubprocessHarness 实际执行：

- terminate；
- 等待；
- kill；
- reap。

建议能力模型改为：

```text
termination_mode:
  none
  cooperative_token
  process_terminate
  process_kill
  process_group_kill
  container_kill

checkpoint_semantics:
  marker
  logical_session
  workspace_snapshot
  executable_process

isolation_level:
  in_process
  subprocess
  sandbox
  container
```

---

# 8. 能否定义为基于 Harness 的 Long-Horizon OS

## 8.1 三种含义

| 含义 | 当前判定 |
|---|---|
| Core 源码依赖意义上的 harness-based | 否。Harness 是可选 L5 integration，Core 不依赖 Harness |
| 控制模型意义上的 OS over Harness | 可以。Harness 可作为被管理的 execution unit |
| 当前默认执行路径意义上的 harness-backed | 尚不成立。默认仍直接调用 Agent executor |

## 8.2 当前最准确表述

英文：

> **LongHorizonOS is a single-host, stateful semantic-compute control plane and execution-authority runtime for long-horizon agents, with optional identity-fenced Harness session adapters.**

中文：

> **LongHorizonOS 是面向长时程 Agent 的单机状态化语义计算控制面与执行权限运行时。它以 VPG 管理语义真值，以 Scheduler/Kernel Claim+Lease 管理准入和执行权限，并可通过显式的身份围栏化适配器控制 Harness 会话。**

可使用的短名称：

- Harness-integrated long-horizon agent operating architecture
- LongHorizonOS control plane over Harness-managed sessions
- Harness-backed execution profile of LongHorizonOS

不建议直接写：

- “基于 Harness 的操作系统”
- “支持任意第三方 Harness”
- “透明 checkpoint/migration”
- “OS 级安全抢占任意 Agent”

## 8.3 让 harness-based 真正成立的最低条件

1. 默认执行统一经过 `HarnessSessionAdapter`；
2. direct callback 被定义成内置 Harness adapter；
3. session identity 加入 process/lease/fencing；
4. 每次 control 前权威检查 live Lease；
5. write-ahead control intent；
6. adapter request idempotency；
7. acknowledgement journal；
8. crash 后 reconcile uncertain request；
9. PREEMPT/REBASE 与 Claim/Lease handoff 统一；
10. AgentOS 明确负责或明确不负责 Harness close/kill/reap；
11. Harness output 统一经过 verifier/Evidence；
12. Harness 的 I/O 和副作用进入受 fencing 的 gateway。

---

# 9. 测评设计

## 9.1 原则：先定义 claim，再定义 benchmark

建议将论文/项目 claim 拆成：

| Claim | 必要测评 |
|---|---|
| C1：选择性修复正确 | hidden oracle、under/over invalidation、false closure |
| C2：增量 VPG 更快 | 等价 projection/event/hash + scale latency |
| C3：调度减少 makespan | 同资源 static-parallel、weighted CP、oracle |
| C4：抢占减少无效计算 | no/always/threshold/oracle preemption |
| C5：Context locality 有收益 | no-cache、LRU/ARC、当前 policy、proposed |
| C6：Harness lifecycle 被 OS 管理 | same Harness，OS off/on，完整 lifecycle |
| C7：故障恢复安全 | crash matrix、stale commit、duplicate effect |

---

## 9.2 四层测评

### A. Policy-quality simulator

排除 Python、SQLite、模型/provider 噪声。

用于：

- 与 ILP oracle 比较；
- 构造 greedy 反例；
- 测 approximation gap；
- 扫 conflict/churn/rework crossover。

### B. Runtime systems microbenchmark

分别测：

- VPG commit；
- Scheduler append；
- reopen/replay；
- D3 cone/proof；
- Context load/evict；
- ConflictGraph；
- resource admission；
- provenance append；
- checkpoint；
- Journal/outbox。

### C. 当前真实 AgentOS path

必须经过：

```text
AgentOS.run_async
  -> Scheduler
  -> Claim/Attempt
  -> Kernel Lease
  -> WorkerPool
  -> verifier
  -> Evidence/VPG
```

### D. 同 Harness 的真实 Agent workload

固定：

- Harness；
- model；
- prompt；
- tools；
- verifier；
- Agent 数；
- resource limits。

唯一变量是 OS policy。

---

## 9.3 Baseline

### 调度 baseline

1. Direct Harness，无 OS；
2. FIFO，固定并行度；
3. static parallel list scheduling；
4. weighted critical path；
5. HEFT/CPOP 风格；
6. work stealing；
7. 当前 conflict/resource/unified greedy；
8. proposed co-scheduler；
9. 小规模 ILP oracle。

### Preemption baseline

1. never preempt；
2. always preempt；
3. remaining-time threshold；
4. current superseded-input preempt；
5. proposed semantic decision；
6. clairvoyant oracle。

### Repair baseline

1. full restart；
2. state-only resume；
3. last checkpoint；
4. oracle task-DAG checkpoint；
5. output-level repair oracle；
6. LongHorizonOS。

### Context baseline

1. no cache；
2. LRU；
3. LFU；
4. ARC/2Q；
5. 当前 priority/lifecycle policy；
6. proposed verified-progress-aware pager；
7. Belady-style offline oracle。

### Resource baseline

1. first fit；
2. best fit；
3. best-fit decreasing；
4. DRF；
5. generalized assignment heuristic；
6. ILP oracle。

---

## 9.4 资源必须等价

不能将：

```text
1 Agent 串行 baseline
```

与：

```text
多 Agent adaptive
```

直接归因于算法。

当前存在混杂的 benchmark：

- `src/lhos/benchmarks/baseline_vs_lhos.py:190-215,323-325`
- `src/lhos/benchmarks/real_build_workload.py:194-233,262-305`

相对公平的模板：

- `src/lhos/benchmarks/scheduling_regimes.py:369-400`

其中 static-parallel 与 adaptive 使用相同 Agent 数。

---

## 9.5 合成 workload 矩阵

### DAG 形状

- chain；
- wide；
- fork-join；
- diamond；
- fan-in/fan-out；
- 多 root；
- power-law；
- heavy critical path；
- 两条节点数相同但重量不同的路径；
- star-conflict 反例；
- stable null case。

### 规模

```text
Tasks: 20, 100, 500, 2,000, 10,000
Agents: 1, 2, 4, 8, 16
Pools: 1, 2, 8
```

### 动态变化

- mutation rate；
- mutation time；
- affected ratio；
- graph-version race；
- watcher delay；
- provenance complete/partial/unknown；
- failure rate；
- retry burst；
- heavy-tail duration；
- conflict density；
- context overlap。

已有 controlled generator：

- `src/lhos/benchmarks/controlled/generator.py:1-43`

但当前 runner 使用 legacy RuntimeStack：

- `src/lhos/benchmarks/runner.py:84-115`

应为 current AgentOS/Harness path 增加 adapter。

---

## 9.6 VPG 专项

规模：

```text
N = 100, 400, 800, 1,600, 3,200, 10,000
```

图：

- chain；
- wide；
- layered diamond；
- sparse random DAG。

操作：

- 单节点 patch；
- 单边 patch；
- validity flip；
- batch 10/100；
- Artifact version change；
- multi-root invalidation。

指标：

- p50/p95/p99 commit latency；
- rows read/written；
- SQLite bytes；
- Python allocations；
- hash CPU；
- affected size；
- differential correctness。

---

## 9.7 Scheduler durability 专项

交叉：

```text
events: 100, 1k, 10k, 100k
active claims: 10, 100, 1k
historical attempts/matches: 100, 1k, 10k
```

测：

- single append；
- batch append；
- idempotency-only mutation；
- reopen latency；
- serialized bytes；
- written bytes；
- corruption detection latency；
- projection rebuild。

---

## 9.8 Context VM 专项

Artifact：

```text
1 MB, 10 MB, 100 MB
```

Page size：

```text
4 KB, 32 KB, 256 KB
```

Selection ratio：

```text
1%, 10%, 100%
```

记录：

- `read_version()` 调用次数；
- 实际读取字节；
- hash 字节；
- selected bytes；
- peak RSS；
- materialize latency；
- page sharing ratio；
- eviction 后真实 RSS/tokens；
- Context/KV hit rate。

优化后应明确断言：

> 同一不可变 ArtifactVersion 的整文件读取次数不随 page count 线性增长。

---

## 9.9 Conflict/resource/budget 专项

```text
tasks: 100, 1k, 10k
conflict density: 0%, 1%, 10%, 50%, 100%
active attempts: 10, 100, 1k
pools: 1, 8, 64
agents: 8, 64, 512
```

指标：

- policy latency；
- batch utility；
- batch cardinality；
- approximation ratio；
- resource fragmentation；
- placement rejection；
- temp set/object allocations；
- oracle gap。

必须包含 star conflict、multidimensional packing 反例。

---

## 9.10 Harness 专项

不能只测“一次 START”。

至少覆盖：

```text
START
CONTINUE
CHECKPOINT
CONTINUE from checkpoint
mid-flight PREEMPT
REBASE
crash/reopen
replacement Harness handoff
close/kill/reap
```

对照：

```text
同一个 Harness
同一个 model/tool workload

Arm A: Harness 自己运行，OS policy off
Arm B: Harness + static OS admission
Arm C: Harness + adaptive LongHorizonOS
```

需要检查：

- 每次 execution 是否都经过 Harness ABI；
- live-Lease fence；
- intent/ack；
- replay；
- replacement identity；
- output 是否统一经过 verifier/Evidence；
- Harness process 是否泄漏。

---

## 9.11 核心指标

### Safety：硬约束

```text
false VERIFIED = 0
false Goal closure = 0
stale commit = 0
overlapping active Claims = 0
superseded Lease commit = 0
duplicate irreversible effect = 0
over-capacity = 0
orphan lease/process = 0
replay divergence = 0
```

Safety 不应与性能加权后互相抵消。

### 性能

- Time to Verified Goal；
- Verified Progress / second；
- Verified Progress / token；
- Verified Progress / dollar；
- verified-progress AUC；
- makespan；
- critical-path stretch；
- work-conserving idle fraction；
- queue wait p50/p95/p99；
- utilization；
- fragmentation。

### 无效计算

- doomed compute milliseconds；
- stale/repeated tokens；
- repair/full-restart work ratio；
- preemption precision/recall；
- harmful preemption；
- net preemption payoff；
- context reread tokens；
- unused materialized bytes。

### OS 开销

- policy decision latency；
- Scheduler CPU/RSS；
- VPG commit latency；
- event bytes；
- SQLite transactions/fsync；
- bytes written per verified task；
- replay/recovery；
- Context materialization；
- checkpoint size/time。

---

## 9.12 故障注入矩阵

逐点 crash：

1. resource reserve 后、Lease 前；
2. Lease 后、Claim event 前；
3. Claim dispatch 后、executor start 前；
4. executor 中途；
5. Harness effect 后、control journal 前；
6. 外部副作用后、ACK 前；
7. operational success 后、verification 前；
8. verification 后、Evidence commit 前；
9. Evidence commit 后、Claim release 前；
10. checkpoint 后、preempt 前；
11. old Harness detach 后、replacement acquire 前；
12. SQLite commit 前后；
13. heartbeat delayed/lost；
14. lease expiry；
15. driver UNKNOWN；
16. duplicate/out-of-order watcher；
17. journal 中间行损坏；
18. Harness 忽略 cooperative interrupt；
19. subprocess 创建孙进程。

恢复后检查：

- projection hash；
- 至多一个 ACTIVE Claim；
- Lease generation；
- no overcommit；
- no false VERIFIED；
- no orphan；
- repair frontier 与 hidden oracle 一致。

---

## 9.13 统计方法

### 合成 workload

- 至少 30 个独立 seed；
- seed 必须真正改变 DAG、duration、failure、churn；
- 所有 policy 使用同一 realization；
- common random numbers。

当前 online-compute canonical seed 实际不改变 workload：

- `docs/benchmarks/ONLINE-COMPUTE-CONTROL.md:123-130`

因此它当前是回归工具，不是统计实验。

### 真实 wall-clock

- paired repetitions；
- 随机或交替 arm 顺序；
- warm-up 单独剔除；
- median、p95、p99；
- bootstrap 95% CI；
- paired permutation/Wilcoxon；
- effect size；
- 多 workload geometric-mean speedup；
- 记录硬件、OS、Python、model/provider、policy 参数。

---

## 9.14 真实任务来源

可以选择：

- SWE-bench：真实 GitHub issue 修复，arXiv:2310.06770；
- Long-Horizon Terminal-Bench：长时程 terminal task，arXiv:2607.20392；
- OSWorld：真实计算机环境任务，arXiv:2404.07972；
- 自建多文件 build/test/fix；
- 版本变化的 ETL/data pipeline；
- 浏览器/API/文件同时变化的 tool workload。

这些 benchmark 本身不直接测 LongHorizonOS 的核心能力，需要额外注入：

- 中途文件/API mutation；
- provenance omission；
- lease failure；
- Harness preemption/rebase；
- verifier change；
- side-effect uncertainty。

---

# 10. 对现有 benchmark 的评价

## 10.1 优点

现有 benchmark 的优点：

- 明确区分 synthetic 与 real path；
- 有 correctness oracle；
- 有 under/over invalidation；
- 有 false VERIFIED；
- 有 Claim/Lease audit；
- 有静态并行 null result；
- 已开始测真实子进程；
- 已有 preemption payoff；
- 已有 VPG history scale；
- 文档对 physical resource 和 distributed boundary 较诚实。

## 10.2 当前不足

1. 大量 workload 是 `asyncio.sleep` 或 deterministic hash；
2. 部分 baseline 使用更少 Agent，资源不等价；
3. 普通 adaptive path 未证明 graph utility 真正进入 conflict-aware 调度；
4. resource benchmark 多为单 pool；
5. assignment 没有绑定真实 placement；
6. canonical seed 不改变 workload；
7. 旧 controlled suite 走 legacy RuntimeStack；
8. Harness benchmark 主要是一次 START；
9. 缺少长期 history；
10. 缺少 Scheduler durable append 曲线；
11. 缺少 Context I/O amplification；
12. 缺少 barrier idle；
13. 缺少真实模型的统计比较；
14. 缺少完整 crash matrix。

## 10.3 Semantic repair benchmark 的正确解读

当前 semantic-repair 已证明：

- 相对 full restart 节省工作；
- 与 oracle task-DAG checkpoint 持平；
- 没有证明优于 oracle task-DAG checkpoint。

文档：

- `docs/benchmarks/SEMANTIC-REPAIR.md:16-27`
- `docs/benchmarks/SEMANTIC-REPAIR.md:95-121`
- `docs/benchmarks/SEMANTIC-REPAIR.md:181-193`

要超过 task-level oracle，必须加入更细语义粒度：

- multi-output task；
- Artifact/Evidence-level dependency；
- verifier/config version；
- task 内局部修复；
- semantic-equivalence pruning。

---

# 11. 建议实施优先级

## 11.1 第一阶段：修复真实闭环问题

1. 消除 `run_async` batch barrier；
2. 实现持续补位、work-conserving 调度；
3. 联合 critical path、conflict、resource、budget；
4. 将 placement assignment 绑定 Scheduler；
5. 修复 kernel-loop parallelism 退化；
6. Context eviction 真正释放状态；
7. Context residency 改为 lease；
8. checkpoint restore 真正恢复；
9. Harness control 加 live-Lease fencing；
10. Harness control 引入 intent/ack。

## 11.2 第二阶段：低风险数据结构优化

1. ConflictGraph 临时/持久邻接索引；
2. ResourceManager per-pool aggregate；
3. Attempt/Claim indexes；
4. Context snapshot `page_by_id`；
5. Artifact quota aggregate SQL；
6. 复合/partial indexes；
7. D3 seed 使用 set；
8. matching agent cost map；
9. ContextCompiler trimming 使用增量 token count。

## 11.3 第三阶段：中风险增量化

1. VPG reverse-dependency work queue；
2. D3 multi-source proof；
3. Context verified buffer/range I/O；
4. ProgressGraph adjacency indexes；
5. weighted critical-path DP；
6. watch radix trie；
7. event-driven Kernel queues；
8. dynamic/incremental cycle detection。

## 11.4 第四阶段：研究级系统

1. Incremental Authenticated VPG；
2. authenticated segmented Scheduler journal；
3. Semantic Co-Scheduler；
4. Adaptive Semantic OCC；
5. verified-progress-aware Context Pager；
6. semantic priority inheritance；
7. semantic congestion control；
8. churn-aware checkpoint placement。

---

# 12. 最终研究定位

## 12.1 最重要的统一问题

当前代码的核心问题可以概括为：

> **局部语义变化导致全局重复计算，而已实现的语义、冲突、资源、上下文和 Harness 控制信号尚未形成一个连续、权威、联合优化的调度器。**

## 12.2 最建议的系统定义

> **LongHorizonOS 是运行在 Harness-managed Agent sessions 之上的语义计算控制平面。它使用 Evidence-backed Verified Progress Graph 管理语义有效性，使用 Scheduler/Kernel Claim、Lease 和 fencing 管理执行权限，并通过在线联合调度、抢占和局部修复减少完成可验证 Goal 所需的无效计算。**

必须附带当前实现边界：

> 当前版本的 Harness 集成仍是 opt-in、bounded、caller-owned；默认执行不是 Harness-backed，资源为逻辑准入而非物理 enforcement，跨 Scheduler/Kernel/Harness/VPG 的原子事务尚未完成。

## 12.3 最有希望的论文级贡献表述

> **一种基于 Verified Progress、输入失效风险、Context residency 与 Harness capability 的在线联合调度、放置与抢占算法，用于长时程 Agent 计算。**

其核心评价标准不是普通 task throughput，而是：

```text
在不产生 false VERIFIED、stale commit 和 ownership violation 的前提下，
是否以更少的 token、时间、金钱、Context I/O 和重做，
更快达到 Verified Goal Closure。
```

---

# 13. 全仓库第二轮微观到宏观审查

本节不是重复前文，而是把全局问题按抽象层次重新组织。顶会系统论文通常不能只列若干 profiling 热点，而需要说明：

1. 微观实现为什么慢；
2. 哪些数据结构造成复杂度增长；
3. 哪些局部算法在全局闭环中失效；
4. 哪些机制虽然已经存在，但没有成为权威路径；
5. 为什么这些问题可被统一成一个新的系统问题。

## 13.1 六层问题分解

| 层次 | 当前问题 | 代表模块 | 优化类型 |
|---|---|---|---|
| L0 常数与对象分配 | 重复 JSON/Pydantic、反复建 set/sorted、bytes 转 hex | VPG、Context、policy DTO | 工程优化 |
| L1 索引与状态结构 | 缺少邻接、倒排、聚合、tail、dirty indexes | Graph、Scheduler、Resource、Provenance | 数据结构 |
| L2 局部算法 | fixed point、per-node BFS、greedy MIS/packing、全图 cycle check | VPG、D3、Conflict、legacy | 算法优化 |
| L3 持久化 | 每次写前全历史验证、全 projection hash | GraphStore、Scheduler store、JSONL provenance | 存储算法 |
| L4 运行时/OS | batch barrier、串行 tick、HOL blocking、placement 丢失 | AgentOS、Kernel、WorkerPool | OS 算法 |
| L5 全局控制面 | validity、conflict、resource、budget、context 分离 | SDK policy modules | 联合优化/论文核心 |

## 13.2 L0：常数级与对象分配热点

这些通常不是论文主贡献，但必须先处理，否则会污染上层算法评测。

### 13.2.1 重复 canonical JSON 与 Pydantic 转换

热点：

- `src/lhos/runtimes/verified_progress/sdk.py:255-262`
- `src/lhos/runtimes/verified_progress/graph_store.py:2368-2383`
- `src/lhos/runtimes/verified_progress/graph_store.py:2591-2612`
- `src/lhos/runtimes/verified_progress/graph_store.py:2936-2959`
- `src/lhos/runtimes/multi_agent/durable_state.py:931-950`
- `src/lhos/sdk/runtime_state.py`
- 多个 policy 的 `_hash_payload()` / `_canonical()`。

已有 commit-local serialization cache 是正确方向，但仍存在：

- SDK diff 前已经序列化；
- Store 内再建立完整 JSON map；
- materialized rows 再读取；
- cache warming 再解码 delta；
- policy DTO 为 decision hash 再次 canonicalize。

建议：

```text
CanonicalEntity {
  typed_model
  canonical_bytes
  content_hash
}
```

在一个 commit/epoch 中只构造一次，并在 validation、diff、hash、persistence 之间传递。

### 13.2.2 Context materialized hash 的 `.hex()` 放大

`src/lhos/agent_os/context/service.py:318-320` 将每页内容转成十六进制字符串再哈希：

```text
N bytes -> 2N 字符 + Python string/object overhead
```

应改为带长度前缀的流式 hash：

```text
hash.update(page_id_length)
hash.update(page_id)
hash.update(content_length)
hash.update(content)
```

### 13.2.3 循环内重复构造 set

代表位置：

- `src/lhos/sdk/resource_policy.py:380-386`
- `src/lhos/sdk/conflict_graph.py:523-538`
- `src/lhos/runtimes/multi_agent/matching.py:73`
- `src/lhos/sdk/compute_budget.py`

例如 resource policy 在每个 candidate 分支内反复：

```python
set(_normalize_ids(...))
```

这些集合应在循环外一次构造。

### 13.2.4 只取最佳项却完整排序

代表位置：

- legacy `CostAwareScheduler.select()`；
- matching decision；
- 部分 victim/choice helper。

如果只取一项且不需要完整审计序列，可用：

```python
min(..., key=...)
max(..., key=...)
heapq.nsmallest(k, ...)
```

如果审计必须保留完整排序，则将“决策所需排序”和“可选完整诊断”分离。

### 13.2.5 大型 facade 导致重复 normalization

`src/lhos/sdk/os.py` 中多条路径分别进行：

- task id normalization；
- graph version lookup；
- Claim/Attempt lookup；
- Context identity materialization；
- policy metadata conversion；
- cleanup audit 构造。

建议引入每个 epoch 的不可变：

```text
EpochExecutionView {
  graph_snapshot
  frontier
  claims
  attempts
  resources
  contexts
  task_metadata
}
```

避免在一次 epoch 内跨 facade 重复查询和转换。

## 13.3 L1：全仓库缺失或不完整的索引

### 13.3.1 VPG 结构索引

需要持久或可重建地维护：

```text
depends_out: task -> prerequisites
depends_in: task -> consumers
produces_out: source -> artifact/evidence
verifies_by_task: task -> verification
evidence_by_verification
artifact_pins_by_task
tasks_by_artifact_uri
goals_by_direct_task
```

当前 readiness 已局部构建 `dep_index`，verification 已局部构建 `verifies_index/produces_index`，证明这一模式有效，但这些索引没有成为 Graph projection 的一等结构。

### 13.3.2 Scheduler lifecycle 索引

建议维护：

```text
claim_by_id
active_claim_by_(graph, task)
claims_by_agent
claims_by_process
attempt_by_id
attempts_by_claim
latest_attempt_by_claim
attempts_by_(graph, task, epoch)
attempt_count_by_epoch
cleanup_marker_by_claim
harness_session_by_claim
```

当前一些索引仅在一次 pass 临时建立，另一些查询仍扫描全部历史 Claim/Attempt。

### 13.3.3 Resource aggregate

建议：

```text
capacity_by_pool
used_by_pool
available_by_pool
reservation_ids_by_pool
reservation_by_owner
```

全扫描只作为 audit/rebuild oracle，不应作为每次 admission 查询。

### 13.3.4 Context residency 与 page indexes

需要：

```text
page_by_id
pages_by_artifact_version
loaded_pages_by_context
contexts_by_agent
residency_by_(agent, page, generation)
pin_count_by_page
bytes_by_context
evictable_heap
```

当前 Scheduler 的 residency 是历史 read set 单调并集，无法代表当前真实驻留。

### 13.3.5 Provenance indexes

建议：

```text
event_by_id
event_by_idempotency_key
events_by_graph
events_by_task
events_by_attempt
reads_by_resource
writes_by_resource
latest_observation_by_resource
```

这既用于查询，也用于快速找到因输入变化而需要 interrupt 的 live Attempt。

### 13.3.6 Watch 与 mount prefix index

Artifact watch 和 mount 都是 longest/prefix match 问题。适合：

- compressed radix tree；
- Patricia trie；
- 规范化 URI 的 interval/range index。

### 13.3.7 Timer 与 expiry index

Kernel 每个 tick 查询过期 Lease。应使用：

- 数据库 `(expires_at)` 索引；
- 进程内 min-heap；
- 或 timer wheel。

同理适用于：

- heartbeat deadline；
- outbox available time；
- retry time；
- schedule reminder；
- Harness control timeout。

## 13.4 L2：图与组合优化算法

### 13.4.1 增量 READY

当前 READY 每次从全部 Task 推导。可维护：

```text
unresolved_dependency_count[task]
active_claim_count[task]
validity[task]
repair_ready[task]
```

当一个依赖变为 VERIFIED：

```text
for consumer in depends_in[dependency]:
  unresolved[consumer] -= 1
  if unresolved == 0:
    update READY
```

当一个依赖失效：

```text
for consumer in depends_in[dependency]:
  unresolved[consumer] += 1
  remove READY
  propagate if previously VERIFIED
```

### 13.4.2 动态拓扑与 cycle detection

当前 Core patch 至少在一个 patch 结束时联合判环，优于 legacy 每条边重建 NetworkX 图。但长期动态图可以使用：

- persistent topological rank；
- Pearce-Kelly 风格 dynamic topological order；
- 只对违反现有 rank 的新增边做局部搜索。

### 13.4.3 Weighted critical path

当前 GraphAnalysis 按 open Task 节点数形成路径。论文需要：

```text
node_weight =
  expected_remaining_time
  + verifier_time
  + context_load_time
  + expected_retry_time
```

并可加入方差/尾延迟：

```text
risk_adjusted_weight = mean + beta * stddev
```

一次逆拓扑 DP 可计算所有 `longest_to_goal`。

### 13.4.4 增量 critical path

项目已有 `IncrementalGraphAnalyzer`：

- `src/lhos/sdk/graph_analysis.py:345-532`

但主路径仍调用全量 `derive_graph_analysis`：

- `src/lhos/sdk/runtime_state.py:408-414`

此外当前 analyzer 为每个 Task 保存完整 path tuple，在长链中累计可能达到 `O(V²)` 引用。

应改为：

```text
distance
predecessor
path_rank/hash
```

最终只重建实际输出的路径。

### 13.4.5 D3 proof forest

使用一次 multi-source traversal 同时构造：

- invalidation cone；
- root cause；
- predecessor；
- distance；
- proof forest；
- repair eligibility。

避免 cone 和 proofs 分成多个重复遍历。

### 13.4.6 Conflict-aware batch

当前算法输出一个 maximal independent set，但不是 maximum-weight independent set。

可采用：

- degree/weight greedy；
- local ratio improvement；
- bitset branch-and-bound；
- 小图 exact MWIS；
- 大图近似与 regret bound。

### 13.4.7 多维资源与 placement

问题同时包含：

- conflict graph；
- multidimensional bin packing；
- generalized assignment；
- model/provider compatibility；
- context locality；
- Agent load。

不应再将“先选 Task”和“再匹配 Agent”完全分离，因为 Task 选择是否可行依赖 placement。

### 13.4.8 Compute budget

当前按 `expected_progress / cost` 排序再逐项装入，对单维 fractional knapsack 有直觉，但对：

- 离散任务；
- 多维 budget；
- dependency unlock；
- conflict；
- shared verifier/context cost；

没有最优性。

应将 unlock value 与 future frontier value纳入，并使用：

- DP/ILP oracle；
- Lagrangian prices；
- online primal-dual。

### 13.4.9 Matching

当前 deterministic best-fit 使用固定加减分：

- specialization；
- locality；
- load；
- cost；
- priority。

问题：

- locality 可能陈旧；
- 固定权重未经校准；
- assignment 未与 batch 联合；
- `_cost_of()` 在排序 key 中线性扫描 Agent pool。

可以改为：

- 预建 Agent map；
- measured service-time/cost calibration；
- min-cost matching；
- batch-level bipartite/generalized assignment。

### 13.4.10 Context eviction

当前需要先明确目标：

```text
minimize total loss
subject to freed capacity >= target
```

loss 可由：

- semantic relevance；
- reuse probability；
- reload cost；
- prefix invalidation；
- recoverability；
- critical-path urgency；
- stale probability；

共同决定。

### 13.4.11 Deadlock

只需发现 wait-for graph 中含环 SCC，而不必枚举所有 simple cycle。

可使用：

- Tarjan SCC；
- wait edge 插入时增量检测；
- SCC 内 semantic-aware victim selection。

## 13.5 L3：持久化与认证数据结构

### 13.5.1 VPG authenticated delta

建议结构：

```text
GraphVersion {
  parent_root
  patch_digest
  node_tree_root
  edge_tree_root
  derived_tree_root
}
```

单实体变化只更新认证路径。

### 13.5.2 Scheduler segmented journal

建议：

```text
segment header
event hash chain inside segment
segment Merkle root
global MMR root
periodic projection checkpoint
```

在线 append 验证 tail 和当前 authenticated root；后台 scrub 验证历史 segment。

### 13.5.3 Provenance segmented store

与 Scheduler 相同，但额外维护 resource inverted index 和 observation epoch。

### 13.5.4 Workspace checkpoint

当前 filesystem checkpoint 会先 hash 全工作区，再 tar 再读一次。建议：

- Merkle directory；
- content-defined chunking；
- dirty-path journal；
- CAS chunks；
- tombstone/rename metadata；
- 定期 full baseline。

### 13.5.5 Group commit

以下操作应尽量合并：

```text
Claim
Attempt
resource reservation
idempotency
dispatch event
```

以及：

```text
operational result
Evidence
VPG derived state
Claim completion
Lease release intent
```

不能跨无法共享事务的系统假装原子，但可以使用 outbox/saga 减少重复 durable publish。

## 13.6 L4：运行时与 OS 算法

### 13.6.1 Work-conserving event loop

需要从 batch epoch 改成 slot-driven：

```text
while goal open:
  process completed/failed/changed events
  update semantic state incrementally
  while capacity available:
    choose next task-placement
    acquire resources/Lease
    dispatch
```

### 13.6.2 Placement-fenced dispatch

Policy 的 assignment 必须成为 Scheduler admission input，而不只是 audit output。

### 13.6.3 Semantic preemption

决策应比较：

```text
expected doomed remaining work
vs
checkpoint + lost progress + restart + context reload + merge overhead
```

### 13.6.4 Semantic priority inheritance

priority 可沿：

- Goal -> critical Task；
- Task -> prerequisite；
- Task -> resource holder；
- Task -> verifier；
- Task -> Harness replacement；

传播到 Kernel `effective_priority`。

### 13.6.5 Semantic congestion control

parallel degree 应从固定 heuristic 升级为反馈控制：

```text
increase when:
  verified throughput grows
  queue exists
  rework low

decrease when:
  stale/rework rises
  p95 latency rises
  resource pressure rises
  verifier backlog rises
```

### 13.6.6 Kernel async I/O

driver dispatch/inspect 不应阻塞整个 tick。应将外部操作转为 future，completion 通过 event 回注。

### 13.6.7 Harness lifecycle ownership

OS 必须明确拥有或不拥有：

- spawn；
- attach；
- heartbeat；
- checkpoint；
- terminate；
- kill tree；
- reap；
- restart；
- orphan reconciliation；
- close。

当前 registry/control 不是完整 lifecycle management。

## 13.7 L5：全局控制面的统一缺口

当前已经有：

- GraphAnalysis；
- FrontierPolicy；
- ConflictGraph；
- ResourcePolicy；
- ComputeBudget；
- UnifiedPolicy；
- SemanticInterrupt；
- ContextRouting；
- ParallelismPolicy。

但存在三类断裂：

1. **排序断裂**：有 conflict/resource 时 graph utility 可能丢失；
2. **placement 断裂**：policy assignment 未被 Scheduler 原子消费；
3. **执行断裂**：默认 executor 路径并非统一 Harness substrate。

因此顶会贡献不应是再增加一个 policy 文件，而应是：

> **将这些状态和约束统一进一个权威、事件驱动、可恢复的决策与执行闭环。**

## 13.8 低风险优化清单

这些可以先做，建立更干净的实验底座：

1. matching 预建 Agent cost map；
2. Context snapshot 预建 `page_by_id`；
3. ResourceManager 维护 `used_by_pool`；
4. ConflictGraph 建 `access_by_task/conflict_neighbors`；
5. D3 seed list 改 set；
6. policy 循环外预构造 verified/stale/invalid sets；
7. Artifact quota 改 aggregate SQL；
8. 增加复合/partial indexes；
9. JSON canonical bytes 在 commit 内共享；
10. Context materialized hash 流式化；
11. Attempt/Claim manager 增加常用 indexes；
12. runtime_state 接入 IncrementalGraphAnalyzer。

## 13.9 中高风险优化清单

1. VPG dirty-set derivation；
2. incremental authenticated projection；
3. Scheduler delta persistence；
4. D3 proof forest；
5. Context shared page/COW；
6. actual eviction/residency lease；
7. event-driven continuous scheduler；
8. placement-fenced admission；
9. semantic preemption；
10. durable Harness intent/ack；
11. Kernel async driver completion；
12. semantic priority inheritance。

## 13.10 模块级全局责任矩阵

下面按核心模块给出“当前职责、主要问题、建议优化、论文相关性”。这是后续 issue、profiling 和消融实验的责任清单。

| 模块 | 当前职责 | 主要问题 | 优化/研究方向 |
|---|---|---|---|
| `runtimes/verified_progress/sdk.py` | patch、派生 validity/READY/closure | 全 projection、fixed point、重复 edge scan | dirty set、work queue、unresolved counters |
| `runtimes/verified_progress/graph_store.py` | GraphVersion、history、hash、recovery | 全图 hash/验证、cache 粗粒度失效、同 connection 并发 | authenticated tree、per-graph generation、read pool |
| `runtimes/verified_progress/readiness.py` | READY 与 topo depth | 每次重建 index，递归深链风险 | 持久 unresolved count、迭代 topo DP |
| `runtimes/verified_progress/verification.py` | Evidence validity | 部分 index 可复用但非 projection 一等结构 | Evidence/Verification 倒排索引 |
| `runtimes/invalidation/*` | cone、proof、frontier | proof per-node BFS | multi-source proof forest |
| `runtimes/multi_agent/scheduler.py` | matching、Claim、Attempt、admission | pass/history scan、重复 READY 校验、三次持久化、placement 重选 | lifecycle indexes、conditional claim、group commit |
| `runtimes/multi_agent/durable_state.py` | Scheduler journal/projection | 每写全历史、全 projection | segmented authenticated log、dirty rows |
| `runtimes/multi_agent/resources.py` | 逻辑资源预留 | 每查询全 reservation | per-pool aggregate/index |
| `runtimes/multi_agent/worker_pool.py` | 并发、heartbeat、interrupt | gather barrier、FIFO HOL | completion stream、fit-aware fair queue |
| `agent_os/kernel/kernel.py` | Kernel tick、driver、process | 串行 await、maintenance 被慢 I/O 阻塞 | event-driven microkernel、budgeted queues |
| `agent_os/services/lease_service.py` | Lease/fencing/deadlock | 每 tick 全表建 wait-for、DFS cycle | incremental wait graph、Tarjan SCC |
| `agent_os/services/journal.py` | Kernel event log | 每 event 查询 PID MAX sequence | per-PID tail、batch allocator、checkpointed replay |
| `agent_os/services/signal_service.py` | durable mailbox | 全 pending scan + PCB N+1 | per-PID mailbox/index/join |
| `agent_os/services/outbox.py` | at-least-once delivery | 单 SQLite writer、scan clock；尚未覆盖所有主路径 | 全路径 intent/outbox integration |
| `agent_os/artifacts/service.py` | Artifact FS、CAS、watch | 多步 commit、watch 全扫、quota N+1、mount 线性 | commit saga/outbox、trie、aggregate SQL |
| `agent_os/context/service.py` | Context load/pin/snapshot | `O(PB)` I/O、假 eviction、无锁、无 GC | shared page cache、residency lease、real eviction |
| `sdk/runtime_state.py` | 跨平面状态投影 | 非原子 optimistic snapshot、全图分析 | Cross-Plane SnapshotToken、incremental analyzer |
| `sdk/os.py` | composition 与执行编排 | 巨型 facade、batch barrier、重复 runtime_state、默认绕过 Harness | epoch view、continuous scheduler、统一 adapter |
| `sdk/conflict_graph.py` | 显式冲突图 | 运行时线性查询、greedy MIS | adjacency/bitset、weighted MIS |
| `sdk/resource_policy.py` | packing 建议 | lexical first-fit、assignment 丢失 | joint placement contract |
| `sdk/compute_budget.py` | verified-progress/cost 排序 | 多维离散问题仍用 ratio greedy | primal-dual/knapsack oracle |
| `sdk/graph_analysis.py` | critical path/unlock | incremental analyzer 未接入；节点数路径 | weighted incremental path |
| `sdk/harness.py` | Harness ABI | identity 缺 Lease token；control 非原子 | full ownership identity、intent/ack |
| `sdk/subprocess_harness.py` | killable child | checkpoint 仅 marker；child tree/restore 有界 | process-group/container checkpoint boundary |
| `provenance/store.py` | provenance log | 每 append 全链/全文件 | segmented store、tail/index |
| `sdk/providers.py` | Facts/Artifact hash authority | 内容只在内存，重启不能恢复 Context bytes | durable content CAS/range supplier |
| `sdk/observability_service.py` | status view | 名义只读却可能 compile；Task×Edge 扫描 | fail-closed read-only、adjacency index |
| `agent_os/storage/sqlite.py` | DB wrapper | 单 connection/global lock 串行所有 read/write | read pool、snapshot transaction、writer queue |

## 13.11 跨平面一致性与并发问题

### 13.11.1 RuntimeState 不是原子跨平面快照

`runtime_state()` 对 VPG、Scheduler、Context 和 Resource 分别读取，主要通过 graph version 双读做乐观检查。

风险：

```text
VPG = version N
Scheduler = Claim 在另一时刻
Resource = reservation 在第三个时刻
Context = snapshot 在第四个时刻
```

即使最终 graph version 未变化，仍可能得到一个现实中从未同时存在的 mixed-version view。

建议定义：

```text
CrossPlaneSnapshotToken {
  graph_version
  graph_projection_hash
  scheduler_generation
  scheduler_event_tail
  resource_generation
  context_generation
  observation_epoch
}
```

Policy 决策和 Scheduler admission 都绑定该 token。无法获得一致快照时 fail closed 或重试。

需要的 adversarial benchmark：

- runtime_state 期间并发 graph commit；
- Claim acquire/release；
- resource reserve/release；
- Context rebase；
- watcher observation；
- 统计 mixed-view、retry 和 decision latency。

### 13.11.2 SQLite 单 connection/global lock

`src/lhos/agent_os/storage/sqlite.py` 使用一个 SQLite connection，并通过全局锁保护 query/transaction。即使开启 WAL：

- 读仍被同进程全局锁串行；
- 不能获得真正的多读者并发；
- Kernel、Artifact、Facts、Scheduler 共用文件时控制面容易成为单锁瓶颈。

建议：

- 单 writer queue；
- read-only connection pool；
- 每次 observation 使用显式 read transaction；
- immutable snapshot/read version；
- 事务边界外不共享 cursor；
- 用 generation/CAS 检测 stale writer。

### 13.11.3 GraphStore connection/cache 粗粒度

GraphStore 的 cache fingerprint 使用 connection 级 `total_changes` 和 `PRAGMA data_version`。任一 graph 或无关表变化可能使 cache 保守失效。

同时不同 graph 可各保留一份完整 projection，长时间创建大量 graph 会导致 RSS 随 graph 数增长。

建议：

- per-graph generation；
- global size-bounded LRU；
- projection model/serialized bytes 分层缓存；
- cold graph 只保留 authenticated root；
- cache metrics：hit rate、bytes、eviction、reload。

### 13.11.4 ContextService 缺少同步边界

ContextService 的 working sets、handles、snapshots、events、idempotency maps 主要是进程内 dict/list，多个 async/thread 调用可能交错：

- load；
- pin/unpin；
- evict；
- snapshot；
- close；
- cleanup。

需要：

- per-process/context lock；
- page-level generation；
- CAS update；
- pin/refcount 原子性；
- state-machine transition；
- durable or reconstructable projection。

## 13.12 长时程 retention、GC 与稳态内存

“Long Horizon”不能只测任务完成，还必须测 10k/100k event 后的稳态资源。

当前可能长期保留：

### Context

```text
_ws_by_pid
_handles_by_pid
_snaps
_idem_*
_events
LoadedContext bytes
```

close/cleanup 主要标记 closed，不删除 bytes、snapshot、working set 或历史 event。

### Scheduler

```text
all Claims
all Attempts
all Events
event index
match log
idempotency keys
residency union
```

### VPG/Provenance

虽然 VPG 有 history compaction primitive，但其他层没有统一 retention policy。

建议定义：

```text
hot state
warm projection
cold authenticated archive
tombstone
retention generation
```

GC 条件必须考虑：

- Evidence/history audit 是否仍需要；
- Claim/Attempt 是否可能被迟到消息引用；
- Harness request idempotency window；
- checkpoint/recovery floor；
- active reader snapshot；
- external effect reconciliation。

评测：

- 1k/10k/100k attempts；
- steady-state RSS；
- GC pause；
- lookup latency；
- reopen time；
- archive bytes；
- stale late-message rejection。

## 13.13 Artifact、Facts 与内容持久性

### 13.13.1 FactsProvider 只持久化 hash，不持久化内容

`sdk_artifact_facts` 持久化 Artifact version/hash，但实际内容 bytes 主要在内存 `_contents`。

结果：

- file-backed AgentOS 重启后能证明 hash/version；
- 但默认不能仅凭自身 DB 恢复非空 Context pages；
- 必须注入外部 content supplier。

这意味着“Context snapshot durable”与“Context bytes 可恢复”需要分开陈述。

建议：

- Artifact FS CAS 成为唯一内容权威；
- Facts 只做版本/hash/index；
- Context provider 使用 CAS range read；
- Evidence/ContextSnapshot 绑定 CAS content ref；
- 重启测试必须从零内存恢复 Context。

### 13.13.2 Artifact commit 是跨 driver/SQLite/journal 多步协议

Artifact commit 涉及：

```text
storage driver CAS commit
version row
artifact current_version
write transaction state
idempotency row
journal event
watch notification
```

它们不能共享一个跨文件系统和 SQLite 的原子事务。

当前 recovery 能处理部分 uncertain 状态，但论文需要正式定义：

- intent；
- linearization point；
- commit receipt；
- notification outbox；
- idempotency；
- crash matrix；
- duplicate notification 语义。

### 13.13.3 Handle quota 与首次写并发

Handle quota check 与插入不是一个统一 conditional transaction；并发 open 可能越过 quota。

首次 `begin_write` 创建 Artifact record 与 idempotency 检查的顺序也可能在并发首次写时产生唯一冲突或重复工作。

应使用：

- conditional insert；
- namespace usage counter；
- transaction-scoped idempotency；
- per-URI creation lease。

## 13.14 调度路径的隐藏重复工作

### 13.14.1 每个 candidate 重查 attempt history

Scheduler 虽在 pass 开头构建部分 `attempts_by_task`，但每个 candidate 仍调用 `count_attempts_for_epoch()` 全扫描 Attempt history。

复杂度：

```text
O(frontier × attempt_history)
```

应维护 `(graph, task, semantic_epoch) -> count`。

### 13.14.2 每个 claim 再取完整 READY frontier

Claim acquisition前重新调用完整 `ready_frontier()` 检查 membership。一个 pass 有 C 个 candidate 时可能形成：

```text
C × READY derivation
```

应使用：

- graph-version-bound READY membership index；
- conditional claim transaction；
- 或一次 frontier proof token，claim 时只验证 token/version/membership。

### 13.14.3 成功 dispatch 至少多次完整 durable publish

成功派发通常包含：

1. Claim proposed event；
2. Lease acquired + execution dispatched；
3. idempotency persistence。

在 durable store 当前全历史/全 projection 路径下，这会将单次 dispatch 写放大数倍。

建议：

- 一个 acquisition transaction；
- 多事件 batch；
- dirty Claim/Attempt rows；
- idempotency 同事务；
- 如果阶段状态必须持久，仍使用增量 rows 而非全 projection。

### 13.14.4 Provider routing 每 Task 重建 runtime_state

`_resolve_provider_route` 可能为每个 dispatched Task 调用 `runtime_state(goal)`，而 runtime_state 会加载和投影完整 Graph、Scheduler、Context、Resource。

一个 batch 会产生：

```text
O(tasks × global_state_size)
```

应在 epoch 开始构造一个 fenced `GlobalRuntimeState`，provider routing 共享该 snapshot。

### 13.14.5 Executor API introspection

Executor signature/API resolution 对稳定 callable 可缓存：

```text
callable identity -> resolved executor API/signature plan
```

避免每 Attempt 重复 inspect。

---

# 14. 顶会论文问题定义

## 14.1 推荐问题名称

> **Online Verified-Progress Scheduling under Mutable Shared State**

中文：

> **动态共享状态下的在线已验证进度调度**

这个名字比“Agent OS”更具体，也比“多 Agent scheduler”更能表达差异。

## 14.2 系统模型

系统包含：

```text
G_t = 动态 Task/Artifact/Evidence 图
H   = Harness sessions 集合
R_t = 资源与 provider 容量
C_t = Context residency
O_t = Claim/Lease/Attempt ownership
E_t = observations 和 external events
```

每个 Task `i` 具有：

```text
dependencies_i
read_set_i / write_set_i
required_evidence_i
resource_vector_i
context_set_i
estimated_remaining_work_i
success_probability_i
input_survival_probability_i
side_effect_class_i
harness_capabilities_i
```

## 14.3 语义状态

建议正式区分：

```text
OperationalComplete:
  Harness/executor 已返回

Verified:
  独立 verifier Evidence 对当前精确 ArtifactVersion 仍适用

Stale:
  曾经 Verified，但当前版本/依赖使 Evidence 不再适用

Committed:
  在有效 Claim/Lease/epoch/read-set fence 下发布到权威状态
```

这四种状态不能合并。

## 14.4 决策变量

每个在线决策点选择：

```text
x_i        是否运行 Task i
y_i,a      是否把 i 放到 Agent/Harness a
z_i,p      是否放到 pool/provider p
q_i        context allocation
k_i        continue/checkpoint/rebase/preempt
b          parallelism degree
```

## 14.5 优化目标

推荐主目标：

```text
minimize
  TimeToVerifiedGoal
+ λ1 * ExpectedDoomedCompute
+ λ2 * TokenCost
+ λ3 * DollarCost
+ λ4 * RepairWork
+ λ5 * ContextReloadCost
+ λ6 * PreemptionCheckpointOverhead
+ λ7 * ControlPlaneOverhead
```

也可等价写为 constrained verified-progress utility maximization。

## 14.6 硬约束

```text
dependency readiness
artifact/evidence applicability
conflict serializability
resource capacity
provider quota
unique authoritative Claim
live Lease/fencing
verifier-gated commit
side-effect recovery policy
fairness/starvation bound
```

安全性不能作为可被性能抵消的 soft penalty。

## 14.7 加速成立条件

LongHorizonOS 相对直接 Harness 的净收益必须满足：

```text
avoided doomed/repeated work
+ better critical-path utilization
+ better resource/context placement
+ selective repair saving
>
graph/provenance overhead
+ scheduling overhead
+ additional verification
+ isolation/staging/merge overhead
+ checkpoint/preemption overhead
```

这解释了为什么：

- 动态、有冲突、有长尾的 workload 可能显著受益；
- 稳定、短、串行、无变化的 workload 可能零收益或负收益。

## 14.8 可证伪研究假设

### H1：Work-conserving

在相同资源和同一 dispatch policy 下，事件驱动连续补位相对 batch barrier 降低：

- idle slot time；
- critical-path start delay；
- Time-to-Verified-Goal。

### H2：联合调度

相对“graph utility、conflict、resource 分开串联”的策略，联合 co-scheduler 在同资源下应：

- verified-progress AUC；
- batch utility；
- placement acceptance。

并降低：

- makespan；
- critical-path stretch；
- resource rejection 与 fragmentation。

### H3：Incremental coherence

局部 ArtifactVersion 变化时，增量 VPG/D3 的处理成本与 affected cone 成比例，而不是与全图规模成比例。

### H4：Semantic preemption

输入中途失效时，基于 remaining-work/payoff 的 preemption 比：

- never preempt；
- always preempt；
- fixed threshold；

获得更高净收益，同时保持 false stale commit 为零。

### H5：Context-aware placement

真实 residency lease + version-aware context placement 相对历史 read-set locality 降低：

- context reload；
- prompt/KV invalidation；
- Time-to-Verified-Goal。

### H6：Cross-Harness generality

同一 OS policy 在 DeepSeek Harness 与 Claude Code adapter 上均保持：

- safety invariants；
- 相似的动态 workload 改善趋势；

证明贡献不依赖单一 Harness。

## 14.9 可能的论文贡献

建议只保留 3-4 个主要贡献：

1. **问题定义**：Mutable shared state 下的 Online Verified-Progress Scheduling；
2. **系统设计**：Harness-agnostic semantic control plane + fenced commit；
3. **算法**：event-driven joint scheduling/placement/preemption；
4. **增量状态结构**：authenticated incremental VPG/coherence；
5. **评测**：跨 DeepSeek Harness/Claude Code、动态 mutation 和 crash campaign。

不要将十几个小 policy 都列为独立 contribution。

## 14.10 不应作为主要 novelty 的能力

以下能力 DeepSeek Harness、Claude Code 或既有系统已经覆盖很多：

- Agent loop；
- tool calling；
- subagent；
- multi-Agent parallel；
- workflow/task DAG；
- session resume；
- checkpoint/rewind；
- context compaction；
- prompt cache；
- sandbox/permission；
- background jobs；
- telemetry；
- MCP/plugin。

LongHorizonOS 的 novelty 必须集中在：

```text
ArtifactVersion/Evidence-backed validity
incremental semantic invalidation
verified-progress objective
Claim/Lease/fenced commit
cross-Harness joint scheduling
semantic preemption and selective repair
```

---

# 15. DeepSeek Harness 对比

## 15.1 名称、版本与资料边界

本文中的“DeepSeek Harness”规范指 DeepSeek AI 官方项目：

```text
DeepSeek Harness (dsh)
github.com/deepseek-ai/deepseek-harness
```

不是泛指“使用 DeepSeek 模型的任何 harness”。本节审查基于官方 `dsh-v0.1.0-rc.7` tag、commit `99f6f02fecdb7dff40c3fbc9470f5907c29f74ca`，发布日期为 **2026 年 8 月 17 日**；截至 **2026 年 8 月 18 日**，官方仍将其标为 developer preview。

说明：

- 产品/仓库名称：DeepSeek Harness；
- 命令/简称：`dsh`；
- Python SDK 发行包：`deepseek-harness-sdk`；
- Python import 名：`deepseek_harness`。

本节只比较公开官方文档和公开源码所描述的能力；不推断未公开的内部实现。

官方一手资料：

- [DeepSeek Harness README](https://github.com/deepseek-ai/deepseek-harness)
- [架构](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/architecture.md)
- [Agent loop](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/core/agent-loop/README.md)
- [Session persistence](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/session/session-persistence/README.md)
- [Checkpoint policy](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/session/session-checkpoint-policy/README.md)
- [Subagent](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/subagent/subagent/README.md)
- [Workflow worker](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/workflow/workflow-worker-thread/README.md)
- [Ralph](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/workflow/tool-ralph/README.md)
- [Sandbox](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/sandbox/sandbox/README.md)

## 15.2 DeepSeek Harness 的正确定位

DeepSeek Harness 是一个高度插件化的 Agent Harness/runtime：

```text
model adapter
+ tool registry
+ Session append-only event log
+ agent loop
+ compaction
+ sandbox/permission
+ subagent/workflow/jobs
+ SDK/ACP/CLI surface
```

其架构可理解为：

> **面向一个或一组 Agent session 的可组合执行微内核。**

它不是公开意义上的跨 Session 全局语义操作系统，因为公开设计中没有将以下对象作为权威一等状态：

```text
Task -> ArtifactVersion -> Evidence -> Verification -> Validity
跨 session Claim/Lease/fencing
全局多维资源 placement
跨 ArtifactVersion 的 stale-result rejection
最小失效锥与 repair frontier
Time-to-Verified-Goal 调度目标
```

这个判断不表示 DSH “没有调度/验证/恢复”。更准确地说：

> DSH 已有强大的 session-local tool、subagent、workflow、日志和协议验证；但公开设计没有跨任务语义有效性、版本化证据和 verified-progress 联合调度器。

## 15.3 DeepSeek Harness 已有能力：不能作为 LongHorizonOS novelty

| DeepSeek Harness 能力 | 公开边界 | 对 LongHorizonOS 的含义 |
|---|---|---|
| Agent turn/step loop | prompt、model、tool pipeline、cancel/retry | 不能说“首次有 agent loop” |
| 工具并发 | exclusive barrier + parallel-safe rolling pool，默认有并发上限 | 不能说“首次支持工具并发” |
| append-only Session log | 日志驱动历史、resume/fork、crash-tail repair | 不能说“首次 session persistence” |
| compaction | token pressure 下 prune/summarize | 不能说“首次 context compaction” |
| Subagent | one-shot、continuable、spawn/fork、background、interrupt | 不能说“首次多 Agent/subagent” |
| Workflow | JavaScript orchestration、fan-out、pipeline、worker thread | 不能说“首次 workflow/DAG” |
| Ralph | fresh child rounds + shared workspace + handoff | 不能说“首次 long-running iteration” |
| Sandbox/permission | file-effect policy、workspace modes、后端 fail-closed | 不能说“首次 sandbox” |
| Jobs/schedule | owner-scoped background jobs、session-local reminder | 不能说“首次 background work” |
| SDK/ACP | programmatic/headless/UI-independent interfaces | 不能说“首次 SDK/ACP” |

## 15.4 DeepSeek Harness 的关键局部限制

### 15.4.1 Tool parallelism 不等于全局调度

DSH 的并发主要发生在一个 Agent step 的 tool-call group：

```text
parallel-safe tool -> bounded rolling pool
exclusive tool -> barrier
result -> model order commit
```

这是单 Agent 内部执行优化，而不是：

```text
跨 Task frontier
跨 Artifact conflict
跨 Agent resource placement
跨 session semantic invalidation
```

此外 tool classification 是 unary 的。若一个 tool 的安全性依赖 sibling 的 read/write set，DSH 文档建议保守地将其视为 exclusive，而不是做全局 conflict analysis。

### 15.4.2 Workflow 是 caller-owned foreground fan-out

DSH workflow 有 `parallel()` 和 `pipeline()`，但公开限制包括：

- 没有 workflow journal；
- 没有 workflow resume；
- 没有 workflow-level token/dollar budget；
- 没有全局 resource placement；
- 没有 ArtifactVersion/Evidence validity；
- parent turn 等待 workflow settle；
- 并发 slot 是局部 FIFO resource。

因此它适合：

```text
一个 Agent 写脚本并行调用 child
```

而不是：

```text
长期动态共享状态下的权威全局 scheduler
```

### 15.4.3 Ralph 是 fresh-agent 顺序轮次，不是 verified repair runtime

Ralph 每轮启动 fresh child，使用共享 workspace 和 bounded structured handoff。公开限制包括：

- completion/blocker 由 worker self-report；
- 没有 independent evaluator；
- 没有 scheduler；
- 没有 process-resume checkpoint；
- 没有 wall-clock/token/price budget；
- 无自动 retry 的普通 child failure；
- 主要是 foreground sequential loop。

这与 LongHorizonOS 的目标形成直接对比：

```text
Ralph:
  workspace continuation + fresh round

LongHorizonOS:
  preserve verified branch
  -> invalidate only affected cone
  -> derive repair frontier
  -> schedule fresh Attempts
  -> reject stale result
```

### 15.4.4 Session checkpoint 不是通用 exactly-once

DSH checkpoint policy 在模型请求、顶层副作用工具和 pre-step 前做 durability barrier，并在 crash 后标记 `TOOL_OUTCOME_UNKNOWN`。

这是好的 fail-closed session durability，但官方明确其记录的是 intent，不是通用 exactly-once 外部效果。

LongHorizonOS 不能简单声称“比 DSH 更 durable”；应将差异精确写为：

```text
DSH:
  session-level execution intent and recovery

LongHorizonOS target:
  Task/Attempt-level input versions
  + Claim/Lease epoch
  + staged output
  + verifier Evidence
  + fenced authoritative commit
```

### 15.4.5 子 Agent workspace conflict 仍由模型协调

DSH 对 parallel sibling 的 workspace effect 没有公开的全局 read/write conflict admission。公开接口将协调责任主要留给模型或调用方。

LongHorizonOS 可在此处提供：

```text
declared/observed read set
declared/observed write set
ConflictGraph
serializable admission
optimistic execution with commit validation
versioned staged merge
```

### 15.4.6 Jobs、sandbox、SDK 的边界

DSH jobs 是进程内、owner-scoped registry，不是跨进程全局 scheduler。

DSH sandbox 主要控制文件 effect/workspace，不是：

- global resource controller；
- network/device credential policy；
- Task authority；
- Claim/Lease；
- semantic commit fence。

DSH SDK/ACP 也不自动提供 LongHorizonOS 所需的精确 task-level control：

- SDK 的高层 run 边界更接近 message admission 到 whole-agent idle；
- ACP 侧重点是 transport/session UI，而非长期 Task/Artifact graph；
- one-shot external provider 可能只有 final text，没有 progress、usage、workspace diff、resume 或 rollback。

## 15.5 DeepSeek Harness 与 LongHorizonOS 的边界表

| 层 | DeepSeek Harness | LongHorizonOS 应负责 |
|---|---|---|
| 模型 | model adapter、stream、tool selection | 不重做模型能力 |
| 单 Agent loop | turn/step/tool lifecycle | 是否启动/继续此 Attempt |
| 工具 | registry、sandbox、permission、局部并发 | 跨 Harness admission 与 effect authority |
| Subagent | child spawn/fork/continuation | Task/Attempt placement 与 ownership |
| Workflow | caller/model 写的 fan-out/pipeline | authoritative frontier scheduling |
| Session | event log、resume、compaction | cross-session semantic state |
| Checkpoint | session durability、intent/recovery | version-aware Attempt checkpoint |
| Completion | worker/tool/goal report | verifier Evidence 与 Goal closure |
| Workspace | shared workspace + caller/model coordination | ArtifactVersion、conflict、staged publish |
| Resource | concurrency cap、round cap、single request max tokens | CPU/GPU/model slot/RPM/TPM/token/$ placement |
| Recovery | local session crash repair | stale repair、Lease recovery、side-effect reconciliation |

一句话：

> **DeepSeek Harness 是高能力的插件化 Agent Harness；LongHorizonOS 应成为多个 DSH/Claude Code/其他 Harness 之上的语义一致性、资源准入和 verified-progress 控制平面。**

## 15.6 LongHorizonOS 相对 DSH 的可辩护优势

下面区分“当前已部分实现”和“论文目标”，不能混写。

| 维度 | 当前仓库已有基础 | 论文目标优势 |
|---|---|---|
| Verified completion | VPG、Evidence、verifier、exact-version binding | Goal closure 不依赖 worker self-report |
| Selective repair | D3 cone/frontier、Artifact version invalidation | 只重做 affected cone，保留 verified branch |
| Stale commit rejection | SDK path 有 Claim/Attempt/epoch/fencing guard | 所有 Harness/tool publish 都经过同一 fence |
| Conflict control | explicit access ConflictGraph | DSH sibling workspace conflict 不再依赖模型自觉 |
| Global scheduling | Frontier/parallelism/resource/budget policy primitives | 联合 validity/conflict/resource/context/rework 调度 |
| Preemption | bounded cooperative interrupt/optional subprocess kill | remaining-work + stale-risk 驱动的语义抢占 |
| Cross-Harness | 目前尚未实现 DSH adapter | 同一 policy 可管理 DSH、Claude、direct executor |
| Recovery | VPG/Scheduler durable primitives | global intent/ack、attempt/lease-aware recovery |

最强的表述不是“DSH 没有这些能力”，而是：

> DSH 优化一个 Agent session 内的执行；LongHorizonOS 优化多个 session 在动态共享版本状态下，为达成可验证 Goal 所花费的总有效计算。

---

# 16. Claude Code 对比

## 16.1 资料边界

本节基于截至 **2026 年 8 月 18 日**的 Anthropic 官方 Claude Code 文档、官方 GitHub 更新记录和官方产品说明。Claude Code 迭代很快，部分能力标注为 experimental/research preview；论文中必须写明所用版本、实验日期、平台和 feature flag。

官方资料：

- [How Claude Code works](https://code.claude.com/docs/en/how-claude-code-works)
- [Subagents](https://code.claude.com/docs/en/sub-agents)
- [Agent Teams](https://code.claude.com/docs/en/agent-teams)
- [Agent View](https://code.claude.com/docs/en/agent-view)
- [Hooks](https://code.claude.com/docs/en/hooks)
- [Context window](https://code.claude.com/docs/en/context-window)
- [Memory](https://code.claude.com/docs/en/memory)
- [Checkpointing](https://code.claude.com/docs/en/checkpointing)
- [Permissions](https://code.claude.com/docs/en/permissions)
- [Sandboxing](https://code.claude.com/docs/en/sandboxing)
- [MCP](https://code.claude.com/docs/en/mcp)
- [Sessions](https://code.claude.com/docs/en/sessions)
- [Monitoring usage](https://code.claude.com/docs/en/monitoring-usage)
- [Agent SDK](https://code.claude.com/docs/en/agent-sdk/overview)

## 16.2 Claude Code 已经具备的 OS-like 能力

Claude Code 不是简单 CLI loop。公开能力包括：

```text
agentic loop
subagents with isolated context
experimental agent teams with task dependency
background agents / Agent View supervisor
Git worktree isolation
resume / fork / branch
file checkpoint / rewind
permission modes and Bash sandbox
hooks lifecycle control
MCP
context compaction and prompt caching
token/cost/rate-limit/OTel observability
```

因此论文中不能把以下作为 LongHorizonOS 主要新颖性：

- 首次多 Agent；
- 首次 coding task DAG；
- 首次 checkpoint/resume；
- 首次 sandbox；
- 首次后台 agent；
- 首次 token/cost observability；
- 首次 hook 或 MCP。

## 16.3 Claude Code 的正确定位

Claude Code 官方将自己描述为包裹 Claude 模型的 agentic harness，提供工具、上下文管理、执行环境和连续 agent loop。

最合理的层次划分是：

```text
Claude Code:
  在一个 session 或局部 team 中决定如何推理、调用工具、编辑和验证

LongHorizonOS:
  决定一个 session/Attempt 是否值得开始、继续、暂停、废弃、
  rebase、抢占或允许提交
```

## 16.4 Claude Code 的关键限制与 LongHorizonOS 机会

### 16.4.1 Agent Teams 有任务依赖，但不是版本化语义图

Claude Code Agent Teams 可提供共享 task list、task dependency、独立 teammate context 和消息通信。但公开文档也明确其是 experimental，并列出：

- task status 可能滞后；
- `/resume`/`/rewind` 不恢复 in-process teammate；
- teammate 关闭要等待 API/tool call；
- workspace file 冲突主要依赖人工拆分所有权；
- 没有 nested teams；
- lead 不可转移。

LongHorizonOS 的差异不是再做一张 task table，而是：

```text
Task completion
!= semantic validity

ArtifactVersion/Evidence change
-> applicability loss
-> STALE cone
-> Repair Frontier
-> verifier-gated reclosure
```

### 16.4.2 Hooks 很强，但不是完整可靠内核

Claude Code hooks 可观察/干预 Session、Tool、Subagent、Task、Worktree、FileChanged 等生命周期，适合快速原型：

- 注入 attempt identity；
- 记录 tool/read/write trace；
- 在 `PreToolUse` 做授权；
- 在 TaskComplete 后运行 verifier；
- 观察 subagent 生命周期；
- 文件变化触发 invalidation。

但 hooks-only 不足以支撑强 OS 语义：

- 部分 async hook 不等待，其控制字段无效；
- 某些 command/HTTP/MCP hook 超时后会回到正常 permission flow；
- `@file` 等 prompt 构建输入不一定经过 Read hook；
- 复杂外部效果仍可能绕过 hook；
- 错误 exit code/decision 配置可能导致 gate 失效。

因此：

```text
hooks = observation/prototype seam
not = sole authoritative control plane
```

### 16.4.3 Context memory/compaction 不是版本化跨 Agent residency

Claude Code 有：

- `CLAUDE.md`；
- auto memory；
- context compaction；
- exact prefix prompt cache；
- subagent context isolation。

这些解决“单个 session 如何保持可用 context”。

LongHorizonOS 可研究：

```text
多个 session
+ Artifact version
+ context page residency
+ critical-path future value
+ reload cost
+ prefix/KV preservation
+ stale risk
```

即：

> 哪些可验证、版本固定的页面应该在哪个 Agent/Harness 上驻留；输入变化时应精确撤销、替换还是迁移哪些页面。

### 16.4.4 Claude checkpoint/rewind 不等于 semantic checkpoint

Claude Code checkpoint 主要是面向交互用户的文件和 conversation undo。公开限制包括：

- 主要跟踪 Claude 的 Edit/Write；
- Bash 引起的文件变化可能不受覆盖；
- background/subagent/外部人工写入可能不完整；
- database/API/deployment 等远端效果不可回滚；
- 不是 version control 替代。

LongHorizonOS 应强调：

```text
Claude Code:
  回到之前的 conversation/file state

LongHorizonOS:
  判断一个 checkpoint 是否仍对当前 ArtifactVersion/read set/Lease epoch 有效，
  并决定复用、局部 repair 或废弃。
```

### 16.4.5 Permission/sandbox 与 semantic authority 是互补关系

Claude Code permission/sandbox 回答：

```text
这个工具动作是否允许执行？
```

LongHorizonOS 应回答：

```text
这个 Attempt 在当前 graph version、Claim、Lease、read set 和
replacement epoch 下，是否仍有权产生可提交效果？
```

二者不能互相替代。

### 16.4.6 Session resume 不等于 Attempt continuity

Claude Code 可恢复 conversation/session，但 LongHorizonOS 仍必须验证：

```text
ArtifactVersion 是否变化
Claim 是否仍有效
Lease 是否过期
replacement Attempt 是否接管
checkpoint read set 是否仍成立
外部 effect 是否 uncertain
```

## 16.5 Claude Code 与 LongHorizonOS 的边界表

| 层次 | Claude Code | LongHorizonOS 应负责 |
|---|---|---|
| 推理 | Claude 模型、tool choice | 不重做 |
| 单 Agent loop | 搜索、编辑、运行、局部 verify | 是否允许 Attempt 继续 |
| 局部 context | history、memory、compaction、prompt cache | 跨 Agent versioned residency |
| 局部并行 | subagent、team、background session | global frontier、placement、admission |
| Task | task list/依赖/status | Artifact/Evidence-backed validity |
| 文件隔离 | worktree、checkpoint | staged output + fenced authoritative publish |
| 权限 | tool/file/domain allow/deny | Claim/Lease/epoch/graph-version authority |
| 恢复 | resume、rewind、supervisor restart | semantic repair、side-effect recovery |
| 可观测性 | token、cost、duration、OTel | verified-progress closed-loop optimization |
| 外部连接 | MCP、hooks、SDK | cross-Harness semantic control plane |

## 16.6 LongHorizonOS 相对 Claude Code 的可辩护优势

论文必须把“当前实现”与“目标实现”分开。

### 当前已有的可证明基础

- VPG 的 READY/VERIFIED/STALE；
- exact-version Evidence applicability；
- causal invalidation cone；
- repair frontier；
- Scheduler Claim/Attempt；
- Kernel Lease/fencing；
- 部分 commit-time freshness guard；
- bounded async interrupt；
- optional killable subprocess；
- durable Scheduler/VPG primitives。

### 尚需实现才能成为实验优势

- 所有 Claude session 统一映射到 Attempt/Claim/Lease；
- staged worktree/authoritative merge；
- actual read/write trace；
- placement contract；
- event-driven refill；
- durable control intent/ack；
- context residency lease；
- cross-Harness adapter。

最强但必须通过实验验证的优势是：

1. **Verified completion，而非局部 self-report completion**；
2. **ArtifactVersion 变化后的 selective repair**；
3. **stale session 的 fenced commit rejection**；
4. **conflict/resource/context/rework 联合调度**；
5. **跨 Harness 一致的语义状态和安全不变量**。

---

# 17. 外置套接能否加速

## 17.1 简短结论

> **不能假定“在 DeepSeek Harness 或 Claude Code 外面套一层 LongHorizonOS 就会加速”。**

如果只是：

```text
wrapper
  -> start harness process
  -> wait final answer
  -> log result
```

通常会增加：

- process/session startup；
- Graph/VPG snapshot；
- JSON/SQLite；
- hook/IPC；
- verifier；
- worktree merge；
- control decision；
- audit log。

稳定、短、单 Agent、无共享状态变化的 workload 很可能更慢。

## 17.2 净收益条件

令：

```text
W_avoided =
  avoided doomed work
  + avoided repeated repair
  + critical-path start gain
  + parallel overlap gain
  + context reuse gain

O_control =
  graph/provenance observation
  + scheduling
  + verification
  + staging/merge
  + checkpoint/preemption
  + persistence
```

只有当：

```text
W_avoided > O_control
```

才应期待净加速。

更精确地：

```text
NetTimeGain =
  BaselineTime
  - LongHorizonOSTime

LongHorizonOSTime =
  useful_compute
  - avoided_doomed_compute
  - overlap_gain
  + control_overhead
  + verification_overhead
  + isolation_merge_overhead
```

论文必须报告 break-even surface，而不是只报告一个正例。

## 17.3 何时可能加速

| Workload 特征 | 外置 LongHorizonOS 的潜在收益 |
|---|---|
| 输入在执行中发生版本变化 | 停掉 doomed compute，局部 repair |
| 多 Agent 共享文件/API | conflict admission、stale merge 拒绝 |
| 有长关键路径 + 大量宽任务 | critical-path-aware dispatch |
| 资源异构或预算紧张 | placement/admission/utility optimization |
| 多次相近任务使用相同 context | residency-aware placement/page reuse |
| 大量已验证 branch + 局部变化 | 保留 branch，避免 full restart |
| 长尾 Task 在接近完成时失效 | payoff-aware preemption/rebase |
| 失败、Lease expiry、迟到返回 | fenced commit/recovery |

## 17.4 何时不会加速，甚至应变慢

| Workload 特征 | 原因 |
|---|---|
| 单个短 Task | 控制面固定成本占主导 |
| DAG 静态且已知、无 mutation | 好的静态计划接近最优 |
| 强串行依赖 | 没有可利用并行度 |
| 所有 Task 同成本且对称 | 排序没有信息增益 |
| Harness 内部已完成最优并行 | 外层只能增加协调开销 |
| read/write/provenance 不可观测 | 必须 serial-only/fail closed |
| 外部副作用不可隔离/不可幂等 | 不能安全 speculative/preempt |
| 使用黑盒 CLI，只得到 final text | 无细粒度状态，无法精确抢占/修复 |

这与当前 README 的诚实 null result 一致：稳定对称 workload 上 adaptive 应与静态持平或略慢，而不是虚构普适加速。

## 17.5 三种接入深度

### 17.5.1 黑盒 CLI wrapper

```text
LongHorizonOS
  -> CLI process
  -> final text / exit code
```

可做：

- 粗粒度任务划分；
- 并行启动；
- workspace 隔离；
- 超时/kill；
- final verifier；
- 外层结果汇总。

做不到：

- 精确 read/write set；
- mid-turn remaining work；
- session state/rebase；
- tool-level fencing；
- precise usage/progress；
- safe in-place shared workspace commit；
- durable task-level resume。

适合 baseline 或快速原型，不适合主论文系统。

### 17.5.2 Hooks/MCP/ACP wrapper

可增加：

- lifecycle observation；
- tool trace；
- attempt identity injection；
- basic permission gate；
- task/event messages；
- cooperative cancel。

但仍有问题：

- 模型可能不调用 MCP；
- hook 可能 timeout/fail-open 或无控制权；
- 部分输入不经过 hook；
- opaque internal subagent/tool state；
- external side effect 无法由 wrapper 逆转。

适合兼容层，不能单独作为 authoritative OS。

### 17.5.3 Native SDK/plugin adapter

这是推荐论文路径。

```text
LongHorizonOS Controller
  -> HarnessAdapter
      -> DeepSeek Harness plugin/SDK event seam
      -> Claude Agent SDK / controlled Claude Code hooks
  -> staged workspace/CAS
  -> verifier
  -> fenced authoritative publish
```

Adapter 最低接口：

```text
start_attempt
observe_event_stream
collect_read_write_trace
checkpoint
interrupt
rebase_or_restart
resume
collect_usage
collect_candidate_outputs
terminate_and_reap
close
```

每个 event/side effect 必须携带：

```text
task_id
attempt_id
claim_id
lease_id
fencing_token
graph_version
semantic_epoch
input_artifact_versions
workspace/worktree/process identity
```

### 17.5.4 深度集成：staged commit

真正的权威路径应是：

```text
Harness writes candidate in isolated worktree/CAS
  -> collect actual reads/writes
  -> independent verifier
  -> validate graph/read-set/Claim/Lease fence
  -> atomic or saga-authorized publish
  -> Evidence attach
  -> VPG VERIFIED
```

Harness 不应直接写最终 authoritative workspace 后再由 OS 事后猜测。

## 17.6 MCP 的正确位置

MCP 可作为 syscall/data-plane：

```text
claim_task
read_artifact
write_candidate
report_evidence
heartbeat
request_checkpoint
query_runtime_state
```

但 MCP 不能是唯一 authority，因为模型可以：

- 忘记调用；
- 走其他工具；
- 直接写 workspace；
- 在 tool call 外读取输入。

因此：

```text
MCP = explicit data plane
SDK/plugin/hooks/process isolation = control and enforcement plane
```

## 17.7 DeepSeek Harness 推荐接法

优先使用 DSH plugin/SDK seam：

- `agent/*` lifecycle；
- `tools/*` pre/post execution；
- session event stream；
- subagent provider；
- jobs；
- session flush/checkpoint barrier。

LongHorizonOS 在外部维护 Task/Artifact/Evidence/Claim/Lease，而 DSH 保持单 session loop。

DSH 的 Claude Code provider 是 one-shot final-text integration，不能成为细粒度 LongHorizonOS 主路径；它缺 continuation/resume/progress/usage/diff/rollback 等关键状态。

## 17.8 Claude Code 推荐接法

优先级：

1. Claude Agent SDK：最适合程序化 session、usage、hook 和 event stream；
2. Claude Code hooks + staged worktree：适合兼容原型；
3. headless CLI：适合粗粒度 baseline；
4. MCP：数据面，不单独承担 authority。

Claude Code 的 Agent Teams/Agent View 可作为强 baseline，而不是被忽略的“普通 Harness”。

---

# 18. 距离真正 LongHorizonOS 还差什么

本节区分当前 `v0.1.x` 已有基础、需要优先闭合的单机系统路径，以及更远期的分布式/产品能力。

## 18.1 当前已具备的基础

当前仓库已有：

```text
VPG exact-version Evidence
READY / VERIFIED / STALE / closure
causal invalidation cone and repair frontier
Claim/Attempt + Kernel Lease/fencing
logical resource admission
durable Scheduler/VPG pieces
Context VM snapshot/binding primitives
conflict/resource/budget/interrupt policy primitives
bounded async interrupt
optional subprocess kill boundary
workspace provenance/watch primitives
transactional outbox primitive
```

这些足以支撑“单机语义控制平面原型”的准确表述。

## 18.2 P0：必须闭合，才能成为 harness-aware single-host OS

### P0-1：统一执行 substrate

当前默认 `run/run_async` 直接调用 `Agent.executor`，Harness registry 是 opt-in。

需要：

```text
all execution
  -> HarnessAdapter
  -> Process/Attempt/Claim/Lease
```

direct callback 也作为内置 Harness adapter，而不是旁路。

### P0-2：完整 execution identity

当前 Harness identity 缺：

```text
process_id
lease_id
lease_fencing_token
workspace/worktree identity
```

每次 control 和每次权威 publish 前都应检查 live Lease。

### P0-3：control intent -> effect -> ack

当前 `control_harness` 顺序为：

```text
await harness.control
-> record HARNESS_CONTROL
```

需要改为：

```text
durable CONTROL_INTENT
-> adapter execute
-> durable CONTROL_ACK
-> reconcile unknown on crash
```

必要时结合 outbox/saga。

### P0-4：staged output + fenced publish

所有 Harness 写入都应：

```text
isolated candidate workspace/CAS
-> verifier
-> freshness/Lease check
-> authoritative publish
```

这是避免 stale session 直接污染 workspace 的关键。

### P0-5：placement-admission contract

Task 选择、Agent/pool/provider placement、resource vector 不能分离成 advisory policy 和另一次 Scheduler matching。

### P0-6：事件驱动 continuous scheduling

必须去除 batch barrier，在 completion/failure/invalidation 后立即 refill。

### P0-7：真实 Context state

需要：

- durable content CAS；
- real page eviction；
- page residency lease；
- close/cleanup 真正释放 bytes；
- Context snapshot 跨进程恢复。

### P0-8：checkpoint semantics 明确化

分开定义：

```text
event durability checkpoint
logical session checkpoint
workspace snapshot
Context snapshot
executable process checkpoint
```

当前不能将 marker 或 logical snapshot 称为 process checkpoint。

## 18.3 P1：论文系统需要的可扩展性和算法闭环

1. Incremental authenticated VPG；
2. Scheduler delta journal；
3. global/cross-plane snapshot token；
4. weighted critical path；
5. joint conflict-resource-budget-placement scheduler；
6. semantic preemption/rebase payoff；
7. context-aware placement；
8. priority inheritance；
9. semantic congestion control；
10. retention/GC；
11. real host/device/resource enforcement；
12. full provenance gateway；
13. external effect idempotency/reconciliation。

## 18.4 P2：更远期，不应先承担

```text
multi-host consensus
leader election
multi-writer Scheduler
distributed artifact consistency
cross-cluster placement
container/microVM fleet
production multi-tenancy
hosted control plane/dashboard
universal arbitrary-I/O provenance
general belief revision
```

这些可以是后续路线，但不应阻止单机系统论文。

## 18.5 成熟度阶梯

| 阶段 | 定义 | 当前状态 |
|---|---|---|
| M0 | 语义控制 primitives | 已有较多基础 |
| M1 | 单机 harness-integrated control plane | 部分完成，默认路径仍未统一 |
| M2 | authoritative single-host Agent OS | 需完成 placement/fence/intent/context/continuous scheduler |
| M3 | 顶会论文系统 | 需算法、adapter、scale/failure/real-harness 评测 |
| M4 | 分布式生产 OS | 未来工作 |

## 18.6 当前最准确的状态描述

截至本次审查，最诚实的表述仍是：

> **LongHorizonOS 是一个已实现部分语义有效性、选择性修复、逻辑资源准入和 fenced ownership 的单机研究原型；它尚未成为默认以 Harness 为 substrate、拥有完整跨平面原子性和真实资源 enforcement 的 Agent OS。**

---

# 19. 论文实验矩阵与决策门

## 19.1 四条实验线

### Track A：算法质量

目的：

- 验证 co-scheduler 不只是更多 Agent；
- 衡量 approximation gap；
- 找到何时该并行、何时该串行、何时该抢占。

方法：

- 小 frontier 使用 ILP/CP-SAT/branch-and-bound oracle；
- 大 frontier 使用同一随机 realization；
- 扫 conflict density、resource heterogeneity、churn、duration variance。

### Track B：系统扩展性

目的：

- 验证增量 VPG/Scheduler/Context 的复杂度；
- 分离算法收益与 Python/SQLite 实现开销。

测：

```text
VPG: 100 -> 10k nodes
Scheduler: 100 -> 100k events
Context: 1 MB -> 100 MB artifact, 4 KB -> 256 KB pages
Provenance: 1k -> 1M events
```

### Track C：真实 Harness

必须固定：

```text
same model
same prompt
same tools
same verifier
same workspace snapshot
same Agent count
same token/$/resource budget
```

对比：

```text
DeepSeek Harness standalone
DeepSeek Harness native parallel/workflow/Ralph
Claude Code standalone
Claude Code subagent/team/static parallel
passive wrapper
LongHorizonOS scheduler-only
LongHorizonOS + invalidation
LongHorizonOS + invalidation + preemption + fenced commit
oracle where feasible
```

### Track D：安全与恢复

逐点故障注入：

- artifact commit；
- Claim/Lease；
- Harness control；
- executor；
- verifier；
- Evidence commit；
- publish；
- outbox ACK；
- watcher duplicate；
- process descendant；
- context restore。

## 19.2 最小可发表实验任务

若时间有限，先完成四组：

### E1：Batch barrier vs event-driven refill

同一 static scheduling policy、同一 resources。

构造：

```text
short task unlocks critical child
long straggler in same original batch
```

主指标：

- idle slot time；
- critical child start delay；
- TTVG；
- policy overhead。

### E2：Joint scheduler vs sequential greedy

同一 frontier，比较：

```text
lexical conflict greedy
graph utility only
resource first-fit
current unified policy
proposed co-scheduler
ILP oracle
```

构造：

- star conflict；
- heterogeneous pools；
- high-unlock low-ratio task；
- weighted critical path；
- budget fragmentation。

### E3：Incremental coherence vs current full refresh

比较：

```text
current VPG/D3
incremental dirty cone
full recompute oracle
```

构造：

- 1% / 10% / 50% / 100% affected；
- chain、diamond、fanout；
- 100 到 10k nodes。

主指标：

- commit/repair p50/p95/p99；
- bytes read/written；
- CPU/RSS；
- output/event/hash equivalence。

### E4：Harness mutation workload

固定一个真实 DSH 或 Claude Code adapter。

构造：

```text
Agent A/B read shared input
writer publishes v2 while readers work
one independent observer branch
long task becomes stale at 90%
```

对比：

```text
native harness
passive wrapper
LHOS no-preempt
LHOS payoff preempt
oracle
```

主指标：

- Time-to-Verified-Goal；
- doomed compute；
- stale token；
- repair work；
- preemption net payoff；
- false/stale commit；
- control overhead。

## 19.3 公平 baseline 的不可妥协规则

不得混淆：

```text
更多 Agent
更强模型
不同 prompt
不同 verifier
不同 worktree
不同 resource cap
不同 provider rate limit
```

与“OS algorithm improvement”。

对每个 arm 固定：

```text
model and version
temperature/effort
tools/MCP
permission/sandbox
workspace snapshot
Agent count
parallel capacity
token/$ budget
verifier
seed/realization
timeout
```

## 19.4 动态 workload 必须覆盖的区域

| 维度 | 建议水平 |
|---|---|
| Task 数 | 20、100、500、2k、10k |
| Agent 数 | 1、2、4、8、16 |
| pool 数 | 1、2、8 |
| conflict density | 0、0.01、0.1、0.5、0.9 |
| mutation rate | 0、低、中、高 |
| mutation timing | 0%、25%、50%、90% progress |
| affected ratio | 1%、10%、50%、100% |
| duration CV | 0、0.5、1、2 |
| context overlap | 0%、25%、50%、75%、100% |
| provenance coverage | complete、partial、unknown |
| failure rate | 0%、1%、5%、20% |

## 19.5 核心指标

### Safety

```text
false_verified
false_goal_closure
stale_commit
overlapping_authoritative_claim
lease_fence_violation
resource_overcommit
duplicate_irreversible_effect
orphan_process
orphan_lease
replay_divergence
```

所有 safety metric 必须为零或显式解释，不可用平均性能抵消。

### Performance/utility

```text
Time-to-Verified-Goal
Verified Progress / second
Verified Progress / token
Verified Progress / dollar
verified-progress AUC
makespan
critical-path stretch
work-conserving idle fraction
queue wait p50/p95/p99
resource utilization
fragmentation
```

### Wasted work

```text
doomed compute ms
stale/repeated tokens
repair/full-restart ratio
rework amplification
preemption precision/recall
harmful preemption
net preemption payoff
context reread bytes/tokens
unused materialized Context bytes
```

### Control-plane overhead

```text
policy latency
VPG commit latency
Scheduler append/reopen latency
SQLite bytes/transactions
CPU/RSS
Context I/O
checkpoint bytes/time
adapter/hook latency
worktree merge latency
```

## 19.6 统计方法

合成 workload：

- 至少 30 个真正不同 seed；
- common random numbers；
- 所有 policy 使用同一 realization；
- 报告 distribution，不只均值。

真实 workload：

- paired repetition；
- randomized/alternating arm order；
- warm-up 分离；
- median、p95、p99；
- bootstrap 95% CI；
- paired permutation 或 Wilcoxon；
- effect size；
- 多 workload geometric mean；
- 保存 raw trace、git commit、机器/OS/Python/model/provider 元数据。

## 19.7 论文接受/停止门

在进入写作前，建议通过以下门：

| Gate | 最低要求 |
|---|---|
| G1 Safety | 动态/崩溃矩阵中无 false VERIFIED/stale commit |
| G2 Algorithm | 与同资源 static/greedy 比有统计显著 TTVG 或 doomed-work 改善 |
| G3 Null case | 稳定对称 workload 中不虚假宣称加速，明确 overhead |
| G4 Scale | 增量实现随 affected cone 而非全图增长 |
| G5 Harness | 至少一个真实 DSH 或 Claude adapter 的端到端动态 mutation 结果 |
| G6 Generality | 同一语义模型可映射至少两类 Harness 或一个 Harness + generic adapter |
| G7 Reproducibility | raw traces、seeds、环境、脚本、oracle 可复现 |

---

# 20. 建议论文摘要骨架

以下不是最终摘要，而是避免论文跑偏的写作骨架。

## 20.1 推荐标题方向

```text
LongHorizonOS: Verified-Progress Scheduling for Agent Harnesses under Mutable Shared State
```

或：

```text
Scheduling What Is Still Worth Computing: Evidence-Backed Control for Long-Horizon Agent Harnesses
```

## 20.2 问题句

> Long-running agent harnesses can persist sessions, spawn subagents, and recover local execution, but they do not generally determine whether in-flight work remains semantically worth completing after shared artifacts, requirements, or evidence change.

## 20.3 核心洞察

> Operational completion is not verified progress. A task result is reusable only while the exact versions and evidence that justify it remain applicable.

## 20.4 系统句

> LongHorizonOS is a harness-agnostic semantic control plane that represents task progress as a versioned evidence graph, fences execution with Claim/Lease ownership, incrementally invalidates only affected work, and jointly schedules placement and preemption toward Time-to-Verified-Goal.

## 20.5 贡献句

建议只写 3-4 条：

1. versioned Evidence/Artifact semantic state and fenced commit model；
2. incremental semantic coherence plus event-driven verified-progress co-scheduler；
3. Harness adapter/staged commit protocol；
4. evaluation across dynamic mutation, real harness execution, scale, and crash recovery。

## 20.6 绝不能写成的 claim

不要写：

```text
the first Agent OS
the first multi-agent scheduler
the first harness with checkpoint
the first agent sandbox
universal acceleration
exactly-once arbitrary side effects
transparent process migration
production distributed OS
```

建议写：

```text
for workloads with mutable shared inputs, explicit/mediated provenance,
and verifiable task outputs, LongHorizonOS reduces doomed computation
and selective repair cost while preserving fenced verified completion.
```

## 20.7 最终一句定位

> **LongHorizonOS 不应被定义为另一个 Agent Harness；它应被定义为多个 Harness 之上的、以 Verified Progress 为优化目标的语义一致性与资源控制平面。**

---

# 21. 核心理念实现度评估

## 21.1 一句话结论

对于本报告开头描述的完整愿景，当前实现可以概括为：

> **语义内核已经比较完整，执行所有权骨架已经存在，在线优化策略已有大量原语；但 always-on、事件驱动、跨 Harness、联合放置与抢占的自治计算管理器尚未闭合。**

因此它不是“理念只写在文档里”，也不是“完整 LongHorizonOS 已经做完”，而是：

```text
Semantic kernel        较强
Safety/ownership       中等偏强
Policy primitives      中等
Integrated controller  偏弱
Physical/production OS 很弱
```

## 21.2 如何理解下面的完成度

完成度不是代码覆盖率，而是根据三个维度做的工程判断：

1. **机制是否存在**：是否已有模型、API、算法和测试；
2. **是否进入主路径**：默认 `run/run_async` 是否真正使用；
3. **是否有端到端证据**：是否通过同资源、真实 Harness/模型和故障实验支持 claim。

一个 policy 文件存在，不代表完整能力已实现；一个 benchmark 有正向结果，也不代表普适性能已经证明。

## 21.3 分层完成度

以下百分比是审查后的近似工程判断，用于确定工作优先级，不是形式化度量。

| 范围 | 近似完成度 | 判断 |
|---|---:|---|
| Semantic Progress Graph 与版本化 Evidence 内核 | **80–85%** | Goal/Task/Artifact/Evidence、版本、validity、READY、closure 基本成立 |
| 选择性失效传播与 task-level repair | **75–85%** | D3 cone/frontier、Goal reopen、task-level oracle parity 已有 |
| 单机 Claim/Attempt/Lease/fencing 执行所有权 | **65–75%** | 主 SDK commit path 较强，但跨 effect/Harness/服务事务仍不完整 |
| RuntimeState 的四类状态观测 | **45–55%** | DTO/投影存在，cognition/context/resource 仍不完整且非原子快照 |
| 自适应 policy 原语集合 | **50–60%** | critical path/conflict/resource/budget/interrupt/routing 等已存在 |
| 默认主路径中的自动联合在线控制 | **25–35%** | policy 分裂、batch barrier、placement 丢失、always-on controller 缺失 |
| Harness 作为统一 execution substrate | **20–30%** | ABI/注册/control/子进程存在，但默认 executor 绕过，lifecycle caller-owned |
| Context OS/真实 paging/residency | **20–30%** | materialize/snapshot 有，真实 eviction、durability、sharing、residency 不成立 |
| 模型/tool/verifier 自动路由与经济闭环 | **15–25%** | 有 advisory/opt-in registry，没有自动选择和充分实测反馈 |
| 物理资源、quota、公平性、分布式运行 | **0–15%** | 主要仍是逻辑 pool 与单机原型 |

综合而言：

```text
若论文 claim 是：
“单机、显式依赖、版本化 Evidence 的语义修复与 fenced execution runtime”
  -> 约 60–70% 已经具备。

若 claim 是：
“本节完整描述的、持续自治、联合调度、跨 Harness 的 LongHorizonOS”
  -> 约 35–40% 已实现。

若 claim 是：
“可普遍把真实 Harness 的 10 小时任务压缩到 3 小时”
  -> 当前实现与实验证据不足 20%，尚未证明。
```

## 21.4 核心理念逐句映射

### 21.4.1 “Harness 让 Agent 连续工作数小时”

**状态：外部 Harness 已能做到；LongHorizonOS 内部只部分接入。**

仓库已有：

- Harness session ABI；
- START/CONTINUE/CHECKPOINT/REBASE/PREEMPT vocabulary；
- logical snapshot/revision；
- bounded replay；
- optional killable subprocess adapter。

但：

- 默认 `run/run_async` 直接调用 `Agent.executor`；
- Harness 不是统一 substrate；
- PREEMPT/REBASE 在普通 `control_harness()` 中被拒绝；
- AgentOS 不完整拥有 adapter close/kill/reap；
- checkpoint 不是 executable process checkpoint。

结论：

> LongHorizonOS 已有 Harness control bridge，但还没有形成“所有长期计算都由 OS 管理的 Harness process model”。

### 21.4.2 “将长期任务表示为持续演化、版本化的 Semantic Progress Graph”

**状态：这是当前实现最强的一层，基本成立。**

已经有：

- Goal、Task、ArtifactRef、Verification、Evidence；
- DEPENDS_ON、PRODUCES、VERIFIES；
- GraphVersion；
- exact ArtifactVersion binding；
- READY、VERIFIED、STALE；
- Goal closure；
- append-only history 与 recovery。

主要剩余问题：

- 更新仍大量全图处理；
- task-level 粒度偏粗；
- verifier/config/external fact 的版本化仍不够完整；
- hidden dependency 会破坏图完备性；
- Context/Resource/Cognition 并不真正存于同一 Graph authority 中。

实现度判断：**高。**

### 21.4.3 “Graph 是整个运行时的全局控制状态”

**状态：语义上部分成立，物理实现上尚未成立。**

当前 VPG 是语义控制状态，但：

- Scheduler Claim/Attempt 在另一 projection；
- Context 在进程内 ContextService；
- Resource 在 AtomicResourceManager；
- Facts/Observation 在另一 authority；
- Harness registry 又是独立内存状态；
- `GlobalRuntimeState` 是一次组合读取，不是一个原子 GlobalEpoch。

所以更准确的当前描述是：

> VPG 是全局语义控制状态的核心，而完整 runtime state 仍由多个 plane 组合而成。

要让原命题真正成立，需要 Cross-Plane SnapshotToken/GlobalEpoch 和统一 decision/commit fence。

### 21.4.4 “知道 VERIFIED、仍有效、刚刚变化、READY/repair frontier”

**状态：大部分成立。**

- VERIFIED/STALE/READY/closure 已实现；
- Artifact version change 可触发 repair；
- workspace observation token/watch/reconcile 已有；
- D3 causal cone/frontier 已有；
- hidden/unknown provenance 可 fail closed。

局限：

- 变化观察主要是显式、caller-owned；
- 没有通用文件/API/browser/tool/world watcher；
- task granularity 内不能精确局部修复；
- watcher baseline 和部分 Context state不跨进程完整恢复。

### 21.4.5 “critical path、可安全并行、冲突和返工风险”

**状态：信号存在，但没有统一成为主路径决策。**

已经有：

- critical path；
- downstream unlock；
- parallel frontier；
- explicit read/write ConflictGraph；
- resource fit；
- churn/rework backoff；
- compute budget；
- waste projection。

问题：

- critical path 主要按节点数，不按预计时长；
- 正常 adaptive path 有 ConflictGraph 时可能绕过 graph-utility ranking；
- conflict/resource/budget 使用分离的 greedy；
- risk 不直接进入一个统一目标；
- placement assignment 最终可能被 Scheduler 重新选择。

### 21.4.6 “结合 cognition state、read/write set、Graph version、进度、token/time”

**状态：观测 DTO 已有，真正 cognition OS 尚未实现。**

已有：

- `AgentSnapshot`；
- Attempt state；
- progress；
- read/write set；
- computation cost；
- context identity；
- semantic epoch；
- stale cognition quarantine；
- UsageLedger 与部分 calibration。

未有：

- Harness/模型内部 belief、plan、uncertainty 的权威持久化；
- 通用 cognitive checkpoint；
- 自动判断“错误方向 reasoning”；
- durable cross-restart usage/calibration；
- 真实 token/$ provider 账单闭环；
- 完整、自动的实际 read/write discovery。

因此这里的“cognition state”当前主要是 **Attempt execution snapshot**，不是完整认知状态机。

### 21.4.7 “持续 online scheduling，而不是一次固定计划”

**状态：有 bounded epoch，但不是真正持续在线。**

已经有：

- `FrontierPolicy`；
- `EpochController`；
- `OnlineComputationController`；
- `EventDrivenSupervisor`；
- bounded `execute_online_epochs`；
- caller-owned workspace watch loop；
- `adaptive=True` 主路径。

但：

- 默认不是 always-on daemon/controller；
- caller 必须显式驱动；
- `run_async` 仍按 batch barrier 规划；
- 一个短 Task 完成后不能立即填充空 slot；
- 跨平面 snapshot 不原子；
- 多个 policy 没有统一成一个权威 co-scheduler。

实现度判断：**概念与 API 中等，自动闭环偏低。**

### 21.4.8 “动态决定串行/并行、并行几个 Agent”

**状态：显式访问和逻辑资源条件下部分实现。**

已经有：

- DynamicParallelismPolicy；
- ResourceAwareParallelismPolicy；
- AdaptiveParallelismPolicy；
- max concurrency；
- per-Agent capacity；
- conflict-aware batch；
- rework backoff。

缺口：

- 当前算法是 greedy，存在任意差反例；
- kernel-loop 入口缺 task resource 时会退化为 serial；
- FIFO weighted limiter 有 HOL blocking；
- physical CPU/GPU/provider quota 未 enforce；
- resource placement 没有与 execution 原子绑定。

### 21.4.9 “等待上游稳定”

**状态：有 bounded heuristic。**

AdaptiveParallelismPolicy 会基于 recent events 判断 upstream churn，并 defer candidate。

但是：

- event window 很小；
- 没有时间归一化；
- 没有 hazard/survival model；
- 不区分高成本与低成本 rework；
- 没有被证明为最优或稳定控制器。

这是可扩展成论文算法的明确入口。

### 21.4.10 “继续、暂停、preempt、restart、incremental rebase”

**状态：动作 vocabulary 和部分路径存在，统一 lifecycle 尚未闭合。**

当前：

- cooperative interrupt delivery 已有；
- late completion quarantine 已有；
- `preempt_superseded=True` 可终止 opt-in subprocess task；
- automatic rebase planner 已有；
- changed-read fresh-Attempt handoff 已有；
- release-then-acquire recovery witness 已有。

未完成：

- 任意 Harness 的强制暂停；
- 原子 Lease/Harness handoff；
- in-place cognitive rebase；
- portable process/context checkpoint；
- automatic watcher -> policy -> handoff -> replacement loop；
- control intent/ack transaction。

### 21.4.11 “复用 cognitive locality 或启动 fresh Agent”

**状态：有 warm-Agent ranking 和建议，没有真实 process lifecycle。**

Scheduler 可根据历史 AgentSnapshot read set给 locality bonus，ComputeRoutingPolicy 可推荐 warm/fresh。

问题：

- 当前 residency 是历史读集合，并不代表仍驻留；
- Context close/evict 不同步删除；
- 不创建、复用、迁移真实进程；
- 不管理 KV cache；
- 不自动启动 fresh provider session。

实现度判断：**advisory 约一半，真实 locality OS 很少。**

### 21.4.12 “给多少最小化 Context”

**状态：有 Context VM 和建议，但真实闭环不足。**

已实现：

- ContextManifest；
- page selection；
- token/byte budget；
- snapshot；
- context-budget recommendation；
- delta/rebase plan；
- prefix stability analysis。

未实现：

- 真正 eviction；
- shared page cache/COW；
- durable content bytes；
- residency lease；
- 动态修改 live Context；
- 与模型 KV cache 的实际接口；
- Context allocation 与 co-scheduler 联合。

### 21.4.13 “强模型还是便宜模型、tool/provider routing”

**状态：推荐和 opt-in adapter 已有，自动经济调度尚未实现。**

已有：

- ComputeRoutingPolicy；
- model tier/verifier strength/context recommendation；
- provider registry；
- explicit metadata opt-in；
- usage/calibration primitive。

缺口：

- 没有自动 provider selection；
- 没有 provider lifecycle/pooling；
- 没有真实价格、RPM/TPM、queue latency 联合模型；
- routing 每 Task 重复 runtime_state；
- 没有跨模型质量/成本实测 benchmark；
- 结果没有形成 Scheduler placement contract。

### 21.4.14 “高风险/high fan-out 节点使用更强 verification”

**状态：有 verification-strength recommendation，尚非自动执行闭环。**

VPG 支持 Verification/Evidence 和 required verification count；ComputeRoutingPolicy 可给 verifier strength 建议。

但尚未：

- 根据 fan-out、side-effect risk、uncertainty 自动分配 verifier；
- 测量 verifier token/time/cost；
- 证明额外验证的边际收益；
- 将 verifier slot/provider 作为实际资源放置。

### 21.4.15 “Requirement/API/Artifact/Evidence/资源变化触发 Semantic Interrupt”

**状态：显式 workspace/graph 变化路径部分成立，通用 world interrupt 未完成。**

已有：

- SemanticInterruptPolicy；
- workspace watcher；
- observation token；
- graph-version/read-set fence；
- direct interrupt delivery；
- stale completion quarantine。

未有：

- 通用 API/database/browser/provider watcher；
- resource pressure 自动 interrupt；
- verifier policy change 自动传播；
- always-on supervisor；
- 所有 Harness 的可靠 control delivery。

### 21.4.16 “停止 stale/低价值工作，保留仍有效计算”

**状态：选择性修复很强；running-work preemption 较窄。**

保留有效 branch：

- D3 已实现并通过 task-level oracle；
- repair 相对 full restart 有显著工作节省；
- unaffected verified branch 会保留。

停止 stale running work：

- explicit declared input supersession 场景已实现；
- cooperative/token-aware 或 opt-in subprocess 可中止；
- arbitrary Python callback 和第三方 Harness 仍不能保证。

低价值而非 stale 的工作：

- policy 可 defer；
- 但没有完整、校准过的 marginal verified-progress controller。

### 21.4.17 “最大化单位 token/time/cost 的 Verified Progress”

**状态：目标函数和多个组件存在，但没有一个统一执行中的优化器。**

已有：

- compute budget；
- verified-progress/cost ratio；
- usage ledger；
- estimate calibration；
- unified policy；
- waste projection；
- token/time/cost metrics。

未完成：

- durable history；
- value 自动估计；
- future unlock value 与 cost 联合；
- provider 真实账单；
- conflict/resource/context/preemption 共同进入一个 objective；
- online learning/controller；
- approximation/competitive guarantee。

### 21.4.18 “减少 reread、重复 reasoning、过早并行、错误方向、stale work”

**状态：观测能力多于自动控制能力。**

已经能够观测或部分控制：

- reread；
- repeated attempts；
- stale/rework；
- premature parallelism；
- wrong-direction proxy；
- conflict overlap；
- unused Context；
- preemption payoff。

但：

- 一部分存在于 waste projection/benchmark，而不是主执行 policy；
- repeated reasoning 的语义检测较弱；
- wrong direction 依赖启发式和声明；
- 没有一个统一 policy 直接最小化五类 waste。

## 21.5 当前真正形成端到端闭环的部分

当前最完整的闭环是：

```text
Task/Goal compile
-> VPG READY
-> Scheduler Claim/resource
-> Kernel Lease/fencing
-> executor
-> verifier
-> exact-version Evidence
-> VPG VERIFIED/closure
```

以及：

```text
Artifact version change
-> Evidence applicability loss
-> causal STALE cone
-> repair frontier
-> fresh Attempts
-> Goal reclosure
```

这两条是当前项目最可依赖、最适合成为论文 correctness substrate 的能力。

## 21.6 当前最核心的三处断裂

### 断裂一：Observe

- hidden reads 不完整；
- world/API/resource watcher 不通用；
- RuntimeState 跨平面非原子；
- Context residency 与内容 durability 不真实。

### 断裂二：Decide

- graph utility、conflict、resource、budget、context、risk 分离；
- critical path 未加权；
- greedy 缺少 oracle/guarantee；
- 没有统一 co-scheduler。

### 断裂三：Act

- placement assignment 丢失；
- default execution 绕过 Harness；
- control 不是 intent/ack；
- preempt/rebase 非原子；
- Context eviction 不生效；
- provider/model/verifier routing 多为 advisory；
- physical resource 不 enforce。

所以最准确的总体评价是：

> **系统已经能“知道一部分什么是对的、什么失效了”，但还不能持续、统一、权威地把这些知识转化为所有真实计算和资源的动作。**

## 21.7 当前实验证据能支持什么

### 已支持

1. **Selective repair**  
   quick semantic-repair 中相对 full restart 平均节省约 `48.6427%` weighted work，同时与 oracle task-DAG checkpoint 持平；这证明 task-level selective repair，而不是超过 oracle。

2. **Doomed-compute preemption**  
   受控 incomplete-declaration workload 中，被取代输入上的子进程计算从约 `4126 ms` 降至 `265 ms`，约减少 94%；但该 benchmark 的 Goal 在测量窗口中仍 open，所以证明的是 avoided doomed work，不是完整 TTVG。

3. **Critical-path dispatch signal**  
   特定 heavy-chain workload 中 static parallel 约 `7.07 s`、LongHorizonOS 约 `6.33 s`，并通过 deterministic chain-priority 指标解释顺序变化；当前改善远不是 3 倍。

4. **Null case**  
   稳定、对称、无决策空间的 workload 上约 `0.93x–1.00x`，表明 adaptive overhead 会存在。

5. **Ownership/repair safety**  
   大量测试覆盖 Claim/Lease、stale commit、repair、reopen、interrupt、workspace validation 等边界。

### 尚未支持

- 真实 DeepSeek Harness 或 Claude Code 上的统计加速；
- 真实模型/GPU/provider economics；
- 10 小时到 3 小时的端到端结果；
- 普适 workload speedup；
- 物理资源 placement；
- 跨 Harness generality；
- 长期 10k/100k Attempt 稳态；
- 完整 crash/effect exactly-once。

## 21.8 “10 小时压到 3 小时”目前处于什么状态

这个数字应被视为**研究目标示例**，不能视为当前能力。

要支持这一 claim，至少需要将总收益拆成：

```text
critical-path acceleration
+ safe parallel overlap
+ avoided doomed compute
+ selective repair
+ context/model routing saving
- OS/control overhead
```

并在同一个真实长时程 workload 中测到：

```text
same Harness
same model
same Agent count/resource cap
same verifier
same final VERIFIED Goal
```

当前 repository 分别证明了其中几个受控局部机制，但尚未在一个统一真实 workload 中将其叠加为 3 倍以上端到端加速。

## 21.9 当前可发表 claim 与目标 claim

### 当前较诚实、接近可证明的 claim

> 在单机、显式/mediated provenance 和可独立验证的 Task DAG 上，LongHorizonOS 通过版本化 Evidence、fenced execution 和 causal invalidation，保留仍有效的进度并对受影响子图执行选择性修复。

### 完成核心算法后可争取的 claim

> LongHorizonOS 在 Harness-managed sessions 之上，使用事件驱动的 Verified-Progress 联合调度、放置和抢占，减少动态共享状态下的 doomed compute 和 Time-to-Verified-Goal。

### 当前不能使用的 claim

> LongHorizonOS 已经可以普遍把任意 Harness 的十小时任务压缩到三小时。

## 21.10 顶会论文就绪度

若现在直接投稿完整系统顶会，当前更像：

- 强工程原型；
- 有较好 correctness substrate；
- 有多个有潜力的 policy primitive；
- 有诚实的边界和初步 benchmark；
- 但核心 co-scheduler、统一 Harness adapter 和真实端到端实验证据仍不够。

最短的论文闭环建议只集中完成三件事：

1. **Event-driven Verified-Progress Co-Scheduler**  
   消除 batch barrier，联合 critical path/conflict/resource/risk/placement。

2. **至少一个真实 Harness adapter + staged fenced commit**  
   优先 DSH native adapter；同时用 Claude Code/Agent SDK 做 generality baseline。

3. **Incremental semantic state + 强实验**  
   实现 affected-cone update，加入 ILP oracle、真实 mutation workload、长期 scale 和 crash campaign。

完成这三项后，当前已有的 VPG、D3、Claim/Lease、Context/interrupt primitives 才会从“功能集合”形成一篇统一的系统论文。

---

# 22. Threats to Validity 与假设边界

## 22.1 构造有效性

### 依赖与 read/write coverage

系统只能对已声明或 mediated/observed 的依赖做正确推导。隐藏的：

- 文件；
- API；
- database；
- browser；
- subprocess；
- environment；
- model/tool side-channel；

可能造成漏边和 under-invalidation。

未知访问必须：

- 标记 `UNKNOWN/PARTIAL`；
- fail closed；
- 或 serial-only。

不能将显式依赖实验外推为任意 Python 的完整 provenance。

### Verifier correctness

LongHorizonOS 能保证：

- Evidence 与精确版本绑定；
- applicability 变化会传播；
- stale owner 不能通过主 fence 提交。

它不能自动证明 verifier 本身语义完备或无 bug。

因此：

```text
no false VERIFIED
```

应解释为：

> 在给定 verifier/evidence policy 正确的假设下，不因版本、ownership 或 replay 错误产生 false VERIFIED。

## 22.2 内部有效性

### 模型和 provider 随机性

LLM output、provider latency、rate limit、cache、network 均可能影响 wall-clock。

需要：

- 固定 model/version；
- 固定 prompt/tools；
- common random realization；
- paired runs；
- 报告 usage missing/unavailable；
- 将 control-plane time 单独记录。

### Decomposition confound

Task decomposition、read/write declaration和 verifier设计可能比 scheduler 本身更影响结果。

应：

- 所有 arm 使用同一 graph；
- 另设 planner/decomposition 独立实验；
- 不把更好的手工 DAG 归因于 OS scheduler。

### Baseline resource confound

必须固定：

- Agent 数；
- concurrency；
- model；
- budget；
- sandbox；
- workspace；
- verifier。

不把更多资源带来的 speedup 归因于 LongHorizonOS。

## 22.3 外部有效性

### Synthetic workload

`asyncio.sleep`、本地 hash、生成 DAG 不代表：

- 真实 LLM；
- GPU inference；
- remote API；
- human interaction；
- production repository；
- browser automation。

必须用真实 Harness mutation workload 补足。

### Harness API 漂移

DeepSeek Harness 当前是 developer preview，Claude Code 的 Agent Teams/Agent View 等也可能是 experimental/research preview。

实验必须记录：

- 日期；
- commit/version；
- feature flags；
- adapter capability；
- unsupported operation。

### Single-host boundary

当前单机 SQLite、单 writer、逻辑 resource 的结论不能外推为：

- distributed scheduler；
- leader election；
- multi-host consensus；
- physical cluster placement。

## 22.4 Safety 边界

### External side effects

任意 database/API/deployment/payment 等效果不能由当前系统普遍提供 exactly-once 或 rollback。

能诚实声称的最多是：

- intent；
- idempotency key；
- fencing；
- at-least-once；
- `UNCERTAIN`；
- reconciliation/compensation。

### Preemption

- in-process arbitrary Python callback 不能强杀；
- cooperative executor 可忽略 interrupt；
- subprocess adapter 当前仍需加强 process-group/Job Object descendant cleanup；
- remote Harness 可能只支持 coarse cancel；
- external effect 不能因 kill 自动撤销。

### Mediated path

安全不变量只覆盖经过：

- Claim/Lease；
- Context/provenance gateway；
- staged output；
- verifier；
- fenced commit；

的路径。

任意 direct filesystem/network/Python I/O 在 soundness envelope 外。

## 22.5 测量与资源边界

### Usage 可用性

token、cost、CPU、RSS、provider usage 可能：

- unavailable；
- self-reported；
- estimated；
- delayed；
- incomplete。

不可将 unavailable 记为 0。结果必须报告：

- available count；
- missing count；
- source；
- authority level。

### Logical 与 physical resource

当前 logical CPU/GPU/RAM/VRAM/model slots 不是物理 enforcement。利用率和 packing 结果不能解释为真实 GPU scheduler 性能。

## 22.6 统计与可复现性

- seed 必须改变 workload realization，而不是只记录 metadata；
- 使用 paired/common-random-number 设计；
- 报告分布和 CI；
- 随机/交替 arm 顺序；
- 分离 warm-up；
- 公开 raw trace 和环境；
- 避免只汇报成功 trial；
- timeout/censoring 必须显式处理。

## 22.7 Null case 与负结果

稳定、短、对称、无变化 workload 中，LongHorizonOS 可能更慢。这不是系统失败，而是控制面开销的必要测量。

论文必须展示：

- 加速区域；
- break-even surface；
- 负收益区域；
- overhead 上限；
- policy fail-closed 行为。

否则“普适加速”claim 不可成立。
