# LongHorizonOS implementation progress — 2026-08-15

**更新时间：** 2026-08-15（CST）  
**当前版本边界：** `v0.1.x` experimental single-host research alpha  
**工作区：** `Downloads/LongHorizonOS-main/LongHorizonOS-main`

## 本轮目标

把“Graph 表示演化中的计算、OS 持续从 Graph 重新调度”继续落到真实的
单机执行路径，同时保持现有 Scheduler/Kernel/VPG 作为唯一权威，避免新增
第二套 Claim、Lease 或 Evidence 语义。

## 已实现

### 1. 一次性 online execution epoch

- 新增 `AgentOS.execute_online_epoch(...)`。
- 真实路径为：
  `observe → reconcile → plan → Scheduler admission → execute → verify → VPG commit → observe`。
- 复用既有 `run_async(..., adaptive=True, max_steps=1)`，不引入第二套执行权威。
- 使用真实 Scheduler、Claim、Attempt、Kernel Lease、executor、verifier 和
  VPG Evidence 提交。
- 返回 `RunResult`，并附带 `meta["online_epoch"]` 审计信息。
- 审计区分：
  - policy-selected task；
  - 实际被 Scheduler dispatch 的 task；
  - serial fallback dispatch；
  - Scheduler skip reason；
  - planned/final graph version。
- `max_dispatches=0` 是严格 no-work：只完成 Goal 注册/编译和结果观察，
  不规划、不持久化 SchedulingEpoch、不创建 Claim/Lease、不执行用户代码。

### 2. post-admission exact-Claim compensation

- `SchedulerSession.run_pass(...)` 在 `observe_vpg()` 或 `reconcile()` 失败时，
  只释放本轮返回的 exact Claim。
- 使用 `expected_claim_id` 防止误释放竞争中产生的 replacement owner。
- 清理失败不会覆盖原始异常；通过 bounded `add_note()` 附加诊断。

### 3. async cancellation cleanup

- `run_async()` 收到 `CancelledError` 后逐 job 做 exact-Claim fenced cleanup。
- 一个 job 的 release/reconcile 失败不会阻断其他 job。
- 保留并重新抛出原始 `CancelledError`，并记录 bounded logger/add-note 诊断。
- 这是 cooperative cleanup，不是任意 Python callback 的 killable cancellation。

### 4. freshness guard 与 durable cleanup marker

- `expected_graph_version` 为显式 adaptive admission 提供可选 freshness fence。
  如果 Graph 在准入期间前进，结果标记为 `policy_stale`，并只对本次返回的
  exact Claim 做补偿；不会执行 reconcile side effect，也不会把 superseded
  policy 下的 ownership 交给 worker。
- `validate_read_set_freshness(...)` 对 Graph version advance 要求 complete
  explicit delta；partial/unknown coverage 或 unidentifiable read fail closed，
  阻止 stale cognition reuse/commit。Same-version partial observation 保持兼容性
  no-op。该 guard 仍是 bounded、single-host primitive，不是跨服务原子计划-准入事务。
- post-admission/cancellation exact cleanup 无法完成时，Scheduler 记录幂等的
  `execution-cleanup.v1` durable audit marker（确定性 SHA-256 `marker_id`）。
  `cleanup_markers` 显示 unresolved marker；`reconcile_cleanup_markers()` 只有在
  exact Claim terminal 且 authoritative lease lookup 确认无 live lease 时才 resolve，
  active/unknown ownership 保持 pending。Marker 不自动释放或重定向 Claim。

### 5. fail-closed DTO 与审计

- `OnlineEpochScheduleResult` 校验 graph、task、Claim、dispatch ownership
  集合和 status 组合的一致性。
- adaptive epoch 审计保留 Scheduler skip reason，并限制记录规模。
- online execution metadata 增加 planned graph identity/version/projection hash
  和 final graph version；这些字段是审计信息，不等于已实现 planning/admission
  原子事务。

## 当前真实验证

### 当前 focused gate

```text
tests/sdk/test_online_epoch.py
tests/sdk/test_online_epoch_execution.py
tests/sdk/test_online_epoch_cleanup.py
tests/sdk/test_adaptive_run.py
tests/sdk/test_async_run.py
48 passed
```

Additional focused cancellation gate:

```text
tests/sdk/test_async_run.py -k cancel
3 passed
```

Static checks:

```text
ruff check: passed
compileall: passed
mypy (touched online-epoch files): passed
```

### 当前完整 non-slow 回归

```text
3157 passed, 1 skipped, 18 deselected, 30 warnings
465.50s
```

日志：

```text
artifacts/full-test-nonslow-online-execution-final-hardening-20260815.log
```

这是 2026-08-15 11:10 完成的最近一次完整基线，早于本轮后续的
cleanup-marker 自动接线收口；最终结果必须以自动接线完成后重新运行的
full non-slow 日志为准。

这只是单机回归证据，不代表生产可用、分布式可用、真实 LLM/GPU 加速或
完整 Harness orchestration。

## 尚未实现/仍为边界

1. **Planning → admission 原子事务仍未完成。** 显式
   `expected_graph_version` freshness fence 已能在准入竞态时 fail closed 并补偿
   exact Claims，但 policy plan、Graph reconcile、Claim/Lease admission 与外部
   Harness ownership 仍不是一个跨服务原子 contract。
2. retained `schedule_online_epoch(..., keep_claims=True)` → Harness execution
   的原子 handoff。
3. watcher → controller → Scheduler → Harness 的 always-on event-driven loop。
4. 自动 Context materialization/rebase、通用 hidden provenance/world observation。
5. 任意 Python callback 的进程隔离和强制终止。
6. 真实 CPU/GPU/RAM/VRAM telemetry、placement、quota、fairness、starvation。
7. irreversible external side effect exactly-once。
8. distributed scheduler/leader election/consensus。
9. artifact/evidence field-level selective repair、semantic equivalence pruning、
   belief revision/contradiction solving。
10. 真实模型、真实 GPU、同任务竞品的统计性性能评测。

## 下一步实现顺序

| 优先级 | 内容 | 验收标准 |
|---|---|---|
| Done (bounded) | adaptive plan/admission graph freshness guard | 注入 graph change 时返回 `policy_stale`，只补偿 exact Claim；不产生错误 VERIFIED 或误释放 replacement owner |
| In progress | cleanup marker 自动接入异常 cleanup 路径 | `run_pass` 与 `run_async` cleanup 失败自动留下 durable marker，重启可见且不误释放 replacement owner |
| P0 | 最新代码 full non-slow 回归 | focused/static/full gate 全绿，最终日志覆盖 freshness guard 与 marker 自动接线 |
| P1 | retained Claim → Harness handoff | exact Claim/Attempt/Lease identity + crash/replay 测试 |
| P1 | watcher-driven interrupt/rebase loop | declared observation 变化能触发 bounded cooperative action |
| P1 | real workload benchmark harness | 同模型/同任务比较 tokens、time、rework、verified progress |

本轮 freshness guard 与 cleanup marker 核心原语已完成；自动接线与其后的
最终 full non-slow 回归仍在进行。完成后再进入 retained Claim → Harness handoff
与 watcher-driven interrupt/rebase loop。这仍是 bounded single-host 增量，
不应把整个“通用 Agent OS”承诺为短期可完成事项。
