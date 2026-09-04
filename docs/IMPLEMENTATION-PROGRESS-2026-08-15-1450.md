# LongHorizonOS implementation progress — 2026-08-15 14:50 CST

## 本轮目标

- 收尾 P1 bounded online-compute vertical slices；
- 对 Context rebase 的真实边界做 fail-closed 审计，不把 planner 宣传成自动 rebase；
- 同步 README、状态清单、路线图和 Mind-VLA 矩阵；
- 运行 focused gate、静态检查和最新完整 non-slow 回归。

## 已实现

- `AgentOS.execute_online_epoch(...)`：沿现有 Scheduler/Claim/Attempt/Lease、
  executor、verifier、VPG authority 执行一个有界 epoch。
- `AgentOS.execute_online_epochs(...)`：调用方显式给出 epoch 和 dispatch 预算，
  每轮重新观察 VPG，遇到闭合、失败、无工作、预算耗尽或上限时保守停止。
- retained Claim → Harness handoff：精确校验 graph/task/agent/process/
  claim/attempt/epoch/lease，支持幂等重放；不声称跨平面原子事务。
- `WorkspaceObservationWatcher` 及 `AgentOS.poll_workspace_and_route(...)`：
  显式文件轮询、ObservationToken、interrupt 校验、exact Attempt fence 和
  cooperative `REBASE/PREEMPT` 路由；拒绝结果进入 `blocked`。
- durable cleanup markers、normalized row-hashed Scheduler projection、
  stale-cognition read-set fence、Context VM snapshot/provenance 接入。
- `RebaseRuntimeBridge`：显式 `AgentSnapshot + ContextGraphDelta` 的 freshness
  检查、`REUSE/REBASE/FULL_RELOAD/BLOCKED` 分类和 Harness request 构造。
- 修复 `rebase_runtime.py` schema-version Literal 类型错误。

## 测试证据

- P1 focused gate：**46 passed**（watcher 25、handoff 4、multi-epoch 8、
  execution 6、cleanup 3）。
- Rebase/online/watcher 合并 focused gate：**68 passed**。
- Ruff、mypy、compileall：通过（当前修复后）。
- 当时记录的完整 non-slow 基线：**3181 passed, 1 skipped, 18 deselected,
  30 warnings in 459.57s**；该数字保留为历史 cutoff。
- 随后完成最终串行 non-slow 回归：**3187 passed, 1 skipped, 18 deselected,
  30 warnings in 443.82s**；日志：
  `artifacts/final-test-nonslow-20260815-serial.log`。该结果覆盖
  watcher route/rejection hardening，作为当前仓库最新完整回归证据。

## 尚未实现

- always-on event-driven controller / 后台 watcher；
- 隐式文件/API/浏览器/Python 读取的自动 provenance discovery；
- Context delta 自动 materialization 和 `run()/run_async()` 主路径 incremental
  cognition；
- Scheduler/Kernel/Harness/VPG 的跨平面 atomic ownership transaction；
- 任意 Python callback 的进程隔离、force-kill 和非 cooperative cancellation；
- 物理 CPU/GPU/RAM/VRAM telemetry、placement、quota/fairness；
- 分布式调度、leader election、consensus；
- 不可逆副作用 exactly-once、belief revision、field-level semantic repair；
- 真实 LLM/GPU/provider workload 的统计 benchmark。

## 当前阶段与下一步

当前仍是 **single-host research alpha / bounded vertical slices**。
下一步应先设计并测试 atomic ownership protocol，再把 live Context rebase
接入 authoritative execution path；在此之前，`RebaseRuntimeBridge` 只能作为
显式、非原子、调用方驱动的 planner/bridge 使用。随后补真实 Harness/provider
工作负载 benchmark，并继续保持所有失败路径 fail-closed。
