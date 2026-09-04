# LongHorizonOS implementation progress — 2026-08-15 14:20 CST

## 本轮目标

- 将 P1 bounded vertical slice 的真实状态同步到 README、实现状态、问题清单、路线图和 Mind-VLA matrix。
- 明确区分“已实现的 caller-invoked primitive”和“尚未实现的 always-on / atomic / physical OS guarantee”。
- 记录 post-P1 完整回归与 watcher follow-up 的覆盖边界。

## 已实现（本轮前后确认）

1. **Durable cleanup marker**
   - `SchedulerSession.run_pass()` 和 `AgentOS.run_async()` 的 exact-Claim cleanup 失败路径自动写 `execution-cleanup.v1`。
   - marker 只做 durable audit/reconcile；terminal Claim 且权威 lease lookup 确认无 live lease 后才能 resolve，不会自动释放 replacement Claim。
2. **Retained Claim → Harness handoff**
   - `AgentOS.handoff_online_epoch_to_harness(...)` 接收 `schedule_online_epoch(..., keep_claims=True)` 的 retained dispatch。
   - 校验 graph/task/Agent/process/Claim/Attempt/semantic epoch/Lease 完整身份；同一身份 replay 幂等；不执行 Harness、不释放 Claim、不宣称跨平面原子事务。
3. **Workspace watcher route façade**
   - `AgentOS.poll_workspace_and_route(...)` / `route_workspace_observation(...)` 提供 one-shot poll/observation-token/interrupt-policy/exact Attempt fence/cooperative `REBASE`/`PREEMPT` 路径。
   - supplied `WorkspaceWatchPoll.interrupts` 会重新校验；`STALE_GRAPH`、`STALE_EPOCH`、`IDENTITY_MISMATCH`、`NOT_RUNNING` 等拒绝状态进入 `blocked`，不误记为 delivered。
4. **Bounded multi-epoch execution**
   - `AgentOS.execute_online_epochs(...)` 按显式 `max_epochs` / dispatch budget 调用真实 `execute_online_epoch(...)`，每轮重新观察 VPG，并在闭合、失败、无 dispatch、无预算或达到上限时停止。
   - 仍是 caller-owned loop，不是后台 daemon，不恢复任意 Python call stack，不消费 retained ownership，不管理外部 Harness。

## 测试证据

- 最新 post-P1 完整 non-slow（在最终 watcher follow-up 之前）：
  - **3181 passed, 1 skipped, 18 deselected, 30 warnings in 459.57s**
  - 日志：`artifacts/full-test-nonslow-p1-watcher-loop-20260815.log`
- 历史/中间基线：
  - cleanup-marker wiring：3166 passed，日志 `artifacts/full-test-nonslow-cleanup-marker-wiring-20260815.log`
  - one-shot execution hardening：3157 passed，日志 `artifacts/full-test-nonslow-online-execution-final-hardening-20260815.log`
- watcher route/rejection follow-up：**25 passed**；Ruff、Mypy、Compileall 通过。
  - 该 follow-up 在 3181 full-run cutoff 之后，因此不能声称被 3181 full run 覆盖。
- P1 聚焦门禁（handoff + watcher + multi-epoch）：此前 **33 passed**；watcher follow-up 后 watcher 单项为 25 passed。

## 仍未实现 / 明确边界

- always-on event-driven controller / background watcher；当前必须 caller 显式调用。
- 自动 hidden provenance / dependency discovery（任意文件、API、浏览器、工具、Python 读取）。
- Context delta 自动 materialization、main-path incremental cognition 和自动 rebase。
- Scheduler/Kernel/Harness 的跨服务 atomic ownership transaction；现有 handoff 是 release-then-acquire / bounded binding。
- 任意 Python callback 的 process/container isolation、force-kill、非 cooperative cancellation。
- 物理 CPU/GPU/RAM/VRAM telemetry、placement、quota、fairness、starvation guarantee。
- distributed multi-host scheduler、leader election、consensus、多 writer 协调。
- 不可逆外部副作用 exactly-once；通用 belief revision、矛盾求解和自主 repair planning。
- 真实 LLM/provider/GPU workload 的统计性收益 benchmark；当前 synthetic/offline benchmark 不能证明真实加速。

## 下一验收点

1. 将 live Context rebase 接入 authoritative execution path，并先设计 atomic ownership protocol；失败必须 fail closed。
2. 用真实 Harness/provider workload 测量 success、tokens、reread、wall time、stale work、verification cost 和 verified-progress/minute。
3. 补 hidden-read / watcher-gap / crash-before-after-dispatch / side-effect sink 测试；保持单机边界，未验证前不宣传通用 Agent OS。
