# LongHorizonOS implementation progress — 2026-08-15 13:00 CST

## 本次周期目标

在 v0.1.x 单机研究原型边界内，把以下 control-plane 能力从“原语”继续
接到真实执行路径，同时保持 VPG、Scheduler、Kernel Claim/Lease 和
Evidence 只有一套权威：

1. cleanup failure 的 durable recovery；
2. retained Claim → Harness 的 exact handoff；
3. watcher → semantic interrupt → cooperative Harness action；
4. 用真实测试和完整 non-slow 回归校验，不夸大为通用 Agent OS。

## 已完成

### P0：Graph freshness fence

- adaptive policy 绑定 `expected_graph_version`；
- Scheduler admission 前/锁内再次校验版本；
- 版本竞态 fail closed；
- 只补偿本轮 exact Claim，不误清理 replacement owner。

### P0：Durable cleanup marker

- Scheduler journal 支持
  `EXECUTION_CLEANUP_REQUIRED / RESOLVED / SUPERSEDED`；
- marker 绑定 `graph/task/claim/attempt/lease`；
- `SchedulerSession.run_pass()` 的 post-admission cleanup failure 自动记录 marker；
- `AgentOS.run_async()` cancellation cleanup failure 自动记录 marker；
- marker 可重启重放，仍是 recovery audit，不会自行释放 Lease。

### One-shot online execution epoch

- `AgentOS.execute_online_epoch(...)` 已接入真实 observe → plan → admission →
  execute → verify → VPG commit 路径；
- `max_dispatches=0` 为严格 no-work；
- metadata 保留 policy 选择、实际 dispatch、skip、graph version 等审计信息。

## 最新验证

### Focused

```text
59 passed
```

覆盖 cleanup markers、freshness、adaptive run、online epoch、async cancellation。

### 完整 non-slow

```text
3166 passed, 1 skipped, 18 deselected, 30 warnings
458.98s
```

日志：

```text
artifacts/full-test-nonslow-cleanup-marker-wiring-20260815.log
```

该结果覆盖本轮 cleanup-marker 自动接线；仍然只是单机回归证据，不等于
真实 LLM/GPU 吞吐、分布式一致性或生产就绪。

### 静态检查

- Ruff：通过；
- Mypy（触及的 online/cleanup 文件）：通过；
- Compileall：通过。

## 当前进行中

### P1：retained Claim → Harness exact handoff

目标是让 `schedule_online_epoch(..., keep_claims=True)` 返回的 exact
Claim/Attempt/Lease 能安全绑定到 Harness，并验证 graph/task/agent/process/
semantic epoch/fencing identity。该能力仍然不恢复任意 Python 调用栈或模型
内部状态。

### P1：watcher-driven cooperative interrupt/rebase

目标是提供显式 one-shot（非后台 daemon）路径，把已声明的 watcher observation
转换为 `SemanticInterrupt`，通过现有 identity/version fence 调用
`deliver_interrupt`，让 Harness 合作式执行 `REBASE`/`PREEMPT`。未知变化必须
fail closed。

## 尚未实现

- always-on watcher/controller/scheduler/harness 后台闭环；
- automatic hidden provenance / universal world observation；
- Context delta 自动 materialization 和通用 incremental cognition；
- CPU/GPU/RAM/VRAM 物理 telemetry、placement、quota、fairness；
- 任意 Python callback 的进程隔离和强制 kill；
- distributed scheduler/leader election/consensus；
- irreversible side-effect exactly-once；
- field-level semantic repair、belief revision；
- 真实模型/GPU/竞品统计 benchmark。

## 下一次同步

下一次代码/测试状态变化后新增一份带时间戳的 progress Markdown；在 P1 两项
focused gate 完成后，再更新 README 和状态文档中的最新完整回归数字。
