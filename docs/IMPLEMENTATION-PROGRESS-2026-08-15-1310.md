# LongHorizonOS implementation progress — 2026-08-15 13:10 CST

## 当前阶段

P0 已收口，正在并行推进两个 P1 bounded vertical slice：

1. `retained Claim → Harness` exact handoff；
2. watcher observation → `SemanticInterrupt` → cooperative
   `REBASE/PREEMPT` one-shot route。

两项都只复用现有 VPG、Scheduler、Kernel Claim/Lease、Harness 和
Evidence authority，不新增第二套 ownership。

## 已完成且已验证

- adaptive Graph freshness fence：policy 版本竞态 fail-closed；
- `EXECUTION_CLEANUP_REQUIRED/RESOLVED/SUPERSEDED` durable marker；
- `SchedulerSession.run_pass()` cleanup failure 自动 marker；
- `AgentOS.run_async()` cancellation cleanup failure 自动 marker；
- one-shot `AgentOS.execute_online_epoch(...)`；
- Context VM materialization、read-set stale cognition commit fence；
- RuntimeStateView、ConflictGraph、SemanticInterruptPolicy、
  ComputeRoutingPolicy 等 bounded advisory primitives。

### 最新测试

```text
Focused cleanup/online gate: 59 passed
Full non-slow: 3166 passed, 1 skipped, 18 deselected, 30 warnings
Full log: artifacts/full-test-nonslow-cleanup-marker-wiring-20260815.log
Ruff / Mypy / Compileall: passed
```

`3166` 是本轮 automatic cleanup-marker wiring 后的完整单机回归；不代表
真实 LLM/GPU 吞吐、分布式一致性或生产就绪。

## 正在实现

### retained Claim → Harness

将 `schedule_online_epoch(..., keep_claims=True)` 返回的 exact
Claim/Attempt/Lease 与 Harness session 绑定，并校验：

```text
graph / graph_version / task / agent / process
claim / attempt / semantic_epoch / lease fencing
```

失败必须 fail-closed，不能释放 replacement owner。

### watcher-driven interrupt/rebase

显式 one-shot 路径接收已声明的 workspace observation，生成并校验
`SemanticInterrupt`，再调用现有 `deliver_interrupt`。它只提供 cooperative
控制，不承诺强制终止任意 Python callback，也不承诺后台 daemon。

## 仍未实现

- always-on event-driven Harness loop；
- hidden provenance / universal world observation；
- Context delta 自动 materialization 和通用 incremental cognition；
- physical CPU/GPU/RAM/VRAM telemetry、placement、quota、fairness；
- 任意 Python 进程隔离/killable cancellation；
- distributed scheduler/consensus；
- irreversible side-effect exactly-once；
- field-level semantic repair、belief revision；
- 真实模型/GPU/竞品统计 benchmark。

## 下一个验收点

P1 两项各自通过 focused tests 后，重新跑 full non-slow，并把新的测试数字
同步到 README、IMPLEMENTATION-STATUS、ISSUE-INVENTORY、ROADMAP 和最新
progress 文件。
