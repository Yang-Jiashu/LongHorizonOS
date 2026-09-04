# LongHorizonOS implementation progress — 2026-08-15 18:10 CST

## 本轮要实现

- 收尾 bounded live Context-rebase SDK façade；
- 补齐 public DTO exports、authority/version/identity fences 和幂等 replay；
- 用完整 non-slow 回归验证本轮源码，而不是只依赖 focused tests；
- 同步 README、状态、问题清单、路线图和 Mind-VLA 实现矩阵。

## 已实现

- `LiveContextRebasePlan` / `LiveContextRebaseApplyResult` 已作为
  `lhos.sdk` public experimental DTO 导出；
- `AgentOS.plan_live_context_rebase(...)` 只接受当前 authoritative VPG
  version，并绑定 exact Claim/Lease、Attempt、Process、AgentSnapshot、
  Context 与 Harness session；
- `AgentOS.apply_live_context_rebase(...)` 在进入 Harness 前重新校验
  Graph、Task、Agent、Process、Claim、Attempt、semantic epoch、Context、
  AgentSnapshot、Harness identity 及 plan/delta/freshness hashes；
- safe `REUSE` 支持 exact `(claim_id, request_id)` 幂等重放，第二次 apply
  返回 `replayed=True`，不会重复调用 Harness hook；
- `REBASE/FULL_RELOAD` 在缺少跨 Scheduler/Kernel/Harness 原子 ownership
  transaction 时继续 fail closed，保持 `ownership_unchanged=True`。

## 测试证据

- 最新完整 non-slow：
  **3201 passed, 1 skipped, 18 deselected, 30 warnings in 452.87s**；
- 日志：
  `artifacts/final-test-nonslow-20260815-live-rebase.log`；
- live rebase/runtime/exports/handoff/Harness-control focused gate：
  **32 passed**；
- Ruff、Compileall、新增测试 Mypy：通过。

## 尚未实现

- `REBASE/FULL_RELOAD` 的跨 Scheduler/Kernel/Harness/VPG 原子 ownership
  transaction；
- Context delta 自动 materialization，以及 `run()/run_async()` 主路径自动
  incremental cognition/rebase；
- always-on event-driven controller 和通用后台 watcher；
- 任意隐藏文件/API/浏览器/Python I/O 的自动 provenance discovery；
- 任意 Python callback 的进程隔离和 force-kill；
- 物理 CPU/GPU/RAM/VRAM telemetry、placement、quota/fairness；
- 分布式调度、leader election、consensus；
- 不可逆外部副作用 exactly-once、belief revision、field-level repair；
- 真实 LLM/GPU/provider 工作负载的统计性收益 benchmark。

## 下一步与预计时间

1. 设计并验证 atomic ownership protocol（预计 1–2 个开发日）；
2. 在该协议之上实现真正的 live `REBASE/FULL_RELOAD` 与 Context
   materialization（预计 2–4 个开发日）；
3. 接入 caller-driven epoch loop 后再逐步演进 always-on controller，并补
   crash/replay/adversarial tests（预计 3–5 个开发日）；
4. 构建真实 Harness/LLM 对比 benchmark（预计 3–7 个开发日，取决于模型、
   预算和任务集）。

当前项目仍是 **single-host research alpha / bounded vertical slices**，
不能宣传为通用生产级 Agent OS。
