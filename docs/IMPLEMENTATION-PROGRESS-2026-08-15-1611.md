# LongHorizonOS implementation progress — 2026-08-15 16:11 CST

## 本轮要实现

- 为 `AgentOS.plan_live_context_rebase(...)` /
  `apply_live_context_rebase(...)` 补 authority-backed 端到端测试。
- 覆盖 `REUSE` apply、same-plan 幂等 replay、`REBASE/FULL_RELOAD`
  fail-closed，以及 Claim/Lease、Harness revision、AgentSnapshot 变化后的
  identity fencing。

## 已实现

- 新增 `tests/sdk/test_live_context_rebase_e2e.py`，使用真实 retained
  Claim/Lease、Scheduler Attempt、durable `AgentSnapshot`、registered Harness
  和权威 VPG version advance。
- 修复 same-plan `REUSE` replay：只有 exact `(claim_id, request_id)` 已存在
  Harness control cache 时，才允许越过“计划前 revision”进入现有
  `control_harness` 幂等重放；Graph、Claim/Lease、Attempt、AgentSnapshot、
  Context 和 Harness identity fence 仍全部执行。
- 第二次 apply 返回 `replayed=True`，Harness hook 不会再次执行。
- 非缓存 Harness revision 变化、AgentSnapshot 变化以及 Claim/Lease 丢失仍然
  fail-closed。

## 测试证据

- 新 E2E 文件：**6 passed**。
- rebase/runtime/public exports/live E2E/handoff 联合门禁：**23 passed**。
- Ruff（`src/lhos/sdk/os.py` 与新测试）：通过。

## 仍未实现

- `REBASE/FULL_RELOAD` 的原子 Scheduler/Kernel/Harness ownership transaction。
- Context delta 自动 materialization。
- `run()` / `run_async()` 主路径自动 incremental cognition/rebase。
- always-on watcher/controller 和非 cooperative process cancellation。

## 下一步

- 主线程执行完整 non-slow 串行回归并同步最终测试数字。
- 原子 ownership protocol 完成前，live façade 继续保持 bounded
  `REBASE/FULL_RELOAD` refusal。


## 16:25 ????

- live rebase plan ??????? authoritative VPG version??? invented future/stale target?
- apply ???? plan/delta/freshness hash ? Graph?Claim/Lease?Task?Agent?Process?Attempt?semantic epoch?AgentSnapshot?Context ? Harness identity?
- exact same-plan `REUSE` ?? `(claim_id, request_id)` ???????? `replayed=True`??????? Harness hook???? revision ??? fail closed?
- ?? `tests/sdk/test_live_context_rebase_fences.py`??? future target?process identity tamper?graph-version tamper?
- live rebase/runtime/exports/handoff/Harness control ?????**32 passed**?Ruff?Compileall ????? Mypy ???
- `REBASE/FULL_RELOAD` ??? bounded refusal?? Scheduler/Kernel/Harness ?? ownership transaction ?????
