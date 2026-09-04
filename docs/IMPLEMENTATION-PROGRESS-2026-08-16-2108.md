# LongHorizonOS Implementation Progress

Timestamp: 2026-08-16 21:08 +08:00

## Target

Complete and release-check the opt-in verified-progress compute-budget
execution path for the single-host online compute-control research alpha.

## Implemented

- Budget-aware `AgentOS.run()` and `AgentOS.run_async()` main paths.
- Dispatch-time declared usage accounting.
- Conservative charging for dispatched failed/stale attempts.
- No charge for Scheduler rejection or graph races with no dispatch.
- Partial-admission audit that separates planned and actually charged usage.
- Five-dimensional hard budget admission.
- `ComputeBudgetRemaining` with `None` for unbounded and `0` for bounded
  exhausted capacity.
- Empty projection-hash fail-closed behavior.
- Immutable in-memory `UsageLedger`.
- Public SDK exports, README/Chinese README, compute-budget guide,
  implementation status, progress record, and changelog updates.
- Controlled compute-budget CLI benchmark with no reported violations.

## Verified

- Entire SDK test directory: 622 passed in the focused audit run.
- Repository static release gates:
  - Ruff format: 636 files already formatted.
  - Ruff lint: passed.
  - Mypy: 276 source files passed.
  - Compileall: passed.
- Final source-tree `compute-budget --json` CLI smoke: passed with
  `violations=[]`.
- Full non-slow: 3411 passed, 1 skipped, 18 deselected, 30 warnings in
  517.71s.
- Full slow marker: 18 passed, 3412 deselected in 1253.07s.

## Remaining

- No remaining release gate for this bounded compute-budget slice.
- Broader unified-policy and real-provider work remains outside this slice.

## Boundary

The execution path charges caller-declared estimates. It does not yet use
durable provider-measured billing, physical GPU accounting, or a distributed
budget authority. The broader unified Adaptive Policy Engine remains future
work.
