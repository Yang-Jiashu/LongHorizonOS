# LongHorizonOS Implementation Progress

Timestamp: 2026-08-16 20:28 +08:00

## Target

Ship the first bounded, opt-in compute-budget execution path with deterministic
graph-derived admission, declared-usage accounting, bounded audits, tests,
documentation, and release-package verification.

## Implemented Since The Previous Snapshot

- Completed budget-aware `AgentOS.run()` and `AgentOS.run_async()` integration.
- Sync execution charges declared estimates for Scheduler-dispatched tasks.
- Async execution charges only tasks that become valid `WorkerJob` instances.
- Failed or stale dispatched attempts still consume declared budget.
- Graph-version races with no dispatch consume no budget.
- Partial Scheduler admission separates planned usage from actual charged usage.
- Budget mode disables the unfiltered adaptive fallback.
- Final `RunResult.meta` exposes the selected policy, hard limits, and cumulative
  declared usage.
- Added 11 main-path sync/async tests; the implementation agent reported
  73 focused tests passing with Ruff and Mypy clean.
- Added `ComputeBudgetRemaining`: `None` now means unbounded and `0` means a
  bounded dimension is exhausted.
- `ComputeBudgetUsage.plus()` now rejects non-usage operands with a clear
  `TypeError`.
- Empty or blank graph projection hashes now fail closed.
- Added focused audit edge tests; the audit agent reported 51 focused tests
  passing with Ruff and Mypy clean.

## In Progress

- Add `ComputeBudgetRemaining` to the public SDK exports.
- Verify and finish README, compute-budget guide, status, progress, and
  changelog updates after the documentation worker stopped unexpectedly.
- Run the combined focused gate against the merged shared workspace.

## Not Yet Complete

- Repository-wide Ruff format/check and Mypy.
- Full non-slow Pytest suite.
- Full slow Pytest suite.
- Rebuilt wheel/sdist containing the compute-budget execution path.
- Fresh-install SDK and CLI smoke tests from the rebuilt wheel.
- Durable provider-measured accounting remains future work; current execution
  charges caller-declared estimates and the `UsageLedger` is in-memory only.

## Estimate

- Export/documentation reconciliation and combined focused gates: 20-40
  minutes.
- Full non-slow/slow suites: approximately 30 minutes of known baseline runtime,
  plus time for any regressions.
- Package rebuild and fresh-install smoke: 15-30 minutes after all gates pass.
