# LongHorizonOS Implementation Progress

Timestamp: 2026-08-16 19:58 +08:00

## Target

Finish the first bounded compute-budget execution path for `AgentOS.run()` and
`AgentOS.run_async()`, with deterministic admission, cumulative declared-usage
accounting, fail-closed behavior, audit metadata, tests, documentation, and
release-package verification.

## Implemented

- Deterministic `VerifiedProgressBudgetPolicy`.
- Exact integer verified-progress/cost ranking.
- Repair-frontier priority.
- Five hard budget dimensions: token, wall time, micro-USD, context, and
  verification.
- Fail-closed handling for missing, malformed, duplicate, or unknown estimates.
- Graph, cognition, active-attempt, terminal-state, and resource admission
  checks.
- Immutable budget plans and canonical decision hashes.
- Controlled `lhos benchmark compute-budget --json` benchmark.
- Immutable in-memory attempt `UsageLedger` that separates estimated, reserved,
  measured, and terminal accounting states.
- Focused policy/facade/benchmark/CLI/accounting tests.

## In Progress

- Complete dispatch-time declared-usage charging in `run_async()`.
- Ensure sync and async epoch audits report actual dispatched work.
- Expose final budget limits and cumulative usage in `RunResult.meta`.
- Add dedicated sync/async main-path tests and failure/race regression tests.
- Verify public SDK exports and update README/status/changelog boundaries.
- Independent logic audit of budget and usage modules.

## Not Yet Complete

- Repository-wide Ruff formatting and lint gate after integration.
- Repository-wide Mypy gate after integration.
- Full non-slow and slow Pytest gates.
- Rebuilt wheel/sdist containing the new feature.
- Fresh-environment install and CLI smoke test of the rebuilt wheel.
- Durable provider-measured billing/accounting. The current execution path
  charges declared estimates; `UsageLedger` is in-memory only.

## Estimate

- Main-path implementation and focused tests: approximately 30-60 minutes.
- Full non-slow/slow gates and fixes: approximately 30-90 additional minutes,
  depending on failures and machine runtime.
- Package rebuild and fresh-install smoke: approximately 15-30 minutes after
  all gates pass.

These estimates are engineering estimates, not a promise that unknown
repository-wide failures will be absent.
