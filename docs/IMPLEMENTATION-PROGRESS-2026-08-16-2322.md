# LongHorizonOS Implementation Progress

Timestamp: 2026-08-16 23:22 +08:00

## Current Milestone

Unify graph-relative verified-progress utility, declared compute budgets,
read/write conflicts, and logical resource capacity into one deterministic
adaptive scheduling epoch.

## Implemented

- New pure `UnifiedAdaptivePolicy`.
- Repair-first exact utility ordering.
- Single-pass conflict, budget, logical-resource, and parallelism admission.
- Backfill: a high-utility task rejected by one constraint does not consume
  budget/resource capacity or prevent a later safe candidate from running.
- Per-task budget, resource, access, conflict, assignment, and utility audit.
- Immutable `UnifiedAdaptivePlan` with a canonical decision hash.
- Conservative active-attempt access checks.
- Explicit unknown-access serial occupancy fence, including when a caller's
  ConflictGraph omits the unknown task.
- Controlled unified-compute benchmark comparing static FIFO, budget-only,
  resource/conflict-only, and unified policy traces.

## Verified

- Unified policy plus benchmark focused gate: 15 passed.
- Ruff format/check: passed for the new policy/benchmark files.
- Mypy: passed for the new policy/benchmark files.

## In Progress

- Wire the unified policy into `AgentOS.run()` and `run_async()`.
- Combination API:
  `adaptive=True, budget_aware=True, resource_aware=True`.
- Preserve budget-only, resource-only, conflict-only, and default behavior.
- Add bounded unified epoch audit and dispatch-time actual usage accounting.

## Remaining

- Public SDK exports.
- `lhos benchmark unified-control --json`.
- README/status/changelog updates.
- Sync/async main-path tests.
- Full static and repository regression gates.
- Rebuilt release artifacts and fresh-install smoke.

## Boundary

The benchmark is deterministic and synthetic. Budget values are declared
estimates and resources are logical Scheduler vectors; this is not yet
real-provider billing, physical GPU scheduling, or real-Harness wall-clock
evidence.
