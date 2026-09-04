# LongHorizonOS implementation progress

**Timestamp:** 2026-08-16 20:42 (+08:00), documentation/export refresh after
the execution-path integration audit  
**Scope:** explicit compute budgets, bounded execution accounting, and
verified-progress utility  
**Release boundary:** experimental single-host research alpha

## Planned slice

1. Define immutable task estimates, cumulative usage, and hard limits for
   tokens, time, money, Context, and verification.
2. Rank graph-ready computation by deterministic expected verified-progress
   utility while preserving repair-frontier priority.
3. Add a read-only `AgentOS` facade and optional bounded epoch audit.
4. Add an explicit opt-in main-path integration for `run()` and `run_async()`.
5. Add immutable attempt usage accounting with separate estimated/reserved/
   measured/terminal states.
6. Add a controlled static-versus-budget-policy benchmark and CLI gate.
7. Document the API and its non-predictive/advisory boundary.
8. Run focused gates, then repository-wide quality/correctness gates and
   rebuild release artifacts only after the source stabilizes.

## Implemented in the current checkout

- `src/lhos/sdk/compute_budget.py`
  - immutable `TaskComputeEstimate`, `ComputeBudgetLimits`, and
    `ComputeBudgetUsage`, plus `ComputeBudgetRemaining` (`None` means
    unbounded; `0` means a bounded dimension is exhausted);
  - five independent declared hard-budget dimensions;
  - exact integer-ratio verified-progress utility;
  - repair-before-READY ordering;
  - graph/cognition/resource/active-attempt/terminal-state guards;
  - missing or unknown estimate fail-closed behavior;
  - immutable auditable plan with a canonical decision hash.
- `AgentOS.plan_budgeted_frontier(...)`
  - observes an already compiled Goal;
  - is read-only by default;
  - may persist a bounded `SchedulingEpoch` audit when explicitly requested;
  - does not create a Claim/Lease, reserve resources, or dispatch work.
- Explicit `budget_aware=True` integration on `AgentOS.run()` and
  `run_async()`
  - requires `adaptive=True`, caller-declared estimates and hard limits;
  - charges only tasks actually dispatched by the authoritative Scheduler;
  - charges dispatched failed/stale attempts conservatively;
  - supports partial admission; deferred tasks retain budget-blocker reasons;
  - a graph race that dispatches no task consumes no declared usage;
  - rejects an unfiltered fallback and remains opt-in; the default path is
    unchanged.
- `src/lhos/sdk/compute_usage.py`
  - immutable `UsageVector`, attempt identity/record/aggregate, and
    `UsageLedger`;
  - strict lifecycle transitions with idempotent replay and conflict checks;
  - terminal outcomes require caller-supplied authoritative measured usage;
  - **pure in-memory accounting only**, not durable Scheduler/VPG state or
    provider billing.
- Controlled benchmark and CLI:
  - `src/lhos/benchmarks/compute_budget.py`;
  - `lhos benchmark compute-budget --json`;
  - benchmark/CLI focused gate: **8 passed** on 2026-08-16.
- Public contract documentation:
  - [`COMPUTE-BUDGET.md`](COMPUTE-BUDGET.md).
- Public SDK export:
  - `ComputeBudgetRemaining` is exported from `lhos.sdk` and included in its
    declared `__all__`.

## Verification completed for this slice

- The stale **23 passed, 1 failed** snapshot is superseded; its ordering
  expectation was corrected to match exact ratio ordering
  (`fast (3/2)`, `tie (1/1)`, `slow (2/3)`).
- Focused policy/facade coverage is recorded as **39 passed** and focused
  `UsageLedger` coverage as **15 passed**; benchmark/CLI coverage is recorded
  as **8 passed**. These gates overlap with other SDK tests and are not a
  repository-wide total.
- The complete SDK test directory passed with **622 passed** during the
  focused audit.
- Repository static gates passed: Ruff format reported **636 files already
  formatted**, Ruff lint passed, Mypy passed over **276 source files**, and
  `compileall` passed.
- Full non-slow: **3411 passed, 1 skipped, 18 deselected, 30 warnings in
  517.71s**.
- Full slow marker: **18 passed, 3412 deselected in 1253.07s**.
- The rebuilt wheel/sdist passed `twine check`; fresh-install SDK, benchmark,
  recovery, and budget-aware execution smokes passed.
- The previously published August 16 wheel predates this feature and must not
  be presented as containing compute-budget support.

## Still not implemented

- Automatic calibration of success probability, input stability, expected
  rework, task value, or real provider price/latency.
- Durable persistence/replay of `UsageLedger` records and provider-trusted
  measured billing. The current ledger is in-memory and requires the caller
  to supply measured values.
- Provider RPM/TPM quota enforcement, physical device placement/isolation,
  fairness/starvation policy, or distributed budget coordination.
- Real-model/GPU/competitor evaluation showing wall-clock, token, or dollar
  improvement.

## Next implementation order

1. Add provider-trusted measurement and real-provider evaluation as a separate
   follow-up; do not relabel declared estimates as measured billing.

No elapsed-time promise is attached to these steps. Completion is determined
by the corresponding test and packaging gates, not by a speculative deadline.
