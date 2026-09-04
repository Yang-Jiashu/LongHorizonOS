# Explicit compute budgets and verified-progress utility

**Status:** experimental, bounded, single-host, opt-in policy and execution
integration

`VerifiedProgressBudgetPolicy` answers one narrow online-compute question:

> Given the current graph-derived READY/repair frontier and caller-declared
> estimates, which tasks are worth admitting into the next bounded batch
> without exceeding the declared compute budget?

It does not replace the Scheduler. It produces an immutable, graph-fenced
proposal; Scheduler readiness/resource admission, Claims, Kernel Leases,
fencing, execution, verification, and VPG commit remain authoritative.
The proposal can be used by the explicit `budget_aware=True` path on
`AgentOS.run()` or `AgentOS.run_async()`. That path is opt-in, requires
`adaptive=True`, explicit estimates and limits, and does not use an
unfiltered Scheduler fallback. The default `adaptive=False` path is unchanged.

## Policy

Repair-ready tasks always remain ahead of ordinary READY tasks. Within each
tier, known candidates are ordered deterministically by the exact integer
ratio:

```text
          verified_progress_units
        × success_basis_points
        × input_stability_basis_points
utility = ─────────────────────────────────────────────
          normalized_cost_units
        + expected_rework_cost_units
```

The common basis-point denominator is omitted while comparing candidates.
Cross multiplication is used instead of floating-point weights, and task ID
provides a deterministic tie-break. This is a declared policy score, not a
learned prediction of real task value or model quality.

After ranking, the policy checks cumulative declared usage against five
independent hard ceilings:

| Dimension | Limit field | Estimate field |
|---|---|---|
| Model/tool tokens | `max_tokens` | `estimated_tokens` |
| Time | `max_wall_time_ms` | `estimated_wall_time_ms` |
| Money | `max_cost_microusd` | `estimated_cost_microusd` |
| Context | `max_context_tokens` | `estimated_context_tokens` |
| Verification | `max_verification_tokens` | `estimated_verification_tokens` |

`None` means that one dimension is unbounded by this policy. Dollar cost uses
integer micro-USD (`$1 = 1_000_000 microusd`). `wall_time_ms` is also summed as
a declared accounting dimension; it is not a measurement or prediction of
parallel wall-clock latency.

Every plan exposes `remaining_before` and `remaining_after` as
`ComputeBudgetRemaining` values. For each dimension, `None` means the caller
did not set a ceiling (unbounded); `0` means a ceiling exists and all capacity
has been consumed. A bounded dimension never becomes negative: over-budget
usage is reported as zero remaining and the policy blocks further admissions
for that dimension.

Missing, malformed, duplicate, or explicitly unknown estimates fail closed.
The policy also defers candidates when the graph fence, cognition projection,
or logical-resource projection is unavailable, when the Goal is closed, when
the task is terminal/stale outside the repair frontier, or when the task
already has an active Attempt.

## Minimal SDK example

`AgentOS.plan_budgeted_frontier(...)` observes an **already compiled** Goal.
The zero-dispatch call below compiles the Goal without running user work.

```python
from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    Goal,
    TaskComputeEstimate,
)

with AgentOS(":memory:") as runtime:
    runtime.add_agent(Agent("worker"))

    goal = Goal("budgeted-work")
    goal.task("cheap-high-value", agent="worker")
    goal.task("expensive-low-value", agent="worker")
    runtime.run(goal, max_dispatches=0)  # compile only; no executor is called

    plan = runtime.plan_budgeted_frontier(
        goal,
        estimates={
            "cheap-high-value": TaskComputeEstimate(
                task_id="cheap-high-value",
                verified_progress_units=10,
                success_basis_points=9_000,
                input_stability_basis_points=9_500,
                normalized_cost_units=2,
                expected_rework_cost_units=1,
                estimated_tokens=2_000,
                estimated_wall_time_ms=20_000,
                estimated_cost_microusd=3_000,
                estimated_context_tokens=1_000,
                estimated_verification_tokens=300,
                known=True,
            ),
            "expensive-low-value": TaskComputeEstimate(
                task_id="expensive-low-value",
                verified_progress_units=2,
                success_basis_points=8_000,
                input_stability_basis_points=8_000,
                normalized_cost_units=10,
                expected_rework_cost_units=5,
                estimated_tokens=8_000,
                estimated_wall_time_ms=60_000,
                estimated_cost_microusd=12_000,
                estimated_context_tokens=4_000,
                estimated_verification_tokens=1_000,
                known=True,
            ),
        },
        limits=ComputeBudgetLimits(
            max_tokens=4_000,
            max_wall_time_ms=30_000,
            max_cost_microusd=5_000,
            max_context_tokens=2_000,
            max_verification_tokens=500,
        ),
        usage=ComputeBudgetUsage(),
        max_parallelism=2,
    )

    print(plan.selected_task_ids)
    # ('cheap-high-value',)

    for decision in plan.decisions:
        print(decision.task_id, decision.reason, decision.budget_blockers)
```

The returned `VerifiedProgressBudgetPlan` includes the graph version and
projection hash, ordered candidates, selected/deferred tasks, per-task
reasons, usage before/after, remaining declared budget, unavailable fields,
and a canonical decision hash. `persist=True` writes only a bounded
`SchedulingEpoch` audit; it still does not reserve budget or dispatch work.
Budget admission is intentionally partial: a plan may select a safe prefix or
subset and defer other READY tasks with explicit budget blockers. The
Scheduler remains the final authority, so a Scheduler partial-admission result
charges only the task IDs it actually dispatches.

The pure policy can also be called with
`plan_verified_progress_budget(...)` or
`VerifiedProgressBudgetPolicy().plan(...)` when the caller already has a
`GlobalRuntimeState`.

## Opt-in execution accounting

For the main SDK path, pass `budget_aware=True` together with
`budget_estimates`, `budget_limits`, and (when continuing a prior run)
`budget_usage`:

```python
result = runtime.run_async(
    goal,
    adaptive=True,
    budget_aware=True,
    budget_estimates=estimates,
    budget_limits=limits,
    budget_usage=prior_usage,
    automatic_rebase=False,
)
```

Each scheduling epoch charges the declared estimate **when the authoritative
Scheduler actually dispatches that task**. A task rejected by the Scheduler,
or a graph race that dispatches nothing, consumes no budget. A dispatched
task
still consumes its declared estimate if its executor later fails or its
attempt becomes stale; this is conservative admission accounting, not a
provider invoice. `RunResult.meta` includes bounded budget limits/usage and
epoch audit fields. Compute-budget v1 is intentionally non-composable with
`resource_aware`, `conflict_graph`, or automatic rebase options; callers must
choose one policy path per run.

`UsageLedger` in `lhos.sdk.compute_usage` is a separate immutable value object
for attempt-level estimated/reserved/measured/terminal records. It accepts
measured usage only when a trusted executor/provider boundary supplies it and
keeps estimated, reserved, and measured values distinct. The ledger is
**purely in memory and not durable**: it is not restored by Scheduler replay,
does not enforce provider billing or quotas, and does not monitor a process.
Persist or export its records through an application-owned durable accounting
system if that is required.

## Controlled benchmark

```bash
lhos benchmark compute-budget --json
```

The deterministic gate compares repair-first lexical selection with the real
verified-progress budget policy under the same declared budget and
parallelism limit. It also checks all five hard-budget dimensions, repair
priority, and unknown-estimate fail-closed behavior.

This is a **controlled estimate benchmark**. It does not call an LLM, calibrate
the estimates, measure physical CPU/GPU/RAM/VRAM use, observe provider quotas,
or demonstrate real wall-clock or dollar savings.

## Current boundary

- Estimates and cumulative usage are supplied explicitly by the caller.
- The policy does not infer Goal-wide historical consumption from active
  `AgentSnapshot` values; callers pass cumulative `ComputeBudgetUsage`.
- It does not reserve tokens, money, Context, time, or verifier capacity.
- Main-path integration is explicit opt-in via `budget_aware=True`; the
  default `run()` / `run_async()` behavior remains unchanged.
- Budget consumption uses caller-declared estimates at dispatch time. It is
  not measured provider usage, provider billing, or a physical resource
  reservation.
- `UsageLedger` is in-memory only and is not part of durable Scheduler/VPG
  replay.
- It does not predict success, stability, rework, or semantic importance.
- It is graph-relative: undeclared dependencies or unobserved world changes
  remain outside the guarantee.
- It is not provider quota enforcement, fairness/starvation control, physical
  resource placement, or distributed scheduling.
