# FrontierPolicy and SchedulingEpoch (experimental)

`FrontierPolicy` is the first small step toward LongHorizonOS's online
long-running-compute-management design.  It consumes an immutable
`GlobalRuntimeState` projection and produces a deterministic
`SchedulingEpoch`: a proposed **WHAT/WHEN** batch for the observed VPG
frontier.

```python
from lhos.sdk import FrontierPolicy, build_runtime_state_view

state = build_runtime_state_view(os, goal)
epoch = FrontierPolicy(max_parallelism=2).plan(state, epoch_id=1)
print(epoch.selected_task_ids)
```

The historical default remains repair-first lexical ordering.  Critical-path
and downstream-unlock ordering is a separate, explicit opt-in:

```python
from lhos.sdk import FrontierPolicy, FrontierRankingStrategy

epoch = FrontierPolicy(
    max_parallelism=2,
    ranking_strategy=FrontierRankingStrategy.GRAPH_UTILITY,
).plan(state, epoch_id=2)
```

The same option is exposed by the read-only facade:

```python
epoch = os.plan_frontier(
    goal,
    max_parallelism=2,
    ranking_strategy="graph_utility",
)
```

## What this primitive guarantees

- candidates come only from the observed READY / repair-ready frontier;
- repair-ready tasks are ranked before ordinary READY tasks;
- the default `repair_first_lexical` strategy is backward compatible;
- opt-in `graph_utility` ranks candidates within the repair boundary by:
  1. membership in the declared-VPG critical path;
  2. earlier position on that path;
  3. larger immediate downstream-unlock value;
  4. lexical task id for deterministic ties;
- terminal (`VERIFIED` / `INVALID`) tasks are never selected;
- a task with a current cognition attempt is not selected a second time;
- selection and tie-breaking are deterministic and hashable;
- the returned Pydantic models are frozen and use tuple-valued collections;
- no scheduler, claim, lease, resource reservation, or executor is called.
- when used alone, this policy does not inspect read/write conflicts; see
  [`CONFLICT-GRAPH.md`](CONFLICT-GRAPH.md) for the separate opt-in
  conflict-aware batch suggestion.

`parallelism_hint` is the size of the proposed batch (bounded by
`max_parallelism`).  It is **not** a resource-admission decision.  If the
runtime projection says logical resources are unavailable, the policy fails
closed and emits no selected tasks.

The graph-utility inputs come from `RuntimeStateView.progress.critical_path`
and `downstream_unlock_values`.  They are relative only to declared VPG
`depends_on` edges.  The unlock value counts immediately unlockable direct
consumers; it is not a prediction of success, token cost, runtime, semantic
importance, or total transitive work.  A distinct
`graph-utility-frontier.v1` policy id makes persisted proposals auditable.

## Explicit non-goals

This is an opt-in research primitive, not a replacement for the current
Scheduler.  It does not yet implement:

- physical CPU/GPU/RAM/VRAM telemetry or task-level resource fitting;
- conflict-aware parallelism is available only through the separate
  `DynamicParallelismPolicy` primitive; it is not integrated into this policy
  or the default Scheduler;
- learned utility, duration estimates, success probability, token/dollar cost,
  or optimal critical-path scheduling;
- preemption, pause, rebase, interrupt delivery, or dynamic worker control;
- automatic provenance/dependency discovery;
- production scheduling guarantees.

The existing Scheduler remains authoritative for **WHO/WHERE** decisions:
eligibility, agent matching, resource admission, Claim, Lease, fencing, and
execution.  Future epochs can safely feed richer state into a policy without
changing those ownership semantics.
