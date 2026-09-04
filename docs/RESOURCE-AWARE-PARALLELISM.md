# Resource-aware parallelism (experimental)

`ResourceAwareParallelismPolicy` is an opt-in planning primitive for the
LongHorizonOS control plane.  It extends the explicit-access
`DynamicParallelismPolicy` with additive logical resource fitting:

```python
from lhos.sdk import (
    ConflictGraph,
    ResourceAwareParallelismPolicy,
    TaskAccessSet,
)

accesses = ConflictGraph.from_access_sets(
    [
        TaskAccessSet(task_id="backend", write_set=("workspace://backend",)),
        TaskAccessSet(task_id="frontend", write_set=("workspace://frontend",)),
    ]
)

suggestion = ResourceAwareParallelismPolicy(max_parallelism=2).suggest(
    runtime_state,
    accesses,
    {
        "backend": {"cpu_millis": 500, "ram_bytes": 256_000_000},
        "frontend": {"cpu_millis": 500, "ram_bytes": 256_000_000},
    },
    epoch_id=1,
)
```

The policy greedily evaluates the repair-ready/ready frontier in deterministic
order (repair first, then explicit-access declarations, then lexical task id).
For every selected task it checks:

1. the task has a known explicit resource request;
2. the runtime projection has a known logical `available` vector;
3. the request fits the selected pool after earlier tasks in this batch;
4. the task has no explicit `ConflictGraph` conflict with selected or active
   attempts; and
5. `max_parallelism` has not been reached.

Pool selection is lexical when `pool_id` is omitted.  A request can pin a pool
with `{"pool_id": "gpu", "resources": {...}}`.  Model slots are additive and
fit by exact slot name.

An absent/unknown access declaration is **serial-only**.  In particular, an
unknown candidate is also deferred while any attempt is active, even when the
active attempt has a complete read/write snapshot: without a candidate
read/write set the policy cannot prove non-overlap.  The decision records
`unknown_access_active_occupancy` and the exact `active:<attempt_id>` blockers.

## Fail-closed boundaries

Missing/malformed requests, unavailable pools, missing capacities, incomplete
active read/write sets, and unknown access declarations never authorize an
unsafe parallel batch.  The result includes `UnavailableField` diagnostics and
`safe_under_declared_resources` / `safe_under_constraints` flags.  A task that
does not fit the current capacity is deferred with deterministic shortage
details; the policy does not pretend that it can wait-list or reserve it.

This is still a **logical scheduler projection**, not physical CPU/GPU/RAM/VRAM
placement.  The policy does not claim work, acquire leases, reserve capacity,
preempt workers, or execute tasks.  The existing Scheduler remains the
authority and must revalidate graph version, eligibility, Claims, Leases,
fencing, and resource admission before dispatch.  Default `run()` and
`run_async()` semantics are unchanged.

## Opt-in execution facade

The same policy can be used as an advisory filter on the SDK execution loops:

```python
result = os_.run(
    goal,
    adaptive=True,
    resource_aware=True,
    max_parallelism=4,
    max_dispatches=16,
)

result = await os_.run_async(
    goal,
    adaptive=True,
    resource_aware=True,
    max_parallelism=4,
    max_concurrency=4,
)
```

`resource_aware=True` is deliberately explicit and requires
`adaptive=True`.  The run loop derives requests from each compiled
`Task.resources` declaration and derives a conservative `ConflictGraph` from
the task's declared `inputs`/`outputs` when a graph is not supplied.  The
policy's `selected_task_ids` are passed only as `allowed_task_ids`; the
Scheduler still decides whether a task is eligible, whether its logical
resource reservation fits, and whether its Claim/Lease can be fenced.  The
result metadata includes `resource_aware: true` and the bounded per-epoch
selection/audit records.  Omitting the flag (the default) preserves the
historical execution path.

### Bounded per-epoch audit

Both `run()` and `run_async()` expose the same resource-policy diagnostic in
each resource-aware adaptive epoch:

```python
epoch = result.meta["adaptive_epochs"][0]
audit = epoch["resource_audit"]

audit["assignments"]       # task, chosen logical pool, fixed resource vector
audit["decisions"]         # run/defer reason and bounded blockers per task
audit["pool_ids"]          # logical pools referenced by the retained records
audit["safe_under_declared_resources"]
audit["safe_under_constraints"]
audit["unavailable"]       # bounded unavailable field/reason summaries
```

This is a `resource-aware-run-audit.v1` **RunResult-only** projection.  It does
not expand the durable `SchedulingEpoch` schema.  The projection retains at
most 32 assignments, 64 decisions, 32 pool ids, and 32 unavailable records.
Each decision retains at most four blockers and each resource vector retains
at most four model slots.  Identifiers are capped at 160 characters and
reasons/blockers at 240 characters.  Every collection includes its full
`*_count` and `*_truncated` signal, while the top-level `truncated` flag reports
any record, nested collection, or string truncation.

Only structured resource-policy fields are admitted.  Prompts, Context
manifests, arbitrary task metadata, candidate payloads, and executor output
are never copied into this audit.  The record explains an advisory decision;
the Scheduler's separately bounded `scheduler_skipped` transcript remains the
authority for actual admission failures.
