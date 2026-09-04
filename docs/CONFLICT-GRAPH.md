# ConflictGraph and conflict-aware parallelism (experimental)

LongHorizonOS now exposes a small, **opt-in** primitive for testing
state-dependent parallelism without changing the default Scheduler:

```python
from lhos.sdk import (
    ConflictGraph,
    DynamicParallelismPolicy,
    TaskAccessSet,
)

conflicts = ConflictGraph.from_access_sets(
    [
        TaskAccessSet(task_id="backend", write_set=("workspace://api.py",)),
        TaskAccessSet(task_id="frontend", read_set=("workspace://api.py",)),
        TaskAccessSet(task_id="docs", write_set=("workspace://README.md",)),
    ]
)
suggestion = DynamicParallelismPolicy(max_parallelism=2).suggest(
    runtime_state,
    conflicts,
    epoch_id=1,
)
```

## What it does

`ConflictGraph` derives deterministic pairwise edges from **explicit exact-match
access declarations**:

- write/write overlap → `WRITE_WRITE`;
- write/read overlap → `READ_WRITE`;
- either declaration marked `known=False` → `UNKNOWN_ACCESS`.

`DynamicParallelismPolicy` then greedily selects an independent batch from the
already observed READY/repair-ready frontier. Repair-ready tasks are considered
first, ties are lexical, and `max_parallelism` is a strict upper bound.

Missing or unknown access declarations are **serial-only**: the policy may
propose one such task when the batch is empty, but never groups it with another
task. The result sets `safe_under_declared_accesses=False` and records an
`UnavailableField`; this is an explicit warning, not proof that hidden
conflicts do not exist.

## What it does not do

This primitive does not:

- discover hidden filesystem/API/tool reads;
- perform wildcard/path/semantic alias analysis;
- infer dependency edges or input stability;
- fit task resource requests to physical CPU/GPU/RAM/VRAM;
- claim work, acquire leases, reserve resources, preempt workers, or execute;
- provide a production safety or throughput guarantee.

The existing Scheduler remains the authority for eligibility, agent matching,
resource admission, Claim, Lease, fencing, and execution. A caller must treat
the suggestion as a deterministic planning hint and revalidate all ownership
and resource conditions before dispatch.

