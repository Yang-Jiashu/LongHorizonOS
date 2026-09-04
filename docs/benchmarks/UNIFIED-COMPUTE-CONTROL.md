# Unified Compute-Control Benchmark

`unified_compute_control` compares four policies on the **same fixed
graph, estimates, access declarations, and logical resource capacity**:

| Mode | Budget ordering | Conflict guard | Logical resource fit |
| --- | --- | --- | --- |
| `static_fifo` | no | no | Scheduler admission only |
| `budget_only` | yes | no | Scheduler admission only |
| `resource_conflict` | no | yes | yes |
| `unified` | yes | yes | yes |

The workload has four tasks, a 1,000 CPU-millicore logical pool, a
two-task parallelism bound, and one deliberate shared-API write/write pair
(which also induces explicit read/write conflicts in the declared graph). The
unified policy first ranks the frontier by repair priority and declared
verified-progress utility, then scans the ranked candidates while applying
the declared conflict and resource constraints. A blocked high-utility task
does not prevent a later safe task from filling the batch.

Run it with:

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m lhos.benchmarks.unified_control
python -m pytest -q tests/benchmarks/test_unified_control.py
```

The canonical controlled output is:

| Mode | Goal closed | Epochs | Scheduler rejections | Stale attempts | Declared tokens |
| --- | ---: | ---: | ---: | ---: | ---: |
| `static_fifo` | yes | 4 | 2 | 1 | 880 |
| `budget_only` | yes | 3 | 2 | 0 | 640 |
| `resource_conflict` | yes | 3 | 0 | 0 | 640 |
| `unified` | yes | 3 | 0 | 0 | 640 |

All modes reach the same declared `VERIFIED` task set and 100 declared
progress units. Relative to the FIFO baseline, the unified trace avoids
10 units of declared stale-work risk, removes one stale attempt and two
resource rejections, and uses 240 fewer declared tokens. The static trace
also exceeds the deliberately shared hard budget because it pays for the
stale backend attempt; the unified trace remains within it.

## Scope

This is a **deterministic synthetic policy benchmark**, not a production
performance evaluation:

- costs are declared integer estimates, not provider-measured usage or billing;
- resources are logical Scheduler vectors, not physical CPU/GPU/RAM/VRAM
  telemetry;
- executors do not call an LLM, external API, or real multi-process runtime;
- no claim is made about model quality, wall-clock speedup, or dollar savings.

The benchmark validates that the policy composition has an auditable,
graph-relative control trace. A real workload evaluation still needs measured
provider usage, calibrated estimates, hidden-provenance cases, and repeated
tasks across independent seeds.
