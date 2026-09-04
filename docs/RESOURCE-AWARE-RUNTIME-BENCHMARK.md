# Resource-Aware Adaptive Runtime Benchmark

## Run it

From the repository root:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python -m lhos.benchmarks.resource_aware_runtime
```

The same report is available through the installed CLI:

```powershell
lhos benchmark resource-aware-runtime --json
```

Both commands print one JSON report to stdout. The CLI exits `0` only when the
report's controlled `valid` gate passes.

## What it compares

Both cases execute the same four independent tasks through the real public
`AgentOS.run_async` path:

```text
adaptive policy
  -> Scheduler logical resource admission
  -> TaskClaim
  -> Kernel Lease
  -> AsyncWorkerPool
  -> verifier
  -> VPG Evidence
```

The single logical Agent pool declares 1,000 CPU millicores. Task requests are:

```text
a-heavy = 700
b-heavy = 700
c-light = 300
d-light = 300
```

The parallelism bound is two.

- `static` is the conflict-aware fixed-parallelism adaptive path without
  policy-side resource fitting.
- `resource_aware` adds the deterministic resource-aware packing policy.

All task read/write declarations are independent, so this benchmark isolates
logical resource fitting rather than conflict serialization.

## Controlled result

The deterministic expected result is:

| Metric | Static / conflict-only | Resource-aware |
|---|---:|---:|
| Goal closed | yes | yes |
| Tasks VERIFIED | 4 / 4 | 4 / 4 |
| Scheduling epochs | 3 | 2 |
| Advisory over-capacity proposals | 1 | 0 |
| Scheduler resource rejections | 1 | 0 |
| Admitted over-capacity batches | 0 | 0 |
| Executor capacity violations | 0 | 0 |
| Dispatched attempts | 4 | 4 |

The static first batch proposes:

```text
a-heavy (700) + b-heavy (700) = 1,400 > 1,000
```

The authoritative Scheduler rejects one task, so no unsafe work is admitted.
It then needs two more epochs to close the Goal.

The resource-aware policy instead selects:

```text
epoch 1: a-heavy (700) + c-light (300) = 1,000
epoch 2: b-heavy (700) + d-light (300) = 1,000
```

This reduces the controlled epoch count from three to two and avoids one
predictable Scheduler rejection while reaching the same VERIFIED Goal.

A machine-readable reference run is checked in at
`artifacts/resource-aware-runtime-20260815.json`.

## What the result does **not** prove

This is a deterministic synthetic systems regression workload. It validates:

- policy-side logical resource fitting;
- Scheduler revalidation and rejection of unsafe advisory proposals;
- Claim, Kernel Lease, verifier, and VPG Evidence traversal;
- closure and capacity-safety metric plumbing.

It does **not** measure or prove:

- physical CPU/GPU/RAM/VRAM utilization or isolation;
- real LLM/provider latency, tokens, cost, or quality;
- production wall-clock acceleration;
- multi-host placement or distributed scheduling;
- hidden provenance or automatic dependency discovery.

`proposal_capacity_violations` therefore means that the advisory selected
batch exceeded declared **logical** capacity. It never means LongHorizonOS
actually admitted or executed an over-capacity batch.
