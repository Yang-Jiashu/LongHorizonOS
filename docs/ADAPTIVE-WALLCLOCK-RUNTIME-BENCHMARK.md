# Adaptive Runtime Real-Wall-Clock Benchmark

## Run it

From the repository root:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python -m lhos.benchmarks.adaptive_wallclock_runtime
```

Or use the installed CLI:

```powershell
lhos benchmark wallclock-adaptive-runtime --json
```

Use `--delay-ms 20` to change the deterministic per-task I/O delay. The run is
bounded to four tasks and completes quickly.

## What is real

Both cases traverse the public execution path:

```text
Graph-derived adaptive policy
  -> Scheduler logical resource admission
  -> TaskClaim
  -> Kernel Lease and fencing
  -> AsyncWorkerPool
  -> async executor
  -> verifier
  -> VPG Evidence
  -> VERIFIED Goal
```

The executor performs actual `asyncio.sleep` work. Elapsed time is measured
with `time.perf_counter`, so this is not a simulated-clock result.

The workload declares one logical pool with 1,000 CPU millicores:

```text
a-heavy = 700
b-heavy = 700
c-light = 300
d-light = 300
```

All task read/write sets are explicit. `a-heavy` and `d-light` share one
declared write target, so the Graph contains a real write/write conflict. The
expected batches never group that pair; this exercises conflict safety without
hiding the resource-packing difference.

## What is compared

- **Static/conflict-only:** fixed lexical parallelism proposes
  `a-heavy + b-heavy` (1,400 logical millicores). The authoritative Scheduler
  rejects one task, so the run needs three epochs.
- **Adaptive:** resource/conflict-aware packing selects `700 + 300` in each
  epoch and closes the same VERIFIED Goal in two epochs.

The stable correctness assertions are:

| Metric | Static | Adaptive |
|---|---:|---:|
| Goal closed | yes | yes |
| Tasks VERIFIED | 4 / 4 | 4 / 4 |
| Scheduling epochs | 3 | 2 |
| Scheduler resource rejections | 1 | 0 |
| Dispatched attempts | 4 | 4 |
| Executor logical-capacity violations | 0 | 0 |
| Claim/Lease-fenced attempts | 4 / 4 | 4 / 4 |

The JSON report additionally contains observed elapsed seconds and an observed
speedup ratio.

A reference run captured on **August 16, 2026** is stored at
`artifacts/adaptive-wallclock-runtime-20260816.json`. That local observation
reported 0.297408 seconds for the static case and 0.227945 seconds for the
adaptive case (1.304736x observed speedup). Treat those timing values as one
machine observation, not a guaranteed performance result.

## Why wall-clock is not a pass/fail threshold

Tiny local runs are sensitive to SQLite, Python, CI, antivirus, and operating
system scheduling noise. Therefore the benchmark reports wall-clock values but
does **not** fail if the measured adaptive case happens to be slower in one
run. The gate checks graph closure, the deterministic epoch/batch trace,
Scheduler rejections, Claim/Lease ownership, Evidence publication, and
resource safety.

For a presentation, run it multiple times and report all observations rather
than selecting the fastest sample.

## Scope

This is a bounded deterministic synthetic I/O systems benchmark. It does not
measure or prove:

- real LLM/provider latency, tokens, cost, or answer quality;
- physical CPU/GPU/RAM/VRAM placement, utilization, isolation, or quotas;
- distributed scheduling;
- hidden provenance discovery;
- production throughput or a general speedup guarantee.

Its narrow claim is reproducible: with the declared graph, accesses, and
logical resources, adaptive policy removes one predictable rejection and one
scheduling epoch while reaching the same VERIFIED Goal through the real
single-host AgentOS control path.
