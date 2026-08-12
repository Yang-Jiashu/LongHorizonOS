# Async AgentOS Benchmark

This benchmark is a controlled, offline regression gate for the public
`AgentOS.run_async(...)` path. It compares the same independent-task workload
with `max_concurrency=1` and `max_concurrency=4`.

## Run it

From a source checkout:

```bash
python scripts/benchmark_multi_agent_runtime.py --check
```

The command prints a JSON report, writes
`artifacts/benchmark_results/multi-agent-runtime.json`, and exits non-zero when
correctness, ownership, capacity, resource-admission, or minimum-speedup gates
fail.

The checked-in default workload uses:

- 24 independent Agent tasks;
- two Agents, each limited to two concurrent tasks;
- a global parallel limit of four;
- a 25 ms `asyncio.sleep` executor per task;
- an independent synchronous verifier per task;
- a full logical resource request per task:
  `500 cpu_millis`, `16 MB RAM`, one GPU, `256 MB VRAM`, and one
  `benchmark-model` slot.

## What it exercises

Both the serial baseline and parallel measurement use the public SDK and run
through:

1. Goal and Task compilation into the VPG;
2. graph-derived readiness;
3. Scheduler matching and Claim creation;
4. all-or-nothing typed resource reservation;
5. Kernel Lease acquisition;
6. executor dispatch through `AgentOS.run_async(...)`;
7. independent verification;
8. Evidence attachment and VPG derivation;
9. semantic Goal closure;
10. Claim completion and resource release.

The gate also checks that:

- every executor starts with a live Claim, Kernel Lease, and RUNNING Attempt;
- every executor starts with its complete resource vector reserved;
- global and per-Agent concurrency limits are respected;
- every Task is dispatched once and ends `VERIFIED`;
- every Claim ends `COMPLETED`;
- every Attempt ends `VERIFIED_SEMANTICALLY`;
- no resource reservation or executor remains active after the run.

## Reference result

The checked-in result is a reproducible reference measurement for this
controlled workload, not a general performance claim. Read the exact values
from:

```text
artifacts/benchmark_results/multi-agent-runtime.json
```

The report includes serial and parallel elapsed time, measured speedup, peak
global and per-Agent concurrency, ownership/resource-admission violations,
Claim and Attempt counts, and the complete correctness contract.

The default gate requires at least `1.5x` speedup. This threshold is
intentionally below the theoretical `4x` executor overlap because the timing
includes Goal compilation, SQLite-backed Scheduler/Kernel/VPG work, serialized
semantic commits, and cleanup.

The checked-in 25 ms workload is timing-sensitive. On a busy local machine it
may fall below the conservative `1.5x` gate even when the correctness contract
passes. For a more stable local performance check, use `--delay-ms 50` or
`--delay-ms 100`, and report the exact parameters with any number.

## Measurement limits

This is an end-to-end **controlled SDK microbenchmark**, not a production
workload benchmark.

It demonstrates bounded overlap and correctness through semantic closure. It
does not establish:

- physical CPU, RAM, GPU, or VRAM isolation or utilization;
- CUDA execution, model-server throughput, token throughput, or API cost;
- speedup for CPU-bound Python callbacks;
- performance under real provider latency, rate limits, or failures;
- distributed scheduling or multi-host coordination;
- a general speedup for arbitrary Agent graphs.

The CPU/RAM/GPU/VRAM/model-slot values are Scheduler-owned logical reservations.
They test atomic admission and release; they do not inspect or constrain
operating-system or device consumption.

Independent tasks can overlap, while dependency edges still gate downstream
readiness. Evidence/VPG commits are serialized within one `run_async` call to
avoid graph-version races, so workloads dominated by semantic commit cost will
not scale like pure I/O overlap.

## Custom runs

```bash
python scripts/benchmark_multi_agent_runtime.py \
  --tasks 48 \
  --delay-ms 50 \
  --max-concurrency 8 \
  --agent-concurrency 4 \
  --agent-count 2 \
  --min-speedup 1.5 \
  --check
```

Custom results are only comparable when the workload, limits, Python version,
machine load, and runtime revision are recorded together.
