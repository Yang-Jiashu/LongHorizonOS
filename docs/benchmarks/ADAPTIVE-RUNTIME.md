# Adaptive Runtime Benchmark

This is a **controlled, offline regression benchmark** for the opt-in
`AgentOS.run_async(..., adaptive=True)` path. It compares the same four-task
workload under:

- **static**: fixed global concurrency, with no conflict policy;
- **adaptive**: an explicit `ConflictGraph` and deterministic greedy
  `DynamicParallelismPolicy`.

Two tasks (`a-conflict` and `b-conflict`) declare a write/write conflict on
`workspace://shared`. Two other tasks are independent. The controlled
executor uses only `asyncio.sleep` and in-memory counters. The static run
deliberately overlaps the conflicting writers, producing one failed/stale
attempt and one retry. The adaptive run serializes the conflicting pair while
retaining parallelism for independent work.

## Real runtime path

Unlike `adaptive_control.py`, which is a policy/metric simulator, this
benchmark executes the public runtime path:

```text
AgentOS.run_async
  -> Scheduler eligibility and logical resource admission
  -> TaskClaim
  -> exclusive Kernel Lease + fencing token
  -> AsyncWorkerPool
  -> controlled fake executor
  -> verifier
  -> Evidence commit
  -> VPG VERIFIED / Goal closure
```

The executor and delay are fake and controlled, but Claims, Leases, attempts,
worker dispatch, verification, Evidence and VPG state are the real runtime
implementations.

## Reproduce

From the repository root:

```bash
python scripts/benchmark_adaptive_runtime.py --check
```

The command prints JSON and writes:

```text
artifacts/benchmark_results/adaptive-runtime.json
```

For a less timing-sensitive local run:

```bash
python scripts/benchmark_adaptive_runtime.py \
  --delay-ms 50 \
  --max-concurrency 2 \
  --check
```

## What the benchmark currently demonstrates

On the checked-in workload, the deterministic work counters should show:

| Mode | Executor attempts | Scheduler attempts | Failed/stale attempts | Rework attempts | Conflict overlaps |
|---|---:|---:|---:|---:|---:|
| Static | 5 | 5 | 1 | 1 | 1 |
| Adaptive | 4 | 4 | 0 | 0 | 0 |

Both modes must close the Goal and verify all four tasks. The adaptive report
also records each scheduling epoch, selected task ids, deferred task ids,
decision hash, and selected parallelism.

The `runtime_audit` section independently checks:

- executor-local attempt counts equal durable Scheduler attempt counts;
- every attempt acquired a TaskClaim, Kernel Lease and positive fencing token;
- exactly four attempts reached `verified_semantically`;
- every VERIFIED task has one valid Evidence binding in the VPG;
- no active Claim, logical resource reservation, or live Kernel Lease remains
  after the run.

Wall-clock values are **measured**, but their ratio is informational only.
They include Python, SQLite, event-loop and host-load noise and are not a
performance promise. Correctness and stale/rework counters are the regression
gate.

## Scope and limits

This benchmark is intentionally narrow:

- access declarations are explicit and exact-match;
- conflict injection is deterministic and in-process;
- the executor is fake; there is no real Harness session or LLM provider;
- there is no token accounting, API billing, or GPU execution;
- there is no physical CPU/RAM/VRAM telemetry or isolation;
- there are no distributed workers or cross-host scheduling;
- there is no automatic provenance/dependency discovery;
- there is no semantic interrupt or running-Agent rebase in this workload;
- there is no claim that adaptive policy improves arbitrary Agent workloads.

Therefore this result supports only the bounded statement:

> Given complete explicit read/write declarations for this workload, the
> current adaptive policy can avoid a known write conflict without sacrificing
> all independent overlap, while the same real AgentOS ownership and VPG
> commit path reaches the same VERIFIED Goal.

It does **not** establish a general "Adaptive Agent OS" speedup.
