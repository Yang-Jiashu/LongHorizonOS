# Implementation Progress — 2026-08-15 18:51 (Asia/Shanghai)

## This slice

Implemented a reproducible **adaptive-vs-static online-compute benchmark**
vertical slice. It is intentionally offline and deterministic, but reports the
quantities needed to evaluate the LongHorizonOS compute-management thesis:

- model/input/output token consumption;
- simulated latency and cost;
- stale work and repeated/re-executed work;
- verified progress and progress-per-token/minute;
- scheduling-epoch parallelism;
- semantic-change event and adaptive rebase count.

## Implemented

- `SimulatedProvider` profile in
  `src/lhos/benchmarks/adaptive_control.py`.
- Provider injection through `run_controlled_benchmark(..., provider=...)` and
  `run_benchmark(..., provider=...)`.
- Provider-aware deterministic accounting for input/output tokens, latency,
  model cost, and verification cost.
- `ControlledRun` audit fields:
  `provider_id`, `stale_task_ids`, `reexecuted_task_ids`,
  `verified_progress_trace`, and `parallelism_trace`.
- Comparison fields for cost reduction, stale-attempt reduction, repeated
  execution reduction, and parallelism peaks.
- CLI flags on `lhos benchmark online-compute`:
  `--provider-id`, `--latency-multiplier`,
  `--input-token-multiplier`, `--output-token-multiplier`, and
  `--output-cost-per-token-usd`.
- Benchmark contract documentation in
  `docs/benchmarks/ONLINE-COMPUTE-CONTROL.md`.
- Regression tests for provider scaling and CLI forwarding/rejection.

## Verification

```text
12 focused benchmark/CLI tests passed
Ruff check passed
Mypy passed for modified source modules
Compileall passed
```

Example:

```bash
python -m lhos.cli.core benchmark online-compute --json \
  --provider-id cheap-sim \
  --latency-multiplier 1.5 \
  --output-token-multiplier 0.5
```

## Not implemented by this benchmark

This slice does **not** claim real LLM quality, external-provider economics,
physical CPU/GPU/RAM/VRAM telemetry, process isolation, distributed scheduling,
automatic provenance discovery, or production throughput. The simulator
isolates policy/accounting behavior; a real Harness-vs-LHOS workload is still
required for a systems-paper performance claim.

## Next step

Use the new fields to produce a multi-seed policy plot/report, then connect the
same metric schema to an injected real Harness/provider adapter without
changing the comparison contract.
