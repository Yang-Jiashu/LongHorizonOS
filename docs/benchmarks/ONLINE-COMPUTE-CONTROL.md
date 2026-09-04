# Online Compute Control benchmark (controlled, offline)

This benchmark is a deterministic regression harness for the LongHorizonOS
framing:

> Harness makes long-running Agents possible; LongHorizonOS manages and
> accelerates the long-running computation.

It compares a fixed-max-parallel static policy with a small adaptive policy
over the same graph and the same simulated work costs. During the first
scheduling epoch an external semantic change updates an unstable API input.
The static policy eagerly overlaps that branch with a conflicting writer, so
some completed work becomes stale and is retried. The adaptive policy uses
explicit stability and write-set declarations to defer the unstable branch,
avoid the write conflict, and schedule it after the graph change.

## Reproduce

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python -c "import json; from lhos.benchmarks.adaptive_control import run_benchmark; print(json.dumps(run_benchmark(), indent=2))"
```

The public CLI is equivalent and is easier to use after installation:

```bash
lhos benchmark online-compute --json
```

The CLI also has a human-readable mode:

```bash
lhos benchmark online-compute
```

The report contains:

- input, output, cached, total, and billable tokens;
- wall time and monetary cost;
- the simulated provider profile (provider id, latency multiplier, token
  multipliers, and token price);
- Context working-set and reread tokens;
- repeated and stale work;
- stale task ids, re-executed task ids, and a verified-progress trace by
  scheduling epoch;
- average and peak parallelism;
- preemption and rebase counters;
- verification tokens, calls, and cost;
- `Verified Progress / Token`;
- `Verified Progress / Minute`.

The canonical scenario is deterministic and does not read the real clock.
Therefore report JSON is reproducible across runs.

### Injecting a provider/cost profile

The benchmark accepts a deterministic provider profile.  This is useful for
checking whether a policy comparison remains meaningful when the nominal model
latency or token price changes:

```bash
${PYTHON:-python} -m lhos.cli.core benchmark online-compute --json \
  --provider-id cheap-sim \
  --latency-multiplier 1.5 \
  --input-token-multiplier 0.75 \
  --output-token-multiplier 0.5 \
  --output-cost-per-token-usd 0.000005
```

The profile is applied identically to static and adaptive runs.  It changes
the accounting scale; it does not emulate a real API, model quality,
throughput curve, queueing behavior, or hardware placement.

The current checked-in reference output is:

| Metric | Static policy | Adaptive policy |
|---|---:|---:|
| Verified task set | 4 / 4 | 4 / 4 |
| Total simulated tokens | 2,760 | 1,440 |
| Simulated wall time | 10.0 s | 5.0 s |
| Stale/repeated work tokens | 1,320 | 0 |
| Verified progress / token | 0.0003623 | 0.0006944 |
| Verified progress / minute | 6.0 | 12.0 |

The comparison is deliberately a controlled policy contrast: the static policy
eagerly overlaps an unstable writer and pays for stale work after the
simulated API change, while the adaptive policy defers that branch. These
numbers are simulated costs and durations from the benchmark scenario, not
measurements of an LLM provider or a host machine.

The machine-readable result also exposes:

```text
comparison.stale_attempt_reduction
comparison.reexecuted_task_reduction
comparison.cost_reduction_usd
static/adaptive.verified_progress_trace
static/adaptive.parallelism_trace
```

These fields are intended for plotting policy behavior (progress over epochs,
parallelism changes, and avoided re-execution), not as a substitute for a
real-model evaluation.

## Bounded multi-seed sweep

For a small reproducibility/robustness report, the same controlled comparison
can be executed for an explicit list of non-negative seeds:

```python
from lhos.benchmarks.adaptive_control import run_multi_seed_benchmark

report = run_multi_seed_benchmark(seeds=(0, 1, 2, 3, 4))
```

The returned mapping contains:

- `runs`: one complete `run_benchmark`-compatible report per seed;
- `summary.comparison`: mean/min/max/count for every numeric comparison
  metric (token/time/cost, stale and repeated work, and progress utility);
- `summary.static_metrics` and `summary.adaptive_metrics`: corresponding
  aggregates for each policy;
- `scope`: an explicit offline/deterministic simulated-provider disclaimer.

The canonical scenario is currently **seed-invariant**: the seed is preserved
in each nested scenario for auditability, but it does not randomize task
durations or outcomes. To evaluate genuinely different workloads, callers must
generate a `ControlledScenario` per seed and run the single-seed API for each
scenario. This sweep is therefore a bounded reporting aid, **not** a
statistically powered real-LLM/GPU evaluation or a production performance
claim.

## What this result does and does not show

The controlled scenario is useful as:

1. a metric-plumbing gate;
2. an executable illustration that a graph change can alter the next
   scheduling epoch;
3. a regression test that adaptive selection reaches the same VERIFIED Goal
   with no more stale/repeated work than the static policy.

It is **not** a real-model performance claim. Task duration, tokens, cost,
stability, provenance and write conflicts are explicit simulated inputs. The
benchmark does not measure model quality, provider economics, physical
CPU/GPU/RAM/VRAM placement, distributed scheduling, hidden dependency
discovery, or production throughput. A statistically powered Harness-vs-LHOS
benchmark using the same real model, tools, tasks and verification remains a
separate research gate.

## Relationship to the SDK controller

`AgentOS.computation_controller(...)` (also available as
`AgentOS.online_control(...)`) exposes the bounded
`observe -> reconcile -> plan -> dispatch -> observe` control-plane seam used
by this framing. The facade requires an already compiled Goal and is
read-only unless a caller injects a dispatcher. It never claims work, acquires
a Lease, executes an Agent, or publishes Evidence on its own. The benchmark is
an offline simulator and does not exercise provider dispatch or physical
resource scheduling.
