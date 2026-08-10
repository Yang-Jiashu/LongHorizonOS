# Semantic-Repair Benchmark

## Claim under test

When an Artifact changes after a Goal has already closed, can the runtime:

1. identify exactly which prior progress is no longer valid;
2. preserve unaffected verified work;
3. derive the minimum ready repair frontier;
4. re-execute through the real scheduler and ownership path;
5. close the Goal again without false `VERIFIED` state?

This benchmark measures semantic repair. It does not measure general model
quality, planning quality or maximum scheduler throughput.

## Compared strategies

| Strategy | Mutation behavior | Intended role |
|---|---|---|
| **Full restart** | Re-executes every task | Safe but wasteful lower baseline |
| **State-only resume** | Trusts completed bits and performs no calls | Cheap but semantically unsafe baseline |
| **Oracle task-DAG checkpoint** | Re-executes the exact affected task suffix | Strong task-level efficiency baseline |
| **LongHorizonOS** | VPG invalidation, graph-derived frontier, scheduler Attempts and Goal reclosure | System under test |

The checkpoint baseline is deliberately oracle-informed. For workloads whose
semantic dependencies are identical to task edges, LongHorizonOS is expected to
match it—not beat it.

## Workloads

### Deterministic DAG sweep

- Sizes: quick `{10, 25}`; full `{10, 25, 50, 100}`
- Topologies: quick `{chain, mixed}`; full
  `{chain, fan_out, fan_in, diamond, mixed}`
- Target affected fractions: quick `{0.1, 0.5, 1.0}`; full
  `{0.1, 0.25, 0.5, 0.75, 1.0}`
- Seeds: quick `{1, 2}`; full `{1, 2, 3}`
- Per-task deterministic work weights: `{1, 5, 10}`
- Trial count: quick `24`; full `300`

Each trial performs an initial real `AgentOS.run`, mutates an Artifact version,
calls `AgentOS.repair`, then re-executes through `AgentOS.run` until the Goal
closes again.

### Real-workspace scenario

The recovery-repair demo uses the real Shell, Workspace, Artifact bridge,
Evidence path and invalidation runtime. Its mutation affects three tasks,
preserves one independent verified task, executes three repair Attempts and
recloses the Goal.

### Live-model probe

The opt-in probe uses the StepCode OpenAI-compatible endpoint and the selected
live model. It closes a four-task Goal, mutates the root of a three-task chain,
then compares actual model calls for each strategy.

Two or more comma-separated API keys are rotated safely and used for parallel
independent baseline calls. The dependency chain inside LongHorizonOS remains
sequential because downstream tasks are not semantically ready yet.

## Correctness oracle

An independent BFS over `DEPENDS_ON` computes:

- the expected affected set;
- the expected preserved set;
- the expected initial repair frontier.

A trial is valid only when all of the following hold:

- `under_invalidation == 0`
- `over_invalidation == 0`
- LongHorizonOS frontier equals the oracle frontier
- `ownership_conflicts == 0`
- `false_verified == 0`
- the final Goal is closed
- observed repair Attempts equal re-executed work

Invalid trials are reported and excluded from performance aggregation.

## Metrics

- **Observed work:** scheduler Attempts, model calls and weighted work
- **Semantic safety:** under/over invalidation, false `VERIFIED`, false closure
- **Ownership safety:** overlapping activated claim intervals
- **Repair shape:** affected, preserved and initial Repair Frontier
- **Latency:** invalidation, reclosure, per-call latency and parallel batch wall time
- **Closure:** final Goal state after repair

`lhos_rerun` is measured from actual scheduler Attempts after invalidation. It
is not inferred from the affected-node count.

## Reference quick result

Generated from `24` deterministic trials:

| Metric | Result |
|---|---:|
| Valid trials | `24 / 24` |
| Mean affected fraction | `0.536667` |
| Mean full-restart reruns | `17.5` |
| Mean oracle checkpoint reruns | `9.416667` |
| Mean LongHorizonOS observed reruns | `9.416667` |
| Mean weighted saving vs full restart | `48.6427%` |
| Mean weighted saving vs checkpoint | `0%` |
| Under / over invalidation | `0 / 0` |
| Ownership conflicts | `0` |
| False `VERIFIED` | `0` |
| State-only false-closure trials | `24 / 24` |
| Mean invalidation latency | `27.518 ms` |
| Mean Goal reclosure latency | `499.138 ms` |

The result demonstrates safe selective repair and substantial savings versus
full restart. It demonstrates parity—not superiority—against the
oracle-informed task-DAG checkpoint.

## Reference live result

With StepCode model `gpt-5.6-sol` and two rotating keys:

| Strategy | Model calls | Semantic outcome |
|---|---:|---|
| Full restart | `4` | Recomputed all tasks |
| State-only resume | `0` | False closure |
| Oracle task-DAG checkpoint | `3` | Correct repair |
| LongHorizonOS | `3` | Goal reclosed; `0` false `VERIFIED` |

LongHorizonOS saved `25%` of model calls versus full restart and matched the
task-DAG checkpoint.

StepCode may omit usage or return zero usage for a successful response.
Therefore token totals are comparable only when `token_totals_complete` is
`true`. Every summary reports `usage_reported_calls` and
`usage_missing_calls`; model-call counts remain the primary live cost metric.

## Reproduce

```bash
# Install from the repository
python -m pip install -e ".[dev]"

# Fast, deterministic and offline
lhos benchmark semantic-repair --quick

# Full deterministic sweep
lhos benchmark semantic-repair --full
```

Optional live GPT probe:

```powershell
$env:STEPCODE_API_KEYS="<key-1>,<key-2>"
lhos benchmark semantic-repair --quick --live-model `
  --model gpt-5.6-sol --live-timeout 90
```

A single key may instead be supplied through `STEPCODE_API_KEY`. Credentials
are read from the process environment and are not written to result files.

Outputs:

- Raw trials:
  `artifacts/oss_productization_e5/raw/trials.jsonl`
- Aggregate summary:
  `artifacts/oss_productization_e5/summaries/summary.json`
- Live result:
  `artifacts/oss_productization_e5/summaries/live_model.json`
- Markdown comparison:
  `artifacts/oss_productization_e5/tables/comparison.md`

The aggregate summary records a SHA-256 digest of the raw trial file.

## What the benchmark does not prove

- It does not prove better general reasoning or task success than other agents.
- It does not yet prove lower repair cost than an oracle task-DAG checkpoint.
- Synthetic DAGs do not represent every long-horizon workload.
- Wall time depends on hardware, storage and provider load.
- The live probe is small and is not a statistically powered model benchmark.
- Weighted task cost is a controlled approximation, not provider billing.

To demonstrate an efficiency advantage beyond task-level checkpointing, future
workloads must include finer semantic structure: multi-output tasks,
Artifact/Evidence-level dependencies, verifier changes, external facts and
selective invalidation within a task.
