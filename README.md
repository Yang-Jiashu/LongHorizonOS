# LongHorizonOS (lhos)

A Verified Progress Graph runtime for long-horizon agents, implementing
Phases 0–8 of `LongHorizonOS_MVP_工程设计规格`, including the Controlled
Benchmark (spec 22–25).

## Architecture

- append-only Event Log + transactional graph projection (spec 5);
- Verified Progress Graph with a strict node state machine (spec 4, 6);
- deterministic Initial Graph Builder + incremental Graph Patches with
  version/cycle/evidence validation (spec 8);
- deterministic Readiness, FIFO + Cost-aware schedulers (spec 9, 11);
- Graph-scoped Context Compiler with caching and context hashes (spec 10);
- Tool Runtime with idempotency keys, crash-safe replay and rollback
  generations (spec 13);
- Verification Gate — agents can never self-verify (spec 14);
- mid-run invalidation: artifact version tracking, STALE/INVALIDATED
  propagation, deterministic must-invalidate rules, Replanning Amplification
  inputs (spec 15);
- checkpoints (noop / filesystem / git) with restore-on-failure and
  restore-on-crash policies, crash injection at all spec 26.2 points,
  crash recovery and resume (spec 16);
- budget management and full event trace (spec 17, 24);
- Controlled Benchmark: deterministic generator, oracle, scripted
  environment, 8 experiment modes, metric suite (spec 22–25).

No real LLM calls anywhere: the scripted FakeWorker drives runs
deterministically, so every experiment is reproducible from (task, mode, seed).

## Quick start

```bash
make install            # pip install -e .[dev]
make test               # python -m pytest tests/ -x -q

# run one task graph
lhos init --db artifacts/lhos.db
lhos run --db artifacts/lhos.db \
  --graph-file tasks/example_task.json \
  --workspace artifacts/smoke_workspace \
  --scheduler fifo
lhos inspect --db artifacts/lhos.db --run-id <RUN_ID>
lhos replay   --db artifacts/lhos.db --run-id <RUN_ID>
lhos inject   --db artifacts/lhos.db --run-id <RUN_ID> \
  --type constraint_changed --payload '{"node_id": "...", "invalidates": ["..."]}'

# run the controlled benchmark (spec 22-25)
lhos benchmark --suite controlled --scheduler fifo,cost_aware --seeds 1,2,3
lhos benchmark --suite controlled \
  --mode transcript,dynamic_graph_fifo,full_lhos --seeds 1,2 --size small
python scripts/build_report.py    # markdown report from the newest results JSON
```

Benchmark results land in `artifacts/benchmark_results/controlled_<ts>.json/.csv`
(one row per preset × mode × seed cell, plus per-cell config snapshots);
run artifacts (SQLite db, workspace, traces) stay under
`artifacts/benchmark_work/runs/<run_id>/`.

## Experiment modes (spec 25)

Every mode reuses the same model (FakeWorker), tools, budget, verification,
seed, task and workspace initialization — only runtime modules differ.

| mode | engine | scheduler | invalidation | local repair | checkpoints |
|---|---|---|---|---|---|
| `transcript` | transcript baseline | — | no graph | no | none (restart from scratch on crash) |
| `static_graph_fifo` | graph | fifo | off | off | noop |
| `dynamic_graph_fifo` | graph | fifo | on | **off** (strands on must-invalidate) | noop |
| `dynamic_graph_local_repair` | graph | fifo | on | on | noop |
| `dynamic_graph_cost_aware` | graph | cost-aware | on | on | noop |
| `full_lhos` | graph | cost-aware | on | on | filesystem + restore policies + trace |
| `oracle_graph_fifo` | graph | oracle-priority fifo | on | on | noop |
| `oracle_graph_cost_aware` | graph | cost-aware + oracle hint | on | on | noop |

Oracle modes see the generator's true criticality as node `priority`; all
other modes get `priority = 0`.

## Scenario presets (spec 22.3)

One deterministic generator, 14 presets, sizes Small=20 / Medium=50 /
Large=100 / XL=200 nodes (spec 22.2): `serial_chain`, `wide_dag`,
`branch_join`, `costly_critical_path`, `upstream_failure`,
`constraint_change`, `artifact_modified`, `worker_crash`, `runtime_crash`,
`post_tool_crash`, `alternative_paths`, `external_wait`, `noop_nodes`,
`risky_shortcut`. Control variables (node count, depth, width, token cost,
failure/constraint/artifact probabilities, crash point, retryability…) are
recorded per task in `control_variables`.

## Metrics glossary (spec 24)

- **success / progress_ratio / failed_nodes / invalidated_nodes** — result metrics.
- **input/output/total_tokens, model_calls, tool_calls, wall_time** — cost
  metrics (tokens are modeled; `model_cost_usd`, `graph_maintenance_tokens`
  and `verification_tokens` are honestly 0 with FakeWorker and deterministic
  rules; `graph_maintenance_events` counts reconciler work instead).
- **scheduler_time / checkpoint_time** — wall-clock instrumentation exported
  in the terminal run event payload.
- **Progress–Budget Curve / AUPBC-{token,time,tool-calls}** — normalized area
  under the verified-progress vs budget curve (higher = progress earned
  earlier/cheaper).
- **Useful Work Ratio** — final-successful-attempt cost of VERIFIED nodes /
  total execution cost.
- **Replanning Amplification** — nodes actually re-executed after
  invalidation / oracle true affected nodes (1.0 = perfect local repair,
  0.0 = no repair, >1 = over-replanning).
- **Invalidated Work Rate** — superseded-attempt cost / total cost.
- **Recovery Overhead** — repeated cost after a crash / remaining estimated
  cost at crash time.
- **Critical-path Stretch** — simulated execution time / oracle
  critical-path time (1.0 = optimal; FakeWorker runs instantly, so
  per-attempt estimated times stand in for execution latency).

## Configuration reference

`RuntimeStack` consumes a plain dict (see `configs/` for YAML examples):

```yaml
scheduler:   {type: fifo | cost_aware, weights: {...}}
budget:      {max_total_tokens: null, max_tool_calls: null, ...}
checkpoint:  {type: noop | filesystem | git,
              restore_on_failure: false, restore_on_crash: false,
              after_verified_node: false}
features:    {invalidation: true,   # false = static-graph ablation
              local_repair: true}   # false = INVALIDATED nodes stay stranded
telemetry:   {jsonl_trace: false, trace_directory: artifacts/traces}
verification: {allow_llm_judge: false}
```

## Tests

```bash
make test    # 161 tests: unit + integration + e2e
```

Coverage includes the state machine, DAG cycles, readiness, invalidation
propagation, scheduler scores, context pruning, patch conflicts, budgets,
evidence, leases, idempotency (spec 26.1), event replay, crash injection /
resume (spec 26.2), plus Phase 8: generator determinism and schema validity
for all 14 presets, metric functions on known cases, and end-to-end
benchmark cells (3 modes × 2 seeds) with completeness, mode-contrast and
reproducibility assertions (every field except wall-clock fields is
bit-identical across reruns).

The spec 31 vertical slice — natural-language goal → initial graph → FIFO →
compiled context → shell/file tools → command verifier → verified commit →
crash → resume → completion, over a tiny Python repository task — is the
end-to-end test `tests/e2e/test_tiny_repository_task.py`.

## Intentional deviations and honest stubs

- Ports sketched as `async` are synchronous (single-worker MVP, spec 11.3);
  the Benchmark Adapter (spec 23) keeps the spec's async signature over a
  sync core.
- CLAIMED_DONE → STALE exists only as a reconciler-level forced transition
  (spec 15 pseudocode vs the section 6 machine).
- The planner is the deterministic InitialGraphBuilder in every mode (no LLM
  planner noise yet), so oracle modes currently differ from dynamic modes
  only through priority hints and scheduling.
- The transcript baseline models tokens as `len(context)//4` and restarts
  from scratch on crash (no persistence — that is the baseline being
  measured); it reuses the real verifier registry.
- `alternative_paths` uses AND semantics (both branches execute; true OR-path
  pruning is not modeled).
- `llm_judge` remains a stub verifier (`allow_llm_judge: false` everywhere).
- Rollback generations: after a checkpoint restore, idempotency keys gain a
  `:gen<N>` suffix (N = CHECKPOINT_RESTORED count) so rolled-back tool
  effects re-execute instead of replaying (spec 13.3/16.3).
