# LongHorizonOS — Semantic-Repair Comparative Benchmark (E5)

## Question
When only a subset of previously-verified work is causally affected, how much
still-valid VERIFIED progress does LongHorizonOS preserve, and how little does it
recompute, versus a full restart or a checkpoint/resume workflow?

## Systems compared
- **Baseline A — Full Restart**: any mutation reruns all previously verified tasks.
- **Baseline B — Checkpoint/Resume**: crash resumes; mutation reruns the static
  downstream suffix (BFS over DEPENDS_ON from the mutated root) — no Evidence
  exact-version, no minimal Repair Frontier.
- **System C — LongHorizonOS**: the frozen Core D3, run through the public SDK
  (`AgentOS.repair`) on a real VPG graph (prior work verified via `AgentOS.run`).

All three share: same DAG topology, same deterministic worker cost (per-task
{1,5,10} units), same mutation root, same Python process, same seeds.

## Correctness oracle (independent)
BFS over `DEPENDS_ON` from the mutated root computes the expected causal affected
set, the expected preserved (prior-verified not affected), and the expected
minimal Repair Frontier (affected tasks whose deps are all unaffected).  The
benchmark asserts `under_invalidation == 0`, `over_invalidation == 0`,
`lhos frontier == oracle frontier`, `ownership_conflicts == 0`,
`false_verified == 0`.  Invalid trials are excluded from performance aggregation
(BENCH-G9).

## Workloads
- Artifact mutation: graph sizes {10,25,50 (quick) / 100,250 (full)}, topologies
  {chain, fan_out, fan_in, diamond, mixed}, affected fractions {0.1..1.0}, seeds
  {1,2,3}.  A `select_root_for_fraction` picks a root whose causal fraction is
  closest to target (honest; fan_out/fan_in have limited deep roots, so their
  actual affected is small).
- Real workspace (Layer B): the E4 `recovery-repair` scenario (real Shell,
  Workspace, Evidence, mutation + selective repair + reclosure) measures preserved
  vs invalidated + Goal closure.
- Falsification: 100% affected must give preservation ≈ 0 (no false saving).
- Small-cone: ~5-10% affected must give preservation ≥ 0.5.

## Metrics
rerun/preserved, weighted_work_rerun, tool/verification/model calls, wall_time_ms,
recovery_latency, repair_frontier_peak, under/over invalidation,
ownership_conflicts, false_verified, final_goal_closed, valid_trial.
`preservation_ratio = lhos_preserved / prior_verified`,
`recomputation_ratio = lhos_rerun / prior_completed`.

## Environment
Python 3.11 (uv), macOS arm64, deterministic, offline, no API key.
Trial counts: quick = 90 trials (3 sizes × 2 topologies × 5 fractions × 3 seeds).

## Reproducibility
```
lhos benchmark semantic-repair --quick
lhos benchmark semantic-repair --full
```
Raw trials → `artifacts/oss_productization_e5/raw/trials.jsonl`; summary →
`summaries/summary.json` (contains `raw_sha256`); regeneration from raw is
deterministic (charts/tables rebuilt from raw only).  No hand-entered numbers.

## Limitations
Synthetic DAGs may not represent every real agent workload; baselines are
policies, not named external frameworks (no claim vs LangGraph/CrewAI); wall-time
depends on the machine; live LLM latency is excluded from the deterministic
benchmark; weighted work uses a simplified unitary cost model.  E5 measures
semantic-repair efficiency, not general agent quality or scheduler throughput.
