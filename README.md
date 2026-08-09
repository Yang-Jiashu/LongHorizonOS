# LongHorizonOS (lhos)

> LongHorizonOS is a state-centric agent operating architecture composed of a
> deterministic execution plane and an evidence-backed semantic control plane.
> The **Agent OS** subsystem provides process, action, capability, lease,
> persistence, artifact, and context primitives (the execution plane); the
> semantic control plane (Verified Progress Graph, Multi-Agent Scheduler,
> Causal Invalidation/Repair) determines progress, readiness, and repair.
> ("Agent OS" = the microkernel + system-services subsystem, NOT the whole
> system.)

## Core Architecture V1: FROZEN

Microkernel · STABLE · Artifact FS / Namespace · STABLE · Context VM · STABLE ·
Verified Progress Graph · STABLE · Multi-Agent Scheduler · STABLE · Causal
Invalidation / Local Repair · STABLE.

Canonical specification: `docs/architecture/LONGHORIZONOS-CORE-V1.md`.
Freeze record: `docs/architecture/CORE-V1-FREEZE.md`.  Milestone tag:
`longhorizonos-core-v1`.

Still future / product / ecosystem work (NOT part of Core V1): real model
integrations, a developer-facing high-level SDK, CLI UX, browser tooling, and
distributed scheduling.

## See the difference

Run the flagship once and watch a real worker crash get recovered, a real
ArtifactVersion change turn only the affected work stale (while unrelated
**VERIFIED** work stays preserved), and a **minimal** repair frontier re-close the
Goal with new exact-version Evidence — no full restart.

```bash
lhos demo recovery-repair          # deterministic, no API key
lhos demo recovery-repair --json   # machine-readable summary
```

- Worker crash → **execution ownership recovered**
- Artifact change → **old Evidence loses current applicability** (history intact)
- Only affected work becomes **STALE**; unrelated **VERIFIED work is preserved**
- A **minimal Repair Frontier** is derived; only necessary work reruns
- Repair requires **new exact-version Evidence**; Goal re-closes

Docs: `docs/demos/RECOVERY-REPAIR.md`.  (The `lhos` CLI is the Core V1 surface;
the legacy spec-20 CLI is reachable via `lhos legacy`.)

## Current Status (Phase D3 — Version-aware causal invalidation and local repair implemented)

Verified Progress Runtime — implemented
Graph-derived Multi-Agent Scheduler — implemented
Version-aware causal invalidation and local repair — implemented

Implemented:

- **Process / Action / Journal** — deterministic state machines, append-only event log
- **Capability / Lease / Signal** — resource ownership, access control, inter-process signaling
- **Crash recovery** — SIGKILL-resilient with exactly-once semantics and UNCERTAIN handling
- **Versioned Artifact FS** — content-addressed immutable storage with atomic writes
- **Namespace isolation** — private per-process namespaces, explicit readonly sharing
- **Optimistic concurrency** — expected_version prevents lost updates
- **Canonical URI security** — path traversal, encoding, and symlink defenses
- **Datetime consistency** — all modules use UTC-aware stamps (X-01 / LEASE-04 / MOD-02 closed)
- **Capability merge + atomic lease journaling** — concurrent-capability and lease acquisitions merge into single rows (CAP-02 / LEASE-01)
- **Verified Progress Runtime (VPG)** — deterministic semantically-closed Task/Goal state graph; evidence-backed VERIFIED derivation; task-local artifact-version invalidation; deterministic READY frontier (priority DESC / topo depth ASC / created ASC / node_id ASC); atomic optimistic patch commit with composite-key idempotency; architecture boundary at L4 (no kernel-internal imports)
- **Graph-derived Multi-Agent Scheduler (D2)** — eligibility (10-predicate deterministic filter using live Kernel Process/Capability state), deterministic best-fit matching (`match_deterministic_best_fit_v1`, integer score + agent_id tie-break, PYTHONHASHSEED- and insertion-order-independent), Kernel-backed exclusive TaskClaims (claim linearization point = exclusive ResourceLease acquisition), per-agent `max_concurrency`, projection + reconciliation + replayable audit log, crash reassignment via real POSIX SIGKILL + scheduler recovery. Architecture boundary: Scheduler imports only VPG public API + Agent OS public SDK — never kernel internals (D2-I1..D15 enforced, 20 forbidden deviations asserted, 120 authentic SIGKILL trials green)
- **Version-aware causal invalidation and local repair (D3)** — version-aware causal invalidation; evidence applicability tracking; deterministic local semantic invalidation cone over DEPENDS_ON edges; preservation of unaffected VERIFIED work; deterministic minimal Repair Frontier; incremental re-verification (Goal reopens and closes via derivation). Authority boundary: D3 derives validity only — it never claims Tasks, never dispatches Agents, never mutates Artifact or Evidence history, and never teaches invalidation semantics to the Scheduler (D3-01..D3-20 mutation-killed).

Semantic closure: Phase C1 all-23 UNCERTAIN items closed with regression tests (FIX) or spec text (DOC); 0 surviving mutations.
Phase D1: VPG runtime — 214+ tests green across 27 test files; 5 flagship demos pass.
Phase D2: Multi-Agent Scheduler — 253 tests green (across eligibility / matching / claims / capacity / attempts / completion / loss / reassignment / reconciliation / recovery / projection-replay / determinism / architecture / mutations / SIGKILL); 6 flagship demos pass; 0 surviving forbidden deviations (D2-01..D2-20).

### Phases completed

| Phase | Name | Key deliverables |
|-------|------|------------------|
| C1.2 | Capability / Lease / Signal | `src/lhos/agent_os/kernel/`, `src/lhos/agent_os/services/` capability + lease |
| C1.1 | Graph / Namespace / Artifact FS v1 | `src/lhos/agent_os/artifacts/`, `src/lhos/agent_os/graph/` |
| **C2** | **Version-bound Context VM** | **context snapshots, deterministic working sets, process-isolated working sets** |
| **D1** | **Verified Progress Runtime** | **VPG runtime: models, DAG, patch protocol, evidence/verification/closure/readiness, projection, recovery, SDK; 5 demos** |
| **D2** | **Graph-derived Multi-Agent Scheduler** | **eligibility + deterministic matching + Kernel-backed exclusive TaskClaims + per-agent concurrency + crash reassignment + Projection/Reconcile/Recovery + replayable audit log; 6 demos** |
| **D3** | **Version-aware causal invalidation + local repair** | **evidence applicability tracking + deterministic invalidation cone + preserved-work preservation + minimal Repair Frontier + incremental re-verification; 6 demos** |

### Phase C2 implementation locations

| Layer | Path |
|-------|------|
| Models | `src/lhos/agent_os/context/models.py` |
| Service | `src/lhos/agent_os/context/service.py` |
| SDK | `src/lhos/agent_os/context/sdk.py` |
| Demos | `examples/agent_os/context_*.py` (6 scripts) |
| Tests | `tests/agent_os/context/` (21+ test files) |

### Phase D1 / D2 implementation locations

| Layer | Path |
|-------|------|
| Runtime package | `src/lhos/runtimes/verified_progress/` |
| Public SDK | `src/lhos/runtimes/verified_progress/sdk.py` (`VerifiedProgressRuntime`) |
| Demos | `examples/verified_progress/*.py` (6 scripts) |
| Tests | `tests/runtimes/verified_progress/` (27+ test files) |

### Phase D3 implementation locations

| Layer | Path |
|-------|------|
| Runtime package | `src/lhos/runtimes/invalidation/` |
| Public runtime | `src/lhos/runtimes/invalidation/runtime.py` (`InvalidationRuntime`) |
| Tests | `tests/runtimes/verified_progress/invalidation/` (52 test cases) |

### Phase D2 implementation locations

| Layer | Path |
|-------|------|
| Scheduler package | `src/lhos/runtimes/multi_agent/` |
| Scheduler SDK | `src/lhos/runtimes/multi_agent/sdk.py` (`create_scheduler`, `SchedulerSession`) |
| Scheduler demos | `examples/multi_agent/*.py` (6 scripts: specialized pipeline, parallel ready, crash reassignment, no-eligible-agent, capacity, semantic/operational separation) |
| Scheduler tests | `tests/runtimes/multi_agent/` (31 test files) |

Not yet implemented:

- **Distributed multi-agent cluster** — out of scope
- **General belief revision / LLM self-repair planner** — out of scope (D3 is deterministic, graph-derived, local repair only)
- **Semantic contradiction solver** — out of scope
- **Distributed repair cluster / multi-host consensus** — out of scope
- Real distributed execution
- Production security hardening

## Quick start

```bash
make install            # pip install -e .[dev]
make test               # python -m pytest tests/ -x -q

# Artifact FS demos
python -m examples.agent_os.private_workspace
python -m examples.agent_os.shared_readonly
python -m examples.agent_os.optimistic_conflict
python -m examples.agent_os.crash_recovery
python -m examples.agent_os.multi_process_artifacts
```

## Architecture


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
make test    # 924 tests: unit + integration + e2e + audit regression gates
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
