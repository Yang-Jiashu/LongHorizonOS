# Mind-VLA note to implementation matrix

**Audit date:** August 16, 2026 (final documented slice)  
**Source:** `C:\Users\yangjiashu\Mind-VLA-笔记.md` (1,069 lines)
**Release boundary:** single-host research alpha

This matrix turns the design note into testable engineering work. A data model
or policy proposal is not marked implemented unless it changes a public
runtime observation or execution path and has a regression test.

## System thesis

> **Harnesses make long-running Agents possible; LongHorizonOS makes
> long-running Agent computation efficient.**

> **Graph represents the evolving computation; the OS continuously schedules
> from the Graph.**

The Harness is a managed execution unit. The versioned Semantic Progress Graph
is the global control state. Scheduler/Kernel ownership remains authoritative
for claims, logical resource admission, leases, and commit fencing.

## Requirement matrix

| Note requirement | Status after this delivery | Implemented surface | Remaining boundary |
|---|---|---|---|
| Harness versus OS boundary | **Implemented as a public bounded protocol** | `lhos.sdk.harness`; session identity, capabilities, control requests/results, state transitions, callable adapter | No universal adapter for every third-party Harness; no cross-process checkpoint portability |
| Graph as evolving computation/control state | **Implemented, graph-relative** | VPG validity/version/dependencies plus graph-derived runtime projection | Missing or unobserved dependencies cannot be inferred |
| Four-plane `GlobalRuntimeState` | **Implemented, read-only** | Progress, Agent cognition, Context VM, logical resources; optional host telemetry is a separate explicit observation | No shared physical inventory/placement; belief state remains bounded to captured snapshots |
| Recent event state | **Implemented, bounded** | Deterministic recent Scheduler/VPG runtime events in `RuntimeStateView` | Not a distributed event bus |
| Critical path and downstream unlock value | **Implemented, declared-DAG-relative** | Deterministic critical-path and unlock projections from VPG dependencies | No learned duration/success estimate; no hidden semantic edges |
| READY, repair, and parallel frontier | **Implemented, bounded** | VPG READY/repair frontier plus graph-relative parallel antichain; explicit `ConflictGraph` refines batching | Hidden read/write conflicts remain unknown |
| `FrontierPolicy` / scheduling epochs | **Implemented, opt-in, default-off** | `EpochController` executes explicit `observe -> reconcile -> plan` epochs with idempotent/fail-closed decisions (10 focused tests); `adaptive=True` remains an advisory Scheduler filter. Default ordering is repair-first lexical; opt-in `graph_utility` uses declared critical-path position and immediate downstream unlock value inside the safe frontier. | Not an always-on autonomous controller; no Claim/Lease mutation; no learned cost/success model or hidden-edge inference |
| Event-driven supervisor | **Implemented, caller-owned/bounded** | `EventDrivenSupervisor` exposes explicit `start -> submit -> step -> stop` plus bounded `run()`/async iteration; each step re-observes runtime state, validates event fingerprints, and executes one bounded online epoch; declared workspace observations can be routed as interrupts. `lhos demo online-supervisor --json` closes a controlled three-task Goal and explicitly reports no daemon/LLM. | No daemon/thread, hidden retry loop, real-model execution, force-kill, automatic Context rebase, or cross-plane ownership transaction |
| Dynamic parallelism | **Implemented, explicit-access/resource-only** | Deterministic greedy batch selection with serial-only unknown access; `resource_aware=True` fits explicit logical vectors on the main run paths | No hidden-access inference, physical placement/isolation, or speculative rollback |
| Conflict graph | **Implemented, explicit-access-only** | Read/write conflict derivation from task declarations | No automatic file/API/browser/tool access discovery |
| Context VM on execution path | **Implemented, bounded** | Each SDK Attempt gets a fenced Context snapshot/identity; materialized page bindings flow automatically into the provenance read-set | Not a sandbox; only materialized pages are covered; no automatic context-quality optimizer |
| Context delta / rebase planning | **Implemented, pure/read-only plus bounded live façade** | `plan_context_rebase`/`build_context_delta` classify explicit graph changes; `AgentOS.plan_live_context_rebase(...)` / `apply_live_context_rebase(...)` bind a plan to the current authoritative VPG version and exact Claim/Lease/Attempt/process/AgentSnapshot/Context/Harness identities. Safe `REUSE` control supports exact request-idempotent replay without re-running the Harness hook; changed-read `REBASE/FULL_RELOAD` persist a durable handoff, admit a fresh Attempt, detach the old Harness, and support same-plan replay | Does not discover the world or hidden reads, materialize arbitrary Context, or run automatically on the main execution path; handoff remains release-then-acquire/non-atomic, callers must register a new Harness or recover the handoff, and no cross-plane atomic ownership transfer exists |
| Durable `AgentSnapshot` | **Implemented** | Attempt/Claim/Agent/Graph identity, read/write sets, Context, progress, cost | No arbitrary Python stack or model hidden-state capture |
| Commit-time read-set validation | **Implemented** | Stale cognition is quarantined before Evidence commit | Only known/mediated reads can be validated |
| Semantic interrupt routing | **Implemented** | Deterministic `CONTINUE`/`DEFER`/`PREEMPT`/`REBASE`/`REVERIFY` policy | Policy correctness is relative to observed state |
| Cooperative interrupt delivery | **Implemented on cooperative async executors** | Active epoch registration, identity-fenced delivery, request/delivered/observed terminal states, exact-claim cleanup | Legacy callbacks that do not accept/poll a token are non-preemptible; no force-kill |
| Harness control actions | **Implemented as protocol semantics** | `START`, `CONTINUE`, `CHECKPOINT`, `REBASE`, `PREEMPT` with capability and identity checks; bounded Claim/Lease handoff transfers exact source claim/attempt/epoch ownership | SDK scheduling does not yet orchestrate every third-party session; handoff is release-then-acquire, not a cross-Scheduler/Kernel atomic transaction |
| Automatic provenance | **Partial, mediated boundaries strengthened** | `context_v1`; valid Context VM `page_bindings` auto-record as exact `source="context_vm"` reads; workspace gateway/watcher; `HTTPToolAdapter` records declared GET/HEAD/OPTIONS reads only after secure exact-version validator/authority checks; `AgentOS.register_external_fact` seeds the authoritative version/hash | Not automatic discovery of arbitrary hidden Python/filesystem/network/browser/tool reads; HTTP adapter is explicit rather than process-wide interception; no universal cross-plane transaction |
| Cognitive locality / reuse versus fresh | **Implemented, bounded advisory + opt-in adapter** | `ComputeRoutingPolicy`/`plan_compute_routing` calculate exact-version overlap, stale/unknown binding penalties, reconstruction-cost signals, and `REUSE_AGENT`/`FRESH_AGENT`; adaptive epochs may record a bounded redacted routing audit from explicit task metadata; `ComputeProviderRegistry` can apply an explicitly selected Context adapter for an eligible Attempt | No warm-process lifecycle mutation, automatic locality decision, or measured main-path utility |
| Model routing | **Implemented, bounded advisory + opt-in provider adapter** | The policy emits provider-independent `CHEAP`/`STANDARD`/`STRONG` tier labels; with `AgentOS(provider_registry=...)`, `adaptive=True`, and explicit `compute_routing.provider_routing.enabled`, a registered model hook can execute after Claim/Context setup (`tests/sdk/test_provider_routing.py`) | No automatic provider selection, provider-aware Scheduler, process lifecycle management, physical resource placement, or economic feedback loop |
| Verification routing | **Implemented, bounded advisory + opt-in provider adapter** | The policy emits `LIGHT`/`STANDARD`/`STRONG` verification-strength labels; the same explicit provider route can invoke a registered verifier hook after execution and before the normal Evidence path (`tests/sdk/test_provider_routing.py`) | No automatic verifier selection, provider-aware scheduling, physical resource routing, or measured risk-based allocation |
| Semantic interrupts from live world watchers | **Implemented as a bounded caller-owned route/loop** | `WorkspaceObservationWatcher` polls declared workspace files, issues observation tokens, and feeds `ARTIFACT_CHANGED` interrupts into `SemanticInterruptPolicy`; `AgentOS.poll_workspace_and_route(...)` / `route_workspace_observation(...)` revalidate supplied interrupts, while `WorkspaceWatchLoop` repeats bounded supervisor epochs; accepted actions route to exact cooperative Attempts and rejected statuses remain blocked | No universal filesystem/API/requirement watcher, always-on daemon, policy-triggered background loop, automatic hidden provenance, or atomic Lease handoff |
| Claim/Lease handoff | **Implemented, bounded** | `prepare_handoff`, `commit_handoff`, and `recover_handoff` persist a durable intent with exact source Claim/Attempt identity and caller-provided `handoff_id`; same-identity replay is idempotent and uncertain commit fails closed as `IN_DOUBT`; `handoff_task`/`handoff_online_epoch_to_harness(...)` retain exact identity fencing | This is a recovery witness over a release-then-acquire path, not a Scheduler/Kernel/Harness/VPG two-phase atomic transaction; failed replacement can leave no owner; no automatic Harness execution/rebase |
| Physical CPU/GPU/RAM/VRAM management | **Partial observation/logical bridge; physical authority open** | Host telemetry plus an explicit reserve-based single-pool logical-capacity bridge; atomic logical reservations and resource-aware batching | No shared inventory, continuous monitoring, enforcement, placement, isolation, quotas, or topology awareness |
| Killable execution isolation | **Open** | Cooperative in-process cancellation only | No process/container sandbox for arbitrary callbacks |
| Irreversible side-effect exactly-once | **Partial** | Mediated Action gateway, receipts, idempotency/fencing primitives | No universal sink-enforced exactly-once transaction |
| Distributed runtime | **Open** | Durable single-writer single-host state | No leader election, consensus, multi-host placement, or multi-writer scheduler |
| Real-model adaptive benchmark | **Controlled benchmark implemented; real evaluation open** | Deterministic offline static-versus-adaptive benchmark with provider profiles, token/time/cost/stale-work/re-execution/progress/parallelism audit fields and CLI injection. `run_multi_seed_benchmark(seeds=(...))` returns per-seed reports and mean/min/max summaries while preserving the single-seed API. | Canonical scenario seeds are audit metadata rather than stochastic workload variation; no statistically powered real LLM/GPU/provider evaluation or direct competitor comparison |
| Resource-aware runtime benchmark | **Implemented, deterministic logical-resource gate** | `lhos benchmark resource-aware-runtime --json`; public `run_async` -> Scheduler -> Claim -> Kernel Lease -> verifier -> VPG Evidence comparison; 3 -> 2 epochs and 1 -> 0 Scheduler resource rejections for the same four-task VERIFIED Goal ([contract](RESOURCE-AWARE-RUNTIME-BENCHMARK.md)) | Synthetic logical capacity only; no wall-clock, physical-utilization, real-model, or distributed-performance claim |
| Wall-clock adaptive-runtime gate | **Implemented, bounded local I/O gate** | `lhos benchmark wallclock-adaptive-runtime --json`; actual `asyncio.sleep` work through the public authority path; stable result is 3 -> 2 epochs and 1 -> 0 Scheduler resource rejections | Local elapsed time is informational only, not a pass/fail or real-LLM/GPU/production-throughput claim |

## Eight-stage delivery sequence

| Stage | Acceptance criterion | Status |
|---:|---|---|
| 1 | Every SDK attempt has an attributable Context snapshot | Done |
| 2 | Durable Agent cognition/provenance snapshot exists | Done |
| 3 | A changed known read cannot commit semantic Evidence | Done |
| 4 | VPG + cognition + Context + logical resources have one immutable view | Done |
| 5 | A graph state produces an auditable scheduling epoch | **Done, bounded**: explicit default-off `EpochController` plus caller-owned `EventDrivenSupervisor` (bounded event loop; no daemon) |
| 6 | Parallelism changes with frontier/conflict state | Done, bounded |
| 7 | A live cooperative attempt can receive, observe, acknowledge, and clean up a fenced interrupt | **Done, bounded**: direct SDK delivery plus verifier commit fence; one-shot workspace route/rejection audit passes 25 focused tests; caller-owned supervisor route is covered; handoff is bounded and non-atomic |
| 8 | Reuse/fresh/model/context/verifier routing improves verified-progress utility | **Implemented as bounded advisory policy plus an opt-in provider execution adapter and controlled provider-profile benchmark; real-provider utility improvement remains open** |

## August 15–16 final-slice verification

The August 15 bounded-control-plane slice added:
the caller-owned `EventDrivenSupervisor`, durable handoff intent
(`prepare_handoff`/`commit_handoff`/`recover_handoff`), and provider-profile fields
in the deterministic online-compute benchmark. These additions are documented
here as bounded primitives; they do not change the release boundary or imply
always-on, cross-plane atomic, physical-resource, or real-provider guarantees.

The later resource batch adds explicit telemetry-to-logical-capacity mapping,
resource-aware main-path batching, bounded per-epoch resource audits, and a
caller-owned capacity-change replanning E2E. The deterministic runtime benchmark
reports 3 -> 2 scheduling epochs and 1 -> 0 Scheduler resource rejections for
the same VERIFIED Goal; this is logical-resource evidence only.

For historical comparison, the latest completed pre-resource-bridge,
non-resource-aware regression included live Context-rebase,
the bounded online-supervisor demo, handoff intent/recovery, multi-seed
benchmark API, fail-closed identity/version fences, and same-plan replay:
**3237 passed, 1 skipped, 18 deselected, 30 warnings in 532.89s**
(`artifacts/final-test-nonslow-20260815-final3.log`). The `3201` live-rebase
run remains a historical baseline
(`artifacts/final-test-nonslow-20260815-live-rebase.log`). Other historical baselines
remain **3187 passed** (`artifacts/final-test-nonslow-20260815-serial.log`),
**3181 passed** (`artifacts/full-test-nonslow-p1-watcher-loop-20260815.log`),
**3166 passed** for cleanup-marker wiring, and **3157 passed** for the one-shot
execution cutoff.
Focused additions include retained Claim -> Harness handoff (4 tests), bounded
multi-epoch execution (8 tests), the mediated workspace commit-validation
slice (47 tests), bounded live rebase/fresh-Attempt handoff (27 tests), and a
watcher route/rejection gate plus caller-owned `WorkspaceWatchLoop` (30 tests),
now covered by the final serial full run. These are bounded
primitives and do not imply automatic provenance, always-on scheduling, atomic
handoff, physical resource management, or main-path Context rebase.
Post-resource focused gates include host-capacity mapping, resource-aware run
audits, the resource-aware runtime benchmark/CLI, and bounded online replanning;
the post-format repository-wide rerun after graph-utility ranking, the
wall-clock adaptive gate, and the Outbox zero-delay retry correction completed
with **3341 passed, 1 skipped, 18 deselected, 30 warnings in 509.05s**
(`artifacts/full-test-nonslow-20260816-post-format.log`). The separate slow
marker gate completed with **18 passed, 3342 deselected in 1224.36s**
(`artifacts/slow-tests-20260816-post-format.log`). Ruff format reports
**626 files already formatted**, while Ruff lint, Mypy, `compileall`, wheel
build/install, and fresh-environment CLI smoke pass locally. This does not
claim a successful GitHub-hosted Actions execution. The earlier `3341` result
(`artifacts/full-test-nonslow-20260816-final-after-outbox.log`) is historical
pre-format evidence only. The earlier
**3321** run remains historical
(`artifacts/full-test-nonslow-after-live-rebase-watchloop-20260815.log`).

## Next implementation gate

The next feature should be **live main-path Context rebase with measured
routing utility and a cross-plane ownership protocol**, not a larger enum.
The current bounded live façade can fence the old Claim, admit a fresh Attempt,
detach the old Harness, and replay the same handoff plan; it remains
release-then-acquire and requires caller registration/recovery. The
multi-epoch loop and workspace watch loop are caller-driven bounded seams, not
an always-on scheduler.
Stage 8 currently provides a deterministic, read-only
`ComputeRoutingPolicy`/`plan_compute_routing` recommendation, a bounded redacted
audit attached to adaptive epoch metadata, and an explicit
`ComputeProviderRegistry` execution adapter. The adapter is enabled only by
`adaptive=True` plus task metadata, runs after Claim/Context setup, and does not
imply automatic provider selection, provider-aware Scheduler mutation, or
physical resource routing. Stage 7 is complete only for the
explicit built-in SDK path and does not imply watcher-driven or third-party
Harness orchestration:

1. Exercise the explicit adapter with one or more real provider/verifier
   implementations without bypassing Claims/Leases.
2. Compare `REUSE_AGENT` and `FRESH_AGENT` on the same task/graph epoch.
3. Measure success, total tokens, reread tokens, wall time, stale work,
   verification cost, and verified progress per token/minute.
4. Add replayable decisions and provider outcome/cost feedback.
5. Keep correctness constraints hard: stale cognition, lost ownership, and
   uncertain side effects cannot be traded for utility.

After that gate, prioritize mediated provenance coverage and killable worker
isolation before physical GPU or distributed scheduling claims.
