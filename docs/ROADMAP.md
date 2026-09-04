# LongHorizonOS implementation roadmap

**Snapshot:** final documented runtime slice on August 16, 2026; older
`20260815` artifact names identify retained historical logs  
**Release boundary:** experimental single-host research alpha (`v0.1.x`)

This roadmap is ordered by correctness first. It deliberately separates
implemented foundations from guarantees that still require a mediated gateway,
adversarial tests, or real workload evidence.

The target control model behind these phases is described in
[Long-Horizon Compute Management](LONG-HORIZON-COMPUTE-MANAGEMENT.md). That
document is a research design proposal; the status table below remains the
source of truth for what the repository actually implements.

The adaptive compute-management sequence is fixed as follows. Statuses in parentheses
are the **current bounded implementation status**, not a production claim:

1. Context VM in `AgentOS.run()` / `run_async()` (**implemented on the SDK
   Attempt path when a manifest/context authority is available**);
2. `AgentSnapshot` plus runtime provenance (**bounded implemented**);
3. commit-time read-set validation and `STALE_COGNITION`
   (**implemented on the main SDK commit path**);
4. a unified `RuntimeStateView` (**implemented, read-only**);
5. `FrontierPolicy` plus `SchedulingEpoch` (**implemented, opt-in proposal;
   explicit default-off `EpochController` with 10 tests; optional
   `graph_utility` ranking remains graph-relative and advisory**);
6. Dynamic Parallelism plus a Conflict Graph (**implemented, opt-in and
   explicit-access-only, with a bounded advisory Scheduler task filter;
   explicit logical resource fitting is available with `resource_aware=True`,
   but this is not an always-on controller or physical-resource optimizer**);
7. Semantic Interrupt plus cooperative preemption/rebase (**policy, direct SDK
   delivery, verifier commit fencing, an explicit bounded Harness control
   bridge, declared-workspace polling watcher, and bounded Claim/Lease handoff
   implemented. Handoff is release-then-acquire (not cross-service atomic); the
   pure Context delta/rebase planner and freshness fail-closed guard are
   available; the one-shot workspace route façade and retained Claim-to-Harness
   binding are now implemented, but they are not an always-on loop and do not
   provide atomic ownership transfer; bounded automatic fresh-Attempt Context
   refresh is implemented on `run()`/`run_async()` when an explicit manifest
   and authoritative Facts are available. Live changed-read `REBASE`/`FULL_RELOAD`
   now use a bounded durable handoff to a fresh Attempt, while cross-plane
   atomic ownership transfer remains open**);
8. Cognitive Locality plus model/context/verifier routing (**bounded advisory
   primitive and explicit provider execution adapter implemented; automatic
   provider scheduling and measured utility remain open**).

The phases below retain the correctness and release gates that support this
sequence. Existing primitives do not imply that a later adaptive step is
already implemented.

## Contract that governs every phase

1. **Minimum repair is graph-relative.** It is minimal only for the dependency
   and provenance graph that the runtime actually accepted.
2. **Unknown authority fails closed.** Unknown provenance, artifact bytes,
   side-effect outcomes, and ownership cannot become `VERIFIED`.
3. **One authority per fact.** Artifact bytes/versions belong to the Artifact
   service; semantic validity to VPG; ownership to Kernel leases; scheduling
   policy to Scheduler.
4. **Every new guarantee requires a regression test and a benchmark.**
5. **Do not broaden the single-host contract until the semantic/ownership loop
   is independently reproducible.**

## Current implementation map

| Area | Status in latest online-control batch | What is actually covered |
|---|---|---|
| VPG invalidation and repair | Implemented, graph-relative | Deterministic causal cone, stale propagation, readiness, repair frontier, exact Artifact bindings. |
| D3 durability | Implemented in GraphStore; bounded SDK projection | D3 envelope is committed with graph refresh, survives reopen, rejects corruption/duplicate IDs, and uses graph-version CAS. `AgentOS.repair()` exposes a task-level summary. |
| Large Goal publication | Implemented for trusted SDK compiler | Goal compilation above `MAX_PATCH_OPS` uses one trusted large-operation transaction and one graph-version increment. |
| Provenance | Partial, mediated boundaries strengthened | Explicit `ExecutionContext`, hash-chained stores, declared/observed coverage, Context VM page-binding auto-read capture, workspace gateway/watcher, and `HTTPToolAdapter` with secure exact-version authority validation for declared reads. Hidden reads outside mediated boundaries remain undiscovered. |
| Observation authority | Partial | Monotonic version/hash checks, durable graph-bound observation tokens, response-loss retry idempotency, atomic same-poll multi-resource VPG/D3 reconciliation, atomic workspace writes/snapshots/CAS, strict workspace authority checks, and explicit `AgentOS.register_external_fact` for mediated external URIs. Repair still has an unsafe integer compatibility path, only declared polling/HTTP mediation, and no cross-system transaction. |
| Claim/evidence causality | Partial | `claim_id`, `attempt_id`, `semantic_epoch`, Lease fencing, active-attempt matching, and built-in provenance-digest validation are enforced on the main SDK path. External-effect binding is not complete. |
| SDK execution closure | Focused gate passing | Sync/async executor and verifier ordering, claim cleanup, repair, capacity, and large-goal regressions pass in the focused snapshot. |
| Context VM on SDK Attempts | Implemented, bounded | `run()`/`run_async()` materialize and fence a Context VM snapshot for each scheduled SDK Attempt; valid materialized page bindings automatically enter the provenance read-set. Explicit manifests are supported; absent manifests receive an empty default. This is not a sandbox or automatic hidden-read detector. |
| Context delta/rebase planner | Implemented, bounded pure/read-only + freshness guard | Explicit graph-delta classification with public exports and `AgentOS.plan_context_rebase`; graph-version advance with partial/unknown coverage or unknown read bindings fails closed; `RebaseRuntimeBridge` can apply an explicit plan to a supplied Harness. It is not wired into `run()`/`run_async()` and does not mutate Context VM. |
| Live Context-rebase façade | Implemented, bounded/fail-closed | `AgentOS.plan_live_context_rebase(...)` binds an explicit delta to the current authoritative VPG version and exact live Claim/Lease/Attempt/process/AgentSnapshot/Context/Harness identities. `apply_live_context_rebase(...)` revalidates them; `REBASE/FULL_RELOAD` persist a durable handoff, fence/release the old Claim, admit a fresh Attempt, detach the old Harness binding, and support exact same-plan replay. The operation remains release-then-acquire/non-atomic, requires caller registration of the replacement Harness or `recover_handoff(...)`, and does not provide cross-plane ownership atomicity. |
| `AgentSnapshot` / stale cognition | Implemented, bounded | Attempt read/write sets, context identity, progress, cost, graph/epoch, and stale-cognition transitions are durable and reopenable. Commit-time read guards quarantine obsolete cognition; direct SDK interrupt delivery also fences late verifier commits. |
| `RuntimeStateView` | Implemented, read-only | `AgentOS.runtime_state(goal)` exposes immutable Progress, Agent/Cognition, Context, and logical Resource projections. It does not mutate state or perform admission. |
| Compute routing / cognitive locality | Implemented, bounded advisory + opt-in provider adapter | `ComputeRoutingPolicy` and `AgentOS.plan_compute_routing(...)` compare explicit exact-version bindings and observed Agent snapshots. Adaptive epochs can attach a bounded redacted audit from explicit `Task.metadata["compute_routing"]`. With `AgentOS(provider_registry=...)`, `adaptive=True`, and explicit `compute_routing.provider_routing.enabled`, registered model/verifier/context hooks can run after Claim/Context setup; focused coverage is in `tests/sdk/test_provider_routing.py`. The adapter does not choose providers automatically, create/reuse processes, mutate the Scheduler, or route physical resources; utility benchmarking remains open. |
| Frontier/interrupt policies | Implemented, opt-in proposals/integration | `FrontierPolicy`, `ConflictGraph`/`DynamicParallelismPolicy`, and `SemanticInterruptPolicy` emit deterministic suggestions. Frontier ordering defaults to repair-first lexical; explicit `graph_utility` uses declared critical-path position and immediate downstream unlock value only within the same safe frontier. With `adaptive=True`, the first two may provide an advisory `allowed_task_ids` filter to `run()`/`run_async()`; adaptive epochs can be persisted as bounded, idempotent `SCHEDULING_EPOCH_PLANNED` audit events; `plan_interrupts(..., persist=True)` may journal bounded proposal metadata. `WorkspaceObservationWatcher` adds an explicit poll -> observation-token -> `ARTIFACT_CHANGED` path; `WorkspaceWatchLoop` repeats that path through caller-owned bounded supervisor epochs. `poll_and_reconcile` batches assigned changes from one poll into one atomic VPG/D3 refresh, while `poll_workspace_and_route(...)` / `route_workspace_observation(...)` provide a one-shot exact Attempt fence and blocked rejection audit. `AgentOS.deliver_interrupt(...)` performs direct exact-identity cooperative delivery for live async SDK batches. `AgentOS.computation_controller(...)` / `online_control(...)` adds an explicit bounded `observe -> reconcile -> plan -> dispatch -> observe` facade, but does not claim or lease work itself. Default `adaptive=False` behavior is unchanged. These surfaces do not force-kill callbacks, bypass Claims/Leases, perform automatic hidden provenance discovery, or control arbitrary third-party Harness sessions. |
| Scheduler-backed online epoch bridge | Implemented, bounded | `AgentOS.schedule_online_epoch(...)` performs policy planning, graph-version checking, and one authoritative Scheduler admission pass using an advisory selected-task filter. The Scheduler creates exact Claim/Attempt/Lease identities; `plan_only=True` creates no ownership, while admitted Claims auto-release unless `keep_claims=True`. Retained Claims require normal execution or `AgentOS.release_online_epoch(...)` cleanup. No Harness, executor, verifier, or semantic commit is invoked. |
| One-shot online execution epoch | Implemented, bounded | `AgentOS.execute_online_epoch(...)` delegates one adaptive epoch to the existing `run_async(..., max_steps=1)` lifecycle, so Scheduler admission, Claim/Attempt/Lease fencing, configured AgentOS executor/verifier callbacks, and VPG Evidence commit are real. Its audit distinguishes policy-selected, actually dispatched, and serial-fallback task IDs and conservatively classifies `completed`, `completed_with_failures`, `no_dispatch`, and strict `no_work_budget` outcomes/phases. It returns `RunResult`, is independent of retained `OnlineEpochScheduleResult` ownership, does not manage an external Harness session, and is not an always-on service. `max_dispatches=0` performs Goal setup/compile-if-missing plus initial result observation, then returns `no_work_budget` without adaptive planning/persistence, ownership, or user code. |
| Bounded multi-epoch execution loop | Implemented, caller-driven | `AgentOS.execute_online_epochs(...)` repeats the one-shot execution path for an explicit epoch/dispatch budget, re-observes VPG after each iteration, and stops conservatively on closure, failure, no dispatch, no budget, or the limit. It is not a daemon, does not consume retained Claims, and does not manage an external Harness. |
| Retained Claim -> Harness handoff | Implemented, bounded | `AgentOS.handoff_online_epoch_to_harness(...)` validates graph/task/Agent/process/Claim/Attempt/semantic-epoch/Lease identity, binds exact Harness adapters, and supports same-identity replay. It does not execute the Harness, release ownership, or create a cross-plane atomic transaction. |
| Post-admission exact-Claim compensation | Implemented, bounded | `SchedulerSession.run_pass(...)` compensates failures after Scheduler admission using exact returned Claim IDs and fencing; replacement owners survive. Compensation failures remain attached to the root exception with bounded `add_note(...)` diagnostics. If cleanup remains incomplete, an idempotent `execution-cleanup.v1` durable marker is recorded and can be reconciled only after terminal Claim plus authoritative no-live-lease checks. This is not an atomic cross-service Claim/Lease/Harness transaction. |
| Async cancellation cleanup | Implemented, bounded | `run_async()` catches worker-pool `CancelledError`, attempts exact-Claim fenced cleanup for every job, continues after per-job/reconcile errors, logs/attaches bounded diagnostics, and re-raises the original cancellation. Incomplete cleanup is retained as a durable audit marker for later reconciliation. This is cooperative single-host cleanup, not killable process isolation or distributed cancellation. |
| Harness control/replay | Implemented, bounded | `AgentOS.register_harness()` / `control_harness()` enforce exact Claim/Attempt/session identity and journal bounded `HARNESS_CONTROL` results. Online-control Harness dispatch also validates graph id/version, task, Agent, Claim, Attempt, and semantic epoch before entering Harness code. Ownerless or ambiguous `START` fails closed; Scheduler/AgentOS must first create the Claim/Attempt and register the matching session. `REBASE` rejects graph rollback and advances the target epoch. The separate Claim/Lease handoff primitive supports exact identity and restart replay but is not cross-service atomic. File-backed reopen can restore logical snapshot and request-idempotency metadata from hash-verified history. Callback/model memory, outputs/details, executable checkpoints, automatic policy-epoch routing, and third-party process control remain open. |
| Online computation controller / CLI | Implemented, explicit/bounded | The facade requires an already compiled Goal and defaults to read-only `RuntimeStateView` observation. An injected dispatcher is optional and remains subject to Harness identity fencing. `lhos benchmark online-compute [--json]` runs a deterministic offline static-vs-adaptive simulator with an injectable `SimulatedProvider` profile and token/latency/cost/stale-work/verified-progress/parallelism metrics. `run_multi_seed_benchmark(seeds=(...))` adds per-seed reports and numeric summaries, but canonical scenario seeds are metadata rather than independent stochastic trials. Neither surface is a real-model, provider, or GPU-throughput benchmark. |
| Event-driven supervisor | Implemented, caller-owned/bounded | `AgentOS.event_supervisor(...)` and `EventDrivenSupervisor` provide explicit `start -> submit -> step -> stop`, bounded `run()`, and async iteration. Each step re-observes state, validates event fingerprints/graph versions, optionally polls a declared watcher, and executes one online epoch. `WorkspaceWatchLoop` is a separate caller-owned bounded polling lifecycle seam. `lhos demo online-supervisor --json` is the deterministic controlled-executor demonstration. No daemon, hidden retry, real LLM, force-kill, universal watcher, or cross-plane ownership transaction. |
| Ownership handoff intent/recovery | Implemented, bounded/recovery witness | `prepare_handoff(...)`, `commit_handoff(...)`, and `recover_handoff(...)` persist exact source identity and `handoff_id`, support replay, and fail closed on uncertain commit. They wrap release-then-acquire and do not provide an atomic Scheduler/Kernel/Harness/VPG transaction. |
| Durable Scheduler replay | Implemented, single-writer boundary | Reopen verifies the event chain, event identity/timestamp columns, snapshot tail/hash, and Claim/Attempt structural invariants. Large projections use `normalized-v1` row storage and changed-row writes. Writers sharing one SQLite file are guarded by generation + state-hash + event-tail CAS, but there is no leader election or supported distributed/multi-writer coordination; projection derivation/hash remains O(N). |
| Kernel Journal append/rebuild concurrency | Implemented, bounded | Independent SQLite connections reserve the writer before reading `next_offset`; the focused two-connection regression produces 40 unique contiguous offsets, and empty rebuild preserves offset zero. This does not widen Scheduler projection mutation beyond its single-writer boundary. |
| Terminal PID reacquisition fence | Implemented, bounded | Terminal state is published before cleanup, and acquisition rejects an existing `EXITED`/`FAILED` PID inside its writer transaction. Cleanup and terminal publication are not one cross-service transaction; general atomic lifecycle handoff remains open. |
| External side effects | Partial primitive; system guarantee open | Injected `ActionGateway` enforces explicit declarations and receipt identity in `secure_mode`; malformed/mismatched/uncertain receipts are recorded as uncertain writes. Zero-delay Outbox retry is now immediately eligible under an explicit logical clock after async publisher failure. Outbox is still at-least-once; arbitrary mail/payment/API effects are not exactly-once or universally reconciled. |
| Action recovery policy | Implemented, compatibility-bounded | `PURE + RETRY` retries a first dispatch exception at most once after fencing revalidation. A durable `retry_count` budget is reserved atomically with an `ACTION_RETRY_RESERVED` journal event; retry exception/unknown becomes `UNCERTAIN`. Legacy rows/events without the budget are conservatively treated as exhausted. `IDEMPOTENT` inspects instead of blindly redispatching; malformed durable policy values fail closed. Direct `SubmitAction` still defaults to `PURE + RETRY`. |
| Sandbox/cancellation | Open | Raw Python callbacks remain host-process code; cancellation is not a killable process boundary. |
| Host resource telemetry | Implemented, observation-only collector | `collect_host_resource_telemetry()` reports CPU/RAM and optional NVIDIA GPU/VRAM observations with explicit availability/failure reasons. The collector does not mutate Scheduler admission or ownership. A separate explicit `derive_host_capacity(...)` / `apply_host_capacity(...)` bridge may update one named logical pool; physical placement, quotas, isolation, and shared capacity authority remain open. |
| Telemetry-to-logical-capacity bridge | Implemented, explicit/single-pool | `HostCapacityPolicy`/`derive_host_capacity(...)` reserve and round down authoritative available values; `AgentOS.apply_host_capacity(...)` atomically updates one named Registry/Scheduler logical pool and rejects capacity below active reservations. It does not poll continuously, partition a shared host, place/isolate processes or devices, or enforce OS quotas. |
| Resource-aware adaptive execution | Implemented, opt-in/bounded | `run()`/`run_async()` with `adaptive=True, resource_aware=True` combine explicit ConflictGraph access with explicit CPU/RAM/GPU/VRAM/model-slot requests and current logical pool capacity. The policy is advisory; Scheduler/Claim/Lease admission remains authoritative. The deterministic [`resource-aware-runtime`](RESOURCE-AWARE-RUNTIME-BENCHMARK.md) gate and bounded [`resource-replanning` E2E](RESOURCE-REPLANNING-E2E.md) exercise the authoritative path without claiming physical or wall-clock acceleration. |
| Real local wall-clock adaptive gate | Implemented, bounded/informational timing | `lhos benchmark wallclock-adaptive-runtime --json` runs actual local async I/O through the public authority path and deterministically proves 3 -> 2 epochs plus 1 -> 0 Scheduler resource rejections. | Elapsed time is informational, not a performance gate or real-model/GPU/production claim. |
| Physical resources/distribution | Open | Resource vectors are logical per-Agent admission. Host telemetry is not a host/device inventory, physical placement or isolation mechanism, and there is no distributed scheduling. |
| Lease renewal/heartbeat | Partial (cooperative) | Kernel/Claim/Scheduler renewal preserves fencing and journals atomically; heartbeat is attempt-scoped. `AsyncWorkerPool` provides an optional cooperative loop (disabled by default) that requires a callback or Scheduler hook and fails closed when none is available; it does not provide killable cancellation. |

## Online compute-management primitives

The first implementation slice of the adaptive design is deliberately
bounded. It gives callers durable observations and deterministic proposals
without silently replacing the existing Scheduler:

- `AgentSnapshot` records the computation identity and cost context of a live
  Attempt (`graph_version`, semantic epoch, read/write sets,
  `ContextIdentity`, progress, and cost).
- Commit-time read validation marks cognition stale and quarantines the Attempt
  before semantic Evidence can be written.
- `RuntimeStateView` provides one immutable input projection for future policy
  engines.
- `FrontierPolicy` ranks the observed READY/repair frontier. Its default is
  repair-first lexical; explicit `graph_utility` prefers declared critical-path
  position and immediate downstream unlock value without changing repair
  priority or safety filters. The conflict policy forms a greedy independent
  batch only from explicit access sets.
- `SemanticInterruptPolicy` turns explicit events into auditable routing
  proposals; `plan_interrupts(..., persist=True)` can optionally journal the
  bounded proposal metadata. `AgentOS.deliver_interrupt(...)` then routes a
  validated cooperative request to a live token-aware SDK executor, while the
  async verifier-to-Evidence commit fence quarantines late/ignored completions.
- `EpochController` provides an explicit/default-off, read-only
  `observe -> reconcile -> plan` loop (10 tests); `plan_context_rebase` provides
  pure explicit Context reuse/reload classification (17 tests). A bounded
  Claim/Lease handoff (8 tests) and explicit HTTP provenance adapter (11 SDK
  boundary tests; 20 low-level+SDK; 29 related authority gate) are now present,
  but none imply an always-on controller, universal network interception, or
  atomic ownership transfer. The separate explicit live façade now supports a
  bounded durable fresh-Attempt handoff for changed reads.
- `OnlineComputationController`, exposed by
  `AgentOS.computation_controller(...)` / `online_control(...)`, adds an
  explicit/default-off `observe -> reconcile -> plan -> dispatch -> observe`
  seam for an already compiled Goal. Without an injected dispatcher it is
  read-only and does not claim, lease, execute, or publish Evidence. The
  identity-fenced Harness dispatcher rejects ownerless `START` proposals.
- `AgentOS.schedule_online_epoch(...)` adds a bounded bridge from policy
  planning to authoritative Scheduler admission and exact-Claim cleanup. It is
  deliberately separate from Harness execution and semantic commit.
- `AgentOS.execute_online_epoch(...)` adds a separate one-shot executing
  vertical slice over the existing `run_async` authority path. It can execute
  configured AgentOS callbacks and commit verified Evidence, but it cannot
  consume retained `OnlineEpochScheduleResult` Claims or start/rebase/resume an
  external Harness session. Its `max_dispatches=0` form is an immediate
  no-work return after Goal setup, not a persisted planning epoch.

These primitives are **not** an always-on Adaptive Scheduler. `adaptive=True`
is an explicit, bounded opt-in on `AgentOS.run()`/`run_async()` that observes
state and supplies an advisory `allowed_task_ids` filter; `adaptive=False`
continues to use the historical Scheduler loop. WHO/WHERE admission remains
Scheduler/Kernel authority. Universal automatic provenance discovery (the
explicit `WorkspaceObservationWatcher` is bounded), physical resource
placement/partitioning,
force-kill isolation, atomic Harness ownership handoff, automatic controller
operation, policy-driven Harness orchestration,
automatic provider
selection, provider-aware Scheduler mutation, and physical-resource
model/context/verifier routing remain open. The explicit
`ComputeProviderRegistry` adapter is available only when a caller opts in with
`AgentOS(provider_registry=...)` and task metadata; it does not bypass
Claims/Leases or publish semantic Evidence.

The new event-driven supervisor is likewise a caller-owned bounded bridge, not a
background service. The handoff intent protocol is a durable recovery witness,
not a two-phase commit. The provider-profile benchmark changes only synthetic
accounting scales and must not be reported as a real provider or GPU result.

## Phase 0 — release contract and regression fence

**Status: mostly complete; maintenance.**

Delivered:

- `docs/PROVENANCE-CONTRACT.md`
- `docs/ISSUE-INVENTORY.md`
- focused provenance, observation-authority, D3, causal-binding, SDK-closure,
  and large-goal tests;
- durable Scheduler corruption tests for event/snapshot tampering, duplicate
  ownership identities, orphan Attempts, malformed idempotency data, and
  rollback, normalized-row tampering, and stale-writer CAS rejection;
- recovery-policy tests for fenced pure retry, retry failure/unknown,
  durable retry-budget reservation/replay, superseded-lease rejection,
  durable replay, and additive legacy action schema migration;
- release documentation that distinguishes focused gates from the full
  repository suite.

Remaining:

- arbitrary hidden-read injection with a false-closure assertion through an
  unrestricted Python/file/API path (the mediated-boundary safety benchmark is
  now available, but it does not discover bypassed reads);
- crash campaigns for every side-effect and commit boundary;
- a GitHub-hosted Actions run that records environment, commit identifier, and
  raw traces for every configured gate. The same configured gates pass in the
  current local reproduction, but hosted execution is not claimed.

## Phase 1 — mediated provenance and observation authority (P0-1/P0-2)

**Status: partial; next implementation priority.**

Already implemented:

1. Immutable provenance events with graph/task/attempt/epoch fields.
2. In-memory and JSONL hash-chain stores with replay/corruption checks.
3. `ExecutionContext` and `context_v1` callback convention.
4. `COMPLETE` / `PARTIAL` / `UNKNOWN` coverage reports and strict fail-closed
   policy.
5. FactsProvider monotonic version/content-hash checks (including reads from
   legacy databases without the observation-token table), Workspace atomic
   write/snapshot/CAS helpers, and the mediated WorkspaceProvenanceGateway with
   strict authority-backed version validation.
6. Graph-bound `ObservationToken` issuance and safe
   `repair(goal, observation=token)` are implemented; the integer-only repair
   call remains a deprecated compatibility path. Equal
   artifact/version/hash/graph observations reuse one durable token, including
   retry after a lost post-commit response.
7. The offline `hidden-provenance` benchmark and CLI gate demonstrate that
   missing or explicitly unknown provenance cannot pass strict admission.
8. `HTTPToolAdapter` mediates declared secure GET/HEAD/OPTIONS reads with exact-version validator/authority checks; `AgentOS.register_external_fact` seeds the authority for an explicit URI. SDK/low-level/related gates pass 11/20/29 tests respectively.
9. `WorkspaceObservationWatcher` polls explicitly declared workspace files,
   issues new observation tokens on content changes, and emits bounded
   `ARTIFACT_CHANGED` interrupt inputs. Assigned changes in one poll are
   validated before one atomic batched VPG/D3 refresh. It does not observe
   unmediated I/O.

Next:

1. Extend the capability-scoped Artifact/Workspace/HTTP/Tool gateway and
   prohibit unmediated reads in secure mode.
2. Persist canonical read-set/write-set records, source validators (ETag or
   snapshot token), coverage digest, and observation epoch.
3. Add HTTP/Tool watcher validators and a cross-system commit protocol around
   the observation token; retain the integer API only as explicitly unsafe
   compatibility. The workspace poller is the bounded baseline.
4. Add hidden-file/API, watcher-gap, deletion, rollback, and same-version/
   different-bytes tests.

Exit gate:

- zero false `VERIFIED` results under hidden-read mutation;
- no “minimum repair” claim for `PARTIAL` or `UNKNOWN` coverage;
- every repair result includes a durable observation/coverage digest.

## Phase 2 — side-effect and ownership closure (P0-3/P0-5)

**Status: binding foundation partial; cooperative heartbeat loop and bounded
Action recovery policy implemented; strict side-effect protocol remains open.**

Already implemented:

1. Operational executor success is recorded before semantic verifier commit.
2. Main SDK Evidence/VPG commits use Lease-generation fencing.
3. Evidence carries `claim_id`, `attempt_id`, `semantic_epoch`,
   `lease_fencing_token`, and a coverage `provenance_digest` when available;
   Scheduler completes only a matching active attempt/lease generation.
4. Claim cleanup is fenced by expected claim identity.
5. Scheduler reconciliation, TTL reclaim, and cooperative attempt-scoped
   heartbeat/lease-renew are available. Renewal preserves the lease fencing
   token, and Lease renew/release projection+journal updates are atomic with
   malformed provider responses rejected fail-closed.
6. `AsyncWorkerPool` provides an optional cooperative heartbeat loop. It is
   disabled by default; enabling it requires `heartbeat_interval` plus a
   callback or Scheduler `heartbeat`/`renew_claim` hook. Missing hooks fail
   closed, and the loop does not provide killable cancellation.
7. Kernel Actions persist side-effect class, recovery policy, and a durable
   retry budget. A first dispatch exception can take one fenced retry only for
   `PURE + RETRY`; retry admission atomically appends
   `ACTION_RETRY_RESERVED` and increments `retry_count`. Retry
   exception/unknown outcomes become `UNCERTAIN` and release the old lease
   bundle. Existing action projections migrate additively; old rows lacking
   retry-budget state are treated conservatively as exhausted.
9. `handoff_task` validates exact Claim/Attempt/semantic epoch identity, persists an idempotent handoff id, and replays after restart (8 tests). It is release-then-acquire, not cross-service atomic, and does not orchestrate a Harness.

8. Terminal process publication has a bounded PID-reacquisition fence:
   terminal state is published before cleanup and `atomic_acquire` rejects an
   existing `EXITED`/`FAILED` PID inside the writer transaction.

Next:

1. Require every external effect and raw driver callback to go through an
   Action/ToolGateway handle under strict admission.
2. Extend effect classification and recovery policy to every driver, and
   change the future major default from compatibility `PURE + RETRY` to
   `UNKNOWN + UNCERTAIN` without reinterpreting old databases.
3. Require sink idempotency/CAS or mark crash-after-dispatch `UNCERTAIN`;
   never blindly retry irreversible/unknown effects.
4. Require the full binding in secure mode and extend it to every external
   side-effect sink; malformed receipts must persist an uncertain write.
5. Propagate heartbeat failure into explicit lease-loss cancellation/recovery
   semantics and add sink reconciliation. The worker-pool loop remains
   cooperative and cannot forcibly terminate arbitrary callbacks.
6. Combine cleanup and terminal publication under a true cross-service
   lifecycle transaction or a durable reconciliation protocol. The current
   terminal-PID fence prevents the bounded reacquisition race but is not that
   transaction.

Exit gate:

- crash-before/after-dispatch and before/after-ack matrix has no duplicate
  irreversible success;
- misclassified custom drivers are rejected under strict admission;
- stale workers cannot close a newer claim;
- healthy long-running workers renew without losing ownership.

## Phase 3 — atomic graph publication and finer-grained repair (P0-6/P1)

**Status: P0-6 implemented for the trusted SDK compiler; finer repair open.**

Delivered:

- trusted large-operation admission for Goal compilation;
- regression proving a 503-operation Goal produces one patch and one graph
  version, with a complete projection and no dispatchable partial graph;
- D3 durable envelope and optimistic graph-version CAS.

Next:

1. If compilation becomes externally staged or multi-process, add a durable
   staging manifest and atomic publish marker.
2. Add output-scoped Artifact/Evidence nodes and multi-output task edges.
3. Version verifier/configuration/external facts as first-class observations.
4. Add independently verifiable semantic-equivalence pruning.
5. Expose complete durable D3 goal/frontier projection and byte-identical
   replay through the SDK.

Exit gate:

- finer-grained workloads save work against the oracle task-DAG checkpoint,
  not only against full restart, with zero false closure;
- D3 replay is durable and byte-identical.

## Phase 4 — incremental runtime performance (P1)

**Status: open; low-risk serialization-cache optimization landed.**

Work items:

- incremental dependency indexes and affected-subgraph derivation;
- incremental validation/hash rather than full projection hashing;
- deletion tombstones and bounded history compaction;
- streaming scheduler passes;
- incremental dirty-index tracking and rolling/Merkle-style projection hashes
  to reduce the remaining O(N) derivation and validation work;
- a Scheduler durability benchmark covering snapshot bytes written, reopen
  latency, and event-append latency as Claim/Attempt history grows;
- p50/p95/p99 commit latency, memory, and write amplification at N=400,
  1,000, and 10,000.

The N=400 storage regression is improved (about 1.64 MB versus the former
~37.9 MB full-copy layout), and a commit-local serialization cache reduces
duplicate JSON work. Full projection derivation/validation/hash still
dominates commit latency; p99 remains host/workload sensitive and can
occasionally approach or exceed 50 ms, so the aspirational 50 ms/10 ms target
is not a stable SLO. Scheduler durability still has O(N) projection
derivation/hash cost and needs a checked-in scale benchmark for reopen latency,
event append latency, and bytes written.

## Phase 5 — optional scale and productization (P2)

Only after Phases 1–4:

- telemetry-backed physical admission/placement using a shared host/device
  inventory (point observations and an explicit single-pool logical-capacity
  bridge are implemented, but shared physical authority is still open);
- process/container sandbox and GPU/VRAM isolation;
- quotas, priorities, fairness, preemption, and starvation analysis;
- multi-host worker delivery, leader election, CAS, and distributed replay;
- hosted observability, migrations, and production SLOs;
- statistically powered real-model/real-GPU/representative workload
  comparisons.

These are separate milestones, not prerequisites for an honest `v0.1.x`
research release.

## Release gates

### Post-batch focused additions

- `tests/sdk/test_epoch_controller.py`: **10 passed**.
- `tests/runtimes/multi_agent/test_handoff.py`: **8 passed** (including
  restart replay; release-then-acquire remains non-atomic).
- `tests/sdk/test_context_delta.py`: **17 passed** (pure/read-only planner and
  public `AgentOS.plan_context_rebase` facade).
- `tests/sdk/integrations/test_http_sdk_boundary.py`: **11 passed**;
  low-level HTTP provenance plus SDK boundary: **20 passed**; related
  HTTP/authority gate: **29 passed**.
- SDK online-compute/freshness package gate: **485 passed**.
- Combined computation-control, freshness, Harness-dispatch, CLI, and
  computation-utility gate: **65 passed**.
- `tests/sdk/test_harness_dispatcher_fencing.py`: **9 passed**. Ownerless or
  ambiguous `START` and stale graph/task/Agent/Claim/Attempt/epoch actions are
  rejected before Harness code.
- `tests/sdk/test_online_epoch.py`: **7 passed** for plan-only behavior,
  authoritative Scheduler admission/cleanup, ownerless Harness fencing,
  advisory selected-task filtering, graph-race cleanup, release-error
  retention, and idempotent terminal cleanup.
- `tests/sdk/test_online_epoch_execution.py`: **6 passed** for one real
  Scheduler/executor/verifier/VPG epoch, failed-executor ownership cleanup,
  already-verified no-op behavior, policy-versus-fallback dispatch audit,
  strict zero-dispatch behavior, and parameter validation.
- `tests/sdk/test_online_epoch_cleanup.py`: **3 passed** for exact Claim/Lease
  cleanup after post-admission observation failure, preserving the root error
  with `add_note(...)` when cleanup itself fails, and replacement-Claim fencing.
- `tests/sdk/test_async_run.py -k cancel`: **3 passed** for cancellation
  cleanup, replacement-Claim fencing, and preservation of the primary
  `CancelledError`.

### `v0.1.x` maintenance

- focused SDK/VPG/provenance regression passes;
- README and release notes state graph-relative semantics and all boundaries;
- offline demos and benchmark scripts remain reproducible;
- `resource-aware-runtime` and the bounded resource-replanning E2E keep the
  same VERIFIED Goal while testing logical capacity packing and explicit
  caller-owned re-observation;
- the full non-slow and separate slow marker suites are reported with exact
  local evidence.

Current August 16 focused snapshot: watcher route/rejection plus caller-owned
`WorkspaceWatchLoop` **30 passed**, mediated workspace commit validation
**47 focused tests**, live rebase/fresh-Attempt handoff **27 focused tests**,
overlapping observation-token/authority/watcher **44 passed**, and
Journal/atomicity/rebuild/SQLite isolation/migration `25 passed`; the
overlapping Journal/Lease combined gate is `56 passed` (`25` + `31`). The
earlier non-slow run completed with
`3142 passed, 1 skipped, 18 deselected, 30 warnings` in `532.37s`
(`artifacts/full-test-nonslow-online-epoch-hardening-20260815.log`). That full
run predates the one-shot execution and cleanup/cancellation additions. The
cleanup-marker wiring baseline was `3166 passed, 1 skipped, 18 deselected,
30 warnings` in `458.98s`; the current post-format non-slow regression
additionally includes opt-in graph-utility frontier ranking, the real local
wall-clock adaptive-runtime gate, and the zero-delay Outbox retry correction:
`3341 passed, 1 skipped, 18 deselected, 30 warnings` in `509.05s`
(`artifacts/full-test-nonslow-20260816-post-format.log`). The separate slow
marker gate completed with `18 passed, 3342 deselected` in `1224.36s`
(`artifacts/slow-tests-20260816-post-format.log`). The earlier `3341` result in
`artifacts/full-test-nonslow-20260816-final-after-outbox.log` is historical
pre-format evidence only. The earlier
`3321` workspace/watch-loop/live-handoff run remains historical
(`artifacts/full-test-nonslow-after-live-rebase-watchloop-20260815.log`). The earlier
`3257` result is the pre-resource-bridge baseline
(`artifacts/final-test-nonslow-20260815-final-sync.log`). The `3201` live-rebase
run remains historical
(`artifacts/final-test-nonslow-20260815-live-rebase.log`). The
historical `3187` final-serial and `3181` watcher logs remain
`artifacts/final-test-nonslow-20260815-serial.log` and
`artifacts/full-test-nonslow-p1-watcher-loop-20260815.log`. Focused gates and
full-run counts overlap and are not additive.
The repository's configured gates pass in the current local reproduction:
Ruff format reports **626 files already formatted**; Ruff lint, Mypy,
`compileall`, wheel build/install, fresh-environment CLI smoke, non-slow tests,
and the separate slow marker gate pass. A successful GitHub-hosted Actions run
is not claimed by this local evidence.

### `v0.2` research milestone

- Phase 1 observation-token gateway and hidden-read benchmark;
- Phase 2 side-effect reconciliation and lease-loss campaign;
- public artifact with raw traces, environment, and commit identifier.

### `v0.3` systems-paper milestone

- finer-grained benchmark beats the oracle task-DAG baseline on work saved;
- durable replay and failure semantics independently audited;
- fair comparisons with workflow engines, asset/data systems, and resource
  schedulers; no “first to have graph/checkpoint/scheduler” claim.
