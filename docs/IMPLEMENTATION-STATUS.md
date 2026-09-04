# LongHorizonOS implementation status

**Verification date:** August 16, 2026 (final documented slice)  
**Current source-fix cutoff:** mediated workspace commit-time validation, caller-owned workspace watch loop, bounded durable live `REBASE`/`FULL_RELOAD` handoff to a fresh Attempt, bounded event-driven supervisor, ownership-handoff intent/recovery, provider-profile benchmark, retained-Harness handoff, bounded multi-epoch execution, explicit host-telemetry-to-logical-capacity mapping, resource-aware `run()` / `run_async()` execution, opt-in graph-utility frontier ranking, the local wall-clock adaptive-runtime gate, and the zero-delay Outbox retry correction over the existing Scheduler/Kernel authority path  
**Workspace:** local source checkout under `Downloads/LongHorizonOS-main`  
**Release boundary:** experimental single-host research alpha (`v0.1.x`)

This file is the short status sheet for the current implementation. It is
intended to prevent a documented primitive, a focused test, and a product
guarantee from being confused with one another.

## What is implemented

### Semantic control plane

- VPG is the authority for task validity, readiness, evidence applicability,
  causal invalidation, repair frontier, and Goal closure.
- Evidence and Artifact references are exact-version bindings.
- D3 invalidation results are persisted in the same SQLite transaction as the
  graph refresh.
- D3 rows are append-only, hash/envelope checked, reopenable, and guarded by
  optimistic graph-version CAS. Goal reopen data (`reopened_goals`) and the
  direct repair frontier are included in the durable payload.
- Goal closure and reopening after invalidation are derived from explicit
  Goal-to-task edges; the D3 payload records reopened goals and direct repair
  frontier data.

### Execution and ownership

- Scheduler Claims and Attempts are connected to Kernel Lease ownership.
- Operational executor success is recorded before semantic verifier commit.
- Main SDK Evidence/VPG commits use Lease-generation fencing.
- Evidence carries `claim_id`, `attempt_id`, `semantic_epoch`,
  `lease_fencing_token`, and (when coverage is available) a
  `provenance_digest`; scheduler completion requires all present bindings to
  match the active attempt/lease generation. The built-in SDK validates the
  digest before commit and rejects malformed/superseded lease bindings
  fail-closed.
- Kernel/Claim/Scheduler renewal and attempt-scoped heartbeat are available.
  Lease renew/release projection and journal updates are atomic. `AsyncWorkerPool`
  also supports an optional cooperative heartbeat loop, disabled by default;
  enabling it requires `heartbeat_interval` and either a heartbeat callback or a
  Scheduler `heartbeat`/`renew_claim` hook. If no usable hook is available, the
  pool fails closed with `HeartbeatFailed`. This loop does not provide killable
  cancellation; arbitrary in-process callbacks remain non-killable.
- Synchronous and asynchronous SDK execution paths support bounded overlap,
  retries, verifier ordering, resource admission, and fenced cleanup.
- Durable Scheduler reopen verifies the event sequence/hash chain, event
  identity and timestamp columns, snapshot hash/tail binding, and projection
  structure. It rejects duplicate Claim/Attempt identities, multiple `ACTIVE`
  Claims for one graph/task, orphan or identity-mismatched Attempts, malformed
  collection fields, and non-string idempotency keys before rebuilding the
  in-memory managers. Large projections use a normalized, row-hashed
  `normalized-v1` representation: the manifest stays small and only changed
  entity rows are upserted, while legacy inline snapshots remain readable.
  Writers sharing one SQLite file are guarded by generation, state-hash, and
  event-tail compare-and-swap checks; this remains a single-writer Scheduler
  boundary, not leader election or supported multi-writer coordination.
- Independent `JournalService` instances sharing one SQLite file reserve the
  writer before reading `next_offset`, preventing duplicate/racing offsets
  across connections. Rebuilding an empty Journal preserves `next_offset == 0`
  rather than inventing offset one. This is a bounded Journal append/rebuild
  guarantee; it does not turn Scheduler projections into a supported
  multi-writer control plane.
- Terminal process publication provides a bounded PID-reacquisition fence:
  terminal state is published before cleanup, and `atomic_acquire` rejects an
  existing `EXITED`/`FAILED` PID while holding its writer transaction. Cleanup
  and terminal publication are not one cross-service transaction, so this does
  not establish a general atomic process/lease lifecycle handoff.
- Kernel Actions persist `side_effect_class` and `recovery_policy`. On a first
  driver dispatch exception, `PURE + RETRY` performs at most one additional
  dispatch only after re-validating the Action's current lease/fencing
  contract. A completed retry still requires `commit_if_fenced()`; a retry
  exception or `UNKNOWN` result becomes `UNCERTAIN` and releases the old lease
  bundle. `IDEMPOTENT` actions inspect rather than blindly redispatch, while
  `NON_REVERSIBLE`/`UNKNOWN` actions fail closed unless an explicit inspect
  policy is selected.
- Existing `actions_projection` databases receive additive columns for resource
  claims, fencing tokens, side-effect class, recovery policy, and durable
  `retry_count`. Historical rows retain compatibility defaults (`pure`/`retry`)
  but are conservatively treated as having exhausted their retry budget when
  the new column/event is absent. Retry admission consumes the budget
  atomically with an `ACTION_RETRY_RESERVED` journal event, so a restart cannot
  grant the same retry twice. This migration does not retroactively prove that
  an old driver was side-effect free, so direct compatibility actions remain
  subject to the documented retry risk.

### Large Goal publication

- The trusted SDK Goal compiler can publish a Goal larger than the ordinary
  `MAX_PATCH_OPS` limit through one large-operation GraphStore transaction.
- The regression case creates 503 operations and observes one patch and one
  graph-version increment, with no intermediate dispatchable projection.

### Provenance and observation primitives

- `ExecutionContext` and `context_v1` callbacks are available.
- Provenance events have graph/task/attempt/epoch identity and can be stored in
  memory or a hash-chained JSONL journal.
- Declared-versus-observed coverage reports support `COMPLETE`, `PARTIAL`, and
  `UNKNOWN`; strict policy denies incomplete coverage.
- Legacy task-id callbacks are explicitly represented as `UNKNOWN` for strict
  coverage rather than being presented as complete provenance.
- FactsProvider rejects non-positive/rollback versions and same-version hash
  changes, and can read legacy databases that do not contain the observation
  token table. Workspace writes support atomic replacement, snapshots, and CAS.
- `AgentOS.observe_artifact(...)` and `observe_workspace_artifact(...)` issue
  graph-bound, content-hash-backed `ObservationToken` records. The safe repair
  path is `repair(goal, observation=token)`; the integer-only repair API remains
  an explicitly unsafe/deprecated compatibility path. `AgentOS.register_external_fact`
  can seed an authoritative external URI/version/hash for an explicitly mediated
  integration; it is not a universal world observer.
- The offline `hidden-provenance` benchmark and CLI gate exercise the
  fail-closed coverage boundary: a missing declared read is `PARTIAL`, an
  explicit unidentifiable read is `UNKNOWN`, and strict admission denies both.
  This does not discover unrestricted Python/file/API reads.
- **Context-page provenance bridge (implemented, bounded):** when a
  `context_v1` Attempt binds a valid Context VM snapshot, each materialized
  `page_binding` is automatically appended to the execution read-set with
  `source="context_vm"` and exact URI/version/content hash. Malformed bindings
  become `UNKNOWN`; hidden reads outside the materialized pages remain outside
  this bridge.
- **Workspace provenance gateway (implemented, mediated):**
  `WorkspaceProvenanceGateway` enforces root/capability confinement, hashes the
  exact bytes read or written, and supports atomic write plus CAS checks. In
  strict mode a supplied positive version must be checked by an explicit
  `version_validator` or `version_authority.read_hash`; a caller integer alone
  is not semantic authority. Audit/compatibility mode may retain an unverified
  caller version with `version_source="caller"`. This is not universal I/O
  interception or a cross-plane transaction. The built-in SDK commit path also
  performs a bounded `validate_read_set_current()` /
  `require_read_set_current()` fence for mediated workspace reads: changed or
  deleted bytes, unavailable paths, and truncated checks fail closed before
  semantic Evidence commit. This point-in-time check is single-host and
  mediated (47 focused commit-validation tests); it is not a filesystem lock
  or a workspace/Facts/VPG atomic transaction. `HTTPToolAdapter` adds an explicit
  secure `context_v1` boundary for declared GET/HEAD/OPTIONS reads, requiring an
  exact-version validator/authority; mutating requests and unknown/transport
  failures remain fail-closed.

### Host resource observation and explicit logical admission bridge

- **Optional host-resource telemetry adapter (implemented):**
  `collect_host_resource_telemetry()` exposes a point-in-time,
  JSON-compatible observation of logical CPU count and host RAM, plus optional
  NVIDIA GPU count/VRAM when `nvidia-smi` is available. Each metric has an
  explicit availability bit and bounded failure reason; missing tools,
  malformed output, timeouts, or contradictory quantities fail closed rather
  than becoming zero capacity.
- **Explicit telemetry-to-logical-capacity bridge (implemented, opt-in):**
  `HostCapacityPolicy` and `derive_host_capacity(...)` apply caller-selected
  reserve fractions with deterministic downward rounding. Required unknown
  planes fail closed; CPU-only pools require an explicit `require_gpu=False`.
  `AgentOS.apply_host_capacity(...)` atomically updates the named registered
  Agent pool's Registry capacity and logical Scheduler allocator under one
  lifecycle lock. Capacity below active reservations is rejected without a
  partial update. No polling or application happens automatically.
- These APIs are not a host/device inventory authority, physical placement
  policy, isolation boundary, quota manager, preemption mechanism, or GPU
  scheduler. A telemetry sample can become stale immediately, and applying the
  same host total to multiple pools would double-count unless the caller
  partitions it explicitly.

### Online compute-management primitives

The first bounded pieces of the long-horizon compute-management design are
implemented. Some are observational/proposal surfaces, while the worker pool
also provides a cooperative control primitive:

- **Context VM on the SDK execution path (bounded):** each scheduled SDK
  Attempt receives an attributable Context VM load/snapshot when
  `AgentOS.run()` or `run_async()` dispatches it. A task may provide an
  explicit `ContextManifest`; tasks without one receive a small empty
  manifest. The snapshot is fenced to the live Attempt and persisted in the
  durable Scheduler projection. This is not a sandbox and does not discover
  arbitrary hidden reads.
- **Context delta/rebase planner (implemented, pure/read-only):**
  `plan_context_rebase`/`build_context_delta` compare explicit version-pinned
  bindings against an explicit graph delta and return immutable `REUSE`,
  `REBASE`, `FULL_RELOAD`, or `BLOCKED` plans. The public SDK exports and
  `AgentOS.plan_context_rebase(...)` facade are covered by 17 tests. The planner
  does not inspect external state, mutate Context VM, interrupt a process, or
  participate in the main `run()`/`run_async()` path.
- **Context/Harness rebase guard (implemented, bounded):**
  `validate_read_set_freshness(...)` requires complete delta coverage before a
  graph-version advance can be declared fresh. Partial or unknown coverage and
  unidentifiable reads return `BLOCKED`; an explicitly unchanged partial delta
  is accepted only when the graph version did not advance.
  `RebaseRuntimeBridge` can turn the resulting explicit plan into
  `CONTINUE`/`REBASE` requests for a supplied Harness and can call an explicit
  non-atomic handoff callback. It is not automatically invoked by
  `run()`/`run_async()`, does not mutate Context VM itself, and does not create
  an atomic ownership transfer.
- **Live Context-rebase SDK façade (implemented, bounded):**
  `AgentOS.plan_live_context_rebase(...)` reads the exact active
  Claim/Lease, Scheduler Attempt, durable `AgentSnapshot`, and registered
  Harness session, then returns the public immutable
  `LiveContextRebasePlan` DTO. `AgentOS.apply_live_context_rebase(...)`
  requires the target to be the current authoritative VPG version and
  revalidates graph/Claim/Lease/task/Agent/process/Attempt/semantic-epoch,
  `AgentSnapshot`, Context, Harness session, and plan/delta hashes before
  entering Harness code. It returns `LiveContextRebaseApplyResult`. Only
  `REUSE` may issue fenced `START`/`CONTINUE`; an exact same-plan replay uses
  the cached request-idempotency result and does not re-run the Harness hook.
  Unrelated revision changes still fail closed. For changed reads,
  `REBASE`/`FULL_RELOAD` now use a bounded durable handoff: the old Claim is
  fenced/released, a fresh Attempt is admitted, the old Harness binding is
  detached, and the same plan can be replayed idempotently. This is
  release-then-acquire rather than a cross-plane atomic transaction; callers
  must register a new Harness for the replacement Attempt and call
  `recover_handoff(...)` when recovery is required. The façade requires
  explicit graph-delta/context inputs, does not discover hidden reads, mutate
  VPG/Context VM, or run automatically from `run()`/`run_async()`. Focused
  live-rebase coverage is 27 tests.
- **Default-off epoch controller (implemented, read-only):** `EpochController`
  performs explicit `observe -> reconcile -> plan` passes with deterministic
  idempotency and fail-closed graph-version checks. Ten focused tests cover the
  default-off contract, bounded runs, conflict batching, and interrupt blocking.
  It does not claim work, acquire leases, or start Harness sessions.
- **Online computation controller (implemented, explicit/bounded):**
  `OnlineComputationController` adds one
  `observe -> reconcile -> plan -> dispatch -> observe` seam around the
  read-only epoch policy. `AgentOS.computation_controller(...)` and its
  `online_control(...)` alias require an already compiled Goal and default to
  `AgentOS.runtime_state(...)` for observation. Without an injected dispatcher
  the facade is read-only: it does not claim work, acquire a Lease, execute
  Agent code, or publish Evidence. It has no background thread or implicit
  retry loop.
- **Scheduler-backed online epoch bridge (implemented, bounded):**
  `AgentOS.schedule_online_epoch(...)` obtains a deterministic policy plan,
  checks graph-version freshness, and can pass its selected task IDs to one
  authoritative Scheduler admission pass. The Scheduler—not the policy
  controller—creates Claims/Attempts, reserves logical resources, and fences
  Kernel Leases. `plan_only=True` creates no ownership. An admitted epoch
  auto-releases new Claims unless `keep_claims=True`; retained Claims require
  normal lifecycle execution or explicit
  `AgentOS.release_online_epoch(...)` cleanup. This API never invokes a
  Harness, executor, verifier, or semantic commit and is not an automatic
  execution loop.
- **One-shot online execution epoch (implemented, bounded):**
  `AgentOS.execute_online_epoch(...)` delegates one
  `observe -> reconcile -> plan -> Scheduler admission -> execute -> verify ->
  VPG commit -> observe` epoch to the existing
  `run_async(..., adaptive=True, max_steps=1)` path. It uses real
  Scheduler/Claim/Attempt/Lease authority, configured AgentOS executor and
  verifier callbacks, and the existing serialized Evidence/VPG commit. It
  returns `RunResult` with additive `meta["online_epoch"]` metadata. This is a
  separate API from `schedule_online_epoch(...)`: it does not accept or consume
  a retained `OnlineEpochScheduleResult`, does not hand off retained Claims,
  and does not create/control an external Harness session. The caller must
  explicitly invoke later epochs. `max_dispatches=0` is an immediate no-work
  return after normal Goal registration/compile-if-missing setup: it does not
  plan or persist an adaptive epoch, create operational ownership, or invoke
  user executor/verifier code. The audit separately records
  `policy_selected_task_ids`, `actual_dispatched_task_ids`,
  `fallback_attempted`, and `fallback_dispatched_task_ids`; the zero-budget
  outcome is `no_work_budget` with only the initial observation phase executed.
  Nonzero execution metadata is conservative: `completed` includes semantic
  commit, `completed_with_failures` omits `commit`, and `no_dispatch` records
  planning/admission without user-code dispatch.
 - **Bounded multi-epoch execution loop (implemented, caller-driven):**
   `AgentOS.execute_online_epochs(...)` invokes the one-shot execution API for a
   caller-specified maximum number of epochs, re-observes the VPG after each
   epoch, and stops on Goal closure, epoch failure, no dispatch, no work budget,
   or the explicit epoch limit. It does not create a daemon, retain Python call
   stacks, consume retained schedule ownership, or manage an external Harness.
 - **Bounded event-driven supervisor (implemented, caller-owned):**
   `EventDrivenSupervisor` (`AgentOS.event_supervisor(...)`) composes explicit
   event submission, optional declared-workspace polling, semantic-interrupt
   validation, RuntimeState re-observation, and one bounded online epoch per
   `step()`/`run()` call. Duplicate event IDs are idempotent only for identical
   fingerprints; stale graph/version, blocked-route, observation, and execution
  failures enter `FAILED_CLOSED`. It has explicit epoch/event budgets and no
  background daemon, implicit retry loop, force-kill, live external-Harness
  rebase, or atomic ownership transaction. The separate main SDK path now
  provides bounded automatic fresh-Attempt Context refresh when an explicit
  manifest and authoritative Facts are available; unsupported or hidden reads
  fail closed. Focused coverage:
   `tests/sdk/test_event_supervisor.py` (8 tests).
   The deterministic public demo is
   `lhos demo online-supervisor --json`; it closes a three-task dependency
   chain through bounded epochs and explicitly reports
   `daemon_started=false` and `uses_llm=false`. It is not an always-on service
   or a performance benchmark.
 - **Ownership handoff intent/recovery (implemented, bounded):**
   `prepare_handoff(...)`, `commit_handoff(...)`, and `recover_handoff(...)`
   durably record an exact source Claim/Attempt and caller-supplied `handoff_id`
   before invoking the existing fenced release-then-acquire path. Same-identity
   replay is idempotent; uncertain `COMMITTING` recovery is `IN_DOUBT`/fail
   closed. This is a recovery witness, not a two-phase transaction across
   Scheduler, Kernel, Harness, and VPG; a future coordinator is still required
   for atomic `REBASE`/`PREEMPT` ownership transfer. Focused coverage:
   `tests/sdk/test_handoff_transaction.py`.
 - **Retained Claim -> Harness handoff (implemented, bounded):**
  `AgentOS.handoff_online_epoch_to_harness(...)` accepts the exact dispatches
  returned by `schedule_online_epoch(..., keep_claims=True)` plus one
  `HarnessSessionAdapter` per Claim. It validates graph/task/Agent/process/
  Claim/Attempt/semantic-epoch/Lease identity before registering sessions and
  supports same-identity replay. It does not execute the Harness, release or
  retarget Claims, rebase Context, or provide an atomic Scheduler/Kernel/
  Harness transaction.
- **Post-admission exact-Claim compensation (implemented, bounded):**
  `SchedulerSession.run_pass(...)` compensates a failed observation/reconcile
  after `schedule_once(...)` using each returned dispatch's
  `expected_claim_id`. A replacement owner cannot be released by this cleanup;
  if cleanup itself fails, the root exception remains primary and receives a
  bounded `add_note(...)` diagnostic with Claim/task identity. This is not an
  atomic cross-service lifecycle transaction.
- **Cancellation cleanup hardening (implemented, bounded):** when
  `run_async()` receives `asyncio.CancelledError` from the worker pool, it
  attempts exact-Claim fenced release for every job, continues after an
  individual release or reconciliation error, logs bounded diagnostics, and
  guardedly attaches them to the original cancellation before re-raising it.
  This preserves replacement Claims and the primary cancellation signal; it is
  cooperative single-host cleanup, not killable process isolation.
- **Durable cleanup markers (implemented, bounded):** if exact Claim cleanup
  cannot be completed, the Scheduler appends an idempotent
  `execution-cleanup.v1` audit marker with a deterministic SHA-256 `marker_id`.
  `cleanup_markers` projects unresolved markers, while
  `reconcile_cleanup_markers()` marks one resolved only after the exact Claim
  is terminal and authoritative lease lookup confirms no lease remains. Active
  or unknown ownership stays pending. Markers are journal-only audit/reconcile
  records; they do not release or retarget Claims and are not an atomic
  cross-service transaction.
- **`AgentSnapshot` (implemented):** Attempt snapshots persist Agent/task,
  graph and semantic epoch, read-set/write-set bindings, `ContextIdentity`,
  resource binding, progress, and computation-cost fields. Snapshot state,
  stale-cognition transitions, and event history survive SQLite close/reopen.
- **Commit-time stale-cognition quarantine (bounded):** the SDK validates the
  Attempt read guard immediately before semantic commit. A changed or
  unavailable binding yields `STALE_COGNITION`/`READ_SET_UNAVAILABLE`; the
  Attempt is quarantined, no new Artifact/Evidence is written, and cleanup
  uses the exact claim fence. `AgentOS.deliver_interrupt(...)` adds a direct
  exact-identity cooperative control path for live async SDK Attempts, while
  the verifier-to-Evidence commit fence rechecks the interrupt before semantic
  publication. This is still not a general watcher-driven rebase loop.
- **`RuntimeStateView` (implemented, read-only):** `AgentOS.runtime_state(goal)`
  projects Progress/Semantic, Agent/Cognition, Context, and logical Resource
  state into an immutable `GlobalRuntimeState`. It observes Scheduler state
  but does not compile goals, claim work, or mutate runtime state. Resource
  values remain logical admission values. Host telemetry affects them only
  after a caller explicitly invokes `apply_host_capacity(...)`; it is never
  treated as physical placement or enforcement.
- **`FrontierPolicy` / `SchedulingEpoch` (implemented, opt-in):** deterministic
  repair-frontier-first WHAT/WHEN suggestions with a bounded parallelism hint.
  The default preserves historical repair-first lexical ordering. Explicit
  `ranking_strategy="graph_utility"` ranks candidates inside the same safe
  frontier by declared critical-path position and immediate downstream unlock
  value; repair priority and all safety filters remain invariant.
  The policy does not call Scheduler, Claim, Lease, or resource admission.
  `AgentOS.run(..., adaptive=True)` and `run_async(..., adaptive=True)` can
  persist bounded `SCHEDULING_EPOCH_PLANNED` journal records; repeated records
  with the same deterministic epoch identity are idempotent, malformed decision
  hashes fail closed, and task-id metadata is bounded.
- **Explicit compute-budget policy (implemented, bounded advisory):**
  `VerifiedProgressBudgetPolicy` and
  `AgentOS.plan_budgeted_frontier(...)` rank the declared graph READY/repair
  frontier by an exact integer expected-verified-progress/cost ratio while
  preserving repair priority. The policy cumulatively enforces caller-declared
  token, time, micro-USD, Context-token, and verification-token ceilings;
  missing or unknown estimates fail closed. It emits an immutable graph-fenced
  proposal and optional bounded epoch audit. The explicit
  `budget_aware=True` option on `AgentOS.run()` / `run_async()` feeds that
  proposal into a bounded adaptive epoch: declared estimates are charged only
  for tasks actually dispatched by the authoritative Scheduler, including
  dispatched failed/stale attempts; graph races with no dispatch consume
  nothing. The default `adaptive=False` path is unchanged, and v1 rejects
  composition with resource-aware/conflict-graph/automatic-rebase options.
  `VerifiedProgressBudgetPlan.remaining_before/after` exposes
  `ComputeBudgetRemaining` values where `None` is unbounded and `0` is a
  bounded exhausted dimension. Admission may be partial; deferred tasks retain
  explicit budget blockers, and only actually dispatched IDs are charged.
  This path does not predict or measure real provider costs, reserve physical
  resources, enforce quotas, or replace Claims/Leases. See
  [`COMPUTE-BUDGET.md`](COMPUTE-BUDGET.md).
- **Attempt usage accounting (implemented, bounded, in-memory):**
  `UsageLedger` records immutable estimated, reserved, measured, and terminal
  outcome vectors keyed by Goal/Task/Attempt identity. Terminal transitions
  require caller-supplied authoritative measured usage; estimates and
  reservations are never silently treated as measurements. The ledger is
  **not durable** and is not restored by Scheduler/VPG replay; it is not a
  provider billing authority, quota manager, or process monitor.
- **`ConflictGraph` / `DynamicParallelismPolicy` (implemented, opt-in):**
   deterministic greedy batches based only on explicit task read/write
   declarations. Missing or unknown access is serial-only and fail-closed.
   `AgentOS.run(..., adaptive=True)` and `run_async(..., adaptive=True)` can
   pass the selected task ids to the existing Scheduler as an advisory
   `allowed_task_ids` filter; the default `adaptive=False` path is unchanged.
   With the additional explicit `resource_aware=True` option, the same main
   path also fits declared CPU/RAM/GPU/VRAM/model-slot requests to the current
   logical pool projection before authoritative Scheduler admission. This is
   not automatic provenance discovery, physical placement/enforcement, or a
   Claim/Lease bypass.
- **Resource-aware run audit and replanning (implemented, bounded):**
   `resource-aware-run-audit.v1` exposes bounded assignments, decision reasons,
   blockers, pool IDs, safety flags, and unavailable summaries in
   `RunResult.meta` without copying prompt/Context payloads. The
   `RESOURCE-REPLANNING-E2E` example runs two caller-owned bounded
   `run_async` invocations, explicitly lowers one logical pool, and verifies
   that the next READY frontier is replanned from two tasks per epoch to one.
- **`SemanticInterruptPolicy` (implemented):** immutable, deterministic routing
  proposals for `CONTINUE`, `DEFER`, `PREEMPT`, `REBASE`, and `REVERIFY`.
  `AgentOS.plan_interrupts(..., persist=True)` can append a bounded
  `SEMANTIC_INTERRUPT_PROPOSED` journal event for audit and replay. The
  `AsyncWorkerPool` and `AgentOS.deliver_interrupt(...)` support exact-claim
  cooperative delivery to running token-aware async dispatchers, with durable-capable
  `REQUESTED`/`DELIVERED`/`OBSERVED`/`CANCELLED` transition callbacks and
  quarantine of completions that ignore a requested interrupt. The SDK
  verifier-to-Evidence commit fence also rejects a late interrupt completion.
    This remains an in-process cooperative boundary: it does not force-kill
    callbacks, provide universal world observation, or perform automatic
    Context rebase/Lease handoff.
- **Explicit workspace watcher (implemented, bounded):**
  `WorkspaceObservationWatcher` / `AgentOS.workspace_watcher(...)` polls only
  caller-declared `WorkspaceTool` resources, compares exact SHA-256 bytes,
  issues graph-bound observation tokens for changed files, and emits
  `ARTIFACT_CHANGED` interrupts for the semantic-interrupt policy. In
  `poll_and_reconcile`, all assigned changes from one poll are validated first
  and committed through one atomic batched VPG/D3 refresh; failed batches keep
  only the submitted resources retry-pending. Deletions are reported without
  inventing a version; unassigned resources remain unhandled rather than
  broadening repair. This does not intercept arbitrary Python, network,
  browser, tool, or subprocess I/O and does not mutate Claims/Leases.
  Observation identity is durable and content/version/graph bound, so retry
  after a lost post-commit response reuses the same token instead of issuing
  another semantic transition. `AgentOS.poll_workspace_and_route(...)` and
  `route_workspace_observation(...)` provide a caller-invoked one-shot route
  from a supplied/polled observation through interrupt policy to exact Attempt
  fencing and cooperative `REBASE`/`PREEMPT`; supplied interrupts are
  revalidated and rejected delivery statuses remain blocked in the audit.
  The original watcher route/rejection hardening subset passes **25 tests**;
  the caller-owned `WorkspaceWatchLoop` adds 5 tests, for **30 tests**
  across the watcher-loop slice. The related observation-token/authority/
  watcher integration gate passes **44 tests** in the current checkout.
  The loop is explicitly bounded and caller-owned: it is polling, not a
  daemon, universal watcher, or distributed service.
- **Harness control bridge (implemented, bounded):** `CallableHarnessAdapter`
  exposes a narrow OS-to-Harness session protocol (`START`, `CONTINUE`,
  `CHECKPOINT`, `REBASE`, and cooperative `PREEMPT` when explicitly
  supported). `AgentOS.register_harness()` and
  `AgentOS.control_harness()` require an exact live Claim/Attempt binding:
  graph, task, Agent, Claim, Attempt, graph version, and semantic epoch must
  match before a request reaches Harness code. Requests are revision- and
  request-idempotent and successful transitions append a bounded
  `HARNESS_CONTROL` Scheduler journal event. File-backed `AgentOS` can replay a
  bounded logical session snapshot and request-idempotency index from the
  hash-verified journal when a replacement adapter presents the same complete
  session identity. Replay validates schema, identity, revision continuity,
  event identity, fingerprints, and duplicate requests. It does **not** restore
  callback/model memory, prompts, outputs/details, Python stacks, checkpoints
  as executable state, or in-flight code; it does not create/renew/release or
  transfer Claims/Leases, mutate ownership projections, publish VPG/Evidence,
  force-kill callbacks, or orchestrate arbitrary third-party Harness
  processes. The optional online-control Harness dispatcher now validates
  graph id/version, task, Agent, Claim, Attempt, and semantic epoch before a
  request reaches Harness code. Ownerless or ambiguous `START` actions fail
  closed; Scheduler/AgentOS must first create the Claim/Attempt and register
  the matching session. `REBASE` requires the current epoch, rejects graph
  rollback, and advances the target epoch. None of these checks makes
  Claim/Lease handoff atomic.

- **Claim/Lease handoff (implemented, bounded):** `handoff_task(...)` validates
  exact source Claim/Attempt/semantic epoch identity, fences the source owner,
  admits a replacement, and persists an idempotent `handoff_id`. Eight focused
  tests cover refusal, stale-release fencing, cleanup, and restart replay. The
  operation is release-then-acquire (not a cross-Scheduler/Kernel atomic
  transaction), does not automatically invoke/rebase a Harness, and may fail
  closed with no owner when replacement admission is refused.
- **`ComputeRoutingPolicy` (implemented, bounded advisory):** explicit
  version-pinned bindings can be compared to observed Agent snapshots to
  compute fail-closed context overlap/locality, an
  `REUSE_AGENT`/`FRESH_AGENT` recommendation, a bounded context-token budget,
  provider-independent model tier, and verification strength. The public
  `AgentOS.plan_compute_routing(...)` facade and module-level SDK symbols are
  read-only policy surfaces. They do not start/reuse processes, create Claims
  or Leases, or mutate the Scheduler. An explicit
  `ComputeProviderRegistry` can additionally invoke registered model,
  verifier, and Context-adapter hooks after Claim/Context setup when
  `adaptive=True` and task metadata opts in; this is an execution adapter,
  not automatic provider selection or physical resource routing.
- **Adaptive compute-routing audit (implemented, bounded):** when
  `adaptive=True` and a task explicitly supplies
  `metadata["compute_routing"]`, each epoch records a bounded, redacted
   decision summary under `result.meta["adaptive_epochs"]`. Malformed metadata
   or a graph-version race becomes an audit status; it does not change the
   selected batch, Claim/Lease, or physical resource placement. Explicit
   provider-routing metadata may invoke the registered adapter described above.
 - **Adaptive-vs-static online-compute benchmark (implemented, offline):**
   `lhos benchmark online-compute` compares the same graph under static and
   adaptive policies and reports tokens, simulated latency/cost, stale and
   re-executed work, verified-progress traces, and epoch parallelism. The
   `SimulatedProvider` profile and CLI multipliers vary accounting deterministically
   without invoking a real model or provider. The benchmark is a metric-plumbing
   and policy-contrast gate, not evidence of LLM/GPU/provider throughput.
   `run_multi_seed_benchmark(seeds=(...))` reuses this deterministic
   single-seed contract and returns per-seed reports plus mean/min/max
   summaries. The canonical scenario is seed-invariant and carries seed as
   audit metadata; actual seed-dependent workload variation must be generated
   by the caller. This remains a reproducibility/aggregation API, not a
    statistically powered real-model evaluation.
- **Resource-aware runtime benchmark (implemented, deterministic):**
  `lhos benchmark resource-aware-runtime --json` runs the same four-task Goal
  through the public `run_async` -> Scheduler -> Claim -> Kernel Lease ->
  verifier -> VPG Evidence path. With a 1,000-millicore logical pool, the
  conflict-only baseline requires three epochs and one Scheduler resource
  rejection; resource-aware packing closes the same four-task VERIFIED Goal in
  two epochs with zero proposal-capacity violations and zero resource
  rejections. Both paths have zero admitted/executor capacity violations. This
  is a synthetic logical-resource regression, not a wall-clock, real-LLM, or
  physical CPU/GPU performance claim. See
  [`RESOURCE-AWARE-RUNTIME-BENCHMARK.md`](RESOURCE-AWARE-RUNTIME-BENCHMARK.md)
  and the bounded
  [`RESOURCE-REPLANNING-E2E.md`](RESOURCE-REPLANNING-E2E.md).
- **Real local wall-clock adaptive-runtime gate (implemented, bounded):**
  `lhos benchmark wallclock-adaptive-runtime --json` executes actual
  `asyncio.sleep` work through the public `run_async` authority path. The stable
  correctness/resource result is **3 -> 2 epochs** and **1 -> 0 Scheduler
  resource rejections** for the same VERIFIED Goal. Local elapsed time is
  informational and is not a pass/fail, real-model, GPU, or production
  throughput claim.

## What remains partial or open

| Area | Current boundary |
|---|---|
| Adaptive policy integration | `adaptive=True` is an opt-in bounded integration: each epoch observes `RuntimeStateView`, derives a deterministic frontier/conflict-aware batch, and passes `allowed_task_ids` to the existing Scheduler. `resource_aware=True` additionally fits explicit task vectors to current logical pool capacity; Scheduler admission remains authoritative. The standalone `EpochController` is default-off/read-only. The explicit `OnlineComputationController` may hand proposals to an injected dispatcher but never claims or leases work itself; there is no always-on autonomous controller. `adaptive=False` remains unchanged. |
| Scheduler-backed online epoch bridge | `AgentOS.schedule_online_epoch(...)` implements one bounded policy-planning -> authoritative Scheduler-admission -> explicit-cleanup path. `plan_only=True` creates no ownership; an admitted epoch validates exact Claim/Attempt/Lease identities and either auto-releases them or retains them for normal execution/`release_online_epoch(...)`. It never invokes a Harness, executor, verifier, or semantic commit. |
| One-shot and bounded multi-epoch execution | `AgentOS.execute_online_epoch(...)` executes one bounded adaptive epoch through the existing `run_async` Scheduler/Claim/Lease/executor/verifier/VPG lifecycle and returns `RunResult`. `AgentOS.execute_online_epochs(...)` repeats that slice for an explicit caller budget and re-observes VPG between epochs, stopping conservatively on closure/failure/no-dispatch/no-budget/limit. Neither API consumes retained Claims or manages an external Harness; neither is always-on. `max_dispatches=0` performs Goal setup/compile-if-missing plus initial result observation, then returns `no_work_budget` without adaptive planning/persistence, ownership, or user code. |
| Resource-aware execution/replanning | Implemented, explicit/bounded | `resource_aware=True` fits declared vectors to logical capacity on the main `run()`/`run_async()` path; `resource_audit` retains bounded reasons. `examples/resource_replanning_e2e.py` proves caller-owned capacity re-observation changes the next batch while Claims, Leases, Attempts, Evidence, and Goal closure remain on the authoritative path. No daemon or physical placement is implied. |
| Post-admission Scheduler compensation | Implemented, bounded | `SchedulerSession.run_pass(...)` releases only exact Claims created by the failed pass when post-admission observation/reconciliation raises; replacement Claims are protected by `expected_claim_id`. Cleanup failures are appended to the original exception via bounded `add_note(...)`. Unresolved cleanup can also be recorded as an idempotent `execution-cleanup.v1` marker and later reconciled only after terminal Claim plus authoritative no-live-lease checks; this is not an atomic cross-service transaction. |
| Conflict-graph completeness | Conflict edges are derived only from explicit read/write declarations. Logical resource fitting is implemented for explicit requests and capacities, but hidden accesses, semantic conflicts, undeclared requests, physical placement, and automatic graph maintenance remain open/fail-closed. |
| Semantic interrupt/preemption | The policy, explicit `WorkspaceObservationWatcher`, one-shot `poll_workspace_and_route(...)`/`route_workspace_observation(...)` façade, direct `AgentOS.deliver_interrupt(...)` path, worker-pool cooperative delivery, verifier commit fence, and bounded Harness control bridge are implemented for explicit observations and opt-in adapters. Supplied interrupts are revalidated; accepted deliveries are distinguished from rejected statuses, which remain blocked. Delivery/control is exact-claim and distinguishes requested/delivered/observed where supported; ignored-token completions and late verifier results are quarantined. Bounded live external-Harness `REBASE`/`FULL_RELOAD` now persists a durable handoff, fences/releases the old ownership, admits a fresh Attempt, and detaches the old Harness; the transfer remains release-then-acquire rather than atomic and requires caller registration or recovery of the replacement Harness. There is no universal watcher, force-kill, automatic controller, or distributed preemption. |
| Context delta/rebase planner | Pure explicit graph-delta classification plus bounded live application are implemented. Graph-version advance with partial/unknown coverage fails closed. `run()`/`run_async()` can perform one bounded automatic fresh-Attempt Context refresh when a complete explicit `ContextManifest` and authoritative Facts are available. Live `REBASE`/`FULL_RELOAD` now persist a durable handoff, admit a fresh Attempt, detach the old Harness, and support same-plan replay; the path remains release-then-acquire/non-atomic and requires caller registration of the replacement Harness or `recover_handoff(...)`. Hidden/unversioned reads block. |
| Harness adaptive benchmark | Implemented, bounded, reproducible | `benchmark harness-adaptive` runs static and conflict-aware adaptive policies through the real SDK Scheduler/Claim/Attempt/Lease/Harness/verifier/VPG path. The built-in provider uses synthetic usage; an explicit provider plugin may report observed usage. It is not a real-model/GPU quality or production-economics benchmark. |
| Cognitive locality and routing | A deterministic, fail-closed advisory policy and an explicit opt-in `ComputeProviderRegistry` execution adapter are implemented. The adapter can call registered model/verifier/context hooks after Claim/Context setup, but it does not select providers automatically, manage warm processes, mutate live Context, route physical resources, or provide an economic feedback loop. |
| Automatic provenance discovery | Context VM page bindings, the mediated workspace gateway, and the explicit polling `WorkspaceObservationWatcher` record exact observations on their declared boundaries. Hidden file/API/tool/Python reads outside those boundaries are not discovered automatically; the watcher never claims to be a universal source observer. |
| Observation authority | Graph-bound observation tokens and workspace version validation are implemented/tested. Strict workspace claims require `version_validator` or `version_authority`; the integer `AgentOS.repair(..., new_artifact_version=...)` compatibility path remains unsafe/deprecated, and workspace/Facts updates are not one cross-system transaction. |
| Causal proof completeness | Main SDK Evidence carries and checks claim/attempt/epoch, lease generation, and provenance digest; malformed bindings fail closed. Adversarial cross-worker, stale-epoch, superseded-lease, malformed-fence, and duplicate-evidence tests pass. Legacy adapters may omit optional fields, and external effect identity is still not universal. |
| External side effects | The opt-in injected `ActionGateway` records explicit effect declarations and writes. Malformed, mismatched, or uncertain receipts are recorded as an unknown/uncertain write and cannot authorize semantic closure. A zero-delay Outbox retry now remains immediately eligible at an explicitly supplied logical timestamp after async publisher failure; positive backoff remains completion-relative. The gateway does not intercept arbitrary I/O; Outbox delivery remains at-least-once, and arbitrary mail, payment, deployment, and remote API effects are not universal exactly-once. |
| Effect-contract admission | `secure_mode` enforces explicit effect-contract fields at the Kernel `SubmitAction` syscall boundary and rejects missing declarations before capability/action/lease admission. The injected `ActionGateway` additionally checks idempotency and receipt identity. Kernel-wide interception of every driver and raw callback is not implemented; compatibility mode still defaults direct `SubmitAction` to `PURE + RETRY`, so a misclassified custom driver can be retried. |
| Callback isolation/cancellation | Raw Python callbacks run in the host process and remain non-killable. `run_async()` now performs best-effort exact-Claim cleanup on `CancelledError`, continues cleanup after per-Claim/reconcile errors, and preserves the primary cancellation with bounded diagnostics; unresolved failures can be left as durable audit markers. This is not process isolation or distributed cancellation. |
| Harness integration | A bounded in-process `AgentOS` control bridge is implemented and tested: exact Claim/Attempt/session identity fencing, revision/request idempotency, and journal-only audit events. `handoff_online_epoch_to_harness(...)` binds retained online-epoch dispatches to exact Harness adapters with same-identity replay, but does not execute or atomically transfer ownership. Online-control actions additionally validate graph/task/Agent/Claim/Attempt/epoch before entering the Harness; ownerless `START` fails closed. Callback/model memory, outputs/details, executable checkpoints, force-kill, and third-party process control remain open. |
| Resource scheduling | CPU/RAM/GPU/VRAM/model-slot values remain logical per-Agent admission reservations. `resource_aware=True` fits declared task vectors before authoritative Scheduler admission. The optional telemetry adapter observes host resources, and the explicit `apply_host_capacity(...)` bridge can update exactly one named logical pool with reserve fractions and active-reservation fencing. There is no automatic polling, shared host/device inventory, physical placement, isolation, quota enforcement, fairness, or multi-host authority. |
| Durability scope | Scheduler/VPG metadata is recoverable and Scheduler reopen now validates event/snapshot integrity plus Claim/Attempt structural invariants. Large Scheduler projections use normalized row storage with a small manifest and changed-row writes. Generation + state-hash + event-tail CAS protects stale writers sharing one SQLite file; the supported model remains one Scheduler writer, with no leader election or distributed/multi-writer coordination. Each update still computes an O(N) in-memory projection hash and does not recover arbitrary Python memory, call stacks, or in-flight code. |
| Reconciliation/liveness | Scheduler reconciliation, TTL reclaim, cooperative attempt-scoped heartbeat/lease renewal, and token-aware interrupt delivery are implemented. The optional heartbeat loop is default-off and fails closed without a usable hook; interrupt cancellation remains cooperative and cannot forcibly terminate a running callback. |
| Process terminal/cleanup atomicity | A bounded terminal-PID fence is implemented: terminal state is published before cleanup and acquisition rejects an existing terminal PID inside the writer transaction. Cleanup plus the terminal transition still span services/transactions; no general atomic lifecycle transaction or ownership handoff is claimed. |
| Repair granularity | Current selective repair is mainly task-level. Multi-output and Artifact/Evidence-field selective repair are future work. |
| Semantic pruning | A changed input is conservatively repaired; no independently verified equivalence proof is available. |
| Distributed runtime | No multi-host scheduler, leader election, multi-writer CAS, or cluster inventory. |
| Performance | VPG history storage is incremental and a commit-local serialization cache reduces duplicate JSON work. The N=400 recorded history workload is about 1.64 MB versus the former ~37.9 MB full-copy layout. Scheduler `normalized-v1` projections keep a small manifest and write only changed rows; full projection derivation/validation/hash remains O(N) and affects commit latency. p99 is host/workload sensitive and occasionally approaches or exceeds 50 ms, so no stable 50 ms/10 ms SLO is claimed. |
| Belief revision/planning | Contradiction solving, general belief revision, and always-on autonomous replanning are not implemented. |

## Focused verification snapshot

The following commands were run with `PYTHONPATH=src`:

```text
pytest -q tests/sdk/test_sdk.py tests/sdk/test_execution_closure.py \
  tests/sdk/test_async_run.py tests/sdk/test_large_goal_atomic_compile.py \
  tests/sdk/test_provenance_integration.py \
  tests/sdk/integrations/test_observation_authority.py
65 passed

pytest -q tests/sdk/integrations/test_observation_tokens.py \
  tests/sdk/integrations/test_observation_authority.py \
  tests/sdk/integrations/test_workspace_watcher.py
35 passed

pytest -q tests/runtimes/multi_agent/test_causal_binding.py \
  tests/runtimes/verified_progress/test_event_causality.py \
  tests/sdk/integrations/test_lease_fenced_vpg.py \
  tests/sdk/integrations/test_crash_boundaries.py
16 passed

pytest -q tests/sdk/test_harness_control_bridge.py
4 passed

pytest -q tests/unit/test_provenance.py tests/unit/test_invalidation.py \
  tests/demo/test_provenance_repair.py \
  tests/runtimes/verified_progress/test_d3_durable_integration.py \
  tests/runtimes/verified_progress/test_graph_store.py \
  tests/runtimes/multi_agent/test_completion.py \
  tests/runtimes/multi_agent/test_durable_scheduler_state.py \
  tests/runtimes/multi_agent/test_scheduler_resources.py
49 passed

pytest -q tests/runtimes/multi_agent/test_durable_scheduler_state.py \
  tests/runtimes/multi_agent/test_scheduler_resources.py
37 passed in the current durable/resources/completion gate

pytest -q tests/sdk/test_semantic_interrupt_runtime.py
6 passed

pytest -q tests/sdk/integrations/test_workspace_watcher.py
25 passed (post-P1 watcher route/rejection hardening)

pytest -q tests/agent_os/test_journal.py \
  tests/agent_os/test_audit_journal_atomicity.py \
  tests/agent_os/test_audit_journal_rebuild.py \
  tests/agent_os/test_sqlite_storage_isolation.py \
  tests/agent_os/test_sqlite_storage_migrations.py
25 passed

Journal/Lease combined focused gate:
56 passed (Journal 25 + Lease 31)

Independent August 14, 2026 verification snapshot (the current checkout,
including the bounded online-compute policy primitives and hidden-read
regressions):

```text
tests/agent_os                         1413 passed (prior snapshot)
tests/sdk                               206 passed
tests/runtimes/multi_agent              359 passed
durable/resources/completion gate        37 passed
strict/recovery/migration gate           20 passed
effect-gateway plus strict/recovery/migration  41 passed
```

These are separate local gates and should not be summed. The post-batch non-slow run on August 14, 2026 completed with
`3092 passed, 1 skipped, 18 deselected, 30 warnings` in `303.33s`; the complete
log is `artifacts/full-test-nonslow-post-batch-20260814.log`. This is a
historical earlier run; the final late-session regression is recorded below.

pytest -q tests/sdk/test_epoch_controller.py
10 passed

pytest -q tests/runtimes/multi_agent/test_handoff.py
8 passed

pytest -q tests/sdk/test_context_delta.py
17 passed

pytest -q tests/sdk/integrations/test_http_sdk_boundary.py
11 passed

pytest -q tests/sdk/integrations/test_provenance_http_gateway.py tests/sdk/integrations/test_http_sdk_boundary.py
20 passed

Related HTTP/authority integration gate: 29 passed

python -m compileall -q src/lhos
pass

python -m ruff check src/lhos tests/sdk tests/unit tests/demo \
  tests/runtimes/verified_progress tests/runtimes/multi_agent
All checks passed

pytest -q tests/sdk/test_provider_routing.py
7 passed

pytest -q tests/benchmarks/test_provider_routing.py
8 passed
```

Historical late-session gates reported on August 15, 2026:

```text
SDK online-compute/freshness package gate
485 passed

Combined computation-control, freshness, Harness-dispatch, CLI, and
computation-utility gate
65 passed

pytest -q tests/sdk/test_harness_dispatcher_fencing.py
9 passed

pytest -q tests/sdk/test_online_epoch_execution.py
6 passed

Historical full non-slow repository regression
3142 passed, 1 skipped, 18 deselected, 30 warnings in 532.37s
```

The historical log is
`artifacts/full-test-nonslow-online-epoch-hardening-20260815.log`.
It predates the one-shot execution, post-admission compensation, and
cancellation hardening. The cleanup-marker wiring baseline then completed with
**3166 passed, 1 skipped, 18 deselected, 30 warnings in 458.98s**; log:
`artifacts/full-test-nonslow-cleanup-marker-wiring-20260815.log`.
  The current post-compute-budget repository evidence is:
  **3411 passed, 1 skipped, 18 deselected, 30 warnings in 517.71s** for the
  non-slow suite; log:
  `artifacts/full-test-nonslow-compute-budget-20260816.log`.
  The separate slow marker gate completed with
  **18 passed, 3412 deselected in 1253.07s**; log:
  `artifacts/slow-tests-compute-budget-20260816.log`.
  Ruff format reported **636 files already formatted**; Ruff lint, Mypy over
  **276 source files**, and `compileall` passed.
  Rebuilt artifacts in `dist-final-20260816-compute-budget` passed
  `twine check`. A fresh-venv installation passed public SDK imports,
  `compute-budget` with no violations, `recovery-repair` with final closure,
  and a real `budget_aware=True` execution smoke. The wheel SHA-256 is
  `2FDBC5D5B78226AC8A83F5CD3F6B54941F57B07974CF6E21105B31CAE80AC145`;
  the source-distribution SHA-256 is
  `AAB8E1EC91D6616E63AC78467F2CF5BAE88318656488FF8E5A039C3238C76565`.
  This evidence includes the opt-in graph-utility frontier strategy, real local
  wall-clock adaptive-runtime gate, zero-delay Outbox retry correction, and
  the opt-in verified-progress compute-budget execution/accounting path.
  The earlier post-format **3341** result and the **3341 passed in 472.28s**
  result are historical evidence only:
  `artifacts/full-test-nonslow-20260816-post-format.log` and
  `artifacts/full-test-nonslow-20260816-final-after-outbox.log`.
  The earlier **3321** workspace/watch-loop/live-handoff run remains a
  historical baseline:
  `artifacts/full-test-nonslow-after-live-rebase-watchloop-20260815.log`.
  The earlier **3307** resource-aware run remains a historical baseline:
  `artifacts/full-test-nonslow-resource-aware-final-20260815.log`.
The earlier **3257** result remains the pre-resource-bridge baseline:
`artifacts/final-test-nonslow-20260815-final-sync.log`.
The `3201` live-rebase run remains a historical baseline:
`artifacts/final-test-nonslow-20260815-live-rebase.log`.
The `3187` final-serial run and `3181` watcher run remain historical baselines:
`artifacts/final-test-nonslow-20260815-serial.log` and
`artifacts/full-test-nonslow-p1-watcher-loop-20260815.log`.
Focused post-change evidence: event supervisor **8 passed**, one-shot execution
**6 passed**, cleanup
**3 passed**, async cancellation cleanup **3 passed**, retained-Harness handoff
**4 passed**, bounded multi-epoch loop **8 passed**, and watcher route/rejection
hardening **25 passed**. These gates overlap and are not additive.

The host-resource telemetry, explicit capacity bridge, resource-aware policy,
main-path integration, Scheduler-resource, benchmark, and CLI gate passed:

```text
pytest -q tests/sdk/test_host_capacity.py \
  tests/sdk/test_resource_telemetry.py \
  tests/sdk/test_resource_policy.py \
  tests/sdk/test_resource_aware_run.py \
  tests/sdk/test_resource_configuration.py \
  tests/sdk/test_runtime_state.py \
  tests/runtimes/multi_agent/test_scheduler_resources.py \
  tests/benchmarks/test_resource_aware_runtime.py \
  tests/cli/test_resource_aware_runtime.py
76 passed
```

This verifies telemetry failures, reserve mapping, single-pool atomic logical
capacity updates, active-reservation rejection, conflict/resource-aware
selection, authoritative Scheduler revalidation, exact Claim/Lease/Evidence
execution, benchmark repeatability, and CLI JSON. It does not test physical
placement, isolation, real GPU utilization, or real-model throughput.

The repository's configured gates were reproduced locally after formatting:
Ruff format reports **626 files already formatted**; Ruff lint, Mypy,
`compileall`, wheel build/install, and fresh-environment CLI smoke pass. The
full non-slow and separate slow marker suites are recorded above. These local
results do not establish that GitHub-hosted Actions have actually executed
successfully, and they are not evidence for real-model, GPU, or distributed
workloads.

## Honest release claim

> LongHorizonOS is a single-host runtime for evidence-backed,
> graph-relative selective repair with Kernel-fenced execution ownership.

It is not yet a general-purpose Agent OS, an automatic provenance oracle, a
physical GPU scheduler, or an exactly-once wrapper for arbitrary external
side effects.

The compatibility default for direct Kernel Actions remains `PURE + RETRY`.
Callers integrating a custom driver must classify its side effects explicitly.
`secure_mode` now rejects missing syscall-level effect declarations, but it
does not sandbox or intercept the driver's actual I/O; a driver can still be
misclassified unless all effects use the mediated gateway.



