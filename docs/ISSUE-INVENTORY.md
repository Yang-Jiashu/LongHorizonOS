# LongHorizonOS unresolved-issues inventory

**Review basis:** local source checkout inspected through the final documented
slice on August 16, 2026 (CST), including the bounded event-driven supervisor,
ownership-handoff intent/recovery, provider-profile benchmark, retained-Harness
handoff, workspace route façade, bounded multi-epoch execution, explicit
host-capacity bridge, resource-aware main execution path and replanning,
mediated workspace commit validation, caller-owned workspace polling, live
fresh-Attempt handoff, opt-in graph-utility ranking, local wall-clock adaptive
gate, and zero-delay Outbox retry correction.  
**Scope:** this document records what the current `v0.1.x` prototype does and
does not guarantee. It is intentionally conservative: an item is not marked
resolved merely because a related primitive exists somewhere in the tree.
Implementation status was rechecked after the SDK execution-path, large-goal
compilation, Action recovery, receipt fail-closed, batched observation,
durable observation-token idempotency, Journal cross-connection
append/rebuild, freshness fail-closed, online-control, and Harness-dispatch
fencing fixes. The latest from-zero non-slow run is complete; see the exact
result below. Earlier post-P1, watcher, handoff, and resource-aware runs are
retained only as historical or focused evidence. See
[`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) for the exact focused
test commands and boundaries.

## Executive summary

The current system is a useful **single-host, graph-relative semantic repair
runtime**:

> Given a correctly declared dependency graph and authoritative observations of
> changed artifacts, it derives evidence applicability, propagates `STALE`
> status, computes a repair frontier, and fences execution ownership.

The following harder claims remain unimplemented or only partially implemented:
automatic provenance discovery, complete observation of the outside world,
cross-plane transactions for real side effects, sandboxed/cancellable arbitrary
Python execution, distributed scheduling, and physical device enforcement.
The current SDK does have an explicit provenance/coverage adapter and
claim/attempt/epoch checks; these are bounded integration primitives, not a
complete provenance gateway or a general reliability guarantee.

## Priority definitions

- **P0 — correctness or safety:** a wrong result can leave false `VERIFIED`
  state, duplicate work, stale ownership, or an unsafe side effect.
- **P1 — research/product capability:** the prototype remains useful, but the
  missing capability limits generality, performance, or a publishable systems
  claim.
- **P2 — scale/productization:** important for a later release, not required to
  make the current narrow contract honest.

## P0: close the correctness boundary first

| ID | Current status | Why it matters | Current code surface | Required fix and acceptance test |
|---|---|---|---|---|
| P0-1 Provenance coverage | **Partial (explicit/mediated boundaries).** `Task`/`Goal` support declared inputs and `context_v1`; valid Context VM `page_bindings` are automatically recorded as exact `source="context_vm"` reads, and `WorkspaceProvenanceGateway` records exact-byte workspace reads/writes. Legacy callbacks remain `UNKNOWN`; raw callbacks and hidden file/API/tool/Python reads outside mediated boundaries can bypass the recorder. | A hidden read creates an omitted edge. The invalidation algorithm can be deterministic and still under-invalidate, leaving a false `VERIFIED` result. | `src/lhos/provenance/*`, `src/lhos/agent_os/context/*`, `src/lhos/integrations/tools/provenance_workspace.py`, `src/lhos/sdk/os.py` | Extend the capability-scoped gateway to every relevant Artifact/Workspace/HTTP/Tool boundary; persist read-set/write-set, content hash and observation epoch; expose `COMPLETE/PARTIAL/UNKNOWN` coverage in durable run results. Add hidden-read, undeclared-file, and undeclared-API mutation tests. |
| P0-2 Observation authority | **Partial → safe token and mediated workspace validation implemented.** `FactsProvider` rejects invalid/rollback versions and same-version hash changes; graph-bound `ObservationToken` is consumed by `repair(goal, observation=token)`. Token identity is durable and derived from canonical artifact/version/hash/graph identity, so a retry after a lost post-commit response reuses the same token. The explicit workspace watcher validates every assigned transition in one poll before one atomic batched VPG/D3 refresh. Strict `WorkspaceProvenanceGateway` version claims require `version_validator(snapshot)` or Facts-like `version_authority.read_hash(pid, uri, version)` and compare the returned hash to observed bytes. Before semantic commit, the SDK also revalidates mediated workspace bytes with a bounded `validate_read_set_current()` check; changed/deleted/unavailable/truncated observations fail closed (47 focused tests). The integer compatibility path can still synthesize `body-vN` and is explicitly unsafe/deprecated; workspace and Facts updates are not one cross-system transaction. | A number can look like a version without being a trusted snapshot of bytes. Equal versions with different bytes, rollback, deletion, or an unobserved external change can invalidate the proof model. | `src/lhos/sdk/observation.py`; `src/lhos/sdk/os.py::{observe_artifact,observe_workspace_artifact,repair,reconcile_observations}`; `src/lhos/sdk/providers.py::FactsProvider`; `src/lhos/sdk/watchers.py`; `src/lhos/integrations/tools/provenance_workspace.py` | Make token issuance/authority-backed validation the default public path and add HTTP/Tool watcher validators plus a cross-system commit protocol. Keep testing tamper, delete, rollback, same-version/different-content, missed watcher events, response loss, aliases, and concurrent duplicate delivery. |
| P0-3 Real side effects bypass the Kernel Action boundary | **Partial primitive; system guarantee open.** The injected `ActionGateway` accepts explicit effect declarations and records malformed, mismatched, or uncertain receipts as unknown/uncertain writes. `secure_mode` rejects legacy callbacks and requires `context_v1`, but arbitrary Python, file, browser, network, subprocess, and SDK I/O can still bypass the boundary. Kernel `SubmitAction` remains a compatibility path with default `PURE + RETRY`; a first dispatch exception gets at most one fenced retry, so a misclassified custom driver can still duplicate an effect. The Outbox now preserves immediate eligibility for a zero-delay retry at an explicit logical timestamp after async publisher failure; this is a deterministic retry fix, not exactly-once delivery. | Kernel fencing protects the recorded Action, not an arbitrary effect that happened before/behind it. A retry after a crash can duplicate an irreversible effect, and a receipt is not proof of exactly-once delivery. | `src/lhos/provenance/recorder.py`; `src/lhos/sdk/provenance.py`; `src/lhos/sdk/os.py`; `src/lhos/agent_os/kernel/kernel.py`; `src/lhos/agent_os/services/action_service.py`; `src/lhos/agent_os/services/outbox.py` | Make strict effect-contract admission apply to every driver and raw callback, require sink-consumed idempotency/CAS or fencing tokens, and route every external effect through a real Action/ToolGateway. Keep malformed/unknown outcomes `UNCERTAIN`; inject crashes before/after dispatch and before/after acknowledgement. |
| P0-4 Callback capability, isolation, and cancellation | **Partial**. `AsyncWorkerPool` now delivers an exact-claim cooperative token to dispatchers that explicitly accept `cancellation_token=`. Token observation emits a bounded transition and an ignored-token completion is quarantined before operational success. Raw/legacy callbacks still run in the host process; cancelling an await or requesting an interrupt cannot stop arbitrary threads or code. | A callback can bypass capabilities or continue mutating state after ownership is lost. This is both a security and a stale-write risk. | `src/lhos/runtimes/multi_agent/worker_pool.py`; `src/lhos/sdk/os.py::_invoke_executor_*`, `_invoke_verifier_*`, `_SDKExecutorDispatcher` | Keep cooperative delivery fail-closed and require secure-mode callbacks to use mediated capabilities. Add process/container isolation or killable workers, plus lease-loss and post-cancel side-effect tests for every public execution adapter. |
| P0-5 Claim/attempt/evidence causal binding | **Implemented on the built-in SDK path; bounded for legacy adapters.** Evidence carries `claim_id`, `attempt_id`, `semantic_epoch`, `lease_fencing_token`, and (when a coverage report exists) `provenance_digest`. The SDK validates the provenance digest before commit, and Scheduler only completes a matching active attempt/lease generation. Malformed or superseded lease/provenance bindings fail closed. Adversarial tests cover cross-worker identity, stale epoch, superseded lease generation, malformed fencing data, and duplicate evidence. Legacy/core graph authors may still omit optional fields, and external effect identity is not universal. | Evidence from another worker or an earlier epoch can complete the wrong ownership record if an integration path bypasses the built-in gate. | `src/lhos/runtimes/multi_agent/scheduler.py::{observe_vpg,_evidence_matches_active_attempt}`, `src/lhos/sdk/os.py::{_commit_verified_outcome,_attach_evidence}`, `src/lhos/runtimes/verified_progress/{models,patch_validator}.py`, `tests/runtimes/multi_agent/test_causal_binding.py` | Keep the built-in gate fail-closed; require the full binding for secure mode and add side-effect sink identity/crash-boundary tests. |
| P0-6 Large Goal compilation | **Implemented for the trusted SDK Goal compiler (bounded)**. `_submit_compiled_ops()` uses the private large-operation admission path so a Goal larger than `MAX_PATCH_OPS` is published as one GraphStore transaction/version; the 503-operation regression passes. Arbitrary user-authored patches remain subject to the normal limit, and there is no separate staging manifest. | A partially compiled graph must not become visible to readiness/scheduling. | `src/lhos/sdk/os.py::_submit_compiled_ops`; `src/lhos/runtimes/verified_progress/{graph_store,patch_validator}.py`; `tests/sdk/test_large_goal_atomic_compile.py` | Keep the trusted path fail-closed and add injected-failure tests at the transaction boundary. If compilation becomes multi-process or externally staged, introduce a durable staging manifest before widening the guarantee. |

## P1: research and product limits

These do not all make the current narrow contract incorrect, but they limit
generality or the strength of future claims.

1. **D3 durability/integration (implemented in the core, bounded in the SDK).**
   `GraphStore` persists append-only D3 envelopes in the same transaction as
   the graph refresh. Reopen, rollback, corruption, append-only, and
   graph-version CAS tests pass. The public `AgentOS.repair()` now supplies and
   exposes the durable record, including `reopened_goals` and frontier data,
   but the SDK result remains a task-level summary; a complete public
   goal-node/D3 projection API and cross-plane replay contract are still future
   work.
2. **Task-level granularity.** The current graph mostly invalidates whole Tasks.
   Multi-output tasks, Artifact/Evidence-level edges, verifier changes, and
   selective repair inside one task are not represented. Add output-scoped
   nodes and compare against an oracle at that finer granularity.
3. **Semantic change pruning.** A changed input conservatively causes repair;
   there is no trusted semantic-equivalence proof that can safely skip a
   recomputation.
4. **Planner/replanner loop.** `src/lhos/agents/real_planner.py` is an
   experimental module, not an always-on automatic provenance discovery and
   repair-planning loop in the SDK composition root. The bounded
   `WorkspaceObservationWatcher` is an explicit file poller, not automatic
   dependency discovery. `HTTPToolAdapter` is likewise an explicit secure
   boundary for declared exact-version reads, and `AgentOS.register_external_fact`
   only seeds an authority record. The pure Context delta/rebase planner now has
   one bounded live application seam: changed-read `REBASE`/`FULL_RELOAD` can
   durably fence/release the old Claim, admit a fresh Attempt, detach the old
   Harness, and replay the same plan. This remains release-then-acquire and
   requires caller registration/recovery; it is not an automatic or atomic
   execution-path rebase loop. The caller-owned watch loop repeats bounded
   polls; `poll_and_reconcile` validates all assigned changes in one poll before
   one atomic batched VPG/D3 refresh, while unassigned resources remain
   interrupt-only.
5. **Lease liveness, freshness, and cooperative interrupts (partial).** Claim,
   SchedulerSession, and Kernel lease renewal/heartbeat primitives now exist.
   Renewal preserves the fencing token, journal/projection updates are atomic,
   and malformed provider responses fail closed. `AsyncWorkerPool` offers an
   optional attempt-scoped cooperative heartbeat loop, disabled by default.
   Enabling it requires `heartbeat_interval` plus a heartbeat callback or
   Scheduler `heartbeat`/`renew_claim` hook; if no usable hook is available,
   the pool fails closed. It does not provide killable cancellation, so
   arbitrary in-process callbacks may continue running after cancellation.
   `validate_read_set_freshness(...)` adds a conservative pre-commit guard:
   graph-version advance with `partial` or `unknown` delta coverage, or an
   unidentifiable read binding, returns `BLOCKED`; same-version empty partial
   observations remain a compatibility no-op. Token-aware async dispatchers
   can receive an exact-claim `preempt`/`rebase` request;
   `REQUESTED`/`DELIVERED`/`OBSERVED`/`CANCELLED` transitions are available to
   a durable callback, and an unobserved completion is quarantined. This is
   not force-kill or a universal watcher-driven SDK/Harness delivery loop. A
   bounded `handoff_task` primitive validates source Claim/Attempt/epoch,
   persists an idempotency key, and replays after restart, but
   release-then-acquire is not a cross-Scheduler/Kernel atomic transfer and it
   does not invoke/rebase a Harness automatically. The direct
   `AgentOS.deliver_interrupt(...)` path and verifier commit fence are covered
   on the built-in async SDK path. Terminal process publication also has a
   bounded PID-reacquisition fence; cleanup plus terminal publication remain
   separate cross-service transactions, so the complete lifecycle is not
   atomic.
6. **Durable scheduler concurrency and projection cost.** Reopen now verifies
   event sequence/hash/id/timestamp, snapshot hash/tail binding, and structural
   Claim/Attempt invariants. It rejects duplicate identities, multiple active
   owners for one graph/task, orphan or identity-mismatched Attempts, and
    malformed collection/idempotency fields. Large projections now use
    `normalized-v1` row storage: a small manifest is updated and only changed
    entity rows are written. Writers sharing one SQLite file are guarded by
    generation, state-hash, and event-tail compare-and-swap checks. This
    remains a single-writer store with no leader election or supported
    distributed/multi-writer coordination; projection derivation, validation,
    hashing, and some row-diff work remain O(N). Independent Journal connections
    now reserve the SQLite writer before reading `next_offset`, preventing
    racing append offsets; this bounded append fix does not make Scheduler
    projection mutation multi-writer safe.
7. **Execution recovery scope.** `AsyncWorkerPool` is in-process execution.
   Durable scheduler/VPG state is recoverable; arbitrary Python memory,
   call stacks, and in-flight code are not.
8. **Resource model scope.** CPU/RAM/GPU/VRAM/model-slot values remain logical
    admission reservations. `resource_aware=True` can fit explicit Task
    vectors to current logical pool capacity before Scheduler admission. An
    optional host telemetry adapter observes CPU/RAM and, when available,
    NVIDIA GPU/VRAM; an explicit `apply_host_capacity(...)` call can
    conservatively update exactly one named logical pool. There is still no
    authoritative shared host/device inventory, physical placement/isolation,
    continuous monitoring, quota/RPM/TPM accounting, preemption, fairness, or
    starvation guarantee.
9. **Commit-path cost.** A commit-local serialization cache now avoids repeated
   JSON encoding within one VPG commit, and the N=400 history storage issue is
   improved. Commits still derive/validate/hash a full projection; p99 is
   workload/host sensitive and occasionally approaches or exceeds 50 ms, so no
   stable 50 ms/10 ms latency SLO is claimed.
10. **Projection completeness.** Deletion/tombstones are absent; some hashes
    (for example frontier/result summaries) do not cover every proof field.
11. **Online compute-management integration (partial).** The repository now
    persists `AgentSnapshot` state, exposes a read-only `RuntimeStateView`, and
    provides opt-in `FrontierPolicy`, `ConflictGraph`/dynamic-parallelism, and
    semantic-interrupt proposal primitives, including the bounded explicit
    workspace watcher. `run(..., adaptive=True)` and
    `run_async(..., adaptive=True)` can apply a bounded advisory task filter
    (derived only from explicit access declarations); the default remains
    unchanged. The default frontier ordering remains repair-first lexical;
    opt-in `graph_utility` ranks only safe frontier candidates by declared
    critical-path position and immediate downstream unlock value.
    `OnlineComputationController`, exposed by
    `AgentOS.computation_controller(...)` and `online_control(...)`, adds an
    explicit one-epoch `observe -> reconcile -> plan -> dispatch -> observe`
    seam for an already compiled Goal. Without a dispatcher it is read-only;
    it never claims, leases, executes, or publishes Evidence. Explicit
    `Task.metadata["compute_routing"]` is also evaluated as a bounded,
    redacted audit summary in `result.meta["adaptive_epochs"]`; it never
    claims, leases, preempts, or rebases. A separate explicit
    `ComputeProviderRegistry` adapter can invoke registered model/verifier/
    context hooks only when `adaptive=True` and task metadata opts in; it does
    not select providers automatically or change physical resource placement.
     Adaptive epochs can be persisted as bounded, hash-identified
     `SCHEDULING_EPOCH_PLANNED` journal records; retries are idempotent and
     malformed decision hashes fail closed.
     `AgentOS.schedule_online_epoch(...)` additionally provides one bounded
     policy-planning -> authoritative Scheduler-admission -> explicit-cleanup
     bridge. It validates exact Claim/Attempt/Lease identities and defaults to
     plan-only; retained Claims require normal lifecycle execution or
     `AgentOS.release_online_epoch(...)`. It never invokes a Harness, executor,
     verifier, or semantic commit.
     `AgentOS.execute_online_epoch(...)` is a separate one-shot execution
     vertical slice. It delegates to `run_async(..., adaptive=True,
     max_steps=1)`, so the existing Scheduler/Claim/Attempt/Lease, Agent
     executor, verifier, and VPG Evidence path remain authoritative. It returns
     `RunResult`; it does not accept or consume a retained
     `OnlineEpochScheduleResult`, hand off retained ownership, or create/control
     an external Harness session. Repeated operation still needs an explicit
     caller loop. Its zero-dispatch case is narrower than a planning-only
     epoch: after Goal registration/compile-if-missing setup,
     `max_dispatches=0` skips adaptive planning/persistence, Claims/Leases, and
     executor/verifier calls and returns the `no_work_budget` audit outcome.
      For nonzero epochs, audit metadata separates the policy-selected task IDs
      from actual Scheduler dispatch and bounded serial fallback. The execution
      outcome is classified conservatively as `completed`,
      `completed_with_failures`, or `no_dispatch` (with the separate strict
      `no_work_budget` case); a failed epoch does not claim semantic commit.
       `AgentOS.execute_online_epochs(...)` provides a caller-owned bounded loop
       over these one-shot epochs and re-observes VPG after each iteration,
       stopping on closure, failure, no dispatch, no work budget, or an explicit
       epoch limit. `AgentOS.handoff_online_epoch_to_harness(...)` binds retained
       exact dispatches to caller-supplied Harness adapters only after complete
       identity validation and same-identity replay checks; it is not an atomic
       Scheduler/Kernel/Harness transaction.
       `AgentOS.event_supervisor(...)` adds a caller-owned, budgeted
       `start -> submit -> step -> stop`/bounded `run()` bridge with explicit
       event fingerprints and fail-closed stale/blocked/failed outcomes; it is
       not a background daemon or hidden retry loop.
       `prepare_handoff(...)`/`commit_handoff(...)`/`recover_handoff(...)`
       add a durable intent/recovery witness around release-then-acquire
       ownership. Replay is idempotent, but the protocol remains
       `IN_DOUBT`/fail-closed after uncertain commits and is not a
       cross-plane atomic transaction.
12. **Conflict graph completeness.** `ConflictGraph` is derived only from
    explicit task read/write declarations. Missing or unknown access is
    serial-only. Explicit logical resource fitting is implemented, but hidden
    file/API/tool accesses, semantic conflicts, undeclared resource requests,
    physical placement, and automatic graph maintenance remain open.
13. **Semantic interrupt and cognition control (bounded).** Commit-time read
    validation can quarantine stale cognition; graph-version advance with
    partial/unknown delta coverage is now fail-closed. `SemanticInterruptPolicy`
    emits deterministic decisions; the explicit `WorkspaceObservationWatcher`
    can turn declared workspace hash changes into graph-bound interrupts;
    `AgentOS.deliver_interrupt(...)` and `AsyncWorkerPool` deliver explicit
    `preempt`/`rebase` requests to token-aware async dispatchers with exact
    claim/attempt identity and observable transition phases. The bounded
    `OnlineComputationController` can pass actions to an injected dispatcher,
    while `make_harness_dispatcher` rejects ownerless/ambiguous or stale
    graph/task/Agent/Claim/Attempt/epoch identities before Harness code runs.
     `REBASE` rejects graph rollback and advances the target epoch. The async
     verifier-to-Evidence commit fence rejects late or ignored interrupt
    completions. The current boundary still lacks force-kill isolation,
    universal world watchers, implicit atomic Lease handoff, and a general
    third-party Harness orchestration loop. Explicit live changed-read
    `REBASE`/`FULL_RELOAD` now uses the bounded durable fresh-Attempt handoff,
    while the main SDK has one bounded Context refresh path for explicit
    manifests and authoritative Facts; unsupported inputs fail closed.
    The explicit
    Harness bridge journals exact-identity control results and replays bounded
    logical session metadata on file-backed reopen, but it does not restore
     callback/model memory or perform ownership handoff. The one-shot
     `AgentOS.poll_workspace_and_route(...)` /
     `route_workspace_observation(...)` façade revalidates supplied poll
     interrupts and records rejected delivery statuses as blocked; it is not a
     background watcher.
14. **Context locality and routing (bounded advisory + opt-in adapter).** Context VM snapshots
     are now created and fenced on the SDK execution path when a task has a
     manifest (with an empty default otherwise). `ComputeRoutingPolicy` and
     `AgentOS.plan_compute_routing(...)` additionally compute deterministic,
     fail-closed overlap/locality signals from explicit version-pinned
     bindings and emit read-only warm-Agent/fresh-Agent, model-tier,
     context-budget, and verification-strength recommendations. They do not
     create/reuse processes, claim work, or change the Scheduler. The explicit
     `ComputeProviderRegistry` execution adapter can call registered provider
     hooks after Claim/Context setup, but automatic provider selection,
     process lifecycle management, physical resource routing, and economic
     feedback remain open.
15. **Public provenance digest and side-effect audit.** Coverage reports are
     attached as Evidence metadata and their digest is validated on the built-in
     SDK path. There is not yet a durable, queryable read/write-set index or a
     universal Action/Outbox sink protocol.
17. **Context delta/rebase and epoch control (bounded).**
    `plan_context_rebase`/`build_context_delta` and
    `AgentOS.plan_context_rebase(...)` provide a deterministic immutable
    classification for explicit graph deltas (17 tests). `EpochController`
    provides an explicit default-off `observe -> reconcile -> plan` seam (10
    tests). Neither mutates Context VM, interrupts or rehomes a running Agent,
    claims work, acquires Leases, or runs automatically.
18. **Mediated HTTP provenance (bounded).** `HTTPToolAdapter` plus
    `AgentOS.register_external_fact` supports secure exact-version declared
    GET/HEAD/OPTIONS reads and fail-closed unknown/transport/mutating paths.
    The SDK boundary has 11 tests, low-level plus SDK coverage has 20 tests, and
    the related authority gate has 29 tests. This is explicit opt-in mediation,
    not process-wide network interception or universal observation.

16. **Action recovery policy and migration boundary.** `RecoveryPolicy` is
    durable and tested: `PURE + RETRY` retries a first dispatch exception once
    under a revalidated fencing contract, while retry failure/unknown outcomes
    become `UNCERTAIN`. The retry budget is durable (`retry_count`) and is
    consumed atomically with an `ACTION_RETRY_RESERVED` journal event. Existing
    action databases migrate additively; historical rows keep `pure`/`retry`
    compatibility labels but missing retry-budget state is conservatively
    treated as exhausted. This is still a compatibility policy, not strict
    effect-contract admission; a custom driver can be misclassified unless the
    caller opts into the mediated `secure_mode` boundary.

## P2: defer until the P0/P1 contract is demonstrated

- Multi-host/distributed runtime, leader election, and consensus.
- Physical GPU/VRAM/device scheduling, placement, and isolation. Telemetry and
  the explicit logical-capacity bridge are not physical capacity enforcement.
- General belief revision and contradiction solving.
- Hosted control plane, dashboard, and operational multi-tenancy.
- Schema migration tooling and production SLOs.
- Statistically powered comparisons using real models, GPUs, provider rate
  limits, and representative long-horizon workloads.

## What is already demonstrated

The checked-in evidence supports the following bounded statements:

- Recovery/repair demo: crash ownership recovery, three affected tasks, one
  preserved independent task, three repair attempts, and Goal reclosure.
- Semantic-repair quick suite: 24/24 valid trials, 48.6427% mean weighted work
  saved versus full restart, 0 under/over-invalidation, and parity (0%
  additional saving) with the oracle task-DAG checkpoint.
- Async SDK microbenchmark: bounded overlap on 24 controlled I/O-shaped tasks;
  the three-pair reference median is 2.124x, not a claim about model/GPU or
  distributed throughput.
- VPG history benchmark (rerun August 13, 2026): N=400 stores 400 revision rows and about 1.64 MB in
  the recorded workload; the former full-copy layout was about 37.9 MB. Full
  projection commit latency is still a separate issue.
- D3 durability: seven focused tests cover same-transaction persistence,
  reopen/replay, rollback, append-only identity, corruption rejection, and
  optimistic graph-version CAS.
- Large Goal publication: a 503-operation SDK Goal compiles to one patch and
  one graph-version increment; no intermediate dispatchable projection is
  exposed.
- Observation-token repair: an earlier 30-test token/repair/D3/large-publication
  gate covers graph mismatch, tamper, same-version immutability, workspace
  reopen, Goal reopen, and the standalone observation-repair quickstart. On
  August 14, 2026, the current observation-token/authority/watcher gate passed
   **35 tests**, including response-loss retry, concurrent duplicate delivery,
   URI-alias identity, and batched multi-resource reconciliation; the
   watcher route/rejection plus `WorkspaceWatchLoop` passed **30 tests**; the
   mediated workspace commit-validation slice has **47 focused tests**; live
   rebase/fresh-Attempt handoff has **27 focused tests**; related
   observation-token/authority/watcher coverage is **44 tests**. These counts
   overlap and are not additive.
- Hidden-provenance safety gate: the offline benchmark reports `PARTIAL` for a
  declared-but-unread input and `UNKNOWN` for an explicitly unidentifiable
  hidden input; both are denied by strict admission while the audit migration
  path remains available. This proves a mediated fail-closed boundary, not
  automatic discovery of arbitrary Python/file/API reads.
- Lease renewal/release atomicity, malformed-provider fail-closed behavior,
  and the optional `AsyncWorkerPool` cooperative heartbeat loop are covered by
  focused Kernel, Scheduler, and worker-pool tests. The loop is disabled by
  default and does not provide killable callback cancellation.
- Durable Scheduler corruption tests cover event JSON/hash tampering, redundant
  timestamp-column tampering, snapshot hash tampering, duplicate Claim
  identity, orphan Attempt identity, malformed idempotency data, stale-writer
  CAS rejection, normalized-row tampering, and transaction rollback. An
  independent August 13 verification snapshot reports 37 tests for the
  durable/resources/completion gate and 343 tests for the full
  `tests/runtimes/multi_agent` package.
- Focused SDK closure gate: 65 tests covering synchronous execution, async
  overlap/capacity, verifier ordering, repair, provenance adapters, observation
  authority, and large-goal publication passed on August 13, 2026. A separate
  broader repository run is not represented by this focused number.
- Recovery-policy and additive-action-migration tests cover one fenced
  `PURE + RETRY`, durable retry-budget reservation/replay, retry
  exception/unknown fail-closed paths, superseded-lease rejection, durable
  policy replay, malformed policy normalization, and legacy
  `actions_projection` upgrades. The independent strict/recovery/migration
  gate reports 20 tests; adding the effect-gateway cases reports 41.
- Online-compute control CLI: `lhos benchmark online-compute --json` runs a
  deterministic four-task simulator. Static and adaptive policies both reach
  the same verified task set; the reference run reports 2,760 vs 1,440
  simulated tokens, 10.0 vs 5.0 simulated seconds, and 1,320 vs 0 stale/repeated
  work tokens. The simulator accepts a deterministic provider profile and also
  reports stale/re-executed task IDs, verified-progress traces, epoch
  parallelism, and cost/re-execution deltas. This is metric plumbing, not
  real-model/provider/GPU acceleration.
- Resource-aware runtime CLI:
  `lhos benchmark resource-aware-runtime --json` runs the same four-task Goal
  through the public AgentOS/Scheduler/Claim/Lease/verifier/VPG path. The
  controlled result reduces scheduling epochs from 3 to 2, advisory
  over-capacity proposals from 1 to 0, and Scheduler resource rejections from
  1 to 0 while both modes reach the same four-task VERIFIED Goal with zero
  admitted/executor capacity violations. This is deterministic
  logical-resource evidence, not physical utilization, wall-clock acceleration,
  or a real-model/GPU result.
- Bounded resource replanning E2E:
  `examples/resource_replanning_e2e.py` and
  `tests/sdk/test_resource_replanning_e2e.py` explicitly apply a 1,000-byte
  logical RAM capacity, run two 500-byte tasks, lower the named pool to 500
  bytes, and replan the remaining frontier one task per epoch until the same
  Goal closes. Resampling is caller-owned; this is not a daemon, physical
  placement controller, or utilization benchmark. Exact contracts are in
  [`RESOURCE-AWARE-RUNTIME-BENCHMARK.md`](RESOURCE-AWARE-RUNTIME-BENCHMARK.md)
  and [`RESOURCE-REPLANNING-E2E.md`](RESOURCE-REPLANNING-E2E.md).
- Multi-seed benchmark API:
  `run_multi_seed_benchmark(seeds=(...))` returns deterministic per-seed
  reports and mean/min/max summaries without changing the single-seed API.
  The canonical scenario is seed-invariant and carries seed as metadata, so
  this validates aggregation/reproducibility rather than statistically
  independent workloads. Seed-dependent scenarios and real-provider trials
  remain open.
- Event-driven supervisor gate: `tests/sdk/test_event_supervisor.py` reports
  **8 passed**. The caller-owned supervisor validates explicit event identity,
  re-observes RuntimeState, optionally routes declared workspace observations,
  executes one bounded online epoch, and enters `FAILED_CLOSED` on stale or
  blocked input. It does not run in the background or perform live external-
  Harness rebase.
  The reproducible CLI surface is
  `python -m lhos.cli.core demo online-supervisor --json`; it uses a controlled
  local executor/verifier and reports `daemon_started=false`, `uses_llm=false`.
- Ownership handoff intent gate: `tests/sdk/test_handoff_transaction.py`
  covers durable prepare/commit/replay behavior. The intent is a recovery
  witness around the existing release-then-acquire path, not an atomic
  Scheduler/Kernel/Harness/VPG transaction.
- Scheduler-backed online epoch gate:
  `tests/sdk/test_online_epoch.py` reports **7 passed**, covering plan-only
  no-ownership behavior, authoritative admission with automatic cleanup,
  ownerless Harness fencing, advisory selected-task filtering, post-admission
  graph-race cleanup, release-error retention, and idempotent terminal cleanup.
  This is not automatic Harness execution.
- One-shot online execution epoch:
  `tests/sdk/test_online_epoch_execution.py` reports **6 passed**, covering a
  real Scheduler -> executor -> verifier -> VPG commit, failed-executor
  Claim/Lease cleanup without semantic commit, already-verified no-op behavior,
  policy-selection-versus-Scheduler-fallback audit, strict zero-work-budget
  behavior, and fail-closed bounds validation. This is an independent AgentOS
  executor slice, not consumption of retained scheduling results or an
   external Harness session loop.
 - Post-admission Scheduler cleanup:
   `SchedulerSession.run_pass(...)` now compensates observation/reconcile
   failures after admission by releasing only exact returned Claim identities.
   Replacement Claims are protected by `expected_claim_id`; if compensation
   fails, the root exception is preserved and receives an `add_note(...)`
   diagnostic. This bounds ownership leakage at the session boundary but is
   not a cross-service atomic transaction.
 - Async cancellation cleanup:
   `run_async()` catches the worker-pool `CancelledError`, attempts exact-Claim
   fenced release for every job, continues after individual release/reconcile
   failures, logs bounded diagnostics, and re-raises the original cancellation.
   Replacement Claims are protected; arbitrary callbacks remain cooperative and
   non-killable. The focused cancellation subset reports **3 passed**. If
   cleanup still cannot complete, the Scheduler records an idempotent
   `execution-cleanup.v1` durable audit marker for the exact Claim/Attempt;
   marker creation does not release or retarget ownership.
- Freshness/rebase guard: the initial SDK regression reported **402 passed**,
  and the final online-compute/freshness package gate reports **485 passed**;
  the focused partial-delta test proves that graph-version advance with
  incomplete coverage fails closed. The combined online-control/freshness/
  Harness/CLI/metrics gate reports **65 passed**. The guard treats
  partial/unknown coverage and unidentifiable reads as fail-closed when the
  Graph version advances; same-version partial observation is a compatibility
  no-op. Optional `expected_graph_version` fencing returns policy-stale on an
  admission race and compensates exact Claims without a reconcile side effect.
- Harness action fencing: ownerless/ambiguous `START`, stale graph/task/Agent/
  Claim/Attempt/epoch identities, and graph-rollback `REBASE` are rejected
  before a Harness hook; the dedicated dispatcher-fencing file reports
  **9 passed**.
- Provider-routing adapter gate: 7 focused SDK tests plus an 8-test
  deterministic callback-vs-provider benchmark pass. In the bounded offline
  workload, both paths close a four-task Goal; the explicit route invokes
  four model and four verifier hooks, does not invoke base callbacks, and
  reports 448 versus 672 synthetic tokens. These are adapter/counter
  measurements, not real provider quality, price, or GPU results.
- Harness adaptive integration gate: `tests/benchmarks/test_harness_adaptive.py`
  and `tests/cli/test_harness_adaptive.py` pass. The benchmark exercises the
  real SDK Scheduler/Claim/Attempt/Lease/Harness/verifier/VPG path and reports
  stale/rework/usage/runtime-audit fields for static versus explicit
  conflict-aware adaptive policy. The built-in provider is deterministic with
  synthetic usage; provider plugins are opt-in observations, not quality or
  production-economics evidence.
- Journal/atomicity/rebuild/SQLite isolation and migration gate: **25 tests**
  passed on August 14, 2026. This includes empty-rebuild offset-zero coverage
  and a 20-pair, two-connection regression that verifies 40 unique contiguous
  Journal offsets. The overlapping combined Journal/Lease gate is **56 tests**
  (`25` Journal + `31` Lease). It does not establish distributed or general
  Scheduler multi-writer correctness.
 - The earlier online-epoch-hardening non-slow run completed with
    `3142 passed, 1 skipped, 18 deselected, 30 warnings` in `532.37s`.
   Reproduction log:
   `artifacts/full-test-nonslow-online-epoch-hardening-20260815.log`.
    That run predates the one-shot execution and cleanup/cancellation hardening.
    The cleanup-marker wiring baseline completed with
    `3166 passed, 1 skipped, 18 deselected, 30 warnings` in `458.98s`.
    The current post-format non-slow regression completed with
    `3341 passed, 1 skipped, 18 deselected, 30 warnings` in `509.05s`
    (`artifacts/full-test-nonslow-20260816-post-format.log`). The separate slow
    marker gate completed with `18 passed, 3342 deselected` in `1224.36s`
    (`artifacts/slow-tests-20260816-post-format.log`). This includes the
    graph-utility frontier strategy, real local wall-clock adaptive-runtime
    gate, and zero-delay Outbox retry correction. The earlier `3341`/`472.28s`
    result is historical pre-format evidence only
    (`artifacts/full-test-nonslow-20260816-final-after-outbox.log`). The `3321`
    workspace/watch-loop/live-handoff run remains historical
    (`artifacts/full-test-nonslow-after-live-rebase-watchloop-20260815.log`). The
    earlier `3257` result is the pre-resource-bridge baseline
    (`artifacts/final-test-nonslow-20260815-final-sync.log`). The `3201`
    live-rebase run remains historical
    (`artifacts/final-test-nonslow-20260815-live-rebase.log`). The prior `3187`
    final-serial and `3181` watcher runs remain historical baselines
    (`artifacts/final-test-nonslow-20260815-serial.log` and
    `artifacts/full-test-nonslow-p1-watcher-loop-20260815.log`).
    The older
    `artifacts/full-test-nonslow-online-execution-final-hardening-20260815.log`
    remains historical evidence for the one-shot execution, exact
    post-admission compensation, and cancellation cleanup available at that
    cutoff; it predates later cleanup-marker and watcher changes. The current
    single-host evidence is the `3341` run recorded above, not a
    production-readiness claim.
- Epoch controller gate: **10 tests**; Claim/Lease handoff gate: **8 tests**
  (including restart replay); Context delta/rebase planner: **17 tests**; HTTP
  SDK boundary: **11 tests**; low-level HTTP provenance plus SDK boundary: **20
  tests**; related HTTP/authority gate: **29 tests**. These counts are focused
  and overlapping where stated.
- Local configured-gate reproduction: Ruff format reports **626 files already formatted**;
  Ruff lint, Mypy, `compileall`, wheel build/install, fresh-environment CLI
  smoke, non-slow tests, and the separate slow marker gate all pass. This does
  not claim that GitHub-hosted Actions actually executed successfully.

These measurements do **not** prove automatic dependency discovery, general
Agent reliability, exactly-once external effects, physical resource control,
real-model acceleration, or production readiness.

## Recommended decision gate

Do not broaden the marketing claim until P0-1 through P0-5 have either been
implemented or explicitly enforced as fail-closed boundaries with adversarial
tests. P0-3 is currently only a mediated primitive; strict admission for every
driver and raw callback remains a gate. P0-6 is implemented only for the
trusted SDK Goal compiler and must not be generalized to arbitrary distributed
graph publication. The next externally defensible claim should be:

> **Evidence-backed, graph-relative selective repair for stateful Agent
> workflows on one host, with Kernel-fenced execution ownership.**
