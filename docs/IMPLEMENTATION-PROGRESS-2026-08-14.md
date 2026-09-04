# LongHorizonOS implementation progress

**Last synchronized:** 2026-08-15 one-shot online execution epoch (UTC+08:00)  
**Update policy:** while the current implementation session is active, this
file is refreshed at least once every 10 minutes.  
**Current release boundary:** experimental single-host research alpha
(`v0.1.x`).

This is an execution log, not a product claim. A feature is marked implemented
only when the code path exists and a focused test has passed.

## Current implementation batch

### Mind-VLA note audit (2026-08-14 20:49 CST)

The note is **not fully implemented**. The repository currently covers the
first bounded slice of the design: graph-relative semantic state,
`RuntimeStateView`, `AgentSnapshot`, Context VM snapshots, commit-time stale
cognition fencing, opt-in frontier/conflict policies, cooperative interrupts,
bounded Harness control/replay, and mediated workspace/HTTP provenance.

The following remain partial or open: an always-on event-driven epoch loop,
automatic Context delta rebase, universal hidden dependency/world observation,
automatic provider/model/verifier selection, atomic Claim/Lease handoff,
killable process isolation, sink-enforced exactly-once side effects, physical
resource telemetry, distributed scheduling, and real-model cost/utility
benchmarks.

The parallel implementation batch is now **code-complete for its bounded
targets**. Four tracks were used: Claim/Lease handoff, the default-off epoch
controller, the explicit HTTP provenance SDK boundary, and the pure Context
delta/rebase planner. Each track has a focused test and static-check result.
These are bounded primitives, not a claim that the full Mind-VLA design or a
general-purpose Agent OS is complete.

| Work item | Status | Evidence / current result | Target |
|---|---|---|---|
| Atomic multi-resource observation reconciliation | Implemented, bounded | Same-poll assigned changes are validated before one VPG/D3 transaction; focused watcher gate passed before the latest edge fixes | Completed |
| Retry after a committed repair whose response was lost | Implemented and focused-tested | Exact observation token reuse prevents a retry from creating a second D3/GraphVersion | Completed |
| URI-alias-safe reconciliation identity | Implemented and focused-tested | `workspace://x` and `vpg://workspace/x` derive the same canonical batch identity | Completed |
| Observation-token issuance concurrency | Implemented and focused-tested | A per-provider `RLock` serializes exact version registration plus token persistence; in-memory, same-connection, and separate-connection cases are covered | Completed; included in `35 passed` observation/watcher gate |
| Journal append concurrency across SQLite connections | Implemented and focused-tested | Journal append now reserves the SQLite writer before reading `next_offset`; a 20-pair two-connection regression verifies 40 unique, contiguous offsets. Empty rebuild preserves offset 0; stress covered 3,600 events without gaps or locks | Completed; included in `25 passed` journal gate |
| Benchmark harness stability | Implemented, focused tests passed | Wall-clock throughput is no longer treated as a correctness assertion; async benchmark uses paired repetitions and a median. Latest controlled result: 2.124x, zero ownership/resource/capacity violations. CLI output-contract tests use a non-performance threshold and passed `3 passed` | Completed |
| SIGKILL and quickstart stability | Focused gates passed | Live-worker matrix: `20 passed`; post-commit/pre-journal recovery scenario: `1 passed` with its internal 20 trials; observation-repair demo: 10/10 successful runs | Completed for the previously failing cases |
| Full non-slow repository regression | Completed | Frozen-tree command `python -m pytest -q -m "not slow" --maxfail=20 --tb=short` with `PYTHONPATH=src` completed with `3037 passed, 1 skipped, 18 deselected, 30 warnings` in `296.70s`; log: `artifacts/full-test-nonslow-frozen-20260814.log` | Completed |
| Late lease/journal transaction audit | Focused gate passed | Lock-wait stale-clock renewal and empty-rebuild offset bugs are fixed; Journal/Lease combined coverage is `56 passed` (Journal 25, Lease 31), with repeated cross-connection stress showing no gaps/locks | Completed for bounded single-host contract |
| Process termination versus lease reacquisition | Implemented, bounded | Terminal state is published before cleanup; `atomic_acquire` rejects existing `EXITED`/`FAILED` PIDs inside the writer transaction. Dedicated lifecycle and kernel gates pass; cleanup plus terminal transition are still not one cross-service transaction | Completed bounded fence; retain full lifecycle transaction as future work |
| CI format/slow-gate honesty | Slow-job fix implemented; format open | Platform/Core Agent OS/D2 jobs now explicitly exclude slow tests; a separate `slow-benchmarks` job preserves the formal slow gate. Ruff format still reports 52 files and remains an open hygiene item; no formatter-wide rewrite was applied without review | Review and land mechanical formatter patch, or document the exact non-green gate |
| README/status/issue/roadmap synchronization | Completed | Public/status documents now record the final frozen-tree result, the corrected CI slow-test filter, the separate `slow-benchmarks` job, and the remaining formatter and cross-service transaction boundaries | Completed |
| Claim/Lease handoff design | Implemented, bounded | Exact handoff, refusal/failure cleanup, idempotent `handoff_id`, and SQLite restart replay; dedicated gate: `8 passed`. It is explicitly release-then-acquire and non-transactional across Scheduler/Kernel services | Atomic cross-service transfer and automatic Harness integration remain open |
| Mediated HTTP/API provenance gateway | Implemented, mediated | Explicit injected transport, canonical request/response identity, exact caller/authority version, canonical URL artifact identity, ETag/version authority, strict fail-closed behavior, and NETWORK provenance; low-level + SDK gate is `20 passed`, with related Context VM/AgentOS/workspace gate `29 passed`. This is not universal network interception or an exactly-once side-effect sink | Keep authority-backed API facts and hidden-dependency limits explicit |
| Event-driven scheduling epoch controller | Implemented, default-off primitive | `observe 鈫?reconcile 鈫?plan` immutable controller; graph-version CAS, idempotency, interrupt proposals; dedicated gate: `10 passed`. It does not claim, lease, dispatch, or execute user code | Always-on integration with the authoritative run loop remains open |
| HTTP provenance SDK integration | Implemented, explicit boundary | Added `HTTPToolAdapter`, `create_http_tool`, `http_tool_context`, exact positive version/authority checks, canonical URL artifact identity, public `AgentOS.register_external_fact`, and AgentOS E2E. SDK boundary gate: `11 passed`; low-level + SDK: `20 passed`; related integration: `29 passed` | No global interception; hidden API/file/Python dependencies remain undiscovered |
| Context delta/rebase planner | Implemented, pure/read-only | Immutable `ContextDelta`/`RebasePlan`, explicit graph delta matching, required refs, `REUSE/REBASE/FULL_RELOAD/BLOCKED`, unknown fail-closed; public exports and `AgentOS.plan_context_rebase`; gate: `17 passed` | Not connected to ContextService/Harness execution; no automatic materialization or rebase |

## Implemented project capabilities

- Evidence-backed VPG validity, exact-version Evidence, graph-relative
  invalidation, Repair Frontier, Goal reopen, and verified reclosure.
- Kernel/Scheduler Claim, Attempt, Lease and fencing-based execution ownership.
- Durable Scheduler/VPG metadata replay within the documented single-writer,
  single-host boundary.
- SDK Context VM snapshots, `AgentSnapshot`, commit-time stale-cognition
  quarantine, and read-only `RuntimeStateView`.
- Opt-in `FrontierPolicy`, explicit-access `ConflictGraph`, bounded dynamic
  parallelism, and persisted scheduling-epoch audit records.
- Cooperative Semantic Interrupt delivery and a bounded exact-identity Harness
  control bridge.
- Explicit workspace observation/watcher path, graph-bound observation tokens,
  and atomic same-poll multi-resource reconciliation.
- Bounded compute-routing advice plus an explicit opt-in provider registry.
- Default-off event-driven epoch controller, bounded Claim/Lease handoff, explicit
  HTTP provenance SDK boundary, and pure Context delta/rebase planner.
- Task-level selective repair benchmark, async controlled benchmark, hidden
  provenance fail-closed benchmark, and VPG history-size regression coverage.

## Not implemented yet

| Missing capability | Planned implementation window |
|---|---|
| Automatic discovery of arbitrary hidden file/API/browser/tool/Python dependencies | `v0.2` provenance milestone; begin after the current single-host correctness gate, target first mediated HTTP/Tool prototype during 2026-08-17 to 2026-08-28 |
| Universal world observation and source validators | Incrementally with `v0.2`; file watcher exists now, HTTP/API validators follow the mediated gateway |
| Automatic Context delta compilation and Harness rebase | After observation/provenance authority is stable; target first bounded end-to-end prototype during 2026-08-31 to 2026-09-11 |
| Atomic Claim/Lease ownership handoff during preempt/rebase | Same phase as bounded Harness rebase; requires crash and stale-worker tests before it is claimed |
| Killable isolation for arbitrary Python callbacks | Process-worker prototype after ownership handoff, tentatively 2026-09 |
| Exactly-once irreversible external side effects | No short-term blanket claim; first sink-specific idempotency/CAS protocol is a `v0.2/v0.3` research item |
| Artifact/Evidence-field-level repair and semantic-equivalence pruning | `v0.3` systems-paper milestone after task-level correctness is frozen |
| Real CPU/GPU/RAM/VRAM telemetry and physical isolation | Later productization phase; not part of the current `v0.1.x` release |
| Distributed scheduling, leader election and consensus | Later scale phase; only after the single-host semantic/ownership model is independently reproducible |
| General belief revision, contradiction solving and autonomous replanning | Research phase after provenance completeness and finer-grained repair |

Dates beyond the current batch are planning targets, not guarantees. They must
be revised when a prerequisite correctness gate fails.

## Current test truth

- Watcher plus observation focused coverage passed before the latest
  concurrency hardening. The current combined observation/authority/watcher
  gate reports `35 passed`.
- Journal/atomicity/rebuild/SQLite coverage reports `25 passed`, including the
  empty-rebuild offset regression and cross-connection allocation test.
- Benchmark stabilization focused gates passed (`15 passed` and `10 passed`);
  Ruff and Mypy passed for those changes.
- The post-review CLI benchmark contract gate passed `3 passed`; the formal
  benchmark still retains its repeated-measurement performance threshold.
- Ruff, Mypy, and `compileall` pass for the current touched source files.
- Previously flaky SIGKILL gates now pass in isolation (`20 passed` and
  `1 passed`), and the observation-repair quickstart completed 10/10 repeated
  subprocess runs.
- The late Lease/Journal atomicity patch passes a dedicated post-change gate:
  `62 passed in 2.19s` across journal, lease, renewal, replay, recovery, and
  process-cleanup tests.
- A post-audit regression gate for lock-wait renewal and empty Journal rebuild
  passes `16 passed in 1.20s`; Ruff lint and Mypy pass for these touched files.
- Journal/Lease focused coverage is now `56 passed` (Journal 25, Lease 31).
  Repeated stress covered 3,600 cross-connection events and 120 blocked
  renewals with zero gaps, lock errors, stale renewals, or timeouts.
- Terminal-PID lifecycle fencing coverage passes `44 passed` together with
  kernel-loop/recovery/lease regressions.
- CI audit found 52 current-tree formatting failures; this is release hygiene,
  not a semantic test result. The platform/core correctness jobs now exclude
  slow tests and a separate `slow-benchmarks` job runs the formal slow gate.
- The workflow now filters slow tests from platform/core correctness jobs and
  adds an explicit `slow-benchmarks` job. The format gate remains non-green
  until the 52-file mechanical formatter debt is reviewed.
- The last intermediate broad run reported `3029 passed, 1 skipped,
  3 deselected, 3 failed`. It is retained as historical context only; the
  frozen-tree run below is the release evidence.
- The previously completed pre-batch non-slow baseline was
  `3030 passed, 1 skipped, 3 deselected, 30 warnings`.
- The final frozen-tree non-slow run is complete: `3037 passed, 1 skipped,
  18 deselected, 30 warnings` in `296.70s`. This is a semantic regression
  result for the current single-host alpha, not a production-readiness claim.
- A post-document-sync focused online-runtime gate passed `388 tests` in
  `28.70s` across the full D2 multi-agent package plus Context VM and semantic
  interrupt integration/persistence coverage.
- `ruff format --check src/lhos examples tests` currently reports exactly
  `52 files would be reformatted, 465 files already formatted`. No
  repository-wide formatter rewrite has been applied during this correctness
  batch.
- HTTP SDK provenance gate (2026-08-14 21:42 CST): `20 passed` across the
  low-level gateway and SDK adapter; related Context VM/AgentOS/workspace
  integration gate: `29 passed`. Ruff, Mypy, and compileall pass for the
  touched HTTP/SDK files.
- Post-batch focused gate (2026-08-14 22:06 CST): `66 passed` across Context
  delta, epoch controller, handoff, HTTP SDK, low-level HTTP, Context VM, and
  provenance integration tests. The Context export/facade extension adds two
  additional focused tests (`17 passed` for the Context delta file).
- Post-batch full non-slow regression (2026-08-14 22:32 CST):
  `3092 passed, 1 skipped, 18 deselected, 30 warnings` in `303.33s`;
  log: `artifacts/full-test-nonslow-post-batch-20260814.log`. This is the
  earlier source-tree regression evidence, later superseded by the final
  late-session result below; it was never a production-readiness claim.

## Next synchronization

Next update is due by **2026-08-14 22:48 CST** while implementation work
continues, or sooner if a new correctness gate changes the release boundary.

## Online-compute vertical slice 鈥?2026-08-14 23:37 CST

### This batch is implementing

- An explicit single-host `observe -> reconcile -> plan -> dispatch -> observe`
  computation-control loop.  It emits auditable actions but keeps Claim,
  Lease, Scheduler, Kernel, Harness, and VPG authority in their existing
  components.
- A bounded Context-delta/Rebase-to-Harness bridge that turns an explicit
  semantic interrupt and read-set delta into `REUSE`, `REBASE`,
  `FULL_RELOAD`, or fail-closed `BLOCKED`, with stale-commit protection.
- Structured Verified-Progress utility metrics and a deterministic
  static-vs-adaptive controlled benchmark.  The benchmark is intended to
  measure rework and useful verified progress, not to claim real LLM/GPU
  throughput.

### Already implemented before this batch

- Versioned VPG validity and repair frontier; RuntimeStateView over progress,
  cognition, context, and logical resources.
- AgentSnapshot/read-set freshness and commit-time `STALE_COGNITION`
  quarantine.
- Deterministic Frontier/ConflictGraph/interrupt policies, cooperative worker
  interrupts, bounded Harness control, and bounded Claim/Lease handoff.
- Explicit Context VM snapshots, workspace observation, and mediated HTTP
  provenance boundaries.

### Still not implemented after this batch

- Universal hidden provenance/world observation; physical device telemetry and
  isolation; arbitrary-process force-kill; exactly-once irreversible effects;
  distributed consensus; automatic model/process lifecycle feedback; and
  general belief revision.
- The new loop remains explicit/bounded and does not itself guarantee a
  production scheduler or a 10-hour-to-3-hour speedup.

### Current schedule

- 23:35鈥?0:10 CST: three parallel implementation tracks and focused tests.
- 00:10鈥?0:25 CST: root integration/public exports and end-to-end smoke test.
- 00:25鈥?0:50 CST: focused/package regression and static checks.
- 00:50 CST onward: update README/status/issue inventory with measured truth.

This entry is a progress synchronization point; claims will be revised after
the focused gates complete.
## Online-compute synchronization — 2026-08-14 late-session batch

This block records the current bounded implementation state; it is not a
production-performance claim.

### Completed

- **Commit-freshness partial coverage now fails closed.**
  `validate_read_set_freshness(...)` rejects a graph-version advance unless
  the supplied graph delta has complete coverage. `unknown` coverage and
  unidentifiable read bindings also block commit/reuse. A partial delta remains
  backward-compatible only when the graph version has not advanced. The
  initial SDK regression reported **402 passed**; the final SDK
  online-compute/freshness package gate reports **485 passed**.
- **Bounded computation-control facade and CLI are implemented.**
  `AgentOS.computation_controller(...)` (alias `online_control(...)`) creates
  an explicit, single-host
  `observe -> reconcile -> plan -> dispatch -> observe` controller for an
  already compiled Goal. Without an injected dispatcher it remains read-only;
  it does not claim work, acquire Leases, execute Agent code, or publish
  Evidence. The deterministic CLI surface is
  `lhos benchmark online-compute [--json]`.
- **Scheduler-backed online epoch bridge is implemented.**
  `AgentOS.schedule_online_epoch(...)` connects deterministic policy planning
  to one authoritative Scheduler admission pass. The Scheduler creates exact
  Claim/Attempt/Lease identities; `plan_only=True` creates no ownership,
  admitted Claims auto-release unless `keep_claims=True`, and retained Claims
  require normal lifecycle execution or
  `AgentOS.release_online_epoch(...)`. The API stops at policy planning ->
  Scheduler admission -> explicit cleanup: it never invokes a Harness, Agent
  executor, verifier, or semantic commit.
- **Harness dispatch fencing is implemented.** Computation actions for active
  Attempts carry Agent/Claim/Attempt/epoch identity. The Harness dispatcher
  validates graph id/version, task, Agent, Claim, Attempt, and semantic epoch
  before entering Harness code. `REBASE` additionally forbids graph rollback
  and advances the session epoch; stale identities fail before the callback.
  Ownerless or ambiguous `START` fails closed: Scheduler/AgentOS must first
  create a Claim/Attempt and register the matching Harness session.

### Focused evidence available now

- `tests/sdk/test_computation_control.py`: **11 passed**.
- `tests/sdk/test_harness_dispatcher_fencing.py`: **9 passed**.
- `tests/sdk/test_online_epoch.py`: **7 passed** after graph-race,
  cleanup-error, and idempotent-release hardening.
- Related existing computation-control/Harness tests: **23 passed**.
- `tests/cli/test_cli.py`: **24 passed**.
- Computation-utility benchmark tests: **8 passed**.
- Combined computation-control, freshness, Harness-dispatch, CLI, and metric
  gate: **65 passed**.
- Ruff passed for the Harness-fencing changes; facade/CLI touched-file Ruff,
  Mypy, and compile checks passed.

### Final regression for this batch

- SDK online-compute/freshness package gate: **485 passed**.
- Full non-slow repository regression before the final hardening:
  **3133 passed, 1 skipped, 18 deselected, 30 warnings** in **447.91s**.
  This is historical context only.
- The deterministic online-compute simulator reports the same verified
  four-task set for both policies, with 2,760 vs 1,440 simulated tokens,
  10.0 vs 5.0 simulated seconds, and 1,320 vs 0 stale/repeated work tokens.
  These are scenario inputs/results, not real provider measurements.

### Remaining open after this batch

- automatic coupling of the explicit controller to watcher events,
  Context materialization/rebase, retained-Claim lifecycle, Lease transfer,
  and Harness lifecycle;
- automatic coupling from `schedule_online_epoch(...)` into Harness execution
  or semantic commit;
- an atomic cross-service Claim/Lease/Harness handoff;
- universal provenance/world observation, killable arbitrary callbacks,
  physical resource scheduling, and real-model/provider utility evidence.

The online-compute controller is explicit and bounded. A configured dispatcher
may enter a registered Harness hook, including a caller-supplied legacy
executor, but the controller itself is not a replacement for Scheduler/Kernel
ownership and is not an always-on autonomous scheduler.

## Online-epoch hardening synchronization — 2026-08-14

### Implemented in this follow-up

- Added fail-closed post-admission graph-version reconciliation to
  `AgentOS.schedule_online_epoch(...)`. If the graph changes between policy
  planning and Scheduler admission, exact newly-created Claims are released
  with fencing; unreleasable Claims are returned as
  `CLEANUP_REQUIRED` instead of being hidden.
- Added Attempt-to-Claim process and graph-version identity checks before an
  online epoch dispatch is exposed.
- Added best-effort exact-Claim cleanup diagnostics for Scheduler release
  exceptions. Cleanup never releases a replacement Claim because every
  release carries `expected_claim_id`.
- Made `release_online_epoch(...)` idempotent for already-terminal Claims.
  The typed result now distinguishes newly released, already-terminal, and
  still-live identities.

### Verification completed

- `tests/sdk/test_online_epoch.py`: **7 passed**.
- Online epoch plus computation-control, freshness, Harness-fencing, and
  adaptive-runtime focused tests: **46 passed**.
- Ruff, Mypy, and `compileall` passed for the touched source/tests.

### Still not implemented

- This remains a bounded, caller-invoked bridge:
  watcher -> controller -> Scheduler -> Harness is not an always-on loop.
- Claim/Lease/Harness handoff is not one cross-service transaction.
- Physical resource telemetry/placement, process isolation, universal
  provenance, automatic Context materialization/rebase, and real-model/GPU
  utility evidence remain open.

The post-hardening full non-slow regression is now complete:
**3142 passed, 1 skipped, 18 deselected, 30 warnings** in **532.37s**.
Log:
`artifacts/full-test-nonslow-online-epoch-hardening-20260815.log`.
This is single-host regression evidence, not a production-readiness claim.

## One-shot online execution synchronization — 2026-08-15

### Implemented in this follow-up

- Added `AgentOS.execute_online_epoch(...)` as the smallest executing online
  epoch vertical slice. It delegates to the existing
  `run_async(..., adaptive=True, max_steps=1)` lifecycle rather than creating a
  parallel execution authority.
- The real Scheduler still derives readiness and admission, creates
  Claim/Attempt/Lease ownership, invokes the configured AgentOS executor and
  verifier, and commits successful Evidence through the existing serialized VPG
  path.
- The result is `RunResult` plus bounded `meta["online_epoch"]` audit metadata.
  It is intentionally distinct from `OnlineEpochScheduleResult`. The audit
  distinguishes policy-selected task IDs, actually dispatched task IDs, and
  bounded serial-fallback task IDs instead of implying that selection always
  equals authoritative Scheduler admission.
  Its phase/outcome transcript is conservative: `completed` includes semantic
  commit, `completed_with_failures` omits `commit`, `no_dispatch` records
  planning/admission without user-code dispatch, and `no_work_budget` records
  strict zero-budget observation only.

### Explicit boundary

- `execute_online_epoch(...)` does **not** accept or consume a retained result
  from `schedule_online_epoch(..., keep_claims=True)`.
- It does not transfer retained Claim/Lease ownership and does not create,
  resume, pause, rebase, or terminate an external Harness session.
- It is one caller-invoked epoch, not an always-on
  watcher -> controller -> Scheduler -> Harness loop. A caller must explicitly
  invoke later epochs.
- `max_dispatches=0` currently means an immediate no-work return after the
  normal Goal registration/compile-if-missing setup. It does not run or persist
  adaptive planning, acquire Claim/Lease ownership, or call executor/verifier
  code; it must not be described as an observe/plan-only epoch. The result
  reports `outcome="no_work_budget"` and only the initial observation phase.

### Verification available now

- `tests/sdk/test_online_epoch_execution.py`: **6 passed**, covering the real
  Scheduler -> executor -> verifier -> VPG commit path, failure cleanup,
  already-verified no-op behavior, policy selection versus Scheduler fallback
  audit, strict `max_dispatches=0` behavior, and argument validation.
- `tests/sdk/test_online_epoch_cleanup.py`: **3 passed**, covering exact
  post-admission Claim/Lease compensation, root-exception `add_note(...)`
  diagnostics when cleanup fails, and replacement-Claim fencing.
- `tests/sdk/test_async_run.py -k cancel`: **3 passed**, covering cancellation
  cleanup across all jobs, replacement-Claim fencing, and preservation of the
  primary `CancelledError` when cleanup/reconciliation reports errors.
- The previously reported **3142 passed** full non-slow run predates this API,
  cleanup hardening, and cancellation hardening. It remains historical
  evidence only. The latest completed full non-slow baseline is **3157 passed,
  1 skipped, 18 deselected, 30 warnings** in **465.50s**; log:
  `artifacts/full-test-nonslow-online-execution-final-hardening-20260815.log`.
  It covers the one-shot execution, exact post-admission compensation, and
  cancellation cleanup available at the time, but predates later automatic
  cleanup-marker wiring. A new full run is required before claiming full-suite
  coverage for that integration.

### Still open

- retained schedule-result -> execution handoff;
- an atomic Claim/Lease/Harness ownership transaction;
- external Harness session lifecycle management;
- an always-on event-driven orchestration service;
- universal observation/provenance, automatic Context rebase, killable worker
  isolation, physical placement, and real-provider utility evidence.

## Post-admission compensation synchronization — 2026-08-15

### Implemented in this follow-up

- `SchedulerSession.run_pass(...)` compensates failures from post-admission
  `observe_vpg`/`reconcile` by releasing only the exact Claim identities
  returned by that pass.
- `expected_claim_id` fencing prevents cleanup from releasing a replacement
  owner created by a race.
- Cleanup exceptions do not replace the root failure; bounded
  `BaseException.add_note(...)` diagnostics preserve the exact Claim/task
  identity and cleanup error for reconciliation.

### Boundary

This is a bounded single-host ownership-leak prevention primitive. If exact
cleanup remains incomplete, the Scheduler records an idempotent
`execution-cleanup.v1` audit marker (deterministic SHA-256 `marker_id`) for
later reconciliation; resolution requires terminal exact Claim state and an
authoritative no-live-lease check. The marker does not perform cleanup itself.
This is not a cross-service atomic Claim/Lease/Harness transaction, and it does
not make arbitrary callbacks killable.

## Cancellation cleanup synchronization — 2026-08-15

`AgentOS.run_async()` now catches worker-pool `CancelledError`, performs
best-effort exact-Claim fenced cleanup for every job, continues after individual
release/reconcile errors, emits bounded logger diagnostics, and guardedly adds
those diagnostics to the original cancellation before re-raising. Replacement
Claims survive the stale cleanup attempt. Incomplete cleanup is retained as an
idempotent `execution-cleanup.v1` durable audit marker for later reconciliation;
the marker does not release or retarget ownership. This is cooperative
single-host cleanup, not killable process isolation or distributed cancellation.

