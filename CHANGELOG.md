# Changelog

All notable changes to LongHorizonOS are documented here. The project follows
an experimental `v0.x` compatibility policy. The current package version is
`0.1.0`; work after the original release baseline remains **Unreleased** until
a new version is explicitly cut.

## Unreleased

### Online compute management

- Added an immutable four-plane runtime projection over semantic progress,
  Agent cognition, Context, and logical resources.
- Added an explicit, opt-in verified-progress compute-budget path for
  `AgentOS.run()` / `run_async()`. It ranks graph-relative READY/repair work,
  preserves repair priority, and supports partial admission under caller
  declared token, time, micro-USD, Context, and verification ceilings.
- Budget plans expose bounded `ComputeBudgetRemaining` values (`None` means
  unbounded; `0` means exhausted). Usage is charged only for task IDs that the
  authoritative Scheduler actually dispatches; rejected tasks and
  no-dispatch graph races consume nothing, while dispatched failed/stale
  attempts retain their declared charge.
- Added immutable attempt-level `UsageLedger` accounting with separate
  estimated/reserved/measured/terminal states. The ledger is in-memory only,
  is not durable Scheduler/VPG replay state, and is not provider billing or
  quota enforcement.
- Added bounded scheduling epochs, a caller-owned event supervisor, and
  caller-owned multi-epoch/watch-loop seams. These APIs do not start a daemon
  or bypass Scheduler/Kernel ownership.
- Added opt-in
  `FrontierPolicy(ranking_strategy="graph_utility")`. The policy keeps repair
  priority and safety filtering, then ranks safe graph-relative candidates by
  declared critical-path position, immediate downstream unlock value, and a
  deterministic lexical tie-break.
- Added explicit read/write conflict batching and opt-in resource-aware
  selection on the main `AgentOS.run(...)` / `run_async(...)` execution path.
  Unknown access is conservatively deferred while live Attempts exist.
- Added explicit host-resource observation and reserve-based mapping into one
  logical Agent pool. This is not continuous physical inventory, placement,
  isolation, quota, or GPU scheduling.
- Critical-path ranking now reaches dispatch. `Scheduler.schedule_once` /
  `run_pass` accept a `dispatch_order` ranking (distinct from the
  `allowed_task_ids` filter) and stably reorder the authoritative VPG frontier;
  the `adaptive=True` epoch now plans with `graph_utility`, and the bounded
  liveness fallback relaxes the filter while keeping the ranking. Added
  `ScheduleResult.dispatch_order_applied` / `.locality_matched` and epoch
  metadata `dispatch_order_applied` / `locality_matched_task_ids`. The ranking
  only reorders already-ready tasks; it never admits an unready one.
- Agent context residency now affects agent selection. Matching scores the
  per-agent overlap between a task's declared reads and each agent's durable
  `AgentSnapshot` read-set (capped by `LOCALITY_BONUS_MAX`), replacing a flat
  non-discriminating locality bonus. Warm-*agent* selection is live;
  warm-*process* reuse is not.
- Added measured-usage calibration. The sync execution path measures real
  wall-clock and records it through `UsageLedger.record_measured`; a bounded
  per-task integer EWMA (0.25x–4x) calibrates the declared cost/token/time
  dimensions of later runs. The audit key `usage_after` became
  `dispatched_declared_usage_after`, with new run-level `measured_usage` and
  `budget_calibration`. Task value, success, stability, rework, and
  verification tokens stay declared; the async executor wall-clock stays
  unobserved; the ledger stays in-memory and non-durable.
- Removed the practical ~165-task SDK ceiling. `build_verification_indices()`
  precomputes VERIFIES/PRODUCES adjacency once per pass, removing an O(N·E)
  hotspot so per-task build cost is flat (a local reference run builds 8000
  tasks in ~4.45 s). `MAX_PATCH_OPS = 500` is unchanged and still guards
  untrusted patches; large trusted goals publish atomically.

### Cognition, Context, and provenance

- Wired Context VM snapshots and page-binding provenance into bounded SDK
  Attempts.
- Added durable Agent snapshots carrying Attempt/Claim/Agent/graph identity,
  explicit read/write sets, Context identity, progress, and bounded cost data.
- Added mediated workspace read-set validation immediately before semantic
  Evidence commit. Changed, deleted, unavailable, or truncated known reads
  fail closed instead of committing stale cognition.
- Added a caller-owned `WorkspaceWatchLoop` over declared paths and bounded
  semantic-interrupt/supervisor epochs. Hidden reads and unregistered world
  changes remain undiscovered.
- Added explicit Context-delta planning and bounded live
  `REBASE`/`FULL_RELOAD` handoff to a fresh Attempt. Handoff intent and exact
  source identity are durable and replayable; replacement remains
  release-then-acquire rather than cross-plane atomic.

### Harness and execution control

- Added a bounded Harness control protocol with identity-fenced
  `START`, `CONTINUE`, `CHECKPOINT`, `REBASE`, and `PREEMPT` requests.
- Added cooperative semantic-interrupt delivery for token-aware async
  executors, ignored-completion quarantine, and a verifier-to-Evidence stale
  result fence.
- Added retained-Claim-to-Harness handoff and handoff recovery witnesses.
  Third-party process lifecycle control, portable checkpoints, and force-kill
  isolation remain outside this boundary.
- Preserved `FAILED_CLOSED` as a terminal supervisor state rather than
  degrading it to `STOPPED`.
- Added an opt-in, killable Harness execution boundary,
  `SubprocessHarnessAdapter` (plus the `harness_child` child protocol),
  exported from `lhos.sdk`. It runs Agent work in a child process so `PREEMPT`
  escalates graceful signal -> hard kill -> reap under a wall-clock watchdog,
  terminating a child that ignores cooperative cancellation. It composes the
  frozen `CallableHarnessAdapter` for identity/revision/checkpoint fencing and
  must declare `preemption_mode="cooperative"` (the frozen v1 vocabulary has no
  `"forceful"` token) while performing a real OS-level kill. It is on no default
  execution path — callers register it explicitly — and in-process Python
  callables remain non-killable.

### Routing and benchmarks

- Added deterministic advisory cognitive-locality, model-tier,
  Context-budget, and verification-strength routing plus an opt-in provider
  adapter. Provider selection and utility learning are not automatic.
- Added `lhos benchmark resource-aware-runtime --json`.
- Added `lhos benchmark wallclock-adaptive-runtime --json`, which runs real
  local asynchronous waits through the public
  `AgentOS.run_async -> Scheduler -> Claim -> Kernel Lease -> worker ->
  verifier -> VPG Evidence` path.
- The recorded wall-clock workload reaches the same four-task VERIFIED Goal
  while reducing scheduling epochs from **3 to 2** and Scheduler resource
  rejections from **1 to 0**. Its approximately **1.305x** local observed
  speedup is informational only and is not a real-LLM/GPU or production
  performance claim.
- Added the `baseline_vs_lhos` wall-clock benchmark (run via
  `python -m lhos.benchmarks.baseline_vs_lhos`; artifact
  `artifacts/baseline-vs-lhos-20260817.json`): a serial single-agent baseline
  versus adaptive LHOS closing the same VERIFIED Goal. The deterministic facts
  are the point — an identical verified task set, **7/10** warm dispatches, and
  a ranking ablation where repair-first lexical ordering selects none of the
  critical path while graph-utility puts its head first. The wall-clock speedup
  (five local runs **1.55x–1.94x**, median **~1.70x**; the single checked-in
  sample **~2.11x**; **2.5x** critical-path ceiling) is informational only. The
  workload is deterministic `asyncio.sleep` tasks — no LLM, no GPU, no real
  engineering task, and no token or monetary saving measured.

### Reliability fixes

- Corrected Transactional Outbox retry timing: a zero-delay retry is anchored
  to the caller's logical dispatch timestamp and remains immediately eligible
  after asynchronous publisher failure. Positive delays remain
  completion-relative, and Claim-expiry fencing is unchanged.
- Hardened durable live-handoff replay/recovery identity checks.
- Added exact-Claim compensation and durable cleanup markers around bounded
  post-admission/cancellation failures.

### Current verification — August 16, 2026

- Full non-slow suite:
  **3411 passed, 1 skipped, 18 deselected, 30 warnings in 517.71s**
  (`artifacts/full-test-nonslow-compute-budget-20260816.log`).
- Slow-marker suite:
  **18 passed, 3412 deselected in 1253.07s**
  (`artifacts/slow-tests-compute-budget-20260816.log`).
- Ruff lint, Ruff formatting across **636 files**, Mypy across 276 `src/lhos`
  source files with missing imports ignored, and `compileall` passed.
- The source-tree `lhos benchmark compute-budget --json` gate passed with no
  reported violations.
- Rebuilt artifacts in `dist-final-20260816-compute-budget` passed
  `twine check`. A fresh-venv install passed public SDK imports, the controlled
  compute-budget benchmark (`15 -> 90`, no violations), recovery/repair
  (`final_closed=true`, `crash_recovered=true`), and a real
  `budget_aware=True` execution smoke.
- Wheel SHA-256:
  `2FDBC5D5B78226AC8A83F5CD3F6B54941F57B07974CF6E21105B31CAE80AC145`.
- Source-distribution SHA-256:
  `AAB8E1EC91D6616E63AC78467F2CF5BAE88318656488FF8E5A039C3238C76565`.
- The earlier `dist-final-20260816-release` artifacts remain historical and
  predate compute-budget support.

These are local executions of gates aligned with the repository configuration;
they are not a claim that a GitHub-hosted workflow run completed.

### Current verification — August 17, 2026

- Full non-slow suite:
  **3473 passed, 1 skipped, 19 deselected, 30 warnings in 506.10s**.
- Ruff formatting (**569 files** across `src`/`tests` already formatted), Ruff
  lint (all checks passed), and Mypy across **282** `src/lhos` source files all
  passed.
- New focused coverage lands with this batch:
  `tests/runtimes/multi_agent/test_context_residency_dispatch.py`,
  `tests/sdk/test_subprocess_harness.py`, `tests/sdk/test_compute_calibration.py`,
  and `tests/benchmarks/test_baseline_vs_lhos.py`.
- These are local executions of the configured gates, not a claim that a
  GitHub-hosted workflow run completed.

### Boundaries retained

- Single-host research alpha; no distributed scheduler, leader election,
  multi-host placement, or supported multi-writer control plane.
- No automatic hidden dependency/provenance discovery or universal world
  watcher.
- Logical resource admission and optional host observation are not physical
  CPU/GPU/RAM/VRAM placement, isolation, quotas, fairness, or topology-aware
  scheduling.
- No atomic Scheduler/Kernel/Harness/VPG/Facts/external-effect transaction and
  no sink-enforced exactly-once guarantee for irreversible effects.
- No killable process/container sandbox for arbitrary Python callbacks.
- No statistically powered real-LLM, real-GPU, or direct-competitor benchmark.

## v0.1.0 — Experimental single-host release

### Semantic control plane

- Added the Verified Progress Graph (VPG) as semantic authority for
  evidence-backed validity, graph-derived readiness, causal invalidation,
  Repair Frontiers, and Goal closure.
- Added exact-version Artifact/Evidence bindings and fail-closed applicability
  checks.
- Added deterministic selective repair that preserves unaffected
  `VERIFIED` work and re-executes the graph-relative affected frontier.
- Added auditable invalidation causes and historical Evidence retention.

### Scheduler, Kernel, and SDK

- Added deterministic Agent eligibility and matching, Claims, Attempts,
  retries, Kernel Lease ownership, fencing, and reconciliation.
- Added public synchronous `AgentOS.run(...)` and bounded asynchronous
  `AgentOS.run_async(...)`.
- Added atomic logical `ResourceVector` admission for CPU millicores, RAM,
  GPU count, VRAM, and named model slots.
- Added Lease-generation fencing on the main SDK Evidence/VPG commit path.
- Added optional durable Scheduler event/state replay with integrity checks,
  Claim/Attempt recovery, idempotency keys, and logical reservation recovery.

### Persistence and tooling

- Replaced sequential full-copy VPG history rows with append-only changed
  entity revisions while retaining versioned projection snapshots and hashes.
- Added compact `READY_FRONTIER_UPDATED` summary payloads with backward reads
  for legacy full-list payloads.
- Added historical reconstruction, retention/compaction, trusted legacy
  migration, read-only inspection commands, and deterministic demo/benchmark
  surfaces.

### Integrations

- Added OpenAI-compatible transport with an offline fake.
- Added capability-governed Shell, Workspace, Git, HTTP-read, and
  command-verifier integrations.
- Added a Transactional Outbox primitive providing internal mutation plus
  delivery intent and external **at-least-once** delivery. It does not provide
  arbitrary exactly-once external effects.

### Initial checked-in measurements

- Semantic repair quick suite: **24 / 24** valid trials;
  **48.6427%** mean weighted work saved versus full restart; **0%** versus the
  oracle task-DAG checkpoint; under-/over-invalidation **0 / 0**; false
  `VERIFIED` states **0**.
- Controlled async AgentOS workload: serial **1.516 s**, concurrent
  **0.789 s**, observed **1.921x** speedup, peak concurrency **4**, and zero
  ownership/resource/capacity violations.
- Incremental VPG history workload at N=400: **400** revision rows, an
  approximately **1.64 MB** database, and **47,892 B** READY-frontier event
  payload versus the former **80,200** full-copy rows and approximately
  **37.9 MB** database.

These measurements are controlled regression evidence, not universal claims
about real models, provider cost, physical GPU throughput, distributed
scheduling, or arbitrary workloads.

See [the v0.1.0 release note](docs/releases/v0.1.0.md) for the release scope,
current source addendum, verification evidence, and explicit limitations.
