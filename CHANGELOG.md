# Changelog

All notable changes to LongHorizonOS are documented here. Version `0.1.0` is
an experimental research release; the public SDK and CLI are still `v0.x`
surfaces.

## [v0.1.0] — Experimental single-host release

### Semantic control plane

- Added the Verified Progress Graph (VPG) as the semantic authority for
  evidence-backed validity, graph-derived readiness, causal invalidation,
  Repair Frontiers, and Goal closure.
- Added exact-version Evidence and Artifact bindings. Historical Evidence is
  retained as fact, but it is not applicable to a superseding Artifact version.
- Added selective repair that preserves unaffected `VERIFIED` work and
  re-executes the graph-derived minimum frontier.
- Added fail-closed validation for stale/unknown Artifact versions and
  auditable `old_version -> new_version` invalidation causes.

### Scheduler and execution

- Added deterministic Agent eligibility/matching, Claims, Attempts, retries,
  Kernel Lease ownership, and reconciliation/recovery.
- Added public synchronous `AgentOS.run(...)` and bounded asynchronous
  `AgentOS.run_async(...)`. Independent executor calls and async verifiers can
  overlap under global and per-Agent limits; semantic Evidence/VPG commits
  remain serialized within one async invocation.
- Added atomic logical `ResourceVector` admission for CPU millicores, RAM
  bytes, GPU count, VRAM bytes, and named model slots. Reservations are
  released on success, failure, cancellation, and reconciliation.
- Added Lease-generation fencing on the main SDK Evidence/VPG commit path and
  claim-identity fencing during cleanup.
- Added optional durable Scheduler event/state replay with an append-only
  hash chain, projection hash, idempotency keys, Claims, Attempts, match log,
  and active logical-reservation recovery.

### Persistence and operator tooling

- Replaced sequential full-copy VPG history writes with append-only changed
  entity revisions while retaining per-version projection snapshots and hashes.
- Persisted `READY_FRONTIER_UPDATED` event payloads as a compact `summary-v1`
  count plus SHA-256 digest, while retaining backward-compatible reads of
  legacy full-list payloads.
- Added historical reconstruction, retention contracts, verified checkpoint
  compaction, and explicit trusted migration for snapshot-less legacy
  projections.
- Added read-only `status`, `inspect`, and `graph` views plus explicit VPG
  lifecycle commands:

  ```text
  lhos vpg history
  lhos vpg compact
  lhos vpg migrate-legacy
  ```

- Added deterministic recovery/repair and async AgentOS benchmark gates with
  machine-readable reports.

### Integrations

- Added OpenAI-compatible transport with an offline fake.
- Added capability-governed Shell, Workspace, and Git integrations and a
  command verifier.
- Added a Transactional Outbox primitive. It provides an internal atomic
  mutation plus delivery intent and external at-least-once delivery; it is not
  yet a cross-plane exactly-once protocol.

### Checked-in measurements

- Semantic repair quick suite: **24 / 24** valid trials; **48.6427%** mean
  weighted work saved versus full restart; **0%** versus the oracle task-DAG
  checkpoint; under/over-invalidation **0 / 0**; state-only false closures
  **24 / 24**.
- Public async AgentOS benchmark (24 tasks, 25 ms controlled I/O delay):
  serial **1.516 s**, concurrent **0.789 s**, **1.921x** speedup, peak
  concurrency **4**, and zero ownership/resource/capacity violations.
- Incremental VPG history benchmark: at N=400, **400** revision rows, a
  **1.64 MB** database, and **47,892 B** of READY-frontier event payload
  versus the former **80,200** full-copy rows (previously measured at about
  **37.9 MB**), a **99.50%** history-row reduction. Durable history and event
  payload growth are linear for this workload; end-to-end commit latency
  remains superlinear because full projection derivation/validation/hash work
  is still performed per commit.

These are controlled regression measurements, not universal claims about real
models, GPU throughput, provider cost, or arbitrary Agent workloads.

### Known limitations

- Single-host/local focus; no distributed scheduler, leader election,
  multi-writer fencing, or cluster inventory.
- Resource vectors are logical per-Agent pools, not physical host/device
  telemetry or isolation. RPM/TPM/API quotas, browser/sandbox/workspace locks,
  preemption, fairness, and starvation guarantees are not implemented.
- Durable replay assumes one Scheduler writer and does not restore arbitrary
  Python memory, call stacks, or in-flight execution.
- The main Lease-to-VPG ordering is fenced, but Facts, Action, Claim
  completion, Lease release, driver-side effects, and external systems are not
  one cross-plane transaction. Irreversible external effects are not
  exactly-once.
- Arbitrary Python executor/verifier callbacks are not automatically Kernel
  sandbox-isolated.
- VPG history writes are incremental, while full projection derive/validate/
  hash work remains. Entity deletion tombstones are not implemented.
- Context VM remains a standalone subsystem and is not wired into the primary
  `AgentOS` facade.
- No statistically powered real-model, real-GPU, or direct competitor
  benchmark is included.

## Unreleased

The repository may contain experimental work beyond `v0.1.0`. Such changes
must not be described as a stable compatibility contract until a new release
is cut and its release gates are rerun.
