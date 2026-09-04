# LongHorizonOS Implementation Progress

**Timestamp:** 2026-08-16 (final documentation sync, Asia/Shanghai)  
**Workspace:** local `LongHorizonOS-main` checkout  
**Release boundary:** experimental single-host research alpha (`v0.1.x`)

## Two-day delivery target

Deliver a defensible initial result for graph-driven long-horizon compute
management: keep the real Scheduler/Claim/Attempt/Lease/Evidence authority path,
close the most visible stale-computation/workspace gaps, expose bounded online
control loops, and publish exact tests without claiming production readiness.

## Implemented in the latest slice

- **Mediated workspace commit fence:** reads made through
  `WorkspaceProvenanceGateway` are re-read immediately before semantic commit.
  Changed, deleted, unavailable, or truncated byte validation fails closed as
  stale/unavailable cognition. This is a single-host point-in-time check, not a
  filesystem lock or a workspace/Facts/VPG transaction.
- **Caller-owned workspace watch loop:** `WorkspaceWatchLoop` repeatedly invokes
  one existing watcher/supervisor step for an explicit `max_steps` budget. It
  creates no daemon, background task, universal watcher, or distributed leader.
- **Bounded live Context handoff:** explicit live `REBASE`/`FULL_RELOAD` now
  persists a handoff intent, fences/releases the old Claim, admits a fresh
  Attempt, detaches the old Harness binding, and supports same-plan replay.
  The caller must register a new Harness for the replacement Attempt and use
  `recover_handoff(...)` when recovery is required. It remains
  release-then-acquire and is not cross-plane atomic.
- **Resource-policy fail-closed correction:** a candidate whose access set is
  unknown is deferred while any active Attempt occupies the epoch. Once active
  occupancy drains, it may run only as the sole serial candidate. The focused
  resource-policy file passes **8 tests**.
- **Graph-utility frontier ranking:** `FrontierPolicy` keeps the historical
  repair-first lexical order by default and adds explicit opt-in
  `ranking_strategy="graph_utility"`. Within the graph-relative frontier it
  prefers critical-path tasks, earlier critical-path positions, and larger
  immediate downstream-unlock value. It remains advisory and does not bypass
  Scheduler admission or infer hidden dependencies.
- **Real wall-clock adaptive runtime gate:** the new
  `wallclock-adaptive-runtime` benchmark executes the public
  `AgentOS.run_async -> Scheduler -> Claim -> Kernel Lease -> AsyncWorkerPool
  -> verifier -> VPG Evidence` path with actual `asyncio.sleep` work. The
  stable result is 3 -> 2 epochs and 1 -> 0 Scheduler resource rejections for
  the same four-task VERIFIED Goal. Local elapsed time is reported only as an
  informational observation, not a pass/fail performance guarantee.
- **Terminal supervisor status correction:** a terminal `FAILED_CLOSED`
  supervisor no longer reports `STOPPED`; the fail-closed state and error are
  preserved for caller-owned watch loops.
- **Outbox retry determinism:** a zero-delay retry now remains immediately
  eligible at an explicitly supplied logical timestamp even if an async
  publisher yields briefly; positive backoff still uses completion time and
  claim fencing is unchanged.
- Documentation was synchronized across README, issue inventory, implementation
  matrix, roadmap, and status sheets so the above bounded capabilities are no
  longer described as wholly unimplemented.

## Verification evidence

```text
full non-slow: 3341 passed, 1 skipped, 18 deselected, 30 warnings in 472.28s
log: artifacts/full-test-nonslow-20260816-final-after-outbox.log
historical pre-final baseline: 3321 passed, 1 skipped, 18 deselected
log: artifacts/full-test-nonslow-after-live-rebase-watchloop-20260815.log

workspace watcher + caller-owned loop: 30 passed
mediated workspace/provenance integration gate: 53 passed
commit-validation focused slice: 47 tests
live rebase/handoff related integration gate: 44 passed
live rebase focused slice: 27 tests
resource-policy focused file: 8 passed
final combined focused gate: 87 passed
wall-clock + adjacent resource benchmark/CLI gate: 14 passed
outbox regression gate: 13 passed
Ruff lint: passed
compileall: passed
Mypy on the changed FrontierPolicy/AgentOS source: passed
```

Focused counts overlap and must not be added together.

## Still not implemented

- automatic discovery of arbitrary hidden Python/file/browser/API/tool reads;
- a universal or always-on world watcher;
- one atomic transaction spanning Scheduler, Kernel, Harness, VPG, workspace,
  Facts, and irreversible external effects;
- force-kill/process/container isolation for arbitrary callbacks;
- physical CPU/GPU/RAM/VRAM placement, quotas, isolation, fairness, or multi-host
  coordination;
- universal exactly-once semantics for payments, email, deployment, and other
  irreversible side effects;
- statistically powered real-LLM/GPU/competitor performance evidence.

## What can be delivered in two days

A credible **single-host research-alpha demonstration**: graph-relative repair,
mediated stale-cognition rejection, caller-owned online observation/control,
bounded fresh-Attempt live handoff, resource-aware fail-closed batching, and
reproducible synthetic/controlled benchmarks. It cannot honestly become a
production-grade universal Agent OS or distributed physical-resource manager in
two days.

## Immediate next gate

1. Add a small real Harness/provider workload that measures verified progress,
   tokens, context rereads, stale work, and re-execution. A real local
   wall-clock runtime gate now exists, but it still uses deterministic local
   work rather than a model/provider.
2. Keep the next correctness priority on mediated provenance coverage and
   cross-plane recovery/side-effect crash campaigns.
