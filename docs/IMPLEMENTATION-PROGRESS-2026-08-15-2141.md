# LongHorizonOS Implementation Progress

**Timestamp:** 2026-08-15 21:41 (Asia/Shanghai)  
**Scope:** current local checkout, single-host research alpha

## What this push was meant to implement

1. Make stale-cognition repair usable on the main SDK execution path.
2. Exercise the Harness boundary with a reproducible adaptive-vs-static benchmark.
3. Preserve existing SDK metadata compatibility.
4. Keep provenance and repair decisions deterministic and fail-closed.
5. Record reproducible tests and CLI evidence without claiming production GPU or
   distributed scheduling.

## Implemented in this push

- Bounded automatic stale-cognition repair in `run_async()`:
  - commit-time read-set freshness failure quarantines the old Attempt;
  - old Claim/Lease ownership is released through the existing lifecycle;
  - an explicit `ContextManifest` is refreshed from authoritative Facts;
  - a fresh Attempt and Context VM snapshot are admitted through the normal
    Scheduler/Kernel path;
  - the verifier and VPG Evidence commit remain authoritative.
- Fail-closed boundaries:
  - hidden, unknown, unversioned, or unhashed reads block automatic repair;
  - missing/ambiguous explicit manifests block automatic repair;
  - repair is bounded by `max_automatic_rebase_dispatches`;
  - this is a fresh Attempt retry, not atomic live Harness handoff.
- Deterministic automatic-rebase decision/replacement hashes are canonicalized
  independent of manifest tuple order.
- `vpg://artifact` URI-only read bindings can derive an artifact identity; other
  external URI schemes remain fail-closed.
- Existing default async `RunResult.meta` shape is preserved unless repair is
  exercised or repair policy is explicitly changed.
- Harness adaptive benchmark and CLI are present:
  `python -m lhos.cli.core benchmark harness-adaptive --json`.
  The default provider is deterministic with synthetic usage accounting; an
  explicit provider factory can report observed usage.

## Verification completed

- Focused automatic-rebase and integration gate: passed.
- Current full non-slow suite:
  `3252 passed, 1 skipped, 18 deselected, 30 warnings`
  (`artifacts/final-test-nonslow-20260815-final5.log`).
- Ruff on touched source/tests: passed.
- `compileall` on touched modules: passed.
- Editable package install with `pip install -e . --no-deps`: passed.
- CLI smoke artifacts:
  - `artifacts/online-supervisor-20260815-final.json`
  - `artifacts/online-compute-20260815-final.json`
  - `artifacts/harness-adaptive-20260815-final.json`

## Still not implemented

- Universal automatic provenance/dependency discovery for arbitrary Python,
  browser, filesystem, API, or implicit semantic reads.
- Atomic Scheduler/Kernel/Harness/VPG ownership transfer.
- Force-kill isolation for arbitrary in-process callbacks.
- Physical CPU/GPU/RAM/VRAM placement and quota-aware cluster scheduling.
- Distributed multi-host coordination/consensus.
- Exactly-once fencing for irreversible external side effects.
- Automatic provider/model/context/verifier routing driven by measured utility.
- Statistically powered real-LLM/GPU workload evaluation.

## Next implementation window

- **Next 2-4 hours:** documentation/status synchronization and one reproducible
  real-provider plugin example (opt-in, no credentials committed).
- **Next 1-2 days:** expand Harness scenarios and measure real provider usage,
  rework, context rereads, and verified-progress-per-cost.
- **Research phase:** automatic provenance discovery, atomic cross-plane handoff,
  physical resource scheduler, and distributed runtime.

The current release boundary remains **experimental single-host research alpha**;
the implemented vertical slice is a concrete foundation for the stated
“Graph represents evolving computation; the OS continuously schedules from the
Graph” thesis, not a claim that all OS-level guarantees are complete.
