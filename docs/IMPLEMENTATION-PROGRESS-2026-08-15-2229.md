# LongHorizonOS Implementation Progress

**Timestamp:** 2026-08-15 22:29 (Asia/Shanghai)  
**Workspace:** local `LongHorizonOS-main` checkout  
**Release boundary:** experimental single-host research alpha

## This interval's implementation targets

- Make bounded stale-cognition repair available on both sync `run()` and async
  `run_async()` paths.
- Keep every replacement Attempt behind the normal Scheduler → Claim → Kernel
  Lease → Context → verifier → VPG Evidence authorities.
- Close URI-only provenance gaps without guessing arbitrary external URI
  identities.
- Preserve old `RunResult.meta` for ordinary calls.
- Add tests for deterministic hashes, URI-only guards, budget exhaustion, and
  Harness-path adaptive execution.

## Implemented

- Sync and async `AgentOS` execution now support:
  - commit-time stale read-set quarantine;
  - explicit-manifest/authoritative-Facts refresh;
  - bounded fresh Attempt/Context VM redispatch;
  - exact old Claim/Lease fencing and normal replacement admission;
  - bounded decision/replacement audit metadata.
- Automatic repair is fail-closed for missing manifests, unknown/hidden reads,
  unversioned/unhashed reads, ambiguous manifest refs, and exhausted budgets.
- Decision hashes and replacement manifest IDs are stable across equivalent
  manifest-ref ordering.
- `vpg://...` URI-only bindings safely derive artifact identity in both planner
  and commit-time guard; HTTP/workspace/arbitrary external URIs remain
  unguessable and fail closed.
- Harness adaptive benchmark runs through the real SDK ownership path and
  compares explicit ConflictGraph policy against a static baseline. Default
  usage is synthetic and clearly labeled; provider plugins are opt-in.
- Ordinary sync/async result metadata compatibility is retained unless repair
  is exercised or its policy is explicitly changed.

## Current verification

- Focused rebase/context/async/adaptive/Harness/CLI gate: **62 passed**.
- Ruff on touched source/tests: **passed**.
- `compileall` on touched modules: **passed**.
- Latest complete repository-wide non-slow run after the sync/URI patches:
  `3257 passed, 1 skipped, 18 deselected, 30 warnings` in `478.12s`;
  `artifacts/final-test-nonslow-20260815-final-sync.log`.
- CLI artifacts:
  - `artifacts/online-supervisor-20260815-final.json`
  - `artifacts/online-compute-20260815-final.json`
  - `artifacts/harness-adaptive-20260815-final.json`

## Not implemented / not claimed

- Universal hidden dependency/provenance discovery.
- Atomic Scheduler/Kernel/Harness/VPG handoff or live third-party Harness
  rebase.
- Force-kill isolation for arbitrary Python callbacks.
- Physical CPU/GPU/RAM/VRAM placement, quotas, or distributed consensus.
- Exactly-once irreversible external effects.
- Automatic measured model/provider/context/verifier routing.
- Real statistically powered LLM/GPU/competitor evaluation.

## Next milestone

Run the final full non-slow suite after the sync/URI patches, refresh all status
counts, and publish a concise release note that distinguishes:

> Harness makes long-running Agents possible; LongHorizonOS makes long-running
> Agent computation efficient.

The current vertical slice demonstrates graph-driven selective repair and
bounded online compute control; it does not yet constitute a general-purpose
production Agent OS.
