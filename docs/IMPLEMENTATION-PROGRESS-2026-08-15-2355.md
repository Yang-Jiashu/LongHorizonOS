# LongHorizonOS Implementation Progress

**Reporting slot:** 2026-08-15 23:55 (Asia/Shanghai)  
**Workspace:** local `LongHorizonOS-main` checkout  
**Release boundary:** experimental single-host research alpha (`v0.1.x`)

## Work targeted in this interval

1. Make declared logical resources affect the bounded adaptive execution path.
2. Add an explicit, fail-closed bridge from host telemetry to one logical
   Scheduler pool without claiming physical placement.
3. Produce a reproducible end-to-end resource benchmark and CLI command.
4. Make resource-policy decisions inspectable without copying prompts or
   arbitrary executor payloads.
5. Demonstrate online re-planning after an explicit capacity re-observation.

## Implemented

- `ResourceAwareParallelismPolicy` is integrated into both `AgentOS.run()` and
  `AgentOS.run_async()` behind `adaptive=True, resource_aware=True`.
- The policy combines explicit read/write declarations, task resource vectors,
  logical pool capacity, active access sets, and the parallelism bound. The
  existing Scheduler/Claim/Lease path remains authoritative.
- `HostCapacityPolicy`, `derive_host_capacity(...)`, and
  `AgentOS.apply_host_capacity(...)` provide an explicit telemetry-to-logical
  capacity bridge. Reserve fractions round down; unknown required planes fail
  closed; capacity below live reservations is rejected atomically.
- Per-epoch `resource_audit` (`resource-aware-run-audit.v1`) records bounded
  assignments, decisions, blockers, pool IDs, safety flags, and unavailable
  reasons. It does not expand the durable SchedulingEpoch schema and does not
  retain prompts, Context, metadata, or executor output.
- `lhos benchmark resource-aware-runtime --json` and
  `python -m lhos.benchmarks.resource_aware_runtime` compare a conflict-only
  baseline with resource-aware packing on the same public `run_async` path.
- `examples/resource_replanning_e2e.py` and its test demonstrate:
  `RAM=1000 -> first batch [a,b]`; explicit re-observation/application of
  `RAM=500`; then `[c]`, `[d]` in separate epochs; final Goal closed.
- README (English/Chinese), resource docs, implementation status, issue
  inventory, roadmap, and this matrix now distinguish logical admission from
  physical resource management.

## Verification completed

```text
resource/online focused suite: 38 passed
resource + host-capacity + benchmark + CLI gate: 76 passed
host-capacity focused gate: 17 passed
resource-aware audit/policy/runtime focused gate: 43 passed
ruff on touched code: passed
compileall on SDK: passed
isolated S31b commit-ceiling test: 1 passed
```

Controlled benchmark result:

```text
static conflict-only: 3 epochs, 1 advisory over-capacity proposal,
  1 Scheduler resource rejection, Goal closed
resource-aware:       2 epochs, 0 proposal violations,
  0 Scheduler resource rejections, Goal closed
admitted/executor capacity violations: 0 in both modes
```

## Remaining work

- The final repository non-slow rerun after concurrent edits stopped completed
  with `3307 passed, 1 skipped, 18 deselected, 30 warnings` in `473.58s`.
  The earlier `3304/1 failure` run was a host-loaded intermediate run; its
  S31b median-ceiling failure reproduced as a pass when isolated.
- Physical CPU/GPU/RAM/VRAM placement, isolation, quotas, preemption, shared
  inventory, and multi-host scheduling remain open.
- Automatic hidden provenance discovery, universal world observation,
  cross-plane atomic Harness/VPG/Kernel transactions, irreversible side-effect
  exactly-once, and real-model/GPU evaluation remain open.
- Resource policy is still explicit/advisory; it does not autonomously poll,
  start daemons, select providers, or force-kill arbitrary callbacks.

## Next timing

- **Completed:** final full non-slow rerun and exact result recording.
- **Next implementation slice:** use the resource-replanning E2E as the basis
  for measured verified-progress/token and rework metrics, then prioritize
  mediated provenance coverage and atomic live Harness rebase.
