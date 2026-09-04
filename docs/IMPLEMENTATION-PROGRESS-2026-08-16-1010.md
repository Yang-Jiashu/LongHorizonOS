# LongHorizonOS Implementation Progress

**Timestamp:** 2026-08-16 10:10 +08:00  
**Release boundary:** experimental single-host research alpha (`v0.1.0`)

## Current conclusion

The bounded two-day research-alpha target is implemented and release-testable.
The complete LongHorizonOS research vision is not finished and must not be
presented as a production, distributed, physical-resource Agent OS.

## Implemented

- Versioned VPG validity, exact-version Evidence, causal invalidation, repair
  frontier, and Goal reclosure.
- Scheduler Claim/Attempt ownership joined to Kernel Lease fencing.
- Durable single-host Scheduler/VPG recovery and normalized large-state
  persistence.
- Context VM snapshots on the SDK execution path and durable Agent cognition
  snapshots.
- Mediated workspace provenance plus commit-time read-set freshness
  validation; stale known cognition cannot commit Evidence.
- Graph-relative `RuntimeStateView`, critical-path/parallel/repair projections,
  explicit Conflict Graph, and resource-aware bounded batching.
- Default-off scheduling epochs, caller-owned event supervisor/watch loop, and
  cooperative identity-fenced semantic interrupt delivery.
- Bounded live `REBASE`/`FULL_RELOAD` handoff to a fresh Attempt with durable
  recovery intent and idempotent replay.
- Opt-in `graph_utility` FrontierPolicy ranking inside the safe frontier.
- Advisory cognitive-locality/model/context/verifier routing plus an opt-in
  provider adapter.
- Logical typed CPU/GPU/RAM/VRAM/model-slot admission and an explicit
  host-telemetry-to-one-pool capacity bridge.
- Fenced Action recovery primitives and Transactional Outbox at-least-once
  delivery; zero-delay async failure retries are immediately eligible under
  an explicit logical clock.

## Verification completed

```text
full non-slow:
3341 passed, 1 skipped, 18 deselected, 30 warnings in 472.28s

log:
artifacts/full-test-nonslow-20260816-final-after-outbox.log

ruff:
passed

mypy src/lhos --ignore-missing-imports:
passed, 273 source files

compileall:
passed

wheel build:
dist-final-20260816/lhos-0.1.0-py3-none-any.whl
```

The wheel was installed from outside the repository into a fresh Python 3.11
virtual environment:

- `lhos demo recovery-repair --json`: exit 0, crash recovered, 3 repairs,
  1 unrelated task preserved, final Goal closed.
- `lhos benchmark wallclock-adaptive-runtime --json`: exit 0, same VERIFIED
  Goal; static 3 epochs / 1 resource rejection versus adaptive 2 epochs /
  0 resource rejections. The observed local speedup in this run was about
  1.22x and is informational only.

## Still open

- Automatic discovery of arbitrary hidden semantic/file/network/tool
  dependencies and a universal world watcher.
- An atomic transaction spanning Scheduler, Kernel, Harness, VPG, Facts,
  workspace, and external effects.
- Sink-enforced exactly-once irreversible side effects.
- Killable process/container isolation for arbitrary Agent callbacks.
- Physical CPU/GPU/RAM/VRAM placement, isolation, quotas, fairness, and
  topology-aware scheduling.
- Distributed scheduling, leader election, and multi-host execution.
- Automatic utility-learned provider/model/context/verifier routing.
- Statistically powered real-LLM/GPU and direct-competitor evaluation.

## Delivery timing

- **Now:** publishable as a clearly labeled single-host research alpha with
  reproducible offline demos, controlled benchmarks, and an installable wheel.
- **Next 1-2 days:** add one small real-provider/Harness evaluation and record
  tokens, context rereads, stale work, wall time, cost, and verified progress.
- **Subsequent research milestones:** universal provenance, atomic external
  effect protocols, process isolation, physical resource scheduling, and
  distributed execution each require separate design and validation work;
  they are not credible two-day features.

