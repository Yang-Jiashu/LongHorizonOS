# LongHorizonOS Implementation Progress

**Timestamp:** 2026-08-16 10:23 +08:00  
**Release boundary:** experimental single-host research alpha (`v0.1.0`)

## Status now

The release candidate for the bounded single-host research-alpha target is
complete and install-tested. The larger Mind-VLA design is intentionally not
complete.

- **Research-alpha scope:** approximately **70-75%** implemented.
- **Full long-term Agent-OS vision:** approximately **25-30%** implemented.

These percentages use different denominators. The first measures the explicit
single-host, graph-relative, bounded-control scope currently claimed by the
repository. The second includes automatic provenance, physical resources,
distributed execution, atomic external effects, and real-provider adaptive
optimization.

## Implemented and verified

- VPG validity/versioning, Evidence fencing, causal invalidation, repair
  frontier, and Goal reclosure.
- Scheduler Claim/Attempt plus Kernel Lease ownership and durable replay.
- Context VM snapshots, Agent cognition/read-set snapshots, and mediated
  workspace freshness fencing.
- RuntimeStateView across progress, cognition, context, and logical resources.
- Conflict-safe/resource-aware batching, `graph_utility` frontier ranking,
  bounded scheduling epochs, caller-owned supervisor/watch loop, and
  cooperative identity-fenced interrupts.
- Bounded live `REBASE`/`FULL_RELOAD` handoff to a fresh Attempt with durable
  recovery intent and replay.
- Advisory locality/model/context/verifier routing and opt-in provider adapter.
- Logical resource admission and explicit host-telemetry-to-one-pool mapping.
- Fenced Action recovery and at-least-once Transactional Outbox retry
  semantics, including the zero-delay logical-clock fix.

Verification:

- **3341 passed, 1 skipped, 18 deselected, 30 warnings** in
  `472.28s`.
- `ruff`, `mypy src/lhos --ignore-missing-imports`, `compileall`, and wheel
  build passed.
- Fresh-wheel CLI smoke passed for recovery repair and wall-clock adaptive
  runtime.
- Wheel Python-source parity: **273/273** files match current source hashes.
- README/docs UTF-8 and relative-link checks passed.
- Slow marker suite: **18 tests currently running**; final result pending.

## Still open

The following are not yet universal system guarantees:

- Hidden dependency/provenance discovery and a universal always-on world
  watcher.
- One atomic transaction across Scheduler, Kernel, Harness, VPG, Facts,
  workspace, and irreversible external effects.
- Sink-enforced exactly-once payments, email, deployment, and other
  irreversible side effects.
- Killable process/container isolation for arbitrary callbacks.
- Physical CPU/GPU/RAM/VRAM placement, quotas, fairness, and topology-aware
  scheduling.
- Distributed scheduler, leader election, and multi-host placement.
- Automatic utility-learned model/context/verifier/provider routing.
- Fine-grained Artifact/Evidence repair and statistically powered real
  LLM/GPU/direct-competitor evaluation.

## Next credible milestone

After the slow-test result, the next bounded milestone is one reproducible
real-provider/Harness workload measuring tokens, context rereads, stale work,
re-execution, wall time, cost, and verified progress. The remaining universal
guarantees are separate research projects, not honest same-day features.

