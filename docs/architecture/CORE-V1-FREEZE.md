# LongHorizonOS — Core Architecture V1 Freeze (Decision Record)

## Why freeze
LongHorizonOS reached an architecture that is state-centric, two-plane, single
authority per fact (authority constitution), import-clean (no cross-plane private
imports), and parameterized by 35 consolidated invariants.  2349 tests pass and
0 fail at this commit; ruff/format/mypy/diff are clean; NoGraph, single-agent VPG,
Context VM neutrality, and worker replaceability are all demonstrated.

## Frozen version
- Freeze commit / tagged HEAD: `680ff76` (+ this milestone's doc commit)
- tags: `longhorizonos-core-v1` (milestone); phase tags
  `longhorizonos-phase-d1/d2/d3-stable-v1` remain immutable at `3d56054`/`8f88797`/`680ff76`.

## What is frozen
- Layer model (core-v1-layer-model.md): microkernel → system services → semantic
  runtimes, downward DAG, L5/L6 minimal/future.
- Two-plane architecture (two-plane-architecture.md): execution plane governs
  operational truth; semantic control plane governs derived semantic truth.
- Authority constitution (core-v1-authority-constitution.md): no fact has two
  authorities; ownership=Kernel Lease, semantic truth=VPG, content/version=Artifact
  FS, repair=D3; applicability=derived.
- Contracts: evidence-and-verification, scheduler, invalidation-and-repair,
  context-vm, runtime-modes, dependency-rules, independence-contracts,
  worker-replaceability-contract, state-centric-thesis.
- 35 core invariants (core-v1-invariants.md).
- Canonical diagram + state flow + canonical recovery/repair demo + projection
  rebuild proof.

## What is NOT frozen (out of Core V1)
- legacy plane (LEGACY / OUT-OF-SCOPE)
- real model providers, browser driver, production shell driver, coding-agent UX,
  web UI, distributed cluster, multi-host, LLM repair planner, contradiction
  solver, future product SDK / CLI UX.

## Authority constitution (summary)
A single authority per fact (see core-v1-authority-constitution.md).  Key:
Kernel Lease = ownership; VPG = semantic truth; Artifact FS = content/version;
D3 = repair derivation; Context VM = what process sees; claims/projections are
caches.

## 35 Core invariants
See core-v1-invariants.md — all PASS in the audited plane.

## Known non-blocking debt (documented, accepted)
- 3 MEDIUM: NodeValidity two-deriver (adjudicated: VPG is the single semantic
  authority; D3 recomputes/propagates); `verified_artifact_versions` snapshot
  redundancy (DERIVED/index, rebuildable); test-only Agent OS internal seam
  (TEST-ONLY ARCHITECTURE EXCEPTION).
- 3 DOC-ONLY: legacy-plane boundary statement; README tagline ("Agent OS" vs
  whole system); "three graphs one journal" → persistence-model terminology.
- 0 BLOCKER, 0 HIGH.

## Legacy boundary
Legacy plane is OUT-OF-SCOPE for Core V1; not deleted/merged; Core V1
compatibility does not cover it.

## Future change policy
See core-v1-compatibility-policy.md: Core-breaking changes require a Core v2
proposal + independent audit.
