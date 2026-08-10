# LongHorizonOS — Core Architecture V1 Freeze (Decision Record)

## Why freeze
LongHorizonOS reached an architecture that is state-centric, two-plane, single
authority per fact (authority constitution), import-clean (no cross-plane private
imports), and parameterized by 35 consolidated invariants. The original freeze
audit recorded a green test/lint/typecheck pass; current releases must satisfy
`docs/RELEASE-CHECKLIST.md` again rather than relying on that historical result.

## Frozen version
- Freeze commit / tagged HEAD: `680ff76` (+ this milestone's doc commit)
- tags: `longhorizonos-core-v1` (milestone); phase tags
  `longhorizonos-phase-d1/d2/d3-stable-v1` remain immutable at `3d56054`/`8f88797`/`680ff76`.

## What is frozen
- Layer and two-plane model in `LONGHORIZONOS-CORE-V1.md` Sections 3–4.
- Authority constitution in Section 13: no fact has two authorities;
  ownership=Kernel Lease, semantic truth=VPG, content/version=Artifact FS,
  repair=D3, applicability=derived.
- Evidence, scheduler, repair, Context VM, runtime-mode, dependency and recovery
  contracts in Sections 7–17.
- Core invariants summarized in Section 16.
- Canonical diagram + state flow + canonical recovery/repair demo + projection
  rebuild proof.

## What is NOT frozen (out of Core V1)
- legacy plane (LEGACY / OUT-OF-SCOPE)
- real model providers, browser driver, production shell driver, coding-agent UX,
  web UI, distributed cluster, multi-host, LLM repair planner, contradiction
  solver, future product SDK / CLI UX.

## Authority constitution (summary)
A single authority per fact (see `LONGHORIZONOS-CORE-V1.md` Section 13). Key:
Kernel Lease = ownership; VPG = semantic truth; Artifact FS = content/version;
D3 = repair derivation; Context VM = what process sees; claims/projections are
caches.

## 35 Core invariants
See `LONGHORIZONOS-CORE-V1.md` Section 16. Every release re-runs the current
test, lint and typecheck gates.

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
See `LONGHORIZONOS-CORE-V1.md` Section 18: Core-breaking changes require a Core
v2 proposal + independent audit.
