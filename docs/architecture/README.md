# Architecture

This directory holds the architecture record for LongHorizonOS, anchored on a
frozen Core V1.

## Canonical documents

| Document | Role |
|----------|------|
| `LONGHORIZONOS-CORE-V1.md` | The canonical, frozen Core V1 specification: the Kernel, the Context VM, the Verified Progress Graph (VPG), the Multi-Agent Scheduler, and Causal Invalidation / Local Repair, plus their public API classification and invariants. |
| `CORE-V1-FREEZE.md` | The freeze record: which artifacts were audited, the architecture-boundary verdict, and the milestone tag `longhorizonos-core-v1`. |

Both are definitive. If a doc elsewhere contradicts them, the spec and freeze
record win.

## Core V1 subsystem map

Each Core subsystem is **STABLE** (frozen). They are the authority in the
system and are described in full in `LONGHORIZONOS-CORE-V1.md`:

- **Kernel** (microkernel) — deterministic Process / Action / Journal, capabilities, exclusive ResourceLease, versioned content-addressed Artifact FS, namespaces.
- **Context VM** — version-bound context snapshots, deterministic working sets, process isolation.
- **Verified Progress Runtime (VPG)** — semantic Task/Goal state machine; evidence-backed VERIFIED derivation; exact-version artifact binding; deterministic READY frontier; atomic optimistic patch commit.
- **Multi-Agent Scheduler** — eligibility from live Kernel state, deterministic best-fit matching, Kernel-backed exclusive TaskClaims, per-agent concurrency, projection/reconciliation/recovery, replayable audit log.
- **Causal Invalidation / Local Repair** — version-aware invalidation cone over DEPENDS_ON edges, preservation of unaffected VERIFIED work, minimal Repair Frontier, incremental re-verification.

## Not part of Core V1

Core V1 is the frozen foundation. On top of it sit evolving product/ecosystem
layers that are explicitly **out of scope** of the freeze: real model
integrations, a developer-facing high-level SDK, the Core-native CLI tooling,
browser tooling, and distributed scheduling. Their presence in this repository
is real, but none of them is a Core V1 contract.

## Where the code lives

Core runtime code is under `src/lhos/agent_os/` (execution plane) and
`src/lhos/runtimes/` (verified_progress, multi_agent, invalidation — control
plane). The developer-facing SDK facade is under `src/lhos/sdk/`, with the
Core-native CLI in `src/lhos/cli/core.py`. See the phase tables in the repository
root `README.md` for a layer-by-layer map of implementation locations.
