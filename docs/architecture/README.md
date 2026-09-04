# Architecture

This directory contains the architecture record for LongHorizonOS. Core V1 is
the frozen authority model; the public SDK, CLI, persistence, and benchmark
surfaces are experimental product layers built on top.

## Canonical documents

| Document | Role |
|---|---|
| `LONGHORIZONOS-CORE-V1.md` | Frozen Core V1 specification for the Kernel, Context VM, VPG, Scheduler, and invalidation/repair runtime. |
| `CORE-V1-FREEZE.md` | Freeze record, authority constitution, and compatibility policy. |

If an implementation or product document appears to contradict the frozen
authority rules, the Core specification wins. Product-layer documents may
describe capabilities added without changing those authorities.

## Core V1 subsystem map

The frozen subsystems are:

- **Kernel** — deterministic Process/Action/Journal, capabilities, exclusive
  Leases, fencing, versioned Artifact FS, and namespaces.
- **Verified Progress Graph (VPG)** — evidence-backed semantic validity,
  exact-version applicability, deterministic `READY` frontier, causal
  invalidation, Repair Frontier, and Goal closure.
- **Multi-Agent Scheduler** — deterministic eligibility/matching, Claims and
  Attempts backed by Kernel leases, retries, logical resource admission, and
  reconciliation. The current implementation also provides an optional durable
  event journal, projection snapshot, hash-chain verification, and active
  resource-reservation recovery.
- **Causal Invalidation / Local Repair** — version-aware invalidation over
  dependency edges, preservation of unaffected verified work, and selective
  re-verification.

The SDK adds a bounded asynchronous worker path (`AgentOS.run_async`) and
strictly typed logical `ResourceVector` requests without moving semantic
authority out of the VPG.

## Authority and closed loops

- **VPG** is the only semantic authority and the source of the scheduling
  frontier. It derives readiness, verification, staleness, Goal lifecycle,
  invalidation, and repair from graph structure plus durable facts.
- **Scheduler** is policy, not semantic authority or execution ownership. It
  consumes the live VPG frontier and Kernel eligibility state, performs
  deterministic matching, and atomically admits complete logical resource
  vectors.
- **Kernel** owns execution resources and exclusive Lease fencing. A Claim row
  is a rebuildable projection of ownership, not a second authority.
- **Agent/tool** performs one attempt and produces Artifact/Verification/
  Evidence inputs; it cannot directly set semantic state.

Execution closes through:

`VPG frontier -> Scheduler Claim + logical reservation -> Kernel Lease ->
Agent/tool -> verifier-backed Evidence -> VPG`.

World changes close through:

`Artifact version change -> stale cone -> Goal reopen -> minimum Repair
Frontier -> fresh Evidence -> Goal closure`.

## Persistence and scope

VPG stores immutable per-version projection snapshots and incremental
changed-entity revisions. The Scheduler can persist Claims, Attempts, match
decisions, idempotency keys, and lifecycle events in a SQLite event/hash-chain
projection. A file-backed `AgentOS` uses durable SQLite state; reopening a
manifest is read-only observability, not arbitrary process checkpoint/restore.

The prototype is single-host and assumes one Scheduler writer. It does not
provide physical CPU/GPU/RAM/VRAM enforcement, distributed consensus, external
side-effect exactly-once, production sandbox isolation, or a web dashboard.

## Not part of Core V1

Real model providers, browser drivers, production shell drivers, coding-agent
UX, web UI, distributed scheduling, and future SDK/CLI UX are product/ecosystem
layers rather than Core V1 contracts. Their presence in this repository does
not imply production support.

## Where the code lives

- `src/lhos/agent_os/` — execution plane and Kernel services.
- `src/lhos/runtimes/verified_progress/` — VPG semantic control plane.
- `src/lhos/runtimes/multi_agent/` — Scheduler, Claims, Attempts, resources,
  worker pool, and durable Scheduler state.
- `src/lhos/runtimes/invalidation/` — causal invalidation and local repair.
- `src/lhos/sdk/` — developer-facing AgentOS facade and DTOs.
- `src/lhos/cli/core.py` — Core V1 observability and explicit VPG lifecycle CLI.
