# LongHorizonOS — Core Concepts

LongHorizonOS is a state-centric agent operating architecture. It rests on a
single, deliberate split: a **deterministic execution plane** that actually runs
work, and an **evidence-backed semantic control plane** that decides what work
is valid, ready, or in need of repair. Understanding that boundary is the key to
the whole system, so we start there.

## The two planes

The execution plane is the microkernel: it supplies processes, actions, an
append-only journal, capabilities, resource leases, artifacts, namespaces, and
context. It is deterministic — given the same inputs and the same sequence of
operations it produces the same outputs — and it is crash-resilient. The control
plane is where meaning lives. A Worker does not "finish" a Task in the sense that
matters; it produces state, and the control plane decides what that state means
for progress. The two never collide: the control plane reads evidence and the 
live Kernel state, but it never reaches into kernel internals to overwrite them.

## A Goal is a graph

A Goal is a DAG of Tasks. Each Task names an agent (or specialization), carries
its dependencies via `depends_on`, and optionally carries a verifier. The
dependency edges mean "this Task cannot be considered done until its
dependencies are." The system never appeals to a fixed FIFO order or a human
triage list: readiness is derived from the graph and from current facts alone.

## Evidence and exact-version binding

Progress is not asserted, it is evidenced. When a Task runs, its executor can
produce an artifact — a content-addressed, immutable, versioned file. A verifier
(scripted, callback, or command) then produces Evidence that names the exact
artifact **and its exact version**. Verification is exact-version bound: evidence
recorded against `draft.txt@2` means nothing for `draft.txt@3`. This is what
makes later invalidation sound — an old claim does not quietly float onto newer
content.

## Validity: VERIFIED, UNVERIFIED, STALE

Every Task has a validity derived by the Verified Progress Runtime (VPG). A Task
is **VERIFIED** when derivation concludes there is current, applicable evidence
for it. It is **UNVERIFIED** while it has no such evidence — including the
default state of any Task that never attached a verifier (the SDK is
fail-closed: it never sets VERIFIED on its own, and never fabricates
ownership). It is **STALE** when it was once VERIFIED but its evidence has lost
current applicability, for example because a newer artifact version superseded
the exact version the evidence was bound to.

## The readiness frontier

From the graph and the current evidence, the runtime derives a **deterministic
readiness frontier**: the set of Tasks that are now ready to run because all
their dependencies are VERIFIED, they have no active claim, and a capable agent
is available. Determinism matters — the frontier ordering is a total, stable
order (by priority, then graph depth, then creation order, then id), so
independent observers and replay logs agree on what happens next even when
running in separate processes.

## Claiming is a linearization point

Dispatching is not best-effort bookkeeping; it is ownership. The multi-agent
scheduler acquires an **exclusive ResourceLease** on each Task claim before it
dispatches a Worker. Because lease acquisition is the kernel's linearization
point for exclusive ownership, two concurrent schedulers can never both claim
the same Task — exactly one wins. That same ownership authority is why a crash
can be recovered: if a claiming process dies (even by SIGKILL), the scheduler
can reassign the Task only after ownership is honestly released or recovered.

## Crash recovery

LongHorizonOS is designed so that a Worker dying with `SIGKILL` is an ordinary
case, not an edge case. The journal is append-only, artifact writes are atomic
and content-addressed, and the scheduler reconciles against live Kernel state.
A Task whose owner vanished becomes schedulable again only after ownership is
released or recovered, and stale owners cannot commit a terminal Kernel Action
state. This does not make driver-side irreversible effects exactly-once: those
sinks do not yet consume the fencing token or perform their own CAS.

## Causal invalidation and local repair

Invalidation is version-aware and causal. When an artifact version changes, the
evidence bound to the superseded version loses current applicability. The system
propagates that only along the causal cone: every Task that transitively depends
on the affected outcome becomes STALE, while unrelated VERIFIED work is left
untouched. The critical guarantee is preservation — a change in one corner of a
verified graph should not force the whole graph to rerun.

From the STALE set the runtime derives a **minimal Repair Frontier**: the
smallest set of affected Tasks that are immediately executable because their
dependencies are currently VERIFIED and they have no active claim. Only the
current frontier is dispatched in that scheduling step. As upstream Tasks are
re-verified, downstream Tasks can enter later frontiers, so the full affected
cone may rerun before the Goal re-closes. Invalidation derives validity only —
it never claims Tasks, never dispatches Workers, and never mutates artifact or
evidence history.

## Authority lives in Core

Every guarantee above is enforced by the frozen Core V1 subsystems — the Kernel,
the VPG, the multi-agent scheduler, and the invalidation/repair runtime. The
developer-facing SDK is a thin facade over that authority: calling
`AgentOS.repair` or `AgentOS.run` drives the same audited Core, it does not
reimplement progress or ownership. This is why a claim about a Goal can be
trusted: it was produced by the same deterministic derivation that the mutation
and SIGKILL audits exercised.

The formal contract lives in `docs/architecture/LONGHORIZONOS-CORE-V1.md`; the
freeze record that declares each subsystem STABLE is
`docs/architecture/CORE-V1-FREEZE.md`.
