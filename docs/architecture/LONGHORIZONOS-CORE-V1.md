# LongHorizonOS — Core Architecture V1 (Canonical Specification)

> LongHorizonOS is a state-centric agent operating architecture built around a
> deterministic execution plane and an evidence-backed semantic control plane.

## 1. Thesis
Agents are persistent state-transition systems, not stateless model loops.  The
execution plane governs processes, actions, capabilities, resource ownership,
persistent artifacts, and bounded materialized context.  The semantic control
plane governs verified progress, logical readiness, deterministic multi-agent
scheduling policy, version-aware invalidation, and minimal local repair.

## 2. System Model
Instances are processes in a microkernel that journal actions, own resources via
exclusive leases, and persist versioned artifacts; a Verified Progress Graph
decides semantic truth; a scheduler decides who should attempt; evidence decides
completion; on change, D3 recomputes what ceases to be true and derives a minimal
repair frontier.

## 3. Two-Plane Architecture
Trusted Execution Plane (operational truth) + Semantic Control Plane (derived
semantic truth & progress), connected only by public Protocols/Adapters/SDK
(provider injection). See `docs/CONCEPTS.md` for the operational-vs-semantic
authority model.

## 4. Layer Model
Microkernel (L2) → System Services (L3: Artifact FS+Namespace, Context VM,
storage/drivers) → Semantic Runtimes (L4: VPG, Scheduler, Invalidation) →
Harness/Integration (L5, minimal) → Applications/Product (L6, future).  Downward
DAG; L6 absent at freeze.

## 5. Execution Plane
`agent_os/*`: Process (PCB/ProcessState), Action (ACB/ActionState), Journal
(append-only KernelEvent), Capability, ResourceLease (exclusive ownership),
Signal, Checkpoint.  Operational history is the Journal.

## 6. Artifact / Namespace Model
Artifact FS is a content-addressed CAS; `ArtifactVersionBinding` pins an exact
version (no "latest" alias).  NamespaceService maps canonical URIs.  Artifact FS
is content/version authority; art history immutable.

## 7. Context VM
Version-bound content materialization (ContentRef/Manifest/Page/WorkingSet/
LoadedContext/Snapshot).  Graph-neutral and task-neutral (decides HOW to fit, not
WHAT). The subsystem is implemented independently but is not yet wired into the
primary `AgentOS` composition root.

## 8. Verified Progress Graph
`runtimes/verified_progress`: nodes (Goal/Task/ArtifactRef/Verification/Evidence)
+ edges (DEPENDS_ON/PRODUCES/VERIFIES), patch-committed, GraphVersion monotonic.
Semantic authority for READY / VERIFIED / STALE / Goal CLOSED (derived).

## 9. Evidence Semantics
Evidence is immutable historical fact bound to exact ArtifactVersions.  Current
applicability is derived.  Old Evidence cannot silently validate a new version.
VERIFIED requires dependencies valid + evidence obligations satisfied +
current-applicable Evidence binding the exact current ArtifactVersion + constraints.

## 10. Multi-Agent Scheduler
`runtimes/multi_agent`: eligibility (deterministic over live Kernel facts),
matching (deterministic best-fit), claims (ownership via Kernel Lease), attempts
(verification-aware).  Never owns semantic truth; never sets READY/VERIFIED.

## 11. Ownership Semantics
Task ownership linearizes on successful Kernel exclusive ResourceLease acquisition
(`vpg://<gid>/task/<tid>/claim`).  Claim rows are projections/caches of the Lease.
`READY != CLAIMED != VERIFIED`.

## 12. Invalidation / Local Repair
`runtimes/invalidation`: evidence applicability loss (over ArtifactVersion truth)
→ deterministic causal cone (only DEPENDS_ON descendants) → preserved unaffected
work → minimal Repair Frontier → D2 scheduling → new Evidence → closure restored.
D3 never claims/dispatches, never marks VERIFIED, never mutates Artifact/Evidence.

## 13. Authority Constitution
One authority per fact: Process/Action Leases/Artifacts/Namespace → execution
plane; GraphVersion/validity/READY/VERIFIED/Goal → VPG; eligibility/matching →
D2; cone/Frontier/applicability → D3 (derived); claim row/verified_artifact_versions/
projections → derived/index.

## 14. Runtime Modes
NoGraph; Single-Agent Verified Graph; Multi-Agent Verified Graph (+ optional D3).
Each valid independently.  Legacy workflow is out-of-scope.

## 15. Failure / Recovery
Crash → process FAIL/EXIT → Kernel releases/fails lease → reconcile marks claim
LOST → reassignment.  SIGKILL 120/120 recovery byte-identity.  Semantic state
persists; worker is replaceable.

## 16. Core Invariants
35 invariants across Execution/Authority/Persistence/Artifact/Context/Semantic/
Scheduling/Invalidation-Layering — all PASS in the recorded Core V1 audit.

## 17. Dependency Rules
Lower must not import higher; Kernel must not know Task/Goal/VPG; Context VM
graph-neutral; VPG not require D2; D2 not require D3 internals; D3 not own
Scheduler/Kernel resources; harness depends downward via SDK/Protocol/Adapter.
Cross-plane edges are provider-injected.

## 18. Compatibility Rules
Core-breaking changes (authority moves, VERIFIED/READY derivation changes,
ownership away from Kernel Lease, VPG↔D2 coupling, D3 claim/dispatch, Context VM
semantic, NoGraph loss, worker non-replaceable, RepairFrontier→arbitrary planner)
require a Core v2 proposal + independent audit.

## 19. Legacy Boundary
The legacy plane (`graph`,`runtime`,`agents`,`domain`,`ports`,`infrastructure`,
`verification`,`benchmarks`,`cli`) is import-disjoint and
**LEGACY / OUT-OF-SCOPE-FOR-CORE-V1**.  Not deleted, not merged, not covered by
Core V1 compatibility.

## 20. Non-Goals
No general belief revision, no semantic contradiction solver, no distributed
repair cluster, no multi-host consensus, no automatic LLM self-repair planner, no
production autonomous self-healing.  These are declared not-implemented and
outside Core V1.
