# Architecture paths and authority map

This page is a code-oriented map for contributors. Paths below are relative to
the repository root. “Authority” means the component that is allowed to decide
the fact; projections and adapters may observe it but must not replace it.

## Composition root

```text
public SDK
  src/lhos/sdk/os.py                  AgentOS
    -> src/lhos/runtimes/verified_progress/sdk.py
    -> src/lhos/runtimes/multi_agent/scheduler.py
    -> src/lhos/agent_os/sdk/client.py
    -> src/lhos/sdk/providers.py      protocol adapters
```

`AgentOS` is a facade, not a second semantic store. It creates the Kernel,
Facts provider, VPG runtime, Scheduler, and registered Agent descriptors.

## End-to-end paths

### Goal compilation and readiness

```text
Goal/Task DTO
  src/lhos/sdk/goal.py
  src/lhos/sdk/task.py
        |
        v
AgentOS._compile_goal()
  src/lhos/sdk/os.py
        |
        v
GraphPatchProposal / AddNodeOp / AddEdgeOp
  src/lhos/runtimes/verified_progress/patches.py
  src/lhos/runtimes/verified_progress/patch_validator.py
        |
        v
GraphStore.submit_patch()
  src/lhos/runtimes/verified_progress/graph_store.py
        |
        v
projection + readiness/closure derivation
  src/lhos/runtimes/verified_progress/{projections,readiness,closure}.py
```

**Current boundary:** `depends_on` edges are supplied by the caller. There is
no automatic complete provenance capture during compilation or execution.
Large goals are compiled through the trusted large-operation path in
`AgentOS._submit_compiled_ops()` when they exceed the ordinary patch limit.
The resulting publication is one GraphStore transaction and one graph-version
increment; this is not a distributed publication protocol.

### Scheduling and ownership

```text
VPG ready_frontier()
  src/lhos/sdk/providers.py::VPGFacade
        |
        v
MultiAgentScheduler.schedule_once()/run_pass()
  src/lhos/runtimes/multi_agent/scheduler.py
  src/lhos/runtimes/multi_agent/{eligibility,matching,requirements}.py
        |
        v
Kernel LeaseAdapter + AtomicResourceManager
  src/lhos/runtimes/multi_agent/{lease_adapter,resources}.py
  src/lhos/agent_os/services/lease_service.py
        |
        v
Claim / Attempt projections
  src/lhos/runtimes/multi_agent/{claims,attempts,models}.py
```

VPG owns semantic readiness; Scheduler owns policy and logical admission;
Kernel leases are the ownership linearization point. The current durable
Scheduler store (`src/lhos/runtimes/multi_agent/durable_state.py`) assumes one
writer; it does not provide distributed leader election or multi-writer CAS.

### Synchronous execution

```text
AgentOS.run()
  -> scheduler.run_pass()
  -> _execute_and_verify()
  -> _invoke_executor()
  -> _invoke_verifier_sync()
  -> _commit_verified_outcome()
  -> _attach_evidence()
  -> VPG patch / derived closure
```

### Asynchronous execution

```text
AgentOS.run_async()
  -> scheduler.run_pass() for bounded batches
  -> AsyncWorkerPool
  -> _SDKExecutorDispatcher.dispatch()
       -> (explicit opt-in) ComputeProviderRegistry.adapt_context()
       -> (explicit opt-in) ComputeProviderRegistry.execute_async()
  -> _invoke_executor_async()
  -> _invoke_verifier_async()
  -> AgentOS.deliver_interrupt(...) [optional live control path]
       -> graph/epoch/claim/task/attempt identity fence
       -> CooperativeCancellationToken
       -> REQUESTED/DELIVERED/OBSERVED/CANCELLED journal phases
  -> semantic_commit_lock (one SDK instance)
  -> interrupt/read-set commit fence
  -> _commit_verified_outcome()
```

Executor overlap is real for the controlled I/O benchmark, but Evidence/VPG
commits are serialized within one `run_async` call. Synchronous callbacks run
in `asyncio.to_thread`; cancelling the await does not kill arbitrary Python
code. A token-aware async executor can be cooperatively interrupted through
`AgentOS.deliver_interrupt(...)`; ignored or late completions are quarantined
before semantic Evidence commit. Secure capability mediation, automatic
watchers/Context rebase, Harness-session orchestration, and killable sandbox
workers remain future work.

When a task explicitly enables
`metadata["compute_routing"]["provider_routing"]["enabled"]` and the
`AgentOS` instance has a `ComputeProviderRegistry`, registered provider hooks
run inside this existing Claim/Context/Worker path. They cannot create Claims
or Leases, bypass the verifier/Evidence fence, or select physical resources.

### Evidence and artifact facts

```text
executor/verifier result
  -> AgentOS._attach_evidence()
  -> FactsProvider.commit_action()
  -> AttachArtifactOp / AttachEvidenceOp
  -> GraphStore + VPG verification/closure
```

`src/lhos/sdk/providers.py::FactsProvider` persists SDK artifact facts and
synthetic action facts. The Artifact service and Kernel Action service also
exist under `src/lhos/agent_os/{artifacts,services}`, but arbitrary executor
side effects are not automatically forced through those services.

### Invalidation and repair

```text
AgentOS.repair()
  -> snapshot_projection()
  -> construct InvalidationCause from artifact/version arguments
  -> run_invalidation_engine()
       src/lhos/runtimes/invalidation/{evidence,cone,frontier,engine}.py
  -> InvalidationResult / RepairFrontier (pure derivation)
  -> refresh_derived_state()
  -> later AgentOS.run() attempts repair
```

The invalidation runtime is pure and graph-relative. It correctly propagates
along declared `DEPENDS_ON` edges and preserves unrelated verified branches.
The public repair path persists the D3 result in the same GraphStore
transaction as the graph refresh. Reopen/replay validates the durable envelope,
append-only identity, corruption checks, and graph-version CAS; the SDK still
exposes a task-level repair summary rather than a universal cross-plane event
log.

## Authority table

| Fact | Authoritative implementation | Current caveat |
|---|---|---|
| Process/Action state, leases, fencing | `src/lhos/agent_os/services/*` and Kernel adapters | Driver-side effects may occur outside the Action boundary. |
| Artifact bytes, hashes, versions | `src/lhos/agent_os/artifacts/*` plus SDK `FactsProvider` | Repair API can still accept caller-supplied integer versions. |
| Graph nodes/edges/GraphVersion | `src/lhos/runtimes/verified_progress/graph_store.py` | Large Goal publication is atomic within one trusted GraphStore transaction; no distributed commit. |
| `READY`, `VERIFIED`, `STALE`, Goal closure | VPG projections/readiness/closure | Validity is only as complete as observed bindings. |
| Eligibility, matching, claims, attempts | `src/lhos/runtimes/multi_agent/*` | Completion needs explicit claim/attempt/evidence binding. |
| Causal cone and repair frontier | `src/lhos/runtimes/invalidation/*` | Minimum is relative to the accepted graph; D3 rows/envelope are durable and reopen-validated. |
| Context materialization | `src/lhos/agent_os/context/*` + SDK Attempt adapter | Bounded Context VM snapshots are materialized/fenced on `AgentOS.run()`/`run_async()`; this is not a sandbox or hidden-read tracer. |
| Compute-routing advisory/adapter | `src/lhos/sdk/compute_routing.py`, `src/lhos/sdk/provider_routing.py`, and `AgentOS.plan_compute_routing` | Explicit version-pinned recommendations plus an opt-in registered provider hook adapter after Claim/Context setup; no automatic provider selection, process lifecycle, physical placement, or Scheduler mutation. |
| Live semantic interrupt routing | `src/lhos/sdk/os.py::AgentOS.deliver_interrupt` + `src/lhos/runtimes/multi_agent/worker_pool.py` | Explicit graph/epoch/claim/task/attempt fence and cooperative token path; no force-kill or watcher-driven orchestration. |

## Target mediated path and remaining closure

The target closed loop is:

```text
Agent/Tool
  -> ExecutionContext (capability-scoped)
  -> Artifact/HTTP/Tool gateway records read/write observations
  -> real Kernel Action with claim/attempt/epoch/fencing token
  -> external sink idempotency/CAS or UNCERTAIN reconciliation
  -> Evidence commit containing provenance digest
  -> VPG invalidation/closure
```

The Context VM page-binding bridge and the root-confined
`WorkspaceProvenanceGateway` now implement bounded mediated portions of this
path: materialized pages and gateway reads/writes are recorded with exact
versions/hashes. The full Artifact/HTTP/Tool gateway and cross-plane Action
transaction remain targets. A callback that reads an undeclared input outside
these mediated boundaries is outside the soundness envelope and must not be
advertised as automatically tracked.

## Contributor checklist

Before changing a cross-plane path, answer:

1. Which component is authoritative for the fact?
2. Is the operation conditional on `graph_version`, `claim_id`,
   `attempt_id`, and lease generation where applicable?
3. Can a crash occur between each external effect and its acknowledgement?
4. What happens when provenance or ownership is unknown?
5. Is there a focused adversarial test and a reproducible benchmark?
