# LongHorizonOS — Public SDK (E1) — API

**Status: experimental SDK v0.x.** This is not SDK 1.0: names, defaults, and
semantics may change without a backward-compatibility guarantee. The facade
drives the frozen Core V1; it does not replace the VPG, Scheduler, or Kernel
authorities.

## Classification

- **PUBLIC (v0.x experimental):** objects intended for application code.
- **EXPERIMENTAL:** the facade may change in a future release.
- **INHERITED (Core):** `lhos.agent_os`, `lhos.runtimes.verified_progress`,
  `lhos.runtimes.multi_agent`, and `lhos.runtimes.invalidation` retain the
  classifications documented in
  [Core V1 Freeze](../architecture/CORE-V1-FREEZE.md).

## Public objects

| Symbol | Purpose | Stability |
|---|---|---|
| `lhos.sdk.AgentOS` / `OS` | Composition root for Kernel, VPG, Scheduler, and repair runtime | EXPERIMENTAL |
| `lhos.sdk.Agent` | Agent descriptor plus optional sync/async executor | EXPERIMENTAL |
| `lhos.sdk.Goal` | Goal/task builder compiled into a real VPG graph | EXPERIMENTAL |
| `lhos.sdk.Task` | Task builder with dependencies, verifier, and resource request | EXPERIMENTAL |
| `AgentOS.add_agent(agent)` | Register an Agent and its logical capacity | EXPERIMENTAL |
| `AgentOS.goal(goal_id, tasks=())` | Create/register a Goal builder | EXPERIMENTAL |
| `AgentOS.run(goal, ...)` | Synchronous scheduling and verification loop | EXPERIMENTAL |
| `AgentOS.run_async(goal, ...)` | Bounded concurrent executor loop; semantic commits remain authoritative | EXPERIMENTAL |
| `AgentOS.deliver_interrupt(goal_or_graph, ...)` | Exact-identity cooperative `preempt`/`rebase` delivery to a live async SDK attempt | EXPERIMENTAL |
| `AgentOS.workspace_gateway(workspace, context, task=None, ...)` | Capability-scoped workspace I/O with provenance recording and Facts-backed version authority | EXPERIMENTAL |
| `AgentOS.runtime_state(goal)` | Immutable four-plane `GlobalRuntimeState` projection | EXPERIMENTAL |
| `AgentOS.plan_frontier(goal, ...)` | Read-only WHAT/WHEN frontier suggestion | EXPERIMENTAL |
| `AgentOS.plan_compute_routing(goal, candidate, agents=(), ...)` | Bounded advisory cognitive-locality/compute-routing decision | EXPERIMENTAL |
| `AgentOS.plan_live_context_rebase(goal, ...)` | Authority-backed, read-only plan for one exact live Claim/Attempt/Harness context decision | EXPERIMENTAL |
| `AgentOS.apply_live_context_rebase(plan)` | Bounded async application of a live plan; only safe `REUSE` control is applied | EXPERIMENTAL |
| `AgentOS.repair(goal, ...)` | Invalidate stale evidence and derive affected/preserved/frontier sets | EXPERIMENTAL |
| `AgentOS.status(goal)` | Read-only `StatusSnapshot` | EXPERIMENTAL |
| `AgentOS.save_run(path)` | Save a durable DB/Goal/Agent manifest for inspection | EXPERIMENTAL |
| `AgentOS.open_run(path)` | Reopen a manifest for read-only observability | EXPERIMENTAL |
| `lhos.sdk.RunResult` | Structured run outcome | EXPERIMENTAL |
| `lhos.sdk.RepairOutcome` | Structured invalidation/repair outcome | EXPERIMENTAL |
| `lhos.sdk.StatusSnapshot` / `StatusView` | Read-only state views (`render_ascii`) | EXPERIMENTAL |
| `lhos.sdk.VerificationOutcome` | Verifier result bound to an exact Artifact version | EXPERIMENTAL |
| `lhos.sdk.scripted_executor` | Deterministic no-key demo/test verifier | EXPERIMENTAL |
| `lhos.sdk.callback_verifier` | Wrap a synchronous verifier callback | EXPERIMENTAL |
| `lhos.sdk.command_verifier` | Run a policy-controlled command and return Evidence input | EXPERIMENTAL |
| SDK errors (`AgentOSError`, `ConfigurationError`, …) | Typed error taxonomy | EXPERIMENTAL |

`ResourceVector` is part of the Scheduler model and is currently imported from
`lhos.runtimes.multi_agent`, not from `lhos.sdk`:

```python
from lhos.runtimes.multi_agent import ResourceVector
```

## Core types and contracts

### `Agent`

```python
Agent(
    name: str,
    *,
    executor: Callable | None = None,
    specializations: tuple[str, ...] = ("python",),
    supported_task_kinds: tuple[str, ...] | None = None,
    supported_tools: tuple[str, ...] | None = None,
    capabilities: tuple[str, ...] | None = None,
    max_concurrency: int = 4,
    cost_weight: float = 1.0,
    model: str | None = None,
    resource_capacity: ResourceVector | dict | None = None,
)
```

`executor(task_id)` may be synchronous or asynchronous. `run()` rejects an
async executor and tells callers to use `await run_async(...)`. `model` is
metadata only; it does not instantiate a provider client.

### `Task` and `Goal`

```python
goal.task(
    task_id,
    *,
    agent="",
    depends_on=(),
    verify=None,
    task_kind="task",
    required_specializations=None,
    required_tools=(),
    max_attempts=3,
    metadata=None,
    resources=ResourceVector(...) | dict | None,
)
```

`resources` is validated strictly and compiled into Scheduler metadata. Scalar
fields are integer quantities (`cpu_millis`, `ram_bytes`, `gpu_count`,
`vram_bytes`); `model_slots` is a named integer map. The complete vector is
admitted atomically against the selected Agent's logical capacity.

### `VerificationOutcome`

A verifier must return:

```python
VerificationOutcome(
    passed: bool,
    artifact_id: str,
    version: int,
    content: str | None = None,
    evidence_note: str = "",
    details: dict[str, Any] = ...,
)
```

`version` is an exact Artifact version. A pass does not directly set
`VERIFIED`; the SDK commits Verification/Evidence and VPG derives semantic
validity. A Task without applicable Evidence remains unverified (fail closed).

### `AgentOS`

```python
AgentOS(
    db_path: str = ":memory:",
    *,
    facts: FactsProvider | None = None,
    read_only: bool = False,
)
```

For a file-backed `db_path`, VPG state and the Scheduler's event/projection
state use durable SQLite storage. Recovery is logical metadata/projection
recovery: it does not restore arbitrary Python memory, call stacks, or an
in-flight executor.

```python
runtime.run(
    goal,
    *,
    max_dispatches: int = 8,
    max_steps: int = 20,
) -> RunResult

await runtime.run_async(
    goal,
    *,
    max_dispatches: int = 8,
    max_steps: int = 20,
    max_concurrency: int = 4,
) -> RunResult
```

For a live `run_async()` batch, an explicit semantic decision can be routed to
the exact running attempt:

```python
delivery = runtime.deliver_interrupt(
    goal,
    claim_id=claim_id,
    task_id=task_id,
    attempt_id=attempt_id,
    action="rebase",  # or "preempt"
    expected_graph_version=graph_version,
    expected_semantic_epoch=semantic_epoch,
    interrupt_id="api-change-18",
    decision_hash=decision_hash,
    reason="API artifact changed",
)
```

The method validates graph, claim, task, attempt, and semantic-epoch identity
before routing a cooperative token to a `context_v1` async executor. Legacy
callbacks are reported as non-preemptible rather than being force-killed.
Transition phases are journaled through the Scheduler. The async SDK also
rechecks the token at the verifier-to-Evidence semantic commit fence, so a late
or ignored interrupt cannot publish `VERIFIED` Evidence. This is not automatic
world watching, Context rebase, Lease handoff, or third-party Harness
orchestration.

For a caller that already has an exact live Claim/Attempt/Harness binding, the
SDK also exposes a bounded Context-rebase façade:

```python
plan = runtime.plan_live_context_rebase(
    goal,
    task_id=task_id,
    claim_id=claim_id,
    agent_id=agent_id,
    graph_delta=explicit_graph_delta,
    target_graph_version=next_graph_version,
    context_snapshot=sealed_context_snapshot,  # optional, when available
    reason="API artifact changed",
)
outcome = await runtime.apply_live_context_rebase(plan)
```

`plan_live_context_rebase(...)` reads and cross-checks the exact active
Claim/Lease, Scheduler Attempt, durable `AgentSnapshot`, and registered Harness
session. The graph delta and Context snapshot are explicit inputs; hidden reads
and world changes are not discovered here. It returns the immutable
`lhos.sdk.LiveContextRebasePlan` DTO. An explicit `target_graph_version` must
equal the current authoritative VPG version; an invented future version or a
stale prior version is refused.

`apply_live_context_rebase(...)` revalidates those identities before entering
Harness code and returns `lhos.sdk.LiveContextRebaseApplyResult`. In the current
single-host alpha, a `REUSE` decision may issue a normal fenced Harness
`START`/`CONTINUE`; applying the exact same plan again uses the existing
request-idempotency result, sets `replayed=True`, and does not execute the
Harness hook twice. The replay exception applies only to the exact cached
`(claim_id, request_id)` after the graph, Claim/Lease, Attempt, process,
AgentSnapshot, Context, and Harness-session identities have been revalidated.
A `REBASE` or `FULL_RELOAD` decision is deliberately
returned as `refused=True` with `ownership_unchanged=True`, because the runtime
does not yet provide an atomic Scheduler/Kernel/Harness Claim/Lease handoff.
The façade never mutates VPG or Context VM, releases a Claim, fabricates a
replacement owner, or claims to be an always-on automatic rebase loop.

`run_async` overlaps independent executor calls subject to the global limit,
each Agent's `max_concurrency`, and atomic logical resource admission. It
revalidates Claim/Lease ownership before committing Evidence, releases claims
on success/failure/cancellation/reconciliation, and serializes Evidence/VPG
commits within one invocation to avoid graph-version races. `Task.verify` may
be synchronous or async in `run_async`; the synchronous `run()` path rejects
async verifiers. Asynchronous Agent executors are supported. If a cooperative
interrupt arrives while verification is still running, the commit fence
quarantines the attempt instead of allowing stale Evidence to close the task.

Both `run()` and `run_async()` enable one bounded automatic fresh-Attempt
repair by default (`automatic_rebase=True`,
`max_automatic_rebase_dispatches=1`). It is attempted only after commit-time
read-set validation marks an Attempt `STALE_COGNITION`, and only when an
explicit `ContextManifest` can be refreshed from authoritative version/hash
Facts. The old Claim/Lease is released and the replacement is admitted through
the normal Scheduler/Kernel path. Unknown, hidden, unversioned, unhashed, or
ambiguous reads fail closed. This is not in-place cognition mutation, live
external-Harness rebase, or an atomic cross-plane ownership transaction.

The bounded compute-routing facade is advisory and deterministic:

```python
decision = runtime.plan_compute_routing(
    goal,
    candidate={
        "task_id": "frontend",
        "criticality": 2,
        "downstream_fanout": 3,
        "input_stability": 0.9,
        "required_bindings": [
            {
                "resource_uri": "artifact://api",
                "version": 17,
                "content_hash": "sha256:...",
            }
        ],
    },
    agents=(),  # optionally explicit RegisteredAgentMetadata values
    reuse_threshold=0.60,
    max_context_budget_tokens=128_000,
)
```

The result contains provider-independent `REUSE_AGENT`/`FRESH_AGENT`,
`CHEAP`/`STANDARD`/`STRONG` model-tier, bounded context-budget, and
`LIGHT`/`STANDARD`/`STRONG` verification-strength labels plus audit reasons.
Only exact, non-stale, version-pinned bindings count toward locality. The
method is read-only: it does not start or reuse a process, claim work, acquire
a Lease, dispatch a provider, mutate Context, or alter `run()`/`run_async()`.
Omitting `agents` is conservative and uses only active cognition already
observed in `RuntimeStateView`; it does not infer hidden dependencies.

### `AgentOS.workspace_gateway`

`workspace_gateway(...)` composes the mediated
`WorkspaceProvenanceGateway` with the current SDK `FactsProvider`:

```python
gateway = runtime.workspace_gateway(
    workspace,
    context,                         # secure context requires context_v1
    task=task,                       # derive capabilities from task inputs/outputs
    strict=True,
)
payload = gateway.read_bytes("input.txt", version=4)
```

Callers may instead pass explicit `readable=`/`writable=` capability URIs when
no task is supplied. In strict mode, a positive `version=` is accepted only
when the exact bytes are validated by `version_validator(snapshot)` or an
authority exposing `read_hash(pid, uri, version)`. The `AgentOS` helper
automatically injects its durable `FactsProvider` when no explicit authority
is supplied; it never auto-registers an unseen version or treats a caller
integer as semantic authority. Thus an unseen strict version fails closed.
`strict=False` retains the explicitly documented compatibility behavior and may
record `version_source="caller"`. Reads/writes remain root-confined,
capability-scoped, hashed, and mediated; direct `open`/Path, network, browser,
subprocess, or other hidden I/O is outside coverage.

```python
runtime.repair(
    goal,
    *,
    artifact_id: str | None = None,
    new_artifact_version: int | None = None,
) -> RepairOutcome
```

Repair requires a graph-referenced Artifact. It records an exact
`old_version -> new_version` cause, computes the causal stale cone, preserves
unaffected verified tasks, and returns the minimum repair frontier.

### Durable manifests

```python
runtime.save_run("run.json")
observer = AgentOS.open_run("run.json")
```

`open_run` is read-only and intended for `status`, `inspect`, and `graph`
surfaces. It must not be treated as a checkpoint for arbitrary user code.

## Authority and guarantees

- VPG is the only semantic authority: it derives `READY`, `VERIFIED`, `STALE`,
  and Goal closure.
- Scheduler owns eligibility, deterministic matching, Claims, retries, and
  logical resource admission; it does not decide semantic truth.
- Kernel Leases own execution authority and fencing.
- Agents/tools perform attempts and produce verifier-backed facts; they cannot
  self-assert final semantic validity.
- The main SDK Lease-to-VPG Evidence commit is fenced against release and
  reassignment races.

## Explicit non-guarantees

- Resource vectors do not provide physical host/device enforcement or telemetry.
- No distributed multi-writer Scheduler, leader election, preemption, fairness,
  quota/RPM/TPM, or starvation guarantee is included.
- Facts, Action, Claim completion, Lease release, driver effects, and external
  systems are not one cross-plane transaction.
- External irreversible side effects are not exactly-once fenced.
- Arbitrary Python callbacks are not automatically sandbox-isolated.
- VPG history writes are incremental, but full projection derive/validate/hash
  work remains; deletion tombstones are not implemented.
- Compute-routing labels are policy recommendations, not concrete model names
  or provider dispatch. No main-path token, wall-clock, dollar, rework, or
  verification-cost improvement is claimed yet.

## Internal (not public)

`lhos.sdk.os._compile_goal`, builder internals, `Goal.compile`, and provider
adapter internals remain importable for power users but are not a stable
developer contract.

## Before SDK 1.0

- Decide whether to provide a `longhorizonos` package alias.
- Stabilize the provider/tool gateway and callback isolation contract.
- Publish compatibility, migration, and distributed-fencing guarantees.
