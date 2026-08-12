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

`run_async` overlaps independent executor calls subject to the global limit,
each Agent's `max_concurrency`, and atomic logical resource admission. It
revalidates Claim/Lease ownership before committing Evidence, releases claims
on success/failure/cancellation/reconciliation, and serializes Evidence/VPG
commits within one invocation to avoid graph-version races. `Task.verify` may
be synchronous or async in `run_async`; the synchronous `run()` path rejects
async verifiers. Asynchronous Agent executors are supported.

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

## Internal (not public)

`lhos.sdk.os._compile_goal`, builder internals, `Goal.compile`, and provider
adapter internals remain importable for power users but are not a stable
developer contract.

## Before SDK 1.0

- Decide whether to provide a `longhorizonos` package alias.
- Stabilize the provider/tool gateway and callback isolation contract.
- Publish compatibility, migration, and distributed-fencing guarantees.
