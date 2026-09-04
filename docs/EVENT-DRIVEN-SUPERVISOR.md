# Bounded Event-Driven Supervisor

**Status:** implemented bounded vertical slice (August 15, 2026)

`EventDrivenSupervisor` is the first explicit bridge between the existing
caller-driven `execute_online_epoch(s)` APIs and the event-driven control-loop
model:

```text
caller
  │
  ├─ start()
  ├─ submit(event)
  ├─ step()
  │    ├─ observe RuntimeState
  │    ├─ poll/route an explicit workspace watcher (optional)
  │    ├─ validate SemanticInterrupt graph/version
  │    ├─ reconcile/plan through existing authorities
  │    ├─ execute one bounded online epoch
  │    └─ observe again
  └─ stop()
```

## Public API

```python
from lhos.sdk import EventDrivenSupervisor

supervisor = agent_os.event_supervisor(
    goal,
    max_epochs=8,
    max_concurrency=2,
    max_dispatches_per_epoch=2,
    max_parallelism=2,
    resource_aware=True,
)

supervisor.start()
supervisor.submit({"kind": "tick", "event_id": "operator-tick"})
step = await supervisor.step()
supervisor.stop("operator-request")
```

The same object supports `await supervisor.run(max_steps=N)` and an explicit
async iterator:

```python
async for step in supervisor:
    print(step.status, step.graph_version)
```

`EventDrivenSupervisor(agent_os, goal, ...)` remains available for embedding
integrations that do not use the SDK factory. Both forms are inert until the
caller explicitly starts/steps them.

## Safety and scope

The supervisor is **caller-owned and bounded**:

- no background thread, daemon, implicit task, or hidden retry loop;
- each step re-observes the authoritative VPG projection;
- duplicate event IDs are idempotent only when their content fingerprint is
  identical; conflicting duplicates fail closed;
- stale graph IDs/versions, blocked watcher routes, observation failures, and
  execution failures enter `FAILED_CLOSED`;
- the pending event queue and epoch count have explicit budgets;
- `stop()` is idempotent and does not release Claims or Leases.

It composes the existing `WorkspaceObservationWatcher`,
`SemanticInterruptPolicy`, and `AgentOS.execute_online_epoch(...)`. It does
**not** claim work itself, force-kill Python callbacks, perform automatic
Context rebase, or establish an atomic Scheduler/Kernel/Harness ownership
transaction. Those remain separate bounded primitives/open work.

When `resource_aware=True`, each step forwards `max_parallelism` and the
current explicit logical pool capacity into the normal
`run_async -> Scheduler -> Claim/Lease` authority path.  The supervisor does
not poll host telemetry itself; callers must explicitly re-observe and apply a
new capacity between steps.  See `RESOURCE-REPLANNING-E2E.md`.

## Test evidence

Focused regression:

```text
tests/sdk/test_event_supervisor.py
8 passed
```

The combined online-control gate (epoch controller, computation controller,
multi-epoch execution, and supervisor) passes:

```text
37 passed
```

## Reproducible SDK demo

The bounded control loop can be exercised without an API key:

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m lhos.cli.core demo online-supervisor --json
```

The demo uses `AgentOS`, `Goal`, a deterministic local executor, and scripted
verifiers.  It explicitly shows:

```text
start -> observe -> execute epoch (prepare)
      -> execute epoch (validate)
      -> execute epoch (publish) -> CLOSED
```

The JSON projection includes graph versions, dispatched and verified task IDs,
epoch statuses, and the terminal stop reason.  It is a **bounded caller-owned
vertical slice**: `daemon_started` is always `false`, `uses_llm` is always
`false`, and the result must not be interpreted as a claim of always-on
supervision, GPU/VRAM telemetry, arbitrary Python isolation, or production
throughput.
