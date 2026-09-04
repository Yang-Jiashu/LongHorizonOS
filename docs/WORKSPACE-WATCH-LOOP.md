# Caller-owned workspace watch loop

`WorkspaceObservationWatcher` already provides one bounded observation pass:

```text
read declared files -> hash bytes -> issue ObservationToken
                  -> reconcile VPG / plan SemanticInterrupt
```

`WorkspaceWatchLoop` adds only the missing lifecycle seam: it invokes that
existing watcher through `EventDrivenSupervisor` for a caller-selected number
of epochs.

```python
import asyncio
from lhos.sdk import WorkspaceWatchLoop

loop = WorkspaceWatchLoop(supervisor, poll_interval=0.25)
result = await loop.run(
    max_steps=10,
    stop_event=operator_stop_event,
    execute=True,
)
print(result.stop_reason, result.polls_completed)
```

The loop is deliberately **caller-owned and bounded**:

- `max_steps` is mandatory; `0` performs no poll;
- `poll_interval` is a sleep between completed steps (it is not a filesystem
  event subscription);
- `stop_event` is optional and is checked before each poll and while sleeping;
- the loop stops when the supervisor becomes terminal;
- no thread, daemon, background task, hidden retry, or process is created;
- a stopped supervisor remains stopped, and a caller can choose
  `stop_supervisor=False` when handling a stop event itself.

Each iteration calls exactly one:

```python
await supervisor.step(poll_workspace=True, execute=...)
```

Consequently, Claims, Leases, VPG reconciliation, and cooperative interrupt
delivery remain owned by their existing authorities. The loop does not add a
new semantic graph or bypass those fences.

## Scope and boundaries

The underlying watcher observes only explicitly registered files through a
root-confined `WorkspaceTool`. Content SHA-256 is the change identity; mtime is
not treated as semantic truth. Create, modify, and delete transitions are
reported. This is a **single-process polling primitive**, not:

- universal filesystem, API, browser, requirement, or world observation;
- automatic provenance discovery for direct `open`, subprocess, or network I/O;
- force-kill or preemption of non-cooperative Harnesses;
- a cross-plane Scheduler/Kernel/Harness/VPG transaction;
- a distributed watcher or leader-elected service.

For a one-shot route use `AgentOS.poll_workspace_and_route(...)`. For repeated
caller-owned epochs use `WorkspaceWatchLoop`.
