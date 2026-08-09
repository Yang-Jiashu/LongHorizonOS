# LongHorizonOS — Quickstart

This guide takes you from installing the package to running a Goal that a real
Kernel-backed scheduler claims, executes, and verifies — approximately five
minutes, no API key required.

## 1. Install

Requires Python 3.11+.

```bash
pip install .                       # from a source checkout
# or, from a built release artifact:
pip install dist/lhos-0.1.0-py3-none-any.whl
```

For development (editable install with the test/lint/dev toolchain, plus a
console script on `PATH`):

```bash
make install
```

## 2. Hello world

Save this file (it is the exact program at
`examples/quickstart/hello_world.py`) as `hello.py`:

```python
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

os_ = AgentOS(":memory:")                                   # composition root
os_.add_agent(Agent("coder", specializations=("python",)))  # real Kernel process

goal = Goal("Hello")
goal.task("Write hello", agent="coder",
          verify=scripted_executor(artifact_id="hello.txt", version=1))

result = os_.run(goal, max_dispatches=4)   # Scheduler claims + dispatches
print(result.task_states, result.verified)
```

Run it:

```bash
python hello.py
```

Expected output:

```
{'Write hello': 'verified'} ['Write hello']
```

What happened under the hood. `AgentOS(":memory:")` is the composition root: it
wires the frozen Core V1 subsystems together — the Kernel (real processes,
actions, leases), the Verified Progress Graph (VPG), the multi-agent scheduler,
and the invalidation/repair runtime. `add_agent(Agent(...))` registers a real
Kernel process and a scheduler descriptor for it. `goal.task(...)` appends a Task
to the VPG DAG. `os_.run(...)` drives the scheduler to claim and dispatch the
readiness frontier, runs the executor, attaches evidence, and lets VPG derive a
validity. Because the task has a verifier (`scripted_executor`), it closes
**VERIFIED**.

### Fail-closed by design

A Task with no verifier stays **UNVERIFIED**. The SDK never sets a task
VERIFIED on its own: validity is only ever derived by Core VPG from attached
evidence. If you want a Task to reach VERIFIED you must attach a `Verifier`
(callback, command, or scripted) that produces evidence.

## 3. A two-task pipeline

A slightly larger example shows a dependency edge, a second agent, and the
readiness frontier:

```python
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

os_ = AgentOS(":memory:")
os_.add_agent(Agent("writer", specializations=("python",)))
os_.add_agent(Agent("checker", specializations=("python",)))

goal = Goal("Pipeline")
first = goal.task(
    "draft", agent="writer",
    verify=scripted_executor(artifact_id="draft.txt", version=1),
)
goal.task(
    "review", agent="checker", depends_on=(first,),
    verify=scripted_executor(artifact_id="review.txt", version=1),
)

result = os_.run(goal, max_dispatches=8)
print(result.task_states)
```

Expected output:

```
{'draft': 'verified', 'review': 'verified'}
```

The `depends_on=(first,)` edge creates a DAG dependency: `review` is only READY
once `draft` is VERIFIED, and `review` is claimed only after `draft`'s ownership
is released. The scheduler derives the deterministic readiness frontier from the
VPG graph; it does not run tasks in an ad-hoc order.

## 4. The command line

A Core-native CLI is installed as `lhos`. The flagship demo replays a real
worker crash and the D3 local repair with no source changes:

```bash
lhos demo recovery-repair          # deterministic, no API key
lhos demo recovery-repair --json   # machine-readable summary
```

There are a few read-only inspection verbs. All of them require a `--state`
path to an existing manifest saved by `AgentOS.save_run`:

| Command | What it shows |
|---------|---------------|
| `lhos status --state <manifest>` | per-Task semantic status + Goal state |
| `lhos inspect task <task-id> --state <manifest>` | a single Task and its evidence |
| `lhos inspect evidence <evidence-id> --state <manifest>` | a single Evidence record |
| `lhos graph --state <manifest>` | the VPG dep graph as ASCII tree |

The legacy spec-20 CLI remains reachable via `lhos legacy` but is out of Core V1
scope.

## 5. Next steps

- `docs/CONCEPTS.md` — the execution-plane vs semantic-control-plane mental model.
- `docs/sdk/PUBLIC-API.md` — the public SDK surface and its stability contract.
- `docs/architecture/LONGHORIZONOS-CORE-V1.md` — the frozen Core V1 specification.
- `docs/demos/RECOVERY-REPAIR.md` — the crash-recovery + local-repair demo.
