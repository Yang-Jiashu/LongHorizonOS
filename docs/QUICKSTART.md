# LongHorizonOS Quickstart

This guide goes from a source checkout to a verified Goal, a recovery-and-
repair run, bounded asynchronous execution, and durable operator inspection.
Python 3.11+ is required; all examples below are offline and need no API key.

## 1. Install from source

```bash
git clone https://github.com/Yang-Jiashu/LongHorizonOS.git
cd LongHorizonOS
python -m pip install .
```

For editable development:

```bash
python -m pip install -e ".[dev]"
```

Check the installed command:

```bash
lhos --help
```

## 2. Run the flagship closed loop

```bash
lhos demo recovery-repair
lhos demo recovery-repair --json
```

The deterministic demo executes real SDK/Core paths:

1. Evidence-backed Goal closure;
2. worker/ownership recovery;
3. an Artifact version change;
4. causal invalidation while preserving unrelated verified work;
5. the graph-derived Repair Frontier;
6. fresh exact-version Evidence and Goal reclosure.

The JSON form is intended for scripts and CI. It includes fields such as
`crash_recovered`, `affected_tasks`, `preserved_tasks`, `repair_frontier`,
`repair_attempts`, and `final_closed`.

## 3. Minimal synchronous SDK example

The exact runnable file is `examples/quickstart/hello_world.py`:

```bash
python examples/quickstart/hello_world.py
```

Its essential code is:

```python
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

with AgentOS(":memory:") as runtime:
    runtime.add_agent(Agent("coder", specializations=("python",)))

    goal = Goal("Hello")
    goal.task(
        "Write hello",
        agent="coder",
        verify=scripted_executor(artifact_id="hello.txt", version=1),
    )

    result = runtime.run(goal, max_dispatches=4)
    print(result.task_states, result.verified)
```

`AgentOS` composes the Kernel, VPG, Scheduler, and repair runtime. A Claim and
Kernel Lease are acquired before an executor runs. A verifier returns an
Artifact version and pass/fail result; Core attaches Evidence and derives
`VERIFIED`. The SDK never trusts an executor's successful return by itself.

`scripted_executor` is deterministic demo/test plumbing, not a model client.
For useful work, provide an `Agent.executor` and an independent `Task.verify`,
or use the shell/workspace/Git/OpenAI-compatible integrations.

## 4. Bounded asynchronous multi-agent execution

`AgentOS.run_async(...)` overlaps independent executors while keeping semantic
commits and ownership checks authoritative:

```python
import asyncio

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome


async def execute(task_id: str) -> None:
    await asyncio.sleep(0.05)  # replace with model/tool work


def verify(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"{task_id}.txt",
        version=1,
        content="verified output",
    )


async def main() -> None:
    with AgentOS(":memory:") as runtime:
        runtime.add_agent(
            Agent(
                "worker",
                executor=execute,
                max_concurrency=2,
                resource_capacity={
                    "cpu_millis": 2_000,
                    "ram_bytes": 2_000_000_000,
                    "gpu_count": 1,
                    "vram_bytes": 8_000_000_000,
                    "model_slots": {"local-model": 2},
                },
            )
        )

        goal = Goal("Parallel verified work")
        for task_id in ("A", "B"):
            goal.task(
                task_id,
                agent="worker",
                verify=lambda task_id=task_id: verify(task_id),
                resources={
                    "cpu_millis": 500,
                    "ram_bytes": 256_000_000,
                    "model_slots": {"local-model": 1},
                },
            )

        result = await runtime.run_async(goal, max_concurrency=2)
        print(result.goal_state, result.verified)


asyncio.run(main())
```

The Scheduler admits the complete resource vector atomically and releases it
on success, failure, cancellation, or reconciliation. These are **logical
per-Agent capacity reservations**; they do not measure or enforce physical
host CPU, RAM, GPU, or VRAM usage. `Task.verify` may be synchronous or async in
`run_async`; the synchronous `run()` path still rejects an async verifier.

## 5. Reproduce the benchmark gates

```bash
lhos benchmark semantic-repair --quick
lhos benchmark async-agentos
python scripts/benchmark_multi_agent_runtime.py --check
python scripts/benchmark_vpg_incremental_history.py --check
```

The semantic-repair benchmark measures correctness and selective work saved
against full restart and an oracle task-DAG checkpoint. The async benchmark is
an end-to-end public-SDK microbenchmark with controlled I/O-shaped tasks. The
VPG benchmark measures durable changed-entity history growth. Read the
measurement contracts before quoting results:

- `docs/benchmarks/SEMANTIC-REPAIR.md`
- `docs/benchmarks/ASYNC-AGENTOS.md`

## 6. Durable run and read-only inspection

Use a file-backed database when another process must inspect the run:

```python
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

with AgentOS("run.db") as runtime:
    runtime.add_agent(Agent("coder"))
    goal = Goal("Ship hello")
    goal.task(
        "Write hello",
        agent="coder",
        verify=scripted_executor(artifact_id="hello.txt", version=1),
    )
    runtime.run(goal, max_dispatches=4)
    runtime.save_run("run.json")
```

Inspect the saved manifest:

```bash
lhos status --state run.json --goal "Ship hello"
lhos inspect --state run.json --goal "Ship hello" task "Write hello"
lhos graph --state run.json --goal "Ship hello"
```

`AgentOS.save_run()` stores the database path, Agent configuration, and Goal
manifest. `AgentOS.open_run()` reopens it for **read-only observability**; it
does not resume arbitrary Python call stacks or execute work.

## 7. VPG history lifecycle

History retention is an explicit operator action:

```bash
lhos vpg history --db run.db --graph GRAPH_ID --json
lhos vpg compact --db run.db --graph GRAPH_ID \
  --retain-from 2 --actor operator --reason "retention policy" --yes --json
```

Snapshot-less legacy projections default to a read-only preview:

```bash
lhos vpg migrate-legacy --db legacy.db --graph GRAPH_ID --json
```

To write the migration, reuse the preview's exact version and projection hash
and provide an operator identity and reason:

```bash
lhos vpg migrate-legacy --db legacy.db --graph GRAPH_ID \
  --trust-projection \
  --expected-current-version VERSION \
  --expected-projection-hash HASH \
  --actor operator --reason "verified backup" --json
```

Compaction is destructive and requires `--yes` plus a verified checkpoint.

## 8. Important boundaries

- This is an experimental **single-host** systems prototype, not a distributed
  cluster scheduler or production Agent OS.
- Resource vectors are logical per-Agent pools; there is no hardware telemetry,
  physical isolation, quota/RPM/TPM accounting, preemption, fairness, or
  starvation guarantee.
- Durable Scheduler replay uses an append-only event/hash chain and projection
  snapshot, but assumes one writer and does not provide leader election or
  multi-writer fencing.
- `run_async` gives real bounded executor overlap, while Evidence/VPG commits
  are serialized within one invocation.
- Lease-to-VPG commits are fenced on the main SDK path, but Facts, Action,
  Claim completion, Lease release, driver effects, and external systems are not
  one cross-plane transaction; irreversible external effects are not
  exactly-once.
- Arbitrary Python executor/verifier callbacks are not Kernel sandbox-isolated.
- VPG history writes are incremental, but full projection derivation,
  validation, and hashing remain; deletion tombstones are not implemented.

## 9. Next steps

- `docs/CONCEPTS.md` — execution plane vs semantic control plane.
- `docs/sdk/PUBLIC-API.md` — experimental SDK surface.
- `docs/architecture/LONGHORIZONOS-CORE-V1.md` — frozen Core contract.
- `docs/demos/RECOVERY-REPAIR.md` — demo mechanics and assertions.
- `docs/benchmarks/SEMANTIC-REPAIR.md` — benchmark protocol and limits.
- `docs/benchmarks/ASYNC-AGENTOS.md` — async benchmark protocol and limits.
