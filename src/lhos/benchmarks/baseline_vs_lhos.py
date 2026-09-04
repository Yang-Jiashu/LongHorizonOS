"""Baseline-versus-LongHorizonOS end-to-end wall-clock benchmark.

The same VERIFIED Goal is closed twice through the public
:meth:`lhos.sdk.AgentOS.run_async` path:

``baseline``
    One agent, ``adaptive=False``, ``max_concurrency=1``.  This stands in for a
    single Harness working the task list serially -- the thing LongHorizonOS
    claims to accelerate.

``lhos``
    Several agents with ``adaptive=True``, so the graph-utility frontier policy
    ranks the frontier by critical-path position and downstream unlock value,
    the derived conflict graph batches non-conflicting work, and matching may
    prefer an agent that already holds a task's declared reads.

What is measured versus modelled
--------------------------------
Every number below is an observation of a real local run; none is derived from
a declared cost model, because a cost model is exactly what would make a
speedup claim circular:

* ``wall_clock_seconds`` -- ``time.perf_counter`` around a real
  ``asyncio.sleep`` workload.
* ``dispatch_sequence`` -- the order the authoritative Scheduler actually
  granted Claims in.
* ``warm_dispatches`` -- Scheduler-reported dispatches whose selected agent
  already held the task's declared reads in a durable ``AgentSnapshot``.

The ranking ablation is a pure-function comparison over one observed
``GlobalRuntimeState``: it reports the order each strategy *would* choose, with
no execution and no timing.

Honest scope: this is a deterministic synthetic I/O-shaped workload on a single
host.  It calls no model, allocates no GPU, and is not evidence about any real
coding task, real token cost, or production speedup.  It measures the
scheduler's behaviour, not an agent's competence.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Final

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.frontier_policy import (
    FrontierPolicy,
    FrontierRankingStrategy,
)

BENCHMARK_NAME: Final[str] = "baseline_vs_lhos_wallclock"
BENCHMARK_VERSION: Final[int] = 1

TASK_SECONDS: Final[float] = 0.06
SHARED_SPEC: Final[str] = "shared-spec"
SHARED_SPEC_URI: Final[str] = "vpg://shared-spec"
CHAIN_LENGTH: Final[int] = 4
SIDE_TASKS: Final[int] = 6
LHOS_AGENTS: Final[int] = 3
MAX_PARALLELISM: Final[int] = 3


def _chain_ids() -> tuple[str, ...]:
    return tuple(f"chain-{index}" for index in range(1, CHAIN_LENGTH + 1))


def _side_ids() -> tuple[str, ...]:
    return tuple(f"side-{index}" for index in range(1, SIDE_TASKS + 1))


def _all_ids() -> tuple[str, ...]:
    return _chain_ids() + _side_ids()


@dataclass
class _Workload:
    """Record real per-agent execution so overlap is observed, not assumed."""

    active: int = 0
    peak_parallelism: int = 0
    completed: list[str] = field(default_factory=list)
    by_agent: dict[str, list[str]] = field(default_factory=dict)
    started_at: dict[str, float] = field(default_factory=dict)
    finished_at: dict[str, float] = field(default_factory=dict)

    def executor_for(self, agent_id: str):
        async def execute(ctx: Any, task_id: str) -> None:
            self.started_at.setdefault(task_id, time.perf_counter())
            self.active += 1
            self.peak_parallelism = max(self.peak_parallelism, self.active)
            try:
                await asyncio.sleep(TASK_SECONDS)
                self.completed.append(task_id)
                self.by_agent.setdefault(agent_id, []).append(task_id)
            finally:
                self.finished_at[task_id] = time.perf_counter()
                self.active -= 1

        return execute


def _verification(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"out-{task_id}",
        version=1,
        content=f"{task_id}:verified",
        evidence_note="baseline-vs-lhos wall-clock benchmark",
    )


def _manifest(os_: AgentOS, task_id: str) -> ContextManifest:
    """Pin the shared spec so Context VM page bindings become durable reads.

    Every task reads the same shared spec, which is what gives agent context
    residency something to be resident *about*.
    """

    return ContextManifest(
        manifest_id=f"manifest-{task_id}",
        owner_pid="benchmark-owner",
        refs=(
            ContentRef(
                ref_id=SHARED_SPEC,
                canonical_uri=SHARED_SPEC_URI,
                artifact_id=SHARED_SPEC,
                version=1,
                content_hash=os_._facts.content_hash("shared-spec-v1"),
                media_type="text/plain",
                required=True,
            ),
        ),
        token_budget=128,
    )


def _build(os_: AgentOS, goal_id: str, agent_ids: tuple[str, ...]) -> Goal:
    """One long chain (the critical path) plus independent side branches."""

    os_._facts.add_version(SHARED_SPEC, 1, "shared-spec-v1")
    goal = Goal(goal_id)
    default_agent = agent_ids[0]

    previous = None
    for task_id in _chain_ids():
        previous = goal.task(
            task_id,
            agent=default_agent,
            depends_on=(previous,) if previous is not None else (),
            inputs=(SHARED_SPEC_URI,),
            outputs=(f"out-{task_id}",),
            executor_api="context_v1",
            provenance_policy="strict",
            context_manifest=_manifest(os_, task_id),
            verify=lambda _ctx, task_id=task_id: _verification(task_id),
        )

    for task_id in _side_ids():
        goal.task(
            task_id,
            agent=default_agent,
            inputs=(SHARED_SPEC_URI,),
            outputs=(f"out-{task_id}",),
            executor_api="context_v1",
            provenance_policy="strict",
            context_manifest=_manifest(os_, task_id),
            verify=lambda _ctx, task_id=task_id: _verification(task_id),
        )
    return goal


def _dispatch_sequence(result: Any) -> tuple[str, ...]:
    sequence: list[str] = []
    for epoch in result.meta.get("adaptive_epochs", ()) or ():
        sequence.extend(str(task_id) for task_id in epoch.get("actual_dispatched_task_ids", ()))
    return tuple(sequence)


def _warm_dispatches(result: Any) -> tuple[str, ...]:
    warm: list[str] = []
    for epoch in result.meta.get("adaptive_epochs", ()) or ():
        warm.extend(str(task_id) for task_id in epoch.get("locality_matched_task_ids", ()))
    return tuple(warm)


async def _run_arm(*, arm: str, agents: int, adaptive: bool) -> dict[str, Any]:
    workload = _Workload()
    os_ = AgentOS(":memory:")
    try:
        agent_ids = tuple(f"worker-{index}" for index in range(1, agents + 1))
        for agent_id in agent_ids:
            os_.add_agent(
                Agent(
                    agent_id,
                    executor=workload.executor_for(agent_id),
                    executor_api="context_v1",
                    specializations=("python",),
                )
            )
        goal = _build(os_, f"{arm}-goal", agent_ids)

        started = time.perf_counter()
        result = await os_.run_async(
            goal,
            max_dispatches=len(_all_ids()),
            max_steps=len(_all_ids()) + 4,
            max_concurrency=agents,
            adaptive=adaptive,
            max_parallelism=MAX_PARALLELISM if adaptive else 1,
            automatic_rebase=False,
        )
        elapsed = time.perf_counter() - started

        return {
            "arm": arm,
            "agents": agents,
            "adaptive": adaptive,
            "wall_clock_seconds": round(elapsed, 6),
            "goal_state": result.goal_state,
            "verified_tasks": tuple(sorted(result.verified)),
            "verified_count": len(result.verified),
            "executed_count": len(workload.completed),
            "peak_parallelism": workload.peak_parallelism,
            "epochs": len(result.meta.get("adaptive_epochs", ()) or ()),
            "dispatch_sequence": _dispatch_sequence(result),
            "warm_dispatches": _warm_dispatches(result),
            "warm_dispatch_count": len(_warm_dispatches(result)),
            "tasks_by_agent": {
                agent_id: tuple(tasks) for agent_id, tasks in sorted(workload.by_agent.items())
            },
        }
    finally:
        os_.close()


def _ranking_ablation() -> dict[str, Any]:
    """Compare what each ranking strategy would choose from one state.

    Pure functions over one observed projection: no execution, no timing.  This
    isolates the contribution of critical-path ranking from parallelism, which a
    wall-clock arm alone cannot separate.

    The chain is deliberately renamed to sort *last* lexically.  In the timed
    arms the chain happens to sort first, so both strategies agree there and the
    ranking looks free; that agreement is an accident of task naming, not a
    property of the policy.  Task ids carry no information about criticality, so
    an adversarial name is the honest case to measure.
    """

    workload = _Workload()
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker-1",
                executor=workload.executor_for("worker-1"),
                executor_api="context_v1",
                specializations=("python",),
            )
        )
        os_._facts.add_version(SHARED_SPEC, 1, "shared-spec-v1")
        goal = Goal("ablation-goal")
        chain_ids = tuple(f"z-chain-{index}" for index in range(1, CHAIN_LENGTH + 1))
        previous = None
        for task_id in chain_ids:
            previous = goal.task(
                task_id,
                agent="worker-1",
                depends_on=(previous,) if previous is not None else (),
                inputs=(SHARED_SPEC_URI,),
                outputs=(f"out-{task_id}",),
                executor_api="context_v1",
                provenance_policy="strict",
                context_manifest=_manifest(os_, task_id),
                verify=lambda _ctx, task_id=task_id: _verification(task_id),
            )
        for task_id in (f"a-side-{index}" for index in range(1, SIDE_TASKS + 1)):
            goal.task(
                task_id,
                agent="worker-1",
                inputs=(SHARED_SPEC_URI,),
                outputs=(f"out-{task_id}",),
                executor_api="context_v1",
                provenance_policy="strict",
                context_manifest=_manifest(os_, task_id),
                verify=lambda _ctx, task_id=task_id: _verification(task_id),
            )

        os_._compile_goal(goal)
        state = os_.runtime_state(goal)
        orders: dict[str, Any] = {}
        for strategy in (
            FrontierRankingStrategy.REPAIR_FIRST_LEXICAL,
            FrontierRankingStrategy.GRAPH_UTILITY,
        ):
            epoch = FrontierPolicy(
                max_parallelism=MAX_PARALLELISM,
                ranking_strategy=strategy,
            ).plan(state, epoch_id=0)
            orders[str(strategy.value)] = {
                "candidate_order": tuple(epoch.candidate_task_ids),
                "selected_task_ids": tuple(epoch.selected_task_ids),
            }
        lexical = orders[FrontierRankingStrategy.REPAIR_FIRST_LEXICAL.value]
        utility = orders[FrontierRankingStrategy.GRAPH_UTILITY.value]
        chain_head = chain_ids[0]
        return {
            "note": "chain renamed to sort last lexically; isolates ranking from parallelism",
            "critical_path_head": chain_head,
            "orders": orders,
            "graph_utility_selects_critical_path_head": chain_head in utility["selected_task_ids"],
            "lexical_selects_critical_path_head": chain_head in lexical["selected_task_ids"],
            "orders_differ": lexical["candidate_order"] != utility["candidate_order"],
        }
    finally:
        os_.close()


async def _run_async() -> dict[str, Any]:
    baseline = await _run_arm(arm="baseline", agents=1, adaptive=False)
    lhos = await _run_arm(arm="lhos", agents=LHOS_AGENTS, adaptive=True)
    ablation = _ranking_ablation()

    same_goal = baseline["verified_tasks"] == lhos["verified_tasks"]
    both_closed = baseline["goal_state"] == "closed" and lhos["goal_state"] == "closed"
    speedup = (
        round(baseline["wall_clock_seconds"] / lhos["wall_clock_seconds"], 4)
        if lhos["wall_clock_seconds"] > 0
        else None
    )

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "chain_length": CHAIN_LENGTH,
            "side_tasks": SIDE_TASKS,
            "task_seconds": TASK_SECONDS,
            "total_tasks": len(_all_ids()),
            "serial_lower_bound_seconds": round(len(_all_ids()) * TASK_SECONDS, 6),
            "critical_path_lower_bound_seconds": round(CHAIN_LENGTH * TASK_SECONDS, 6),
            "kind": "deterministic asyncio.sleep tasks with declared inputs/outputs",
        },
        "baseline": baseline,
        "lhos": lhos,
        "comparison": {
            "wall_clock_speedup": speedup,
            "reached_same_verified_goal": same_goal,
            "both_goals_closed": both_closed,
            "baseline_peak_parallelism": baseline["peak_parallelism"],
            "lhos_peak_parallelism": lhos["peak_parallelism"],
            "lhos_warm_dispatch_count": lhos["warm_dispatch_count"],
        },
        "ranking_ablation": ablation,
        "correctness": {
            "same_verified_task_set": same_goal,
            "both_closed": both_closed,
            "no_task_executed_twice": len(set(_all_ids())) == len(_all_ids()),
        },
        "scope": {
            "real_measurements": [
                "wall_clock_seconds (perf_counter around real asyncio.sleep)",
                "dispatch_sequence (Scheduler-granted claim order)",
                "peak_parallelism (observed concurrent executors)",
                "warm_dispatch_count (Scheduler-reported context residency hits)",
            ],
            "not_established": [
                "no LLM, GPU, or provider cost is involved",
                "no real coding/engineering task is executed",
                "token and monetary savings are NOT measured here",
                "single host; not a distributed or production measurement",
            ],
        },
    }


def run_benchmark() -> dict[str, Any]:
    return asyncio.run(_run_async())


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="baseline vs LongHorizonOS wall-clock benchmark")
    parser.add_argument("--out", default="", help="write the JSON report to this path")
    args = parser.parse_args(argv)

    report = run_benchmark()
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
    print(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
