"""Scheduling-regime benchmark: what does *online* scheduling actually buy?

The other benchmarks here compare LongHorizonOS against a serial single agent.
That is the wrong opponent.  This system's claim is specifically that it performs
**continuous online scheduling rather than generating one fixed plan up front**,
so the honest competitor is a good *static* plan executed without replanning.  If
a static plan matches the adaptive one, the thesis is empty.  The adaptive arm
should win specifically when the graph *changes mid-run*, because that is exactly
when a plan computed up front has gone stale.

So this benchmark is a 3x2 design:

Arms
    ``serial``           one agent, no adaptivity -- the floor.
    ``static_parallel``  N-way concurrency in static graph order, no per-epoch
                         replanning, no conflict-aware batching.  This is the
                         real competitor.
    ``lhos_adaptive``    graph-utility ranking, conflict-aware batching and
                         context-residency matching, replanned every epoch.

Conditions
    ``stable``  nothing changes; the up-front plan stays valid.
    ``churn``   one of two independent upstream artifacts changes after the goal
                closes, so half the graph is superseded and half is still valid.

The expected result is deliberately unflattering to the adaptive arm: under
``stable`` the static plan should very nearly match it, because there is nothing
to react to.  The interesting number is the ``churn`` condition, where the graph
records which work survived and only the invalidated cone is redone.  A result
showing no adaptive advantage under churn would be a finding, not a bug, and is
reported as-is.

The workload has **two independent roots** on purpose.  A single shared root
would make every change invalidate everything, so "preserve still-valid work"
would be unmeasurable -- the preserved count would always be zero.

Honest scope: tasks are real child processes doing real deterministic CPU work,
and all durations are real wall-clock, but the workload is generated rather than
a real repository.  No model or API is involved, so this measures the scheduler,
not an agent's competence, and says nothing about token cost.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.subprocess_agent import subprocess_task_executor

BENCHMARK_NAME: Final[str] = "scheduling_regimes"
BENCHMARK_VERSION: Final[int] = 1

ROOTS: Final[tuple[str, ...]] = ("core-a", "core-b")
WORK_ITERATIONS: Final[int] = 120_000
AGENTS: Final[int] = 3
DEFAULT_REPEAT: Final[int] = 5


@dataclass(frozen=True)
class Shape:
    """How many modules hang off each root, and how heavy each one is.

    Duration heterogeneity is what makes scheduling order matter at all.  With
    uniform tasks on a symmetric graph there is no critical path and no decision
    to make, so every arm reaches the same makespan and a scheduler cannot
    demonstrate anything.  Root A is always built as a serial chain, root B
    always fans out; the shape decides whether that asymmetry is large enough to
    bound the makespan.
    """

    name: str
    chain_length: int
    branch_count: int
    chain_weight: int
    branch_weight: int


# Kept because it is the shape that produced a *null* result: both roots equal,
# so a static plan is already optimal and online scheduling can only match it.
# Deleting it would delete the boundary of the claim.  Measured
# chain_priority_index here was 0.536 vs 0.500 -- a single dispatch position.
SYMMETRIC: Final[Shape] = Shape(
    name="symmetric",
    chain_length=3,
    branch_count=3,
    chain_weight=1,
    branch_weight=1,
)
# A long serial chain bounds the makespan while wide cheap branches compete for
# the same slots, so the order the scheduler picks actually changes when the
# chain finishes.
CRITICAL_PATH: Final[Shape] = Shape(
    name="critical-path",
    chain_length=10,
    branch_count=30,
    chain_weight=4,
    branch_weight=1,
)
SHAPES: Final[dict[str, Shape]] = {shape.name: shape for shape in (SYMMETRIC, CRITICAL_PATH)}
DEFAULT_SHAPE: Final[Shape] = CRITICAL_PATH


def _is_chain(task_id: str) -> bool:
    return task_id.startswith(("core-a", "mod-a"))


def _weight_of(task_id: str, shape: Shape) -> int:
    return shape.chain_weight if _is_chain(task_id) else shape.branch_weight


def _dependencies_of(shape: Shape) -> dict[str, tuple[str, ...]]:
    """The same edges ``_build_goal`` declares, as a plain map.

    Needed to tell a *dependency stall* (nothing was runnable) apart from a
    *barrier stall* (something was runnable and a slot was free, but the batch
    had not returned yet). Without that split, an idle slot on a serial chain
    looks identical to scheduler waste.
    """

    deps: dict[str, tuple[str, ...]] = {}
    for root in ROOTS:
        deps[root] = ()
        chain = root == ROOTS[0]
        previous = root
        for module in _modules_of(root, shape):
            deps[module] = (previous,) if chain else (root,)
            if chain:
                previous = module
    return deps


def _barrier_stall(
    spans: list[tuple[str, float, float]], slots: int, shape: Shape
) -> dict[str, Any]:
    """Capacity that was free while runnable work was already waiting for it.

    ``run_async`` plans a batch and awaits all of it before replanning, so a slot
    freed by a short task cannot be refilled until the batch's slowest task
    finishes. This integrates ``min(free slots, tasks runnable but not started)``
    over time, which is exactly what a work-conserving scheduler could have used.

    Both clamps matter. Without the ``free slots`` term, a deliberately
    deprioritized task looks like waste; without the ``runnable`` term, a serial
    chain's genuine dependency stall does. And taking the *minimum* rather than
    summing per task is what keeps it comparable to wall-clock: thirty tasks
    waiting on one 2-second barrier is 2 slot-seconds of lost capacity, not 60.
    """

    if slots <= 1 or not spans:
        return {"reclaimable_slot_seconds": None, "note": "single slot or no spans"}

    ended = {task: end for task, _start, end in spans}
    deps = _dependencies_of(shape)
    origin = min(start for _task, start, _end in spans)

    runnable_at: dict[str, float] = {}
    for task, _start, _end in spans:
        required = deps.get(task, ())
        if any(dep not in ended for dep in required):
            continue
        runnable_at[task] = max((ended[dep] for dep in required), default=origin)

    edges = sorted(
        {bound for _task, start, end in spans for bound in (start, end)} | set(runnable_at.values())
    )
    reclaimable = 0.0
    for left, right in itertools.pairwise(edges):
        midpoint = (left + right) / 2
        active = sum(1 for _task, start, end in spans if start <= midpoint < end)
        waiting = sum(
            1
            for task, start, _end in spans
            if task in runnable_at and runnable_at[task] <= midpoint < start
        )
        reclaimable += (right - left) * min(max(0, slots - active), waiting)

    busy_span = edges[-1] - edges[0] if len(edges) > 1 else 0.0
    capacity = busy_span * slots
    return {
        "reclaimable_slot_seconds": round(reclaimable, 6),
        "reclaimable_fraction_of_capacity": (
            round(reclaimable / capacity, 4) if capacity > 0 else None
        ),
        "note": "min(free slots, runnable-not-started) integrated over time; not double counted",
    }


_CORE_SOURCE: Final[str] = """import hashlib


def digest(seed: str, iterations: int) -> str:
    value = seed.encode("utf-8")
    for _ in range(iterations):
        value = hashlib.sha256(value).digest()
    return value.hex()
"""


def _root_key(root: str) -> str:
    return root.replace("-", "_")


def _modules_of(root: str, shape: Shape) -> tuple[str, ...]:
    # Root A is the serial critical path; root B is the wide filler.
    count = shape.chain_length if root == ROOTS[0] else shape.branch_count
    return tuple(f"mod-{root[-1]}-{index:02d}" for index in range(count))


def _all_task_ids(shape: Shape) -> tuple[str, ...]:
    ids: list[str] = []
    for root in ROOTS:
        ids.append(root)
        ids.extend(_modules_of(root, shape))
    return tuple(ids)


def generate_workspace(root_dir: Path, *, iterations: int, shape: Shape = DEFAULT_SHAPE) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    for root in ROOTS:
        (root_dir / f"{_root_key(root)}.py").write_text(_CORE_SOURCE, encoding="utf-8")
        for module in _modules_of(root, shape):
            (root_dir / f"{module.replace('-', '_')}.py").write_text(
                f"import {_root_key(root)}\n\n\n"
                f"def work() -> str:\n"
                f'    return {_root_key(root)}.digest("{module}", '
                f"{iterations * _weight_of(module, shape)})\n",
                encoding="utf-8",
            )
    return root_dir


def _command_for(root_dir: Path, iterations: int, shape: Shape) -> Any:
    def build(task_id: str) -> list[str]:
        if task_id in ROOTS:
            module = _root_key(task_id)
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                f"import {module}; "
                f"{module}.digest('{task_id}', {iterations * _weight_of(task_id, shape)}); "
                "sys.exit(0)"
            )
        else:
            module = task_id.replace("-", "_")
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                f"import {module} as m; sys.exit(0 if len(m.work()) == 64 else 1)"
            )
        return [sys.executable, "-c", script, str(root_dir)]

    return build


@dataclass
class _Observed:
    active: int = 0
    peak_parallelism: int = 0
    completed: list[str] = field(default_factory=list)
    # Order tasks actually began executing.  Wall-clock on a graph this small is
    # swamped by host noise (measured +/-20%), but *whether the scheduler chose to
    # advance the critical path* is a deterministic property of that order, so the
    # critical-path claim is evidenced without needing repetitions.
    start_order: list[str] = field(default_factory=list)
    # (task_id, started_at, ended_at) on one perf_counter timeline, so slot
    # occupancy can be reconstructed after the run.  ``run_async`` plans a batch
    # and awaits *all* of it before replanning, so a slot freed by a short task
    # cannot be refilled until the batch's slowest task finishes.  That idle
    # capacity is invisible in makespan alone.
    spans: list[tuple[str, float, float]] = field(default_factory=list)

    def wrap(self, execute: Any) -> Any:
        async def run(ctx: Any, task_id: str) -> None:
            self.start_order.append(task_id)
            self.active += 1
            self.peak_parallelism = max(self.peak_parallelism, self.active)
            started = time.perf_counter()
            try:
                await execute(ctx, task_id)
                self.completed.append(task_id)
            finally:
                self.active -= 1
                self.spans.append((task_id, started, time.perf_counter()))

        return run


def _slot_occupancy(spans: list[tuple[str, float, float]], slots: int) -> dict[str, Any]:
    """How much of the available concurrency actually held work.

    ``idle_slot_seconds`` integrates ``slots - active`` across the busy window.
    It is an **upper bound** on recoverable time, not a claim about waste: an
    idle slot is only waste if a READY task existed to fill it, and a serial
    chain legitimately leaves slots empty.  The bound is still informative,
    because a work-conserving scheduler is the only thing that *could* reclaim
    any of it.
    """

    if slots <= 1 or not spans:
        return {
            "slots": slots,
            "idle_slot_seconds": None,
            "idle_slot_fraction": None,
            "note": "single slot or no spans; slot idle is not defined",
        }

    edges = sorted({bound for _task, start, end in spans for bound in (start, end)})
    busy_span = edges[-1] - edges[0]
    if busy_span <= 0:
        return {
            "slots": slots,
            "idle_slot_seconds": None,
            "idle_slot_fraction": None,
            "note": "zero-length window",
        }

    idle = 0.0
    for left, right in itertools.pairwise(edges):
        width = right - left
        midpoint = (left + right) / 2
        active = sum(1 for _task, start, end in spans if start <= midpoint < end)
        idle += width * max(0, slots - active)

    capacity = busy_span * slots
    return {
        "slots": slots,
        "busy_window_seconds": round(busy_span, 6),
        "idle_slot_seconds": round(idle, 6),
        "idle_slot_fraction": round(idle / capacity, 4) if capacity > 0 else None,
        "note": "upper bound on reclaimable capacity; some idleness is genuine dependency stall",
    }


def _chain_priority_index(start_order: list[str]) -> float | None:
    """Mean normalised start position of the critical-path chain.

    ``0.0`` means the chain ran first; ``1.0`` means it was deferred to the very
    end.  ``None`` when nothing started, which is reported rather than scored.
    """

    if not start_order:
        return None
    chain_positions = [
        index / max(1, len(start_order) - 1)
        for index, task_id in enumerate(start_order)
        if _is_chain(task_id)
    ]
    if not chain_positions:
        return None
    return round(sum(chain_positions) / len(chain_positions), 4)


def _verification(task_id: str, version: int = 1) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"built-{task_id}",
        version=version,
        content=f"{task_id}:built:v{version}",
        evidence_note="child process exited 0 on real CPU work",
    )


def _build_goal(goal_id: str, agent_id: str, shape: Shape) -> Goal:
    """Two independent roots so a change invalidates half the graph, not all."""

    goal = Goal(goal_id)
    for root in ROOTS:
        root_task = goal.task(
            root,
            agent=agent_id,
            inputs=(f"src/{_root_key(root)}.py",),
            outputs=(f"built-{root}",),
            executor_api="context_v1",
            verify=lambda _ctx, root=root: _verification(root),
        )
        chain = root == ROOTS[0]
        previous = root_task
        for module in _modules_of(root, shape):
            created = goal.task(
                module,
                agent=agent_id,
                depends_on=(previous if chain else root_task,),
                inputs=(f"built-{root}", f"src/{module.replace('-', '_')}.py"),
                outputs=(f"built-{module}",),
                executor_api="context_v1",
                verify=lambda _ctx, module=module: _verification(module),
            )
            if chain:
                previous = created
    return goal


async def _run_arm(
    *,
    arm: str,
    adaptive: bool,
    agents: int,
    condition: str,
    iterations: int,
    shape: Shape,
    dispatch_lookahead: int = 1,
) -> dict[str, Any]:
    workspace = Path(tempfile.mkdtemp(prefix=f"lhos-regime-{arm}-"))
    observed = _Observed()
    os_ = AgentOS(":memory:")
    try:
        generate_workspace(workspace, iterations=iterations, shape=shape)
        command = _command_for(workspace, iterations, shape)
        agent_ids = tuple(f"builder-{index}" for index in range(1, agents + 1))
        for agent_id in agent_ids:
            os_.add_agent(
                Agent(
                    agent_id,
                    executor=observed.wrap(
                        subprocess_task_executor(command, timeout_seconds=180, poll_seconds=0.01)
                    ),
                    executor_api="context_v1",
                    specializations=("python",),
                )
            )
        goal = _build_goal(f"{arm}-{condition}", agent_ids[0], shape)
        total = len(_all_task_ids(shape))

        started = time.perf_counter()
        first = await os_.run_async(
            goal,
            max_dispatches=total,
            max_steps=total + 6,
            max_concurrency=agents,
            adaptive=adaptive,
            max_parallelism=agents if adaptive else 1,
            automatic_rebase=False,
            dispatch_lookahead=dispatch_lookahead,
        )
        initial_seconds = time.perf_counter() - started
        initial_executed = len(observed.completed)

        repair: dict[str, Any] = {"applied": False}
        if condition == "churn" and first.goal_state == "closed":
            # One of two independent roots changes.  Its cone must be redone; the
            # other root's subtree must be preserved -- that preservation is the
            # quantity a static plan cannot exploit.
            changed_root = ROOTS[0]
            report = os_.repair(
                goal,
                artifact_id=f"built-{changed_root}",
                new_artifact_version=2,
            )
            before = len(observed.completed)
            repair_started = time.perf_counter()
            second = await os_.run_async(
                goal,
                max_dispatches=total,
                max_steps=total + 6,
                max_concurrency=agents,
                adaptive=adaptive,
                max_parallelism=agents if adaptive else 1,
                automatic_rebase=False,
                dispatch_lookahead=dispatch_lookahead,
            )
            repair = {
                "applied": True,
                "changed_artifact": f"built-{changed_root}",
                "affected": tuple(sorted(report.affected)),
                "preserved": tuple(sorted(report.preserved)),
                "affected_count": len(report.affected),
                "preserved_count": len(report.preserved),
                "repair_frontier": tuple(report.frontier),
                "seconds": round(time.perf_counter() - repair_started, 6),
                "tasks_reexecuted": len(observed.completed) - before,
                "goal_state": second.goal_state,
            }

        return {
            "arm": arm,
            "condition": condition,
            "adaptive": adaptive,
            "agents": agents,
            "initial_seconds": round(initial_seconds, 6),
            "initial_goal_state": first.goal_state,
            "initial_verified": len(first.verified),
            "initial_executed": initial_executed,
            "start_order": tuple(observed.start_order),
            # Mean start position of the heavy serial chain, normalised to [0,1].
            # The chain bounds the makespan, so a scheduler that defers it in
            # favour of cheap filler scores higher -- and worse.
            "chain_priority_index": _chain_priority_index(observed.start_order),
            "peak_parallelism": observed.peak_parallelism,
            # Batch-barrier cost: run_async awaits a whole batch before
            # replanning, so capacity freed early sits idle.  Reported as a
            # bound, because a dependency stall also looks like an idle slot.
            "slot_occupancy": _slot_occupancy(observed.spans, agents),
            # The barrier cost specifically: runnable, capacity free, still not
            # dispatched.  Dependency stalls are excluded, so this is the part a
            # work-conserving scheduler could actually reclaim.
            "barrier_stall": _barrier_stall(observed.spans, agents, shape),
            "total_tasks": total,
            "repair": repair,
        }
    finally:
        os_.close()
        shutil.rmtree(workspace, ignore_errors=True)


_ARMS: Final[tuple[tuple[str, bool, int], ...]] = (
    ("serial", False, 1),
    ("static_parallel", False, AGENTS),
    ("lhos_adaptive", True, AGENTS),
)


async def _run_async(
    *, iterations: int, shape: Shape = DEFAULT_SHAPE, dispatch_lookahead: int = 1
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for condition in ("stable", "churn"):
        for arm, adaptive, agents in _ARMS:
            results.append(
                await _run_arm(
                    arm=arm,
                    adaptive=adaptive,
                    agents=agents,
                    condition=condition,
                    iterations=iterations,
                    shape=shape,
                    dispatch_lookahead=dispatch_lookahead,
                )
            )

    def find(arm: str, condition: str) -> dict[str, Any]:
        return next(r for r in results if r["arm"] == arm and r["condition"] == condition)

    def ratio(numerator: float, denominator: float) -> float | None:
        return round(numerator / denominator, 4) if denominator > 0 else None

    stable_static = find("static_parallel", "stable")
    stable_lhos = find("lhos_adaptive", "stable")
    churn_static = find("static_parallel", "churn")
    churn_lhos = find("lhos_adaptive", "churn")

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "kind": "real child processes performing deterministic sha256 work",
            "shape": shape.name,
            "independent_roots": len(ROOTS),
            "chain_length": shape.chain_length,
            "branch_count": shape.branch_count,
            "chain_weight": shape.chain_weight,
            "branch_weight": shape.branch_weight,
            "total_tasks": len(_all_task_ids(shape)),
            "iterations_per_task": iterations,
            "dispatch_lookahead": dispatch_lookahead,
            "why_two_roots": (
                "a single shared root would invalidate the whole graph on any change, "
                "making preserved-work unmeasurable"
            ),
        },
        "results": tuple(results),
        "headline": {
            # The comparison that actually tests the thesis.
            "stable_static_vs_lhos_seconds": ratio(
                stable_static["initial_seconds"], stable_lhos["initial_seconds"]
            ),
            "serial_vs_lhos_seconds": ratio(
                find("serial", "stable")["initial_seconds"], stable_lhos["initial_seconds"]
            ),
            "churn_static_repair_seconds": churn_static["repair"].get("seconds"),
            "churn_lhos_repair_seconds": churn_lhos["repair"].get("seconds"),
            "churn_static_tasks_reexecuted": churn_static["repair"].get("tasks_reexecuted"),
            "churn_lhos_tasks_reexecuted": churn_lhos["repair"].get("tasks_reexecuted"),
            "churn_preserved_count": churn_lhos["repair"].get("preserved_count"),
            "churn_affected_count": churn_lhos["repair"].get("affected_count"),
        },
        "interpretation": {
            "thesis_under_test": (
                "online replanning beats a good static plan only when the graph changes"
            ),
            "expected_stable": "static_parallel should nearly match lhos_adaptive",
            "expected_churn": "only the invalidated cone is redone; the other root survives",
            "a_null_result_is_a_finding": True,
        },
        "scope": {
            "real": [
                "child processes performing real CPU work",
                "wall-clock via perf_counter",
                "verification evidence is the child's exit status",
                "invalidation and the preserved set are computed by the runtime",
            ],
            "not_established": [
                "no model or API; nothing here is about token cost",
                "workload is generated, not a real repository",
                "single host; absolute durations are arbitrary",
            ],
        },
    }


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


async def _run_repeated(
    *, iterations: int, repeat: int, shape: Shape = DEFAULT_SHAPE
) -> dict[str, Any]:
    """Run the whole grid ``repeat`` times and report per-cell distributions.

    A single pass is not reportable.  On this workload the run-to-run spread of
    the static-vs-adaptive ratio was measured at 1.08x-1.28x on *identical*
    code, so quoting one pass means quoting whichever number the machine
    happened to produce.  Any effect smaller than that spread is unresolvable
    without repetition.
    """

    grids = [await _run_async(iterations=iterations, shape=shape) for _ in range(repeat)]
    cells: dict[str, list[dict[str, Any]]] = {}
    for grid in grids:
        for row in grid["results"]:
            cells.setdefault(f"{row['condition']}/{row['arm']}", []).append(row)

    summary: dict[str, Any] = {}
    for key, rows in sorted(cells.items()):
        initial = [float(r["initial_seconds"]) for r in rows]
        repairs = [
            float(r["repair"]["seconds"]) for r in rows if r["repair"].get("seconds") is not None
        ]
        reexec = [
            int(r["repair"]["tasks_reexecuted"])
            for r in rows
            if r["repair"].get("tasks_reexecuted") is not None
        ]
        preserved = [
            int(r["repair"]["preserved_count"])
            for r in rows
            if r["repair"].get("preserved_count") is not None
        ]
        reclaimable = [
            float(r["barrier_stall"]["reclaimable_fraction_of_capacity"])
            for r in rows
            if r.get("barrier_stall", {}).get("reclaimable_fraction_of_capacity") is not None
        ]
        summary[key] = {
            "initial_seconds_median": round(_median(initial), 4),
            "initial_seconds_min": round(min(initial), 4),
            "initial_seconds_max": round(max(initial), 4),
            "repair_seconds_median": round(_median(repairs), 4) if repairs else None,
            # Fraction of concurrency that sat free while runnable work waited.
            # This is the batch-barrier cost and it bounds what a work-conserving
            # dispatcher could reclaim; the serial arm has one slot so it is None.
            "reclaimable_capacity_fraction_median": (
                round(_median(reclaimable), 4) if reclaimable else None
            ),
            "reclaimable_capacity_fraction_max": round(max(reclaimable), 4)
            if reclaimable
            else None,
            # Deterministic structural facts: these must not vary at all, and a
            # spread here would mean the graph itself is behaving nondeterministically.
            "tasks_reexecuted_distinct": sorted(set(reexec)),
            "preserved_count_distinct": sorted(set(preserved)),
        }

    stable_static = summary["stable/static_parallel"]["initial_seconds_median"]
    stable_lhos = summary["stable/lhos_adaptive"]["initial_seconds_median"]
    stable_serial = summary["stable/serial"]["initial_seconds_median"]
    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "repeat": repeat,
        "workload": grids[0]["workload"],
        "interpretation": grids[0]["interpretation"],
        "scope": grids[0]["scope"],
        "cells": summary,
        "headline": {
            "static_vs_lhos_median": (
                round(stable_static / stable_lhos, 4) if stable_lhos > 0 else None
            ),
            "serial_vs_lhos_median": (
                round(stable_serial / stable_lhos, 4) if stable_lhos > 0 else None
            ),
        },
    }


def run_benchmark(
    *,
    iterations: int = WORK_ITERATIONS,
    repeat: int = 1,
    shape: Shape | str = DEFAULT_SHAPE,
) -> dict[str, Any]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    if isinstance(shape, str):
        if shape not in SHAPES:
            raise ValueError(f"unknown shape {shape!r}; expected one of {sorted(SHAPES)}")
        shape = SHAPES[shape]
    if repeat == 1:
        return asyncio.run(_run_async(iterations=iterations, shape=shape))
    return asyncio.run(_run_repeated(iterations=iterations, repeat=repeat, shape=shape))


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=BENCHMARK_NAME)
    parser.add_argument("--out", default="")
    parser.add_argument("--iterations", type=int, default=WORK_ITERATIONS)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--shape", choices=sorted(SHAPES), default=DEFAULT_SHAPE.name)
    args = parser.parse_args(argv)

    report = run_benchmark(iterations=args.iterations, repeat=args.repeat, shape=args.shape)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
    print(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
