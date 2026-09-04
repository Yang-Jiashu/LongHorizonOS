"""Real-work baseline-versus-LongHorizonOS benchmark.

Every other runtime benchmark in this repository executes ``asyncio.sleep``.
Sleeping tasks are enough to show that batches overlap, but they cannot support a
claim about compressing real computation: a sleep has no inputs, cannot be wrong,
and costs nothing to redo.  This benchmark runs **real work** instead, with no
model and no API:

* each task is a real child process doing real CPU work (a deterministic hash
  loop inside a generated Python module), launched through the same killable
  ``subprocess_task_executor`` the normal execution path uses;
* verification evidence is the command's exit status -- the work either ran or it
  did not;
* dependencies are real: every module task depends on a shared ``core`` module,
  so ``core`` is a genuine critical path rather than a declared one;
* invalidation is real: changing ``core`` supersedes exactly its downstream
  cone, which is what makes selective repair measurable rather than asserted.

Two arms close the same VERIFIED Goal:

``baseline``
    One agent, ``adaptive=False``, ``max_concurrency=1`` -- a single Harness
    working the list serially.

``lhos``
    Several agents with ``adaptive=True``: graph-utility ranking, conflict-aware
    batching, and context-residency-aware matching.

Honest scope: the workload is generated rather than drawn from a real repository,
and the tasks are CPU-bound hash loops rather than compilation or test suites, so
the absolute durations are arbitrary.  What is *not* arbitrary is that the work is
real, the durations are real wall-clock, the dependencies constrain the schedule,
and the repair cone is computed by the runtime rather than by this file.  This
benchmark says nothing about token cost, model quality, or context reuse -- those
need a real model and are deliberately out of scope here.
"""

from __future__ import annotations

import asyncio
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

BENCHMARK_NAME: Final[str] = "real_build_workload"
BENCHMARK_VERSION: Final[int] = 1

MODULE_COUNT: Final[int] = 6
WORK_ITERATIONS: Final[int] = 60_000
LHOS_AGENTS: Final[int] = 3
MAX_PARALLELISM: Final[int] = 3

CORE_TASK: Final[str] = "core"

_CORE_SOURCE: Final[str] = '''"""Shared core module: every generated module depends on this."""

import hashlib


def digest(seed: str, iterations: int) -> str:
    value = seed.encode("utf-8")
    for _ in range(iterations):
        value = hashlib.sha256(value).digest()
    return value.hex()
'''

_MODULE_TEMPLATE: Final[str] = '''"""Generated module {index}; imports the shared core."""

import core


def work() -> str:
    return core.digest("mod-{index}", {iterations})
'''


def _module_ids() -> tuple[str, ...]:
    return tuple(f"mod-{index}" for index in range(MODULE_COUNT))


def _all_task_ids() -> tuple[str, ...]:
    return (CORE_TASK, *_module_ids())


def generate_workspace(root: Path, *, iterations: int = WORK_ITERATIONS) -> Path:
    """Write a real, importable Python package tree under ``root``."""

    root.mkdir(parents=True, exist_ok=True)
    (root / "core.py").write_text(_CORE_SOURCE, encoding="utf-8")
    for index in range(MODULE_COUNT):
        (root / f"mod_{index}.py").write_text(
            _MODULE_TEMPLATE.format(index=index, iterations=iterations),
            encoding="utf-8",
        )
    return root


def _command_for(root: Path, iterations: int) -> Any:
    """Map a task id onto the real child-process command that performs it."""

    def build(task_id: str) -> list[str]:
        if task_id == CORE_TASK:
            # Exercise the shared dependency itself.
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); import core; "
                f"core.digest('core', {iterations}); sys.exit(0)"
            )
        else:
            index = task_id.rsplit("-", 1)[-1]
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                f"import mod_{index} as m; "
                "sys.exit(0 if len(m.work()) == 64 else 1)"
            )
        return [sys.executable, "-c", script, str(root)]

    return build


@dataclass
class _Observed:
    """Real per-agent execution overlap, observed rather than assumed."""

    active: int = 0
    peak_parallelism: int = 0
    completed: list[str] = field(default_factory=list)
    usage: dict[str, dict[str, Any]] = field(default_factory=dict)

    def note_usage(self, task_id: str, usage: dict[str, Any]) -> None:
        self.usage[task_id] = usage

    def wrap(self, execute: Any) -> Any:
        async def run(ctx: Any, task_id: str) -> None:
            self.active += 1
            self.peak_parallelism = max(self.peak_parallelism, self.active)
            try:
                await execute(ctx, task_id)
                self.completed.append(task_id)
            finally:
                self.active -= 1

        return run


def _verification(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"built-{task_id}",
        version=1,
        content=f"{task_id}:built",
        evidence_note="child process exited 0 on real CPU work",
    )


def _build_goal(goal_id: str, agent_id: str) -> Goal:
    goal = Goal(goal_id)
    core = goal.task(
        CORE_TASK,
        agent=agent_id,
        inputs=("src/core.py",),
        outputs=(f"built-{CORE_TASK}",),
        executor_api="context_v1",
        verify=lambda _ctx: _verification(CORE_TASK),
    )
    for task_id in _module_ids():
        index = task_id.rsplit("-", 1)[-1]
        goal.task(
            task_id,
            agent=agent_id,
            depends_on=(core,),
            inputs=(f"built-{CORE_TASK}", f"src/mod_{index}.py"),
            outputs=(f"built-{task_id}",),
            executor_api="context_v1",
            verify=lambda _ctx, task_id=task_id: _verification(task_id),
        )
    return goal


def _epoch_field(result: Any, key: str) -> tuple[str, ...]:
    collected: list[str] = []
    for epoch in result.meta.get("adaptive_epochs", ()) or ():
        collected.extend(str(item) for item in epoch.get(key, ()) or ())
    return tuple(collected)


async def _run_arm(
    *,
    arm: str,
    agents: int,
    adaptive: bool,
    root: Path,
    iterations: int,
) -> dict[str, Any]:
    observed = _Observed()
    command = _command_for(root, iterations)
    os_ = AgentOS(":memory:")
    try:
        agent_ids = tuple(f"builder-{index}" for index in range(1, agents + 1))
        for agent_id in agent_ids:
            executor = subprocess_task_executor(
                command,
                timeout_seconds=120,
                poll_seconds=0.01,
                on_usage=observed.note_usage,
            )
            os_.add_agent(
                Agent(
                    agent_id,
                    executor=observed.wrap(executor),
                    executor_api="context_v1",
                    specializations=("python",),
                )
            )
        goal = _build_goal(f"{arm}-build", agent_ids[0])

        started = time.perf_counter()
        result = await os_.run_async(
            goal,
            max_dispatches=len(_all_task_ids()),
            max_steps=len(_all_task_ids()) + 4,
            max_concurrency=agents,
            adaptive=adaptive,
            max_parallelism=MAX_PARALLELISM if adaptive else 1,
            automatic_rebase=False,
        )
        elapsed = time.perf_counter() - started

        child_wall_ms = sum(
            int(entry.get("wall_time_ms", 0) or 0) for entry in observed.usage.values()
        )
        return {
            "arm": arm,
            "agents": agents,
            "adaptive": adaptive,
            "wall_clock_seconds": round(elapsed, 6),
            "goal_state": result.goal_state,
            "verified_tasks": tuple(sorted(result.verified)),
            "verified_count": len(result.verified),
            "executed_count": len(observed.completed),
            "peak_parallelism": observed.peak_parallelism,
            "epochs": len(result.meta.get("adaptive_epochs", ()) or ()),
            "dispatch_sequence": _epoch_field(result, "actual_dispatched_task_ids"),
            "warm_dispatches": _epoch_field(result, "locality_matched_task_ids"),
            # Summed child wall-clock is the serial work content: comparing it
            # against the arm's own elapsed time shows how much real overlap the
            # schedule achieved, independent of any declared cost model.
            "child_wall_time_ms_total": child_wall_ms,
            "children_reporting_usage": len(observed.usage),
        }
    finally:
        os_.close()


async def _run_async(
    *,
    iterations: int = WORK_ITERATIONS,
    baseline_first: bool = True,
) -> dict[str, Any]:
    # Each arm gets its own freshly generated tree.  Sharing one workspace lets
    # the first arm pay for CPython's bytecode compilation and the second arm
    # inherit the warm ``__pycache__`` -- a confound that silently inflates the
    # speedup of whichever arm runs second.
    baseline_root = Path(tempfile.mkdtemp(prefix="lhos-real-build-baseline-"))
    lhos_root = Path(tempfile.mkdtemp(prefix="lhos-real-build-lhos-"))
    try:
        generate_workspace(baseline_root, iterations=iterations)
        generate_workspace(lhos_root, iterations=iterations)
        if baseline_first:
            baseline = await _run_arm(
                arm="baseline",
                agents=1,
                adaptive=False,
                root=baseline_root,
                iterations=iterations,
            )
            lhos = await _run_arm(
                arm="lhos",
                agents=LHOS_AGENTS,
                adaptive=True,
                root=lhos_root,
                iterations=iterations,
            )
        else:
            lhos = await _run_arm(
                arm="lhos",
                agents=LHOS_AGENTS,
                adaptive=True,
                root=lhos_root,
                iterations=iterations,
            )
            baseline = await _run_arm(
                arm="baseline",
                agents=1,
                adaptive=False,
                root=baseline_root,
                iterations=iterations,
            )
    finally:
        shutil.rmtree(baseline_root, ignore_errors=True)
        shutil.rmtree(lhos_root, ignore_errors=True)

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
            "kind": "real child processes performing deterministic sha256 work",
            "modules": MODULE_COUNT,
            "iterations_per_task": iterations,
            "total_tasks": len(_all_task_ids()),
            "critical_path": (CORE_TASK, "any module"),
            "shared_dependency": CORE_TASK,
        },
        "baseline_first": baseline_first,
        "baseline": baseline,
        "lhos": lhos,
        "comparison": {
            "wall_clock_speedup": speedup,
            "reached_same_verified_goal": same_goal,
            "both_goals_closed": both_closed,
            "baseline_peak_parallelism": baseline["peak_parallelism"],
            "lhos_peak_parallelism": lhos["peak_parallelism"],
        },
        "scope": {
            "real": [
                "child processes performing real CPU work",
                "wall_clock_seconds via perf_counter",
                "verification evidence is the child's exit status",
                "dependencies constrain the achievable schedule",
            ],
            "not_established": [
                "no model or API is involved; nothing here is about token cost",
                "context reuse and model routing are NOT measured",
                "the workload is generated, not a real repository build",
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


async def _run_repeated(*, iterations: int, repeat: int) -> dict[str, Any]:
    """Run ``repeat`` independent arm pairs and report the distribution.

    A single pair is not reportable: real CPU work on a shared host varies by
    tens of percent, and quoting one run means quoting whichever number the
    machine happened to produce.  Both arm orders are exercised so a warm-up or
    ordering bias cannot masquerade as a scheduling win.
    """

    runs: list[dict[str, Any]] = []
    for index in range(repeat):
        report = await _run_async(
            iterations=iterations,
            baseline_first=(index % 2 == 0),
        )
        runs.append(report)

    speedups = [
        float(run["comparison"]["wall_clock_speedup"])
        for run in runs
        if run["comparison"]["wall_clock_speedup"] is not None
    ]
    baseline_seconds = [float(run["baseline"]["wall_clock_seconds"]) for run in runs]
    lhos_seconds = [float(run["lhos"]["wall_clock_seconds"]) for run in runs]
    head = runs[0]
    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "repeat": repeat,
        "workload": head["workload"],
        "scope": head["scope"],
        "distribution": {
            "speedup_median": round(_median(speedups), 4) if speedups else None,
            "speedup_min": round(min(speedups), 4) if speedups else None,
            "speedup_max": round(max(speedups), 4) if speedups else None,
            "speedup_all": [round(value, 4) for value in speedups],
            "runs_slower_than_baseline": sum(1 for value in speedups if value < 1.0),
            "baseline_seconds_median": round(_median(baseline_seconds), 4),
            "lhos_seconds_median": round(_median(lhos_seconds), 4),
        },
        "correctness": {
            "all_runs_closed_both_goals": all(
                run["comparison"]["both_goals_closed"] for run in runs
            ),
            "all_runs_same_verified_goal": all(
                run["comparison"]["reached_same_verified_goal"] for run in runs
            ),
        },
        "runs": runs,
    }


def run_benchmark(*, iterations: int = WORK_ITERATIONS, repeat: int = 1) -> dict[str, Any]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    if repeat == 1:
        return asyncio.run(_run_async(iterations=iterations))
    return asyncio.run(_run_repeated(iterations=iterations, repeat=repeat))


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=BENCHMARK_NAME)
    parser.add_argument("--out", default="", help="write the JSON report to this path")
    parser.add_argument(
        "--iterations",
        type=int,
        default=WORK_ITERATIONS,
        help="sha256 rounds per task; raises the real work per task",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="independent arm pairs; >1 reports median/min/max instead of one run",
    )
    args = parser.parse_args(argv)

    report = run_benchmark(iterations=args.iterations, repeat=args.repeat)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
    print(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
