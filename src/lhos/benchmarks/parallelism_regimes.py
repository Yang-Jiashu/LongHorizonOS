"""Does choosing the parallelism degree *online* beat the best fixed degree?

`decide_parallelism` picks a degree per epoch from observed runtime state: how
wide the frontier is, how much the graph has been churning, how much recent work
ended in rework. Nothing had ever measured whether that beats simply picking a
good number once.

The baseline is deliberately hard: **the best fixed degree in hindsight.** Not a
degree chosen badly on purpose, and not degree 1. Sweeping every fixed degree and
comparing against the winner is the only honest opponent, because in production
nobody would keep a fixed degree they had measured to be bad. If online selection
cannot beat the best fixed degree, that is the finding.

The workload has two phases on purpose:

``wide``    a fan-out of independent tasks -- more slots strictly help.
``narrow``  a serial chain -- extra slots cannot be used at all.

A single-phase workload has one right answer for the whole run, so a fixed degree
is trivially optimal and online selection has nothing to decide. That is the same
trap that made an earlier benchmark in this repository measure nothing: it
compared adaptive against static on a workload with no decision in it. Here the
optimal degree genuinely changes partway through, which is the only condition
under which "choose online" can mean anything.

Honest scope: real child processes doing real deterministic CPU work, real
wall-clock. No model or API, so nothing here is about token cost or model
quality. The phase boundary is generated rather than observed from a real
repository, and a null result is reported as a null result.
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

BENCHMARK_NAME: Final[str] = "parallelism_regimes"
BENCHMARK_VERSION: Final[int] = 1

WIDE_ROOT: Final[str] = "wide-root"
WIDE_COUNT: Final[int] = 12
NARROW_LENGTH: Final[int] = 6
# The narrow phase must be heavy enough that its serial cost is not lost in
# noise, otherwise the phase change is invisible and every degree looks alike.
WIDE_WEIGHT: Final[int] = 1
NARROW_WEIGHT: Final[int] = 3

WORK_ITERATIONS: Final[int] = 40_000
DEGREES: Final[tuple[int, ...]] = (1, 2, 4, 8)
MAX_DEGREE: Final[int] = max(DEGREES)
DEFAULT_REPEAT: Final[int] = 7

_CORE_SOURCE: Final[str] = """import hashlib


def digest(seed: str, iterations: int) -> str:
    value = seed.encode("utf-8")
    for _ in range(iterations):
        value = hashlib.sha256(value).digest()
    return value.hex()
"""


def _wide_tasks() -> tuple[str, ...]:
    return tuple(f"wide-{index:02d}" for index in range(WIDE_COUNT))


def _narrow_tasks() -> tuple[str, ...]:
    return tuple(f"narrow-{index:02d}" for index in range(NARROW_LENGTH))


def _all_task_ids() -> tuple[str, ...]:
    return (WIDE_ROOT, *_wide_tasks(), *_narrow_tasks())


def _weight_of(task_id: str) -> int:
    return NARROW_WEIGHT if task_id.startswith("narrow") else WIDE_WEIGHT


def _is_narrow(task_id: str) -> bool:
    return task_id.startswith("narrow")


def generate_workspace(root_dir: Path, *, iterations: int) -> Path:
    root_dir.mkdir(parents=True, exist_ok=True)
    (root_dir / "core.py").write_text(_CORE_SOURCE, encoding="utf-8")
    for task_id in (*_wide_tasks(), *_narrow_tasks()):
        (root_dir / f"{task_id.replace('-', '_')}.py").write_text(
            "import core\n\n\n"
            "def work() -> str:\n"
            f'    return core.digest("{task_id}", {iterations * _weight_of(task_id)})\n',
            encoding="utf-8",
        )
    return root_dir


def _command_for(root_dir: Path, iterations: int) -> Any:
    def build(task_id: str) -> list[str]:
        if task_id == WIDE_ROOT:
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                f"import core; core.digest('{task_id}', {iterations}); sys.exit(0)"
            )
        else:
            module = task_id.replace("-", "_")
            script = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                f"import {module} as m; sys.exit(0 if len(m.work()) == 64 else 1)"
            )
        return [sys.executable, "-c", script, str(root_dir)]

    return build


def _verification(task_id: str, version: int = 1) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"built-{task_id}",
        version=version,
        content=f"{task_id}:built:v{version}",
        evidence_note="child process exited 0 on real CPU work",
    )


def _build_goal(goal_id: str, agent_id: str) -> Goal:
    """Wide fan-out first, then a serial chain that depends on all of it.

    The chain's head depends on every wide task, so the phases cannot overlap.
    Without that barrier the two phases would interleave and the optimal degree
    would be constant again.
    """

    goal = Goal(goal_id)
    root = goal.task(
        WIDE_ROOT,
        agent=agent_id,
        inputs=("src/core.py",),
        outputs=(f"built-{WIDE_ROOT}",),
        executor_api="context_v1",
        verify=lambda _ctx: _verification(WIDE_ROOT),
    )
    wide_handles = []
    for task_id in _wide_tasks():
        wide_handles.append(
            goal.task(
                task_id,
                agent=agent_id,
                depends_on=(root,),
                inputs=(f"built-{WIDE_ROOT}", f"src/{task_id.replace('-', '_')}.py"),
                outputs=(f"built-{task_id}",),
                executor_api="context_v1",
                verify=lambda _ctx, task_id=task_id: _verification(task_id),
            )
        )
    previous: Any = tuple(wide_handles)
    for task_id in _narrow_tasks():
        created = goal.task(
            task_id,
            agent=agent_id,
            depends_on=previous if isinstance(previous, tuple) else (previous,),
            inputs=(f"src/{task_id.replace('-', '_')}.py",),
            outputs=(f"built-{task_id}",),
            executor_api="context_v1",
            verify=lambda _ctx, task_id=task_id: _verification(task_id),
        )
        previous = created
    return goal


@dataclass
class _Observed:
    active: int = 0
    peak_parallelism: int = 0
    completed: list[str] = field(default_factory=list)
    spans: list[tuple[str, float, float]] = field(default_factory=list)

    def wrap(self, execute: Any) -> Any:
        async def run(ctx: Any, task_id: str) -> None:
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


def _phase_parallelism(spans: list[tuple[str, float, float]]) -> dict[str, Any]:
    """Peak overlap within each phase, which is what a degree choice controls.

    Reported per phase because a single peak over the whole run hides the point:
    a degree that is right for the fan-out is wasted capacity on the chain, and a
    degree that is right for the chain throttles the fan-out.
    """

    if not spans:
        return {"wide_peak": None, "narrow_peak": None}

    def peak(subset: list[tuple[str, float, float]]) -> int | None:
        if not subset:
            return None
        edges = sorted({bound for _t, start, end in subset for bound in (start, end)})
        best = 0
        for left, right in itertools.pairwise(edges):
            midpoint = (left + right) / 2
            best = max(best, sum(1 for _t, s, e in subset if s <= midpoint < e))
        return best

    return {
        "wide_peak": peak([row for row in spans if not _is_narrow(row[0])]),
        "narrow_peak": peak([row for row in spans if _is_narrow(row[0])]),
    }


async def _run_arm(
    *,
    arm: str,
    degree: int | None,
    iterations: int,
) -> dict[str, Any]:
    """``degree=None`` means let the runtime choose per epoch."""

    workspace = Path(tempfile.mkdtemp(prefix=f"lhos-par-{arm}-"))
    observed = _Observed()
    os_ = AgentOS(":memory:")
    try:
        generate_workspace(workspace, iterations=iterations)
        command = _command_for(workspace, iterations)
        # Every arm is given the same number of agents.  Restricting the agent
        # pool instead of the degree would make the arms structurally unequal and
        # the comparison meaningless.
        agent_ids = tuple(f"builder-{index}" for index in range(1, MAX_DEGREE + 1))
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
        goal = _build_goal(f"{arm}", agent_ids[0])
        total = len(_all_task_ids())

        started = time.perf_counter()
        if degree is None:
            from lhos.sdk.kernel_loop import drive_goal_to_closure

            run = await drive_goal_to_closure(
                os_,
                goal,
                max_epochs=total + 8,
                max_concurrency=MAX_DEGREE,
                adaptive_parallelism=True,
            )
            goal_state = "closed" if run.goal_closed else run.stop_reason
            verified = len(run.verified_task_ids)
            # The degree the policy actually picked each epoch.  Without this the
            # adaptive arm is a black box and a win could not be attributed to
            # the decision rather than to loop overhead differences.
            chosen = tuple(
                int(getattr(decision, "chosen_degree", 0)) for decision in run.parallelism_decisions
            )
        else:
            result = await os_.run_async(
                goal,
                max_dispatches=total,
                max_steps=total + 8,
                max_concurrency=degree,
                adaptive=True,
                max_parallelism=degree,
                automatic_rebase=False,
            )
            goal_state = result.goal_state
            verified = len(result.verified)
            chosen = (degree,)
        elapsed = time.perf_counter() - started

        return {
            "arm": arm,
            "degree": degree,
            "seconds": round(elapsed, 6),
            "goal_state": goal_state,
            "verified": verified,
            "executed": len(observed.completed),
            "peak_parallelism": observed.peak_parallelism,
            "phase_parallelism": _phase_parallelism(observed.spans),
            "degrees_chosen": chosen,
            "total_tasks": total,
        }
    finally:
        os_.close()
        shutil.rmtree(workspace, ignore_errors=True)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


async def _run_async(*, iterations: int, repeat: int) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    arms: list[tuple[str, int | None]] = [(f"fixed_{degree}", degree) for degree in DEGREES]
    arms.append(("adaptive", None))
    for index in range(repeat):
        # Alternate order so a warm-up effect cannot pose as a win for whichever
        # arm happens to run first.
        ordered = arms if index % 2 == 0 else list(reversed(arms))
        for arm, degree in ordered:
            runs.append(await _run_arm(arm=arm, degree=degree, iterations=iterations))

    cells: dict[str, Any] = {}
    for arm, _degree in arms:
        rows = [row for row in runs if row["arm"] == arm]
        seconds = [float(row["seconds"]) for row in rows]
        cells[arm] = {
            "seconds_median": round(_median(seconds), 4),
            "seconds_min": round(min(seconds), 4),
            "seconds_max": round(max(seconds), 4),
            "goal_states": sorted({str(row["goal_state"]) for row in rows}),
            "peak_parallelism_distinct": sorted({int(row["peak_parallelism"]) for row in rows}),
            "wide_peak_distinct": sorted(
                {
                    int(row["phase_parallelism"]["wide_peak"])
                    for row in rows
                    if row["phase_parallelism"]["wide_peak"] is not None
                }
            ),
            "narrow_peak_distinct": sorted(
                {
                    int(row["phase_parallelism"]["narrow_peak"])
                    for row in rows
                    if row["phase_parallelism"]["narrow_peak"] is not None
                }
            ),
        }

    fixed_cells = {arm: cells[arm] for arm, degree in arms if degree is not None}
    best_fixed_arm = min(fixed_cells, key=lambda arm: fixed_cells[arm]["seconds_median"])
    best_fixed = fixed_cells[best_fixed_arm]["seconds_median"]
    adaptive = cells["adaptive"]["seconds_median"]

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "repeat": repeat,
        "workload": {
            "kind": "real child processes performing deterministic sha256 work",
            "wide_count": WIDE_COUNT,
            "narrow_length": NARROW_LENGTH,
            "wide_weight": WIDE_WEIGHT,
            "narrow_weight": NARROW_WEIGHT,
            "total_tasks": len(_all_task_ids()),
            "iterations_per_unit_weight": iterations,
            "why_two_phases": (
                "a single-phase workload has one correct degree for the whole run, so a "
                "fixed degree is trivially optimal and online selection has nothing to decide"
            ),
        },
        "cells": cells,
        "headline": {
            # The only comparison that tests the claim: the best fixed degree is
            # chosen *after* seeing every fixed arm, which is the strongest
            # opponent available and deliberately unflattering.
            "best_fixed_arm": best_fixed_arm,
            "best_fixed_seconds_median": best_fixed,
            "adaptive_seconds_median": adaptive,
            "best_fixed_vs_adaptive": round(best_fixed / adaptive, 4) if adaptive > 0 else None,
            "adaptive_beat_best_fixed": adaptive < best_fixed,
            "all_arms_closed": all(cell["goal_states"] == ["closed"] for cell in cells.values()),
        },
        "runs": tuple(runs),
        "interpretation": {
            "thesis_under_test": (
                "choosing the parallelism degree online beats the best single fixed degree "
                "when the optimal degree changes during the run"
            ),
            "a_null_result_is_a_finding": True,
            "why_the_baseline_is_hard": (
                "the best fixed degree is selected in hindsight from the measured sweep, "
                "so the adaptive arm is not being compared against a strawman"
            ),
        },
        "scope": {
            "real": [
                "child processes performing real CPU work",
                "wall-clock via perf_counter",
                "verification evidence is the child's exit status",
                "every arm is given the same agent pool; only the degree differs",
            ],
            "not_established": [
                "no model or API; nothing here is about token cost or model quality",
                "generated two-phase workload; absolute durations are arbitrary",
                "single host",
            ],
        },
    }


def run_benchmark(
    *, iterations: int = WORK_ITERATIONS, repeat: int = DEFAULT_REPEAT
) -> dict[str, Any]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    return asyncio.run(_run_async(iterations=iterations, repeat=repeat))


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=BENCHMARK_NAME)
    parser.add_argument("--out", default="")
    parser.add_argument("--iterations", type=int, default=WORK_ITERATIONS)
    parser.add_argument("--repeat", type=int, default=DEFAULT_REPEAT)
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
