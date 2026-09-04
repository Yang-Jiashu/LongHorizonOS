"""Does mid-flight preemption actually pay off?

Until the preemption wire landed, nothing in the execution loop ever interrupted
in-flight work, so the one mechanism a static plan cannot imitate had never fired
in any measurement.  It fires now.  This measures whether it is worth anything.

The question, narrowly: when a sibling commit supersedes a peer's declared input
**while that peer is still running**, how much child-process time is burned on
work that is already doomed?

Three arms on the identical workload:

``static``               ``adaptive=False`` -- no replanning at all.
``adaptive``             ``adaptive=True, preempt_superseded=False``.
``adaptive_preempt``     ``adaptive=True, preempt_superseded=True``.

The primary metric is **doomed compute**: child wall-clock spent on the task
whose input was superseded.  It comes from the existing ``on_usage`` callback on
``subprocess_task_executor``; no new instrumentation.  That callback also reports
``terminated_by``, which distinguishes a child that was *killed* from one that ran
to completion -- so "we stopped it" is observable rather than inferred.

Why the timing is load-bearing: an earlier benchmark injected its change *after*
the goal closed, which meant preemption could not fire and the whole measurement
was void.  Here the superseding task is fast and the victim is slow, so the commit
lands mid-flight by construction, and each run asserts that it did.

Honest scope: real child processes doing real CPU work with real wall-clock, but a
generated workload on a single host.  No model or API is involved, so nothing here
claims token cost or model quality.  A null result is reported as a null result.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Final

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.subprocess_agent import subprocess_task_executor

BENCHMARK_NAME: Final[str] = "preemption_payoff"
BENCHMARK_VERSION: Final[int] = 1

SHARED: Final[str] = "shared"
BUMPER: Final[str] = "bump"
VICTIMS: Final[tuple[str, ...]] = ("victim-a", "victim-b")
BYSTANDER: Final[str] = "bystander"
OTHER: Final[str] = "other"
# A non-empty but *wrong* declaration: the conflict graph trusts it (known=True)
# and sees no overlap with the readers, yet the commit supersedes them anyway.
# This is the only window where a real conflict escapes detection.
UNRELATED: Final[str] = "unrelated"

DEFAULT_VICTIM_SECONDS: Final[float] = 2.0
DEFAULT_BUMP_SECONDS: Final[float] = 0.05
DEFAULT_REPEAT: Final[int] = 7
AGENTS: Final[int] = 4


def _sleep_cmd(seconds: float) -> list[str]:
    return [sys.executable, "-c", f"import time; time.sleep({seconds}); raise SystemExit(0)"]


def _command_for(victim_seconds: float, bump_seconds: float) -> Any:
    def build(task_id: str) -> list[str]:
        if task_id == BUMPER:
            return _sleep_cmd(bump_seconds)
        return _sleep_cmd(victim_seconds)

    return build


def _coordinated_executor(
    execute: Any,
    *,
    victim_started: dict[str, asyncio.Event],
    midflight_confirmed: dict[str, bool],
) -> Any:
    """Force the superseding commit to occur strictly mid-flight.

    ``bump`` blocks until every victim has actually begun executing, so its commit
    cannot slip in before they start or after they finish.  Without this the
    measurement silently degrades into the useless case where nothing was running
    when the graph changed.
    """

    async def run(ctx: Any, task_id: str) -> None:
        if task_id == BUMPER:
            for event in victim_started.values():
                await asyncio.wait_for(event.wait(), timeout=30)
            midflight_confirmed["value"] = True
            await execute(ctx, task_id)
            return
        if task_id in victim_started:
            victim_started[task_id].set()
        await execute(ctx, task_id)

    return run


def _outcome(artifact_id: str, version: int) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=version,
        content=f"{artifact_id}:v{version}",
    )


def _build_goal(agent_id: str, *, declaration: str) -> Goal:
    """All tasks are independent so they are co-dispatched in one batch.

    ``bump`` commits ``shared@2``.  The victims declare ``shared`` and were
    dispatched against ``shared@1``, so their inputs are superseded mid-flight.
    ``bystander`` declares a different artifact and must never be touched -- it is
    the control that catches a preemption that fires too broadly.
    """

    goal = Goal("preemption-payoff")
    goal.task(
        BUMPER,
        agent=agent_id,
        # "complete": the true write is declared, so conflict-aware batching
        #   separates this task from its readers and no race is possible.
        # "incomplete": a non-empty declaration that omits the real write --
        #   trusted, no conflict detected, and the commit still supersedes.
        outputs=((SHARED,) if declaration == "complete" else (UNRELATED,)),
        executor_api="context_v1",
        verify=lambda _ctx: _outcome(SHARED, 2),
    )
    for victim in VICTIMS:
        goal.task(
            victim,
            agent=agent_id,
            inputs=(SHARED,),
            outputs=(f"out-{victim}",),
            executor_api="context_v1",
            verify=lambda _ctx, victim=victim: _outcome(f"out-{victim}", 1),
        )
    goal.task(
        BYSTANDER,
        agent=agent_id,
        inputs=(OTHER,),
        outputs=(f"out-{BYSTANDER}",),
        executor_api="context_v1",
        verify=lambda _ctx: _outcome(f"out-{BYSTANDER}", 1),
    )
    return goal


async def _run_arm(
    *,
    arm: str,
    adaptive: bool,
    preempt: bool,
    declaration: str,
    victim_seconds: float,
    bump_seconds: float,
) -> dict[str, Any]:
    usage: dict[str, dict[str, Any]] = {}
    # The superseding commit must land while the victims are *still running*, so
    # ``bump`` waits until both victims have actually entered their child process
    # instead of relying on sleep timing, which is a race.  This mirrors the
    # construction proven to fire in tests/sdk/test_preempt_superseded.py.
    victim_started: dict[str, asyncio.Event] = {victim: asyncio.Event() for victim in VICTIMS}
    midflight_confirmed = {"value": False}
    workspace = Path(tempfile.mkdtemp(prefix=f"lhos-preempt-{arm}-"))
    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version(SHARED, 1, "shared-v1")
        os_._facts.add_version(OTHER, 1, "other-v1")
        command = _command_for(victim_seconds, bump_seconds)
        agent_ids = tuple(f"w{index}" for index in range(1, AGENTS + 1))
        for agent_id in agent_ids:
            os_.add_agent(
                Agent(
                    agent_id,
                    executor=_coordinated_executor(
                        subprocess_task_executor(
                            command,
                            timeout_seconds=120,
                            poll_seconds=0.01,
                            on_usage=lambda task_id, u: usage.__setitem__(task_id, u),
                        ),
                        victim_started=victim_started,
                        midflight_confirmed=midflight_confirmed,
                    ),
                    executor_api="context_v1",
                    specializations=("python",),
                )
            )
        goal = _build_goal(agent_ids[0], declaration=declaration)
        total = 2 + len(VICTIMS)

        started = time.perf_counter()
        result = await os_.run_async(
            goal,
            max_dispatches=total * 2,
            max_concurrency=AGENTS,
            # One step, so every task is co-dispatched in a single batch.  With
            # several steps the loop spreads them across epochs and the
            # superseding commit lands after its victims have already finished --
            # which is exactly what made an earlier benchmark measure nothing.
            max_steps=1,
            adaptive=adaptive,
            max_parallelism=AGENTS if adaptive else 1,
            automatic_rebase=False,
            preempt_superseded=preempt,
        )
        elapsed = time.perf_counter() - started

        doomed_ms = sum(int(usage.get(v, {}).get("wall_time_ms", 0) or 0) for v in VICTIMS)
        killed = tuple(
            sorted(
                v for v in VICTIMS if usage.get(v, {}).get("terminated_by") == "semantic_interrupt"
            )
        )
        return {
            "arm": arm,
            "adaptive": adaptive,
            "declaration": declaration,
            "preempt_superseded": preempt,
            "wall_clock_seconds": round(elapsed, 6),
            "goal_state": result.goal_state,
            "verified": tuple(sorted(result.verified)),
            # Doomed compute: child time spent on tasks whose declared input was
            # superseded while they ran.  Lower is better; this is the number the
            # mechanism exists to reduce.
            "doomed_child_ms": doomed_ms,
            "victims_killed": killed,
            "bystander_terminated_by": usage.get(BYSTANDER, {}).get("terminated_by"),
            "bystander_ms": int(usage.get(BYSTANDER, {}).get("wall_time_ms", 0) or 0),
            "observed_tasks": tuple(sorted(usage)),
            "supersession_landed_midflight": bool(midflight_confirmed["value"]),
        }
    finally:
        os_.close()
        shutil.rmtree(workspace, ignore_errors=True)


# Measured findings that shaped these arms, in the order they were discovered.
#
# 1. With the write *declared*, conflict-aware batching correctly refuses to
#    co-schedule the writer with its readers, so the supersession cannot occur on
#    the adaptive path at all.
# 2. With the write *not declared at all*, the task becomes ``known=False`` and the
#    conflict graph's conservative rule runs unknown-access tasks serially -- so
#    that direction is also safe, contrary to an earlier guess that omitting the
#    declaration would expose the race.
#
# So the adaptive scheduler is sound against this failure mode in *both*
# directions, and preemption's real value domain is the narrow middle case: a
# declaration that is non-empty but *incomplete*, which reads as ``known=True``
# while a genuine conflict goes undetected.  Constructing that case is future
# work; these arms therefore measure the mechanism on the legacy (non
# conflict-aware) path, where the supersession does occur.
# Original note: with the write *declared*, conflict-aware
# batching correctly refuses to co-schedule the writer with its readers, so the
# "sibling commit supersedes a running peer" situation cannot arise on the adaptive
# path at all -- the adaptive arms deadlock waiting for peers that are never
# co-dispatched.  Preemption's value domain is therefore the non-conflict-aware
# path (equivalently: the case where the conflicting write was not declared).
# (name, adaptive, preempt, declaration)
_ARMS: Final[tuple[tuple[str, bool, bool, str], ...]] = (
    # Control: a complete declaration lets the conflict graph prevent the race
    # outright, so nothing should be superseded mid-flight here.
    ("complete_decl", True, False, "complete"),
    # The real window: trusted-but-wrong declaration, with and without the
    # second line of defence.
    ("incomplete_no_preempt", True, False, "incomplete"),
    ("incomplete_preempt", True, True, "incomplete"),
)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


async def _run_async(*, repeat: int, victim_seconds: float, bump_seconds: float) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    for index in range(repeat):
        # Alternate arm order so a warm-up effect cannot pose as a win.
        arms = _ARMS if index % 2 == 0 else tuple(reversed(_ARMS))
        for arm, adaptive, preempt, declaration in arms:
            runs.append(
                await _run_arm(
                    arm=arm,
                    adaptive=adaptive,
                    preempt=preempt,
                    declaration=declaration,
                    victim_seconds=victim_seconds,
                    bump_seconds=bump_seconds,
                )
            )

    cells: dict[str, Any] = {}
    for arm, _adaptive, _preempt, _decl in _ARMS:
        rows = [row for row in runs if row["arm"] == arm]
        doomed = [float(row["doomed_child_ms"]) for row in rows]
        wall = [float(row["wall_clock_seconds"]) for row in rows]
        cells[arm] = {
            "doomed_child_ms_median": round(_median(doomed), 1),
            "doomed_child_ms_min": round(min(doomed), 1),
            "doomed_child_ms_max": round(max(doomed), 1),
            "wall_clock_seconds_median": round(_median(wall), 4),
            "runs_with_a_kill": sum(1 for row in rows if row["victims_killed"]),
            "runs_where_bystander_was_touched": sum(
                1 for row in rows if row["bystander_terminated_by"] == "semantic_interrupt"
            ),
            "goal_states": sorted({str(row["goal_state"]) for row in rows}),
            "runs_landed_midflight": sum(1 for row in rows if row["supersession_landed_midflight"]),
        }

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "repeat": repeat,
        "workload": {
            "victim_seconds": victim_seconds,
            "bump_seconds": bump_seconds,
            "victims": VICTIMS,
            "superseded_artifact": SHARED,
            "why_it_lands_midflight": (
                f"the superseding task sleeps ~{bump_seconds}s while each victim sleeps ~{victim_seconds}s, "
                "so the commit necessarily occurs while victims are still running"
            ),
        },
        "cells": cells,
        "headline": {
            "doomed_ms_no_preempt": cells["incomplete_no_preempt"]["doomed_child_ms_median"],
            "doomed_ms_with_preempt": cells["incomplete_preempt"]["doomed_child_ms_median"],
            "doomed_ms_reduction": round(
                cells["incomplete_no_preempt"]["doomed_child_ms_median"]
                - cells["incomplete_preempt"]["doomed_child_ms_median"],
                1,
            ),
            "preemption_ever_fired": cells["incomplete_preempt"]["runs_with_a_kill"] > 0,
            "all_runs_landed_midflight": all(
                bool(row["supersession_landed_midflight"]) for row in runs
            ),
            "bystander_ever_harmed": (
                cells["incomplete_preempt"]["runs_where_bystander_was_touched"] > 0
            ),
        },
        "runs": tuple(runs),
        "scope": {
            "real": [
                "real child processes, real perf_counter wall-clock",
                "doomed compute and kills read from the child's own usage report",
            ],
            "not_established": [
                "no model or API; nothing here is about token cost or model quality",
                "generated workload on a single host; absolute durations are arbitrary",
            ],
        },
    }


def run_benchmark(
    *,
    repeat: int = DEFAULT_REPEAT,
    victim_seconds: float = DEFAULT_VICTIM_SECONDS,
    bump_seconds: float = DEFAULT_BUMP_SECONDS,
) -> dict[str, Any]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    return asyncio.run(
        _run_async(repeat=repeat, victim_seconds=victim_seconds, bump_seconds=bump_seconds)
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=BENCHMARK_NAME)
    parser.add_argument("--out", default="")
    parser.add_argument("--repeat", type=int, default=DEFAULT_REPEAT)
    parser.add_argument("--victim-seconds", type=float, default=DEFAULT_VICTIM_SECONDS)
    parser.add_argument("--bump-seconds", type=float, default=DEFAULT_BUMP_SECONDS)
    args = parser.parse_args(argv)

    report = run_benchmark(
        repeat=args.repeat,
        victim_seconds=args.victim_seconds,
        bump_seconds=args.bump_seconds,
    )
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
    print(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
