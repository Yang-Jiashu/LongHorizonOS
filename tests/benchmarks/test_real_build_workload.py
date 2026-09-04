"""Regression tests for the real-work baseline-versus-LHOS benchmark.

Wall-clock is deliberately not gated: it is real, and therefore noisy on a shared
host.  The benchmark itself reports a distribution across repetitions; these
tests pin the deterministic facts that make that distribution meaningful -- same
VERIFIED goal from both arms, real overlap in one and none in the other, and a
schedule that respects the shared dependency.
"""

from __future__ import annotations

from lhos.benchmarks.real_build_workload import (
    CORE_TASK,
    MODULE_COUNT,
    run_benchmark,
)

# Small enough to stay fast, large enough that the child does real work.
_FAST_ITERATIONS = 2_000


def test_both_arms_reach_the_same_verified_goal() -> None:
    report = run_benchmark(iterations=_FAST_ITERATIONS)

    assert report["comparison"]["both_goals_closed"] is True
    assert report["comparison"]["reached_same_verified_goal"] is True
    assert report["baseline"]["verified_count"] == MODULE_COUNT + 1
    assert report["lhos"]["verified_count"] == MODULE_COUNT + 1


def test_every_task_ran_as_a_real_child_process() -> None:
    report = run_benchmark(iterations=_FAST_ITERATIONS)

    for arm in ("baseline", "lhos"):
        assert report[arm]["executed_count"] == MODULE_COUNT + 1
        # Usage is captured from the child, so a reporting child proves the work
        # left the parent process rather than being simulated in-process.
        assert report[arm]["children_reporting_usage"] == MODULE_COUNT + 1
        assert report[arm]["child_wall_time_ms_total"] > 0


def test_only_the_adaptive_arm_overlaps_work() -> None:
    report = run_benchmark(iterations=_FAST_ITERATIONS)

    assert report["baseline"]["peak_parallelism"] == 1
    assert report["lhos"]["peak_parallelism"] > 1


def test_the_shared_dependency_is_scheduled_before_its_consumers() -> None:
    """``core`` is a real dependency, so no module may be dispatched before it."""

    report = run_benchmark(iterations=_FAST_ITERATIONS)
    sequence = report["lhos"]["dispatch_sequence"]

    assert sequence, "adaptive arm recorded no dispatch sequence"
    assert sequence[0] == CORE_TASK
    assert all(task_id != CORE_TASK for task_id in sequence[1:])


def test_repetition_reports_a_distribution_rather_than_one_run() -> None:
    report = run_benchmark(iterations=_FAST_ITERATIONS, repeat=2)

    distribution = report["distribution"]
    assert report["repeat"] == 2
    assert len(distribution["speedup_all"]) == 2
    assert distribution["speedup_min"] <= distribution["speedup_median"]
    assert distribution["speedup_median"] <= distribution["speedup_max"]
    assert report["correctness"]["all_runs_same_verified_goal"] is True
    # Both arm orders must be exercised so warm-up cannot pose as a win.
    assert [run["baseline_first"] for run in report["runs"]] == [True, False]
