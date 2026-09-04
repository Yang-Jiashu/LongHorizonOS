"""Regression tests for the parallelism-regime benchmark.

Wall-clock is not gated. What is gated is that the comparison stays *honest*:
the baseline must remain the best fixed degree chosen in hindsight, every arm
must be given the same agent pool, and every arm must actually close the Goal.

The measured result is currently a **null**: online degree selection loses to the
best fixed degree. These tests exist to keep that reading trustworthy rather than
to defend it, because the most likely way this benchmark goes wrong is by
quietly acquiring an easier baseline.
"""

from __future__ import annotations

import pytest

from lhos.benchmarks.parallelism_regimes import (
    DEGREES,
    MAX_DEGREE,
    NARROW_LENGTH,
    WIDE_COUNT,
    run_benchmark,
)

_FAST_ITERATIONS = 1_200


@pytest.fixture(scope="module")
def report() -> dict:
    return run_benchmark(iterations=_FAST_ITERATIONS, repeat=1)


def test_every_arm_closes_the_same_goal(report: dict) -> None:
    """A faster arm that failed to close would be meaningless."""

    assert report["headline"]["all_arms_closed"] is True
    expected = 1 + WIDE_COUNT + NARROW_LENGTH
    for row in report["runs"]:
        assert row["goal_state"] == "closed", row["arm"]
        assert row["verified"] == expected, row["arm"]


def test_baseline_is_the_best_fixed_degree_not_the_worst(report: dict) -> None:
    """The opponent must be the winner of the sweep, not a strawman."""

    headline = report["headline"]
    fixed_medians = {
        arm: cell["seconds_median"]
        for arm, cell in report["cells"].items()
        if arm.startswith("fixed_")
    }
    assert len(fixed_medians) == len(DEGREES)
    assert headline["best_fixed_seconds_median"] == min(fixed_medians.values())
    assert headline["best_fixed_arm"] in fixed_medians


def test_fixed_arms_actually_reached_their_degree(report: dict) -> None:
    """A degree that never materialised would make the sweep fictional."""

    for degree in DEGREES:
        cell = report["cells"][f"fixed_{degree}"]
        # The wide phase is a pure fan-out, so it is the only phase where the
        # requested degree can be observed at all.
        assert cell["wide_peak_distinct"], f"degree {degree} produced no wide-phase overlap"
        assert max(cell["wide_peak_distinct"]) <= degree


def test_narrow_phase_cannot_use_extra_slots(report: dict) -> None:
    """Documents why this workload does not yet exercise the decision.

    The narrow phase is a serial chain, so every arm is pinned to one concurrent
    task there regardless of its degree. Extra slots therefore cost nothing, which
    makes "always pick the maximum" optimal and leaves online selection with
    nothing to win. Locking this in so the limitation is not forgotten: a workload
    where a higher degree carries a real penalty is required before this benchmark
    can test the mechanism rather than its overhead.
    """

    for cell in report["cells"].values():
        assert cell["narrow_peak_distinct"] == [1]


def test_adaptive_arm_records_the_degrees_it_chose(report: dict) -> None:
    """Without the per-epoch degrees the adaptive arm is unattributable."""

    adaptive_runs = [row for row in report["runs"] if row["arm"] == "adaptive"]
    assert adaptive_runs
    for row in adaptive_runs:
        assert row["degree"] is None
        assert row["degrees_chosen"], "no parallelism decision was recorded"
        assert all(int(value) >= 0 for value in row["degrees_chosen"])
        assert max(int(value) for value in row["degrees_chosen"]) <= MAX_DEGREE
