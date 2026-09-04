"""Regression tests for the scheduling-regime benchmark.

Wall-clock is not gated. On this workload the run-to-run spread of the
static-versus-adaptive ratio was measured at 1.08x-1.28x on *identical* code, so
any assertion about it would be an assertion about the host.

What is gated is everything that makes the measurement interpretable:

* both workload shapes still build, because the *symmetric* one is the shape that
  produced a null result and it silently stopped being reachable once the module
  was retuned for the asymmetric one;
* the adaptive arm schedules the critical-path chain earlier than a static plan,
  which is the actual claim and is a deterministic property of dispatch order
  rather than of timing;
* the reclaimable-capacity metric obeys its own definition, because a capacity
  metric that over-counts would manufacture a batch-barrier problem that is not
  there -- an earlier version of it summed per task and reported 30 seconds of
  loss inside a 4-second run.
"""

from __future__ import annotations

import pytest

from lhos.benchmarks.scheduling_regimes import (
    AGENTS,
    CRITICAL_PATH,
    SHAPES,
    SYMMETRIC,
    run_benchmark,
)

# Small enough to stay fast, large enough that each child does real CPU work.
_FAST_ITERATIONS = 1_500


@pytest.fixture(scope="module")
def critical_path_report() -> dict:
    return run_benchmark(iterations=_FAST_ITERATIONS, repeat=1, shape=CRITICAL_PATH)


def _stable(report: dict, arm: str) -> dict:
    return next(
        row for row in report["results"] if row["arm"] == arm and row["condition"] == "stable"
    )


def test_both_shapes_are_reachable_and_distinct() -> None:
    """The symmetric shape is the null result; losing it loses the claim's boundary."""

    assert set(SHAPES) == {"symmetric", "critical-path"}
    symmetric = run_benchmark(iterations=_FAST_ITERATIONS, repeat=1, shape=SYMMETRIC)
    assert symmetric["workload"]["total_tasks"] == 2 + 2 * SYMMETRIC.chain_length
    assert symmetric["workload"]["shape"] == "symmetric"


def test_workload_metadata_reports_the_real_shape(critical_path_report: dict) -> None:
    """Metadata used to report a stale constant instead of the actual counts."""

    workload = critical_path_report["workload"]
    assert workload["shape"] == "critical-path"
    assert workload["chain_length"] == CRITICAL_PATH.chain_length
    assert workload["branch_count"] == CRITICAL_PATH.branch_count
    assert workload["total_tasks"] == 2 + CRITICAL_PATH.chain_length + CRITICAL_PATH.branch_count


def test_adaptive_starts_the_critical_path_earlier(critical_path_report: dict) -> None:
    """The claim, stated over dispatch order rather than wall-clock."""

    static = _stable(critical_path_report, "static_parallel")
    adaptive = _stable(critical_path_report, "lhos_adaptive")

    assert static["chain_priority_index"] is not None
    assert adaptive["chain_priority_index"] is not None
    assert adaptive["chain_priority_index"] < static["chain_priority_index"]


def test_reclaimable_capacity_never_exceeds_idle_capacity(critical_path_report: dict) -> None:
    """Reclaimable capacity is idle capacity that runnable work was waiting for.

    It is therefore a subset of idle capacity by construction. Asserting the
    containment is what catches a metric that double counts waiting tasks.
    """

    for arm in ("static_parallel", "lhos_adaptive"):
        row = _stable(critical_path_report, arm)
        idle = row["slot_occupancy"]["idle_slot_seconds"]
        reclaimable = row["barrier_stall"]["reclaimable_slot_seconds"]
        assert idle is not None and reclaimable is not None
        assert 0.0 <= reclaimable <= idle + 1e-6, f"{arm}: {reclaimable} exceeds idle {idle}"
        assert row["barrier_stall"]["reclaimable_fraction_of_capacity"] <= 1.0


def test_single_slot_arm_reports_capacity_as_unavailable(critical_path_report: dict) -> None:
    """One slot cannot have spare capacity, so the metric must decline to score it."""

    serial = _stable(critical_path_report, "serial")
    assert serial["agents"] == 1
    assert serial["barrier_stall"]["reclaimable_slot_seconds"] is None
    assert serial["slot_occupancy"]["idle_slot_seconds"] is None


def test_parallel_arms_actually_used_their_slots(critical_path_report: dict) -> None:
    """A parallel arm that never overlapped would make every ratio meaningless."""

    for arm in ("static_parallel", "lhos_adaptive"):
        row = _stable(critical_path_report, arm)
        assert row["agents"] == AGENTS
        assert row["peak_parallelism"] > 1


def test_churn_preserves_the_untouched_root(critical_path_report: dict) -> None:
    """Only the changed root's cone is redone; the other root's subtree survives."""

    churn = next(
        row
        for row in critical_path_report["results"]
        if row["arm"] == "lhos_adaptive" and row["condition"] == "churn"
    )
    repair = churn["repair"]
    assert repair["applied"] is True
    assert repair["preserved_count"] > 0
    assert repair["affected_count"] > 0
    assert not set(repair["affected"]) & set(repair["preserved"])


def test_dispatch_lookahead_trades_utilisation_for_critical_path_order() -> None:
    """Admitting surplus work up front is measured to be a bad trade.

    Handing the pool more than ``max_concurrency`` jobs does make it
    work-conserving -- ``AsyncWorkerPool`` refills a freed slot from within a
    batch on its own. But ranking is then applied once over a larger set, so the
    order is committed earlier and the per-batch re-ranking that produced the
    critical-path advantage is lost.

    Measured at 60k iterations: reclaimable capacity fell 40% -> 23% while
    ``chain_priority`` degraded 0.34 -> 0.68 across lookahead 1/2/4, and
    wall-clock did not improve. This test pins the *direction* of that trade at a
    fast setting rather than the magnitudes, so it documents why
    ``dispatch_lookahead`` defaults to 1 and must not be raised as a free win.
    """

    import asyncio

    from lhos.benchmarks.scheduling_regimes import _run_arm

    async def chain_priority_at(lookahead: int) -> float:
        row = await _run_arm(
            arm="lhos_adaptive",
            adaptive=True,
            agents=AGENTS,
            condition="stable",
            iterations=_FAST_ITERATIONS,
            shape=CRITICAL_PATH,
            dispatch_lookahead=lookahead,
        )
        assert row["chain_priority_index"] is not None
        return float(row["chain_priority_index"])

    async def run() -> tuple[float, float]:
        return await chain_priority_at(1), await chain_priority_at(4)

    baseline, widened = asyncio.run(run())
    assert baseline <= widened, (
        "raising dispatch_lookahead should not improve critical-path order; "
        f"lookahead=1 gave {baseline}, lookahead=4 gave {widened}"
    )
