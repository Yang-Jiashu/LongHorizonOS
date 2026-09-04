"""Regression tests for the baseline-versus-LongHorizonOS benchmark.

Wall-clock is deliberately not asserted: it is real but noisy, and a timing
gate would make CI flaky without proving anything about the scheduler.  What is
asserted are the deterministic mechanism facts the benchmark exists to show --
same VERIFIED goal, real overlap, critical-path interleaving, context-residency
hits, and the ranking ablation.
"""

from __future__ import annotations

from lhos.benchmarks.baseline_vs_lhos import run_benchmark


def test_both_arms_reach_the_same_verified_goal() -> None:
    report = run_benchmark()

    assert report["comparison"]["both_goals_closed"] is True
    assert report["comparison"]["reached_same_verified_goal"] is True
    assert report["baseline"]["verified_count"] == report["workload"]["total_tasks"]
    assert report["lhos"]["verified_count"] == report["workload"]["total_tasks"]


def test_lhos_arm_actually_overlaps_work_and_baseline_does_not() -> None:
    report = run_benchmark()

    assert report["baseline"]["peak_parallelism"] == 1
    assert report["lhos"]["peak_parallelism"] > 1


def test_lhos_advances_the_critical_path_in_every_epoch() -> None:
    """The chain is what bounds the makespan, so it must not be starved."""

    report = run_benchmark()
    sequence = report["lhos"]["dispatch_sequence"]
    chain = [task_id for task_id in sequence if task_id.startswith("chain-")]

    assert chain == sorted(chain), "chain must be dispatched in dependency order"
    # The last chain task cannot be the very last dispatch by accident alone:
    # a starving policy finishes every side branch first.
    assert sequence.index("chain-2") < sequence.index("side-6")


def test_context_residency_produces_warm_dispatches() -> None:
    report = run_benchmark()

    assert report["lhos"]["warm_dispatch_count"] > 0
    assert set(report["lhos"]["warm_dispatches"]).issubset(set(report["lhos"]["dispatch_sequence"]))


def test_ranking_ablation_isolates_critical_path_ordering() -> None:
    """Lexical ranking misses the critical path once task ids are adversarial."""

    ablation = run_benchmark()["ranking_ablation"]

    assert ablation["orders_differ"] is True
    assert ablation["graph_utility_selects_critical_path_head"] is True
    assert ablation["lexical_selects_critical_path_head"] is False
