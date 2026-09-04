"""Tests for the bounded conflict-aware adaptive-runtime benchmark."""

from __future__ import annotations

import pytest

from lhos.benchmarks.adaptive_runtime import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_CONCURRENCY,
    run_benchmark,
)


def test_adaptive_runtime_controlled_gate() -> None:
    report = run_benchmark(
        delay_seconds=DEFAULT_DELAY_SECONDS,
        max_concurrency=DEFAULT_MAX_CONCURRENCY,
    )

    assert report["valid"]
    assert report["violations"] == []
    assert report["static"]["correctness"]["goal_closed"]
    assert report["adaptive"]["correctness"]["goal_closed"]
    assert report["static"]["stale_work"] >= 1
    assert report["static"]["rework_attempts"] >= 1
    assert report["adaptive"]["stale_work"] == 0
    assert report["adaptive"]["rework_attempts"] == 0
    assert report["adaptive"]["conflict_overlap_events"] == 0
    assert report["comparison"]["executed_attempt_reduction"] >= 1

    for mode, expected_attempts in (("static", 5), ("adaptive", 4)):
        case = report[mode]
        audit = case["runtime_audit"]
        assert case["executed_attempts"] == expected_attempts
        assert audit["scheduler_attempts"] == expected_attempts
        assert audit["claims_created"] == expected_attempts
        assert audit["claims_with_kernel_lease"] == expected_attempts
        assert audit["claims_with_positive_fence"] == expected_attempts
        assert audit["attempt_state_counts"]["verified_semantically"] == 4
        assert audit["pass_evidence_nodes"] == 4
        assert audit["valid_evidence_bindings_by_task"] == {
            "a-conflict": 1,
            "b-conflict": 1,
            "c-independent": 1,
            "d-independent": 1,
        }
        assert audit["active_claims_after_run"] == 0
        assert audit["active_reservations_after_run"] == 0
        assert audit["live_kernel_leases_after_run"] == 0

    assert report["static"]["runtime_audit"]["attempt_state_counts"]["failed"] == 1
    assert report["static"]["runtime_audit"]["scheduler_event_counts"]["execution_failed"] == 1
    assert "failed" not in report["adaptive"]["runtime_audit"]["attempt_state_counts"]
    assert report["scope"]["fake_executor"] is True
    assert report["scope"]["wall_clock_measured"] is True
    assert "TaskClaim" in report["scope"]["authoritative_path"]
    assert "Kernel Lease" in report["scope"]["authoritative_path"]


def test_adaptive_trace_is_conflict_safe_and_repeatable() -> None:
    first = run_benchmark(delay_seconds=0.002, max_concurrency=2)
    second = run_benchmark(delay_seconds=0.002, max_concurrency=2)

    for report in (first, second):
        assert report["valid"]
        assert report["adaptive"]["selected_parallelism_peak"] == 2
        assert report["adaptive"]["selected_parallelism_trace"][:2] == [2, 2]
        assert report["adaptive"]["attempts_by_task"] == {
            "a-conflict": 1,
            "b-conflict": 1,
            "c-independent": 1,
            "d-independent": 1,
        }

    # Wall-clock values include local scheduling/SQLite noise; compare only
    # the deterministic safety and work counters.
    assert first["static"]["attempts_by_task"] == second["static"]["attempts_by_task"]
    assert first["adaptive"]["records"] == second["adaptive"]["records"]
    for mode in ("static", "adaptive"):
        first_audit = dict(first[mode]["runtime_audit"])
        second_audit = dict(second[mode]["runtime_audit"])
        # Each AgentOS instance intentionally creates a fresh graph identity.
        first_audit.pop("graph_id")
        second_audit.pop("graph_id")
        assert first_audit == second_audit


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"delay_seconds": 0}, "delay_seconds"),
        ({"delay_seconds": True}, "delay_seconds"),
        ({"max_concurrency": 1}, "max_concurrency"),
        ({"max_concurrency": True}, "max_concurrency"),
    ],
)
def test_adaptive_runtime_rejects_invalid_parameters(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        run_benchmark(**kwargs)
