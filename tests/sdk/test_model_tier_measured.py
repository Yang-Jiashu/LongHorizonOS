"""Measured verification history raises the model tier.

Every other tier signal is structural (fan-out, critical path) or declared.  This
one is the only signal derived from *outcomes*: a task that keeps failing is
evidence that it needs a stronger model.  It composes one-directionally, like
the graph floor -- measured failure may raise the tier, never lower it.

A single failure must not escalate anything, or one unlucky attempt would push
every task onto the expensive model.
"""

from __future__ import annotations

from lhos.sdk.compute_calibration import TaskOutcomeCounts
from lhos.sdk.compute_routing import (
    MEASURED_MIN_SAMPLES,
    ModelTier,
    _measured_failure_floor,
)


def _counts(*, verified: int, failed: int) -> TaskOutcomeCounts:
    return TaskOutcomeCounts(task_id="t", verified=verified, work_failed=failed)


def test_no_history_raises_nothing() -> None:
    floor, success_bp, samples = _measured_failure_floor(None)

    assert floor is None
    assert samples == 0
    assert success_bp == -1


def test_a_single_failure_is_noise_and_does_not_escalate() -> None:
    floor, _bp, samples = _measured_failure_floor(_counts(verified=0, failed=1))

    assert samples < MEASURED_MIN_SAMPLES
    assert floor is None


def test_mostly_failing_task_is_pushed_to_strong() -> None:
    floor, success_bp, samples = _measured_failure_floor(_counts(verified=1, failed=4))

    assert samples == 5
    assert success_bp == 2_000
    assert floor is ModelTier.STRONG


def test_middling_task_is_pushed_to_standard() -> None:
    floor, success_bp, _samples = _measured_failure_floor(_counts(verified=7, failed=3))

    assert success_bp == 7_000
    assert floor is ModelTier.STANDARD


def test_reliable_task_is_left_alone() -> None:
    floor, success_bp, _samples = _measured_failure_floor(_counts(verified=10, failed=0))

    assert success_bp == 10_000
    assert floor is None


def test_quarantined_attempts_do_not_count_as_work_failures() -> None:
    """Input churn is a verdict about the inputs, not about the model."""

    churned = TaskOutcomeCounts(task_id="t", verified=2, work_failed=0, input_churned=6)

    floor, success_bp, samples = _measured_failure_floor(churned)

    assert samples == 2
    assert success_bp == 10_000
    assert floor is None
