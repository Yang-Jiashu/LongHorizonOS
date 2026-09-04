"""Outcome-derived calibration of the two observable ranking inputs.

The budget policy ranks work by expected verified progress per unit cost.  Cost
is corrected from measured usage elsewhere; these tests cover the other half --
how often a task actually verifies, and how often its inputs held still long
enough for it to commit.  Both were caller-declared opinions, which made the
ranking an optimisation of numbers the caller made up.

The load-bearing distinction here is that a quarantined attempt is *not* a work
failure.  Blaming the executor for upstream churn would push the scheduler away
from tasks whose real problem is instability, which is the opposite of the
intended behaviour.
"""

from __future__ import annotations

from types import SimpleNamespace

from lhos.sdk.compute_budget import TaskComputeEstimate
from lhos.sdk.compute_calibration import (
    BASIS_POINTS_SCALE,
    OUTCOME_PRIOR_SAMPLES,
    calibrate_outcome_estimate,
    calibrate_outcome_estimates,
    observe_task_outcomes,
)


def _attempt(task_id: str, state: str) -> SimpleNamespace:
    return SimpleNamespace(task_id=task_id, state=state)


def _estimate(task_id: str = "t", *, success: int = 5_000, stability: int = 5_000):
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=10,
        success_basis_points=success,
        input_stability_basis_points=stability,
        normalized_cost_units=1,
        known=True,
    )


def test_no_history_is_the_identity() -> None:
    estimate = _estimate()

    corrected, audits = calibrate_outcome_estimate(estimate, {})

    assert corrected == estimate
    assert audits == ()


def test_observed_failures_pull_the_success_rate_down() -> None:
    outcomes = observe_task_outcomes(
        [
            _attempt("t", "verified_semantically"),
            _attempt("t", "failed"),
            _attempt("t", "failed"),
            _attempt("t", "crashed"),
        ]
    )
    estimate = _estimate(success=10_000)

    corrected, audits = calibrate_outcome_estimate(estimate, outcomes)

    assert corrected.success_basis_points < estimate.success_basis_points
    success_audit = next(a for a in audits if a.field_name == "success_basis_points")
    assert success_audit.sample_count == 4
    assert success_audit.observed_basis_points == 2_500
    # Blended against the declared rate with the pseudo-sample prior.
    expected = (10_000 * OUTCOME_PRIOR_SAMPLES + 1 * BASIS_POINTS_SCALE) // (
        OUTCOME_PRIOR_SAMPLES + 4
    )
    assert success_audit.corrected == expected


def test_quarantined_attempts_lower_stability_not_success() -> None:
    """Input churn is a verdict about the inputs, not about the work."""

    outcomes = observe_task_outcomes(
        [
            _attempt("t", "verified_semantically"),
            _attempt("t", "stale_cognition"),
            _attempt("t", "stale_cognition"),
        ]
    )
    estimate = _estimate(success=10_000, stability=10_000)

    corrected, _audits = calibrate_outcome_estimate(estimate, outcomes)

    # One verified, zero work failures: the success rate is not punished.
    assert corrected.success_basis_points == estimate.success_basis_points
    assert corrected.input_stability_basis_points < estimate.input_stability_basis_points


def test_stable_inputs_raise_a_pessimistic_declared_stability() -> None:
    outcomes = observe_task_outcomes([_attempt("t", "verified_semantically") for _ in range(6)])
    estimate = _estimate(stability=0)

    corrected, _audits = calibrate_outcome_estimate(estimate, outcomes)

    assert corrected.input_stability_basis_points > 0


def test_value_and_rework_stay_declared() -> None:
    """Progress value is the caller's objective; rework units have no scale."""

    outcomes = observe_task_outcomes([_attempt("t", "failed") for _ in range(5)])
    estimate = _estimate().model_copy(update={"expected_rework_cost_units": 7})

    corrected, _audits = calibrate_outcome_estimate(estimate, outcomes)

    assert corrected.verified_progress_units == estimate.verified_progress_units
    assert corrected.expected_rework_cost_units == 7


def test_in_flight_and_preempted_attempts_carry_no_verdict() -> None:
    outcomes = observe_task_outcomes(
        [
            _attempt("t", "dispatched"),
            _attempt("t", "running"),
            _attempt("t", "preempted"),
        ]
    )

    assert outcomes == {}


def test_unrelated_tasks_do_not_contaminate_each_other() -> None:
    outcomes = observe_task_outcomes(
        [
            _attempt("good", "verified_semantically"),
            _attempt("bad", "failed"),
            _attempt("bad", "failed"),
        ]
    )

    assert outcomes["good"].work_failed == 0
    assert outcomes["bad"].verified == 0
    good, _ = calibrate_outcome_estimate(_estimate("good", success=8_000), outcomes)
    bad, _ = calibrate_outcome_estimate(_estimate("bad", success=8_000), outcomes)
    assert good.success_basis_points > bad.success_basis_points


def test_container_shape_is_preserved_and_unknown_entries_pass_through() -> None:
    outcomes = observe_task_outcomes([_attempt("t", "failed")])

    mapping, audits = calibrate_outcome_estimates({"t": _estimate(), "raw": {"x": 1}}, outcomes)
    assert isinstance(mapping, dict)
    assert mapping["raw"] == {"x": 1}
    assert audits

    sequence, _ = calibrate_outcome_estimates([_estimate(), "not-an-estimate"], outcomes)
    assert isinstance(sequence, tuple)
    assert sequence[1] == "not-an-estimate"


def test_calibration_is_deterministic_across_attempt_order() -> None:
    attempts = [
        _attempt("t", "verified_semantically"),
        _attempt("t", "failed"),
        _attempt("t", "stale_cognition"),
    ]
    first, _ = calibrate_outcome_estimate(_estimate(), observe_task_outcomes(attempts))
    second, _ = calibrate_outcome_estimate(
        _estimate(), observe_task_outcomes(list(reversed(attempts)))
    )

    assert first == second
