"""Measurement + calibration tests for the compute-budget loop.

These cover the four contract points that close the "declared vs measured"
credibility gap:

1. measured usage is recorded from a real budget-aware execution;
2. a calibrated estimate moves toward the measured value;
3. zero measured history is a strict no-op (today's behavior is preserved);
4. the run/audit output distinguishes declared projection from measured
   observation.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent import ComputationCost
from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeBudgetLimits,
    Goal,
    TaskComputeEstimate,
    UsageLedger,
    UsageVector,
    VerificationOutcome,
)
from lhos.sdk.compute_calibration import (
    MAX_MULTIPLIER_MICROS,
    MIN_MULTIPLIER_MICROS,
    calibrate_estimate,
    calibrate_estimates,
)


def _pass(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(passed=True, artifact_id=task_id, version=1, content=f"{task_id}-v1")


def _estimate(
    task_id: str,
    *,
    tokens: int = 100,
    wall_time_ms: int = 100,
    cost_microusd: int = 100,
    context_tokens: int = 100,
    verification_tokens: int = 100,
) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=1,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=1,
        estimated_tokens=tokens,
        estimated_wall_time_ms=wall_time_ms,
        estimated_cost_microusd=cost_microusd,
        estimated_context_tokens=context_tokens,
        estimated_verification_tokens=verification_tokens,
        known=True,
    )


def _budget_kwargs(estimates, limits):
    return {
        "adaptive": True,
        "budget_aware": True,
        "budget_estimates": estimates,
        "budget_limits": limits,
        "automatic_rebase": False,
    }


def _measured_ledger(
    *,
    declared: UsageVector,
    measured: UsageVector,
    task_id: str = "t",
    attempt_id: str = "a1",
) -> UsageLedger:
    ledger = UsageLedger.empty()
    ledger = ledger.record_estimate("g", task_id, attempt_id, declared)
    return ledger.record_measured("g", task_id, attempt_id, measured)


# 1. Measured usage is recorded from a real execution -----------------------


def test_measured_usage_recorded_from_real_execution() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda _t: ComputationCost(
                    input_tokens=12,
                    output_tokens=8,
                    monetary_micros=500,
                ),
                specializations=("python",),
            )
        )
        goal = Goal("calib-measured")
        goal.task("t", agent="worker", verify=lambda: _pass("t"))

        result = os_.run(
            goal,
            max_dispatches=1,
            **_budget_kwargs(
                {"t": _estimate("t", tokens=100)}, ComputeBudgetLimits(max_tokens=100)
            ),
        )

        assert result.verified == ["t"]
        # The MEASURED state is no longer dead code: a record exists carrying
        # both the declared estimate and the measured outcome.
        aggregate = os_.usage_ledger.aggregate(goal_id="calib-measured")
        assert aggregate.attempt_count == 1
        assert aggregate.measured.tokens == 20  # 12 + 8, from the executor
        assert aggregate.measured.cost_microusd == 500
        # Same figure surfaces on the RunResult under an unambiguous label.
        assert result.meta["measured_usage"]["tokens"] == 20
        assert result.meta["measured_usage"]["cost_microusd"] == 500
    finally:
        os_.close()


# 2. A calibrated estimate moves toward the measured value ------------------


def test_calibrated_estimate_moves_toward_measured() -> None:
    ledger = _measured_ledger(
        declared=UsageVector(
            tokens=10,
            wall_time_ms=10,
            cost_microusd=10,
            context_tokens=10,
            verification_tokens=10,
        ),
        measured=UsageVector(tokens=20, wall_time_ms=5, cost_microusd=30),
    )
    estimate = _estimate(
        "t",
        tokens=10,
        wall_time_ms=10,
        cost_microusd=10,
        context_tokens=10,
        verification_tokens=10,
    )

    corrected, audit = calibrate_estimate(estimate, ledger)

    assert audit is not None and audit.changed
    assert corrected.estimated_tokens == 20  # 2.0x toward measured
    assert corrected.estimated_wall_time_ms == 5  # 0.5x toward measured
    assert corrected.estimated_cost_microusd == 30  # 3.0x toward measured
    # Dimensions with no positive measured signal are left untouched.
    assert corrected.estimated_context_tokens == 10
    assert corrected.estimated_verification_tokens == 10


def test_calibration_multiplier_is_bounded() -> None:
    ledger = _measured_ledger(
        declared=UsageVector(tokens=10),
        measured=UsageVector(tokens=100_000),  # 10_000x raw ratio
    )
    corrected, audit = calibrate_estimate(_estimate("t", tokens=10), ledger)
    assert audit is not None
    # Clamped to the max multiplier: 10 * 4.0x == 40, not 100_000.
    assert corrected.estimated_tokens == 10 * MAX_MULTIPLIER_MICROS // 1_000_000
    assert corrected.estimated_tokens == 40


# 3. Zero history is a strict no-op -----------------------------------------


def test_zero_history_is_a_noop() -> None:
    estimate = _estimate("t", tokens=10)
    corrected, audit = calibrate_estimate(estimate, UsageLedger.empty())
    assert corrected == estimate
    assert audit is None


def test_calibrate_estimates_zero_history_preserves_mapping() -> None:
    estimates = {"t": _estimate("t", tokens=10), "raw": {"task_id": "raw"}}
    calibrated, audits = calibrate_estimates(estimates, UsageLedger.empty())
    assert calibrated == estimates
    assert audits == ()


def test_unknown_estimate_is_never_calibrated() -> None:
    ledger = _measured_ledger(
        declared=UsageVector(tokens=10),
        measured=UsageVector(tokens=50),
    )
    unknown = TaskComputeEstimate.unknown("t")
    corrected, audit = calibrate_estimate(unknown, ledger)
    assert corrected == unknown
    assert audit is None
    assert MIN_MULTIPLIER_MICROS < MAX_MULTIPLIER_MICROS  # sanity on the band


# 4. The audit distinguishes declared projection from measured observation --


def test_audit_distinguishes_declared_from_measured() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda _t: ComputationCost(input_tokens=20, output_tokens=0),
                specializations=("python",),
            )
        )
        goal = Goal("calib-audit")
        goal.task("t", agent="worker", verify=lambda: _pass("t"))

        result = os_.run(
            goal,
            max_dispatches=1,
            **_budget_kwargs(
                {"t": _estimate("t", tokens=100)}, ComputeBudgetLimits(max_tokens=100)
            ),
        )

        # Declared projection (charged at dispatch) and measured observation
        # are reported under separate, unambiguous keys and genuinely differ.
        assert result.meta["budget_usage"]["tokens"] == 100
        assert result.meta["measured_usage"]["tokens"] == 20

        audit = result.meta["adaptive_epochs"][0]["budget_audit"]
        assert "planned_usage_after" in audit
        assert "dispatched_declared_usage_after" in audit
        # The formerly ambiguous "usage_after" (a summed *declared* value that
        # read like realized usage) must not exist anymore.
        assert "usage_after" not in audit
        assert audit["dispatched_declared_usage_after"]["tokens"] == 100
        # First run has no prior measured history: calibration is a no-op.
        assert result.meta["budget_calibration"] == ()
    finally:
        os_.close()


def test_calibration_closes_loop_across_runs() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda _t: ComputationCost(input_tokens=20, output_tokens=0),
                specializations=("python",),
            )
        )
        first = Goal("calib-loop-1")
        first.task("t", agent="worker", verify=lambda: _pass("t"))
        first_result = os_.run(
            first,
            max_dispatches=1,
            **_budget_kwargs(
                {"t": _estimate("t", tokens=100)}, ComputeBudgetLimits(max_tokens=1000)
            ),
        )
        assert first_result.meta["budget_calibration"] == ()
        assert os_.usage_ledger.aggregate(task_id="t", goal_id="calib-loop-1").measured.tokens == 20

        # A later run for the same task-kind is now corrected downward toward
        # the 20 tokens actually measured (bounded by the 0.25x floor -> 25).
        second = Goal("calib-loop-2")
        second.task("t", agent="worker", verify=lambda: _pass("t"))
        second_result = os_.run(
            second,
            max_dispatches=1,
            **_budget_kwargs(
                {"t": _estimate("t", tokens=100)}, ComputeBudgetLimits(max_tokens=1000)
            ),
        )

        calibration = second_result.meta["budget_calibration"]
        assert len(calibration) == 1
        assert calibration[0]["task_id"] == "t"
        token_dim = next(d for d in calibration[0]["dimensions"] if d["dimension"] == "tokens")
        assert token_dim["declared"] == 100
        assert token_dim["corrected"] < 100  # moved toward the measured 20
        assert token_dim["sample_count"] >= 1
    finally:
        os_.close()
