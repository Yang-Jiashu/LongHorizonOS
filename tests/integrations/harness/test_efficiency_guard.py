"""Focused tests for the opt-in Harness efficiency guard."""

from __future__ import annotations

from lhos.integrations.harness import (
    HarnessBestCheckpoint,
    HarnessContinuationAction,
    HarnessContinuationDecision,
    HarnessEfficiencyDisposition,
    HarnessEfficiencyEstimate,
    HarnessEfficiencyGuardObservation,
    HarnessQualityObservation,
    HarnessUsage,
    QualityConstrainedEfficiencyGuard,
)


def _candidate(
    action: HarnessContinuationAction = HarnessContinuationAction.RESTART_COMPACTED,
) -> HarnessContinuationDecision:
    return HarnessContinuationDecision(
        action=action,
        reason="context_bloat:cache_tokens_per_call",
        context_score=2.0,
        cache_tokens_per_call=50_000,
        decision_hash="a" * 64,
    )


def _estimate(
    *,
    avoided_tokens: int = 300,
    overhead_tokens: int = 50,
    avoided_wall_ms: int = 300,
    overhead_wall_ms: int = 50,
    confidence: float = 1.0,
) -> HarnessEfficiencyEstimate:
    return HarnessEfficiencyEstimate(
        baseline=HarnessUsage(
            output_tokens=100,
            cache_read_tokens=900,
            wall_time_ms=1_000,
            tool_calls=10,
        ),
        avoided=HarnessUsage(
            output_tokens=avoided_tokens,
            wall_time_ms=avoided_wall_ms,
            tool_calls=3,
        ),
        overhead=HarnessUsage(
            output_tokens=overhead_tokens,
            wall_time_ms=overhead_wall_ms,
            tool_calls=1,
        ),
        confidence=confidence,
    )


def _observation(**updates: object) -> HarnessEfficiencyGuardObservation:
    values: dict[str, object] = {
        "task_id": "task-1",
        "estimate": _estimate(),
    }
    values.update(updates)
    return HarnessEfficiencyGuardObservation(**values)


def test_disabled_guard_preserves_candidate_and_does_not_require_estimate() -> None:
    candidate = _candidate(HarnessContinuationAction.RESUME)

    decision = QualityConstrainedEfficiencyGuard().decide(
        candidate,
        HarnessEfficiencyGuardObservation(task_id="task-1"),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.APPLY
    assert decision.effective_action is candidate.action
    assert decision.reason == "guard_disabled"
    assert decision.net_token_units == 0
    assert decision.net_wall_time_ms == 0


def test_one_shot_task_bypasses_lhos_even_with_positive_estimate() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)

    decision = guard.decide(
        _candidate(),
        _observation(one_shot=True),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.BYPASS_NATIVE
    assert decision.effective_action is None
    assert decision.reason == "one_shot_bypass"


def test_single_phase_capability_is_bypassed() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)

    decision = guard.decide(
        _candidate(),
        _observation(expected_phase_count=1),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.BYPASS_NATIVE
    assert decision.reason == "single_phase_bypass"


def test_missing_or_low_confidence_estimate_falls_back_to_native() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)

    missing = guard.decide(
        _candidate(),
        HarnessEfficiencyGuardObservation(task_id="task-1"),
    )
    low_confidence = guard.decide(
        _candidate(),
        _observation(estimate=_estimate(confidence=0.2)),
    )

    assert missing.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert missing.reason == "efficiency_estimate_missing"
    assert low_confidence.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert low_confidence.reason == "efficiency_estimate_low_confidence"


def test_positive_net_token_and_wall_savings_apply_candidate() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)

    decision = guard.decide(_candidate(), _observation())

    assert decision.disposition is HarnessEfficiencyDisposition.APPLY
    assert decision.effective_action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "estimated_efficiency_accepted"
    assert decision.net_token_units == 250
    assert decision.net_wall_time_ms == 250
    assert decision.net_tool_calls == 2
    assert decision.efficiency_score > 0


def test_expected_token_regression_is_rejected_even_when_wall_time_improves() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    estimate = _estimate(avoided_tokens=20, overhead_tokens=100)

    decision = guard.decide(_candidate(), _observation(estimate=estimate))

    assert decision.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert decision.reason == "estimated_token_regression"
    assert decision.net_token_units == -80
    assert decision.net_wall_time_ms == 250


def test_predicted_quality_regression_falls_back_before_running_optimization() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    quality = HarnessQualityObservation(
        reference_score=0.80,
        candidate_score=0.70,
        uncertainty=0.01,
    )

    decision = guard.decide(
        _candidate(),
        _observation(predicted_quality=quality),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert decision.reason == "quality_regression_predicted"
    assert decision.quality_delta_lower_bound == -0.11


def test_observed_quality_regression_requests_best_checkpoint_restore() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    quality = HarnessQualityObservation(
        reference_score=0.80,
        candidate_score=0.65,
        uncertainty=0.01,
    )
    checkpoint = HarnessBestCheckpoint(
        checkpoint_id="cp-7",
        task_id="task-1",
        quality_score=0.80,
        graph_version=4,
        verified=True,
    )

    decision = guard.decide(
        _candidate(),
        _observation(
            graph_version=4,
            observed_quality=quality,
            best_checkpoint=checkpoint,
        ),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.RESTORE_BEST_CHECKPOINT
    assert decision.reason == "quality_regression_restore_checkpoint"
    assert decision.checkpoint_id == "cp-7"
    assert decision.quality_delta_lower_bound == -0.16


def test_incompatible_checkpoint_cannot_be_used_for_restore() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    quality = HarnessQualityObservation(
        reference_score=0.80,
        candidate_score=0.60,
    )
    checkpoint = HarnessBestCheckpoint(
        checkpoint_id="wrong-version",
        task_id="task-1",
        quality_score=0.80,
        graph_version=3,
        verified=True,
    )

    decision = guard.decide(
        _candidate(),
        _observation(
            graph_version=4,
            observed_quality=quality,
            best_checkpoint=checkpoint,
        ),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert decision.reason == "quality_regression_observed"
    assert decision.checkpoint_id is None


def test_lower_quality_checkpoint_cannot_mask_quality_regression() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    quality = HarnessQualityObservation(
        reference_score=0.80,
        candidate_score=0.60,
    )
    checkpoint = HarnessBestCheckpoint(
        checkpoint_id="lower-quality",
        task_id="task-1",
        quality_score=0.70,
        graph_version=4,
        verified=True,
    )

    decision = guard.decide(
        _candidate(),
        _observation(
            graph_version=4,
            observed_quality=quality,
            best_checkpoint=checkpoint,
        ),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.FALLBACK_NATIVE
    assert decision.reason == "quality_regression_observed"


def test_verifier_passed_candidate_is_never_replaced_by_speculative_guard() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    candidate = _candidate().model_copy(update={"reason": "verifier_passed"})
    poor_quality = HarnessQualityObservation(
        reference_score=1.0,
        candidate_score=0.1,
    )

    decision = guard.decide(
        candidate,
        _observation(observed_quality=poor_quality),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.APPLY
    assert decision.effective_action is candidate.action
    assert decision.reason == "verifier_passed"


def test_verifier_passed_one_shot_is_still_a_success_boundary() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    candidate = _candidate().model_copy(update={"reason": "verifier_passed"})

    decision = guard.decide(
        candidate,
        _observation(one_shot=True),
    )

    assert decision.disposition is HarnessEfficiencyDisposition.APPLY
    assert decision.reason == "verifier_passed"


def test_guard_decision_hash_is_deterministic() -> None:
    guard = QualityConstrainedEfficiencyGuard(enabled=True)
    first = guard.decide(_candidate(), _observation())
    second = guard.decide(_candidate(), _observation())

    assert first == second
    assert first.decision_hash == second.decision_hash
