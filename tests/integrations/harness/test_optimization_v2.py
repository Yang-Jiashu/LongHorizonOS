"""Tests for SemanticContextPolicyV2 (optimization_v2.py)."""

from __future__ import annotations

from lhos.integrations.harness.optimization import (
    HarnessContinuationAction,
    HarnessPhaseObservation,
    HarnessQualityProbe,
)
from lhos.integrations.harness.optimization_v2 import SemanticContextPolicyV2
from lhos.integrations.harness.protocol import HarnessUsage


def obs(
    phase: int,
    sid: str,
    *,
    cache: int = 0,
    calls: int = 1,
    writes: tuple[str, ...] = (),
    reads: tuple[str, ...] = (),
    streak: int = 0,
    rejections: int = 0,
    passed: bool = False,
    events: int | None = None,
    tool_calls: int = 0,
    error_calls: int = 0,
    test_calls: int = 0,
    max_tokens: bool = False,
    unknown_io: bool = False,
    preempted: bool = False,
    preempt_streak: int = 0,
) -> HarnessPhaseObservation:
    return HarnessPhaseObservation(
        phase_index=phase,
        usage=HarnessUsage(
            cache_read_tokens=cache, model_calls=calls, tool_calls=tool_calls
        ),
        event_count=phase if events is None else events,
        read_set=reads,
        write_set=writes,
        verifier_passed=passed,
        max_tokens_checkpoint=max_tokens,
        session_id=sid,
        verifier_rejection_count=rejections,
        verifier_rejection_repeat_streak=streak,
        unknown_io=unknown_io,
        slice_preempted=preempted,
        consecutive_slice_preemptions=preempt_streak,
        quality_probe=HarnessQualityProbe(
            write_calls=len(writes),
            distinct_writes=len(writes),
            test_calls=test_calls,
            error_calls=error_calls,
            tool_calls=tool_calls,
            write_repeat_ratio=0.0,
            error_ratio=(error_calls / tool_calls) if tool_calls > 0 else 0.0,
        ),
    )


def decide(policy, timeline):
    return policy.decide(
        tuple(timeline[:-1]),
        timeline[-1],
        original_instruction="do the task",
    )


def test_warmup_respected_even_at_high_cache() -> None:
    """v1's guard branch beheaded sessions at phase 1; v2 never restarts
    before the warm-up window regardless of cache pressure."""
    policy = SemanticContextPolicyV2()
    timeline = [obs(1, "s1", cache=40_000)]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_warmup"


def test_within_limits_resumes() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=5_000),
        obs(2, "s1", cache=10_000),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_within_limits"


def test_verifier_passed_resumes() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [obs(1, "s1", cache=80_000, passed=True)]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_verifier_passed"


def test_loop_escape_restarts_on_identical_failures_below_hard_line() -> None:
    """Byte-identical verifier failures + no writes: restart helps even with
    a thin handoff -- the current context is the problem."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000),
        obs(3, "s1", cache=11_000, streak=1, rejections=1),
        obs(4, "s1", cache=12_000, streak=2, rejections=2),
        obs(5, "s1", cache=12_500, streak=3, rejections=3),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "v2_loop_escape"
    assert decision.guard_triggers == ("verifier_failure_loop",)
    assert len(decision.bounded_handoff_items) >= 1


def test_loop_escape_rate_limited() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000, streak=3, rejections=3),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_converging_failures_not_interrupted() -> None:
    """streak == 1 means the latest failure differs from the previous one:
    the agent is still iterating -- continuity is valuable."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000),
        obs(3, "s1", cache=20_000, streak=2, rejections=2),
        obs(4, "s1", cache=25_000, streak=1, rejections=3),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_converging_failures"


def test_converging_write_not_interrupted() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000),
        obs(3, "s1", cache=25_000, writes=("file:///app/a.py",)),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_converging_write"


def test_thin_handoff_blocks_cost_restart() -> None:
    """modflow6 pattern: over the soft line with nothing to carry -> resume."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000),
        obs(3, "s1", cache=30_000),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_thin_handoff"


def test_rich_handoff_allows_cost_restart() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000, reads=("file:///app/in1.dat",)),
        obs(2, "s1", cache=10_000, writes=("file:///app/out1.py",)),
        obs(
            3,
            "s1",
            cache=25_000,
            reads=("file:///app/in2.dat", "file:///app/in3.dat"),
        ),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "v2_cost_investment"
    assert len(decision.bounded_handoff_items) >= 4


def test_severe_valve_bypasses_thin_handoff() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000),
        obs(3, "s1", cache=20_000),
        obs(4, "s1", cache=70_000),  # >= 4x soft line, past rate limit
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "v2_severe_valve"


def test_severe_valve_is_rate_limited() -> None:
    """Rate limiting replaces the absolute cap: a severe session restarted
    recently waits for the window instead of churning or being capped."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=70_000),
        obs(2, "s1", cache=80_000),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    # valve is rate-limited this early in the session; the cost path then
    # stops at the amnesia gate (nothing to carry).
    assert decision.reason == "v2_thin_handoff"


def test_unpaid_restart_suspends_cost_restarts() -> None:
    """The previous restart re-bloated >= 1.5x within the window without any
    new write, so the next soft-line restart is suspended."""
    policy = SemanticContextPolicyV2()
    timeline = [
        # previous session rode to 30K
        obs(1, "s1", cache=10_000),
        obs(2, "s1", cache=30_000),
        # new generation born from a restart re-bloats instantly (>= 45K
        # within 3 phases) and writes nothing
        obs(3, "s2", cache=46_000),
        obs(4, "s2", cache=47_000),
        obs(5, "s2", cache=47_500),
        # later, over the soft line with a rich handoff available...
        obs(
            6,
            "s2",
            cache=20_000,
            reads=("file:///app/in1.dat",),
        ),
        obs(
            7,
            "s2",
            cache=25_000,
            reads=("file:///app/in2.dat", "file:///app/in3.dat"),
        ),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_unpaid_restart"


def test_cap_is_backstop_not_operating_constraint() -> None:
    policy = SemanticContextPolicyV2(max_restarts=1)
    timeline = [
        obs(1, "s1", cache=9_000),
        obs(2, "s1", cache=10_000, reads=("file:///app/a",)),
        obs(3, "s2", cache=20_000, reads=("file:///app/b",)),
        obs(4, "s2", cache=30_000, reads=("file:///app/c", "file:///app/d")),
        # past cooldown now; even a severe cache level must hit the cap first
        obs(5, "s2", cache=50_000),
        obs(6, "s2", cache=70_000),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_restart_cap"


def test_config_validation() -> None:
    import pytest

    with pytest.raises(ValueError):
        SemanticContextPolicyV2(soft_cache_tokens_per_call=0)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(hard_cache_ratio=1.0)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(loop_repeat_streak=1)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(convergence_window_phases=1)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(stop_loss_cache_tokens=0)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(max_converging_deferrals=0)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(slice_relax_soft_ratio=0.9)
    with pytest.raises(ValueError):
        SemanticContextPolicyV2(slice_max_seconds=10, slice_min_seconds=30)


# ---------------------------------------------------------------- halt gates


def _quiet_timeline(phases: int, *, events: int = 100) -> list:
    return [
        obs(
            index,
            "s1",
            cache=5_000,
            events=events,
            tool_calls=2,
        )
        for index in range(1, phases + 1)
    ]


def test_converged_halt_fires_on_quiet_window() -> None:
    """P2: a session with no writes/tests/verifier engagement and flat events
    is semantically converged -- halt instead of riding the budget.
    The gate defaults OFF (dead-session vs in-flight-solver ambiguity at the
    observation level); these tests exercise the mechanism explicitly."""
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    decision = decide(policy, _quiet_timeline(7))
    assert decision.action == HarnessContinuationAction.TERMINATE
    assert decision.reason == "v2_converged_halt"
    assert decision.guard_triggers == ("semantic_convergence",)
    assert decision.bounded_handoff_items == ()


def test_converged_halt_blocked_by_writes_in_window() -> None:
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    timeline = _quiet_timeline(6) + [
        obs(7, "s1", cache=5_000, events=100, tool_calls=2, writes=("file:///app/a",))
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_converged_halt_blocked_for_young_session() -> None:
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    decision = decide(policy, _quiet_timeline(5))
    assert decision.action == HarnessContinuationAction.RESUME


def test_converged_halt_blocked_by_verifier_engagement() -> None:
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    timeline = _quiet_timeline(6) + [
        obs(7, "s1", cache=5_000, events=100, tool_calls=2, rejections=1, streak=1)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_converged_halt_blocked_by_unknown_io() -> None:
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    timeline = _quiet_timeline(6) + [
        obs(7, "s1", cache=5_000, events=100, tool_calls=2, unknown_io=True)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_converged_halt_blocked_by_event_growth() -> None:
    policy = SemanticContextPolicyV2(convergence_halt_enabled=True)
    timeline = [
        obs(index, "s1", cache=5_000, events=index * 100, tool_calls=2)
        for index in range(1, 8)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_stop_loss_halt_fires_after_burn_without_progress() -> None:
    """P1: burn since the last progress marker crosses the stop-loss -- the
    middle rung between no governance and the 80M hard cap.  Events keep
    growing (agent is busy) so this is NOT the convergence gate firing."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=500_000, calls=50, events=index * 100, tool_calls=5)
        for index in range(1, 21)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.TERMINATE
    assert decision.reason == "v2_stop_loss_halt"
    assert decision.guard_triggers == ("stop_loss_burn",)


def test_stop_loss_resets_on_recent_progress() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=500_000, calls=50, events=index * 100, tool_calls=5)
        for index in range(1, 19)
    ]
    timeline.append(
        obs(19, "s1", cache=500_000, calls=50, events=1900, tool_calls=5,
            writes=("file:///app/out.py",))
    )
    timeline.append(obs(20, "s1", cache=500_000, calls=50, events=2000, tool_calls=5))
    decision = decide(policy, timeline)
    # only 2 phases (~1M cache) since the last write -> below the stop-loss
    assert decision.action == HarnessContinuationAction.RESUME


def test_stop_loss_debt_survives_restart() -> None:
    """A restart that restores no productivity must not clear the debt."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=500_000, calls=50, events=index * 100, tool_calls=5)
        for index in range(1, 11)
    ]
    timeline += [
        obs(10 + index, "s2", cache=500_000, calls=50, events=1000 + index * 100,
            tool_calls=5)
        for index in range(1, 8)
    ]
    # 10 x 500K + 7 x 500K = 8.5M since any progress; s2 is past its grace.
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.TERMINATE
    assert decision.reason == "v2_stop_loss_halt"


def test_stop_loss_grace_for_fresh_generation() -> None:
    """A fresh generation gets stop_loss_min_session_phases phases to prove
    life even when the debt already exceeds the threshold."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=500_000, calls=50, events=index * 100, tool_calls=5)
        for index in range(1, 21)
    ]
    timeline += [
        obs(20 + index, "s2", cache=100_000, calls=50, events=2100 + index * 50,
            tool_calls=5)
        for index in range(1, 4)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_halt_gates_disabled_fall_through() -> None:
    policy = SemanticContextPolicyV2(stop_loss_halt_enabled=False)
    decision = decide(policy, _quiet_timeline(7))
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_within_limits"
    # convergence halt defaults OFF: the same quiet window must not halt
    # unless the experiment explicitly opts in.
    decision = decide(SemanticContextPolicyV2(), _quiet_timeline(7))
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_within_limits"


# ----------------------------------------------------------- early error spin


def test_early_error_spin_restarts_below_cache_line() -> None:
    """P6 blind zone: a young session spinning on tool errors with zero
    writes gets restarted even though the cache line was never crossed."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=5_000, tool_calls=5, error_calls=5)
        for index in range(1, 5)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "v2_early_error_spin"
    assert decision.guard_triggers == ("early_error_spin",)


def test_early_error_spin_blocked_by_writes() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=5_000, tool_calls=5, error_calls=5),
        obs(2, "s1", cache=5_000, tool_calls=5, error_calls=5,
            writes=("file:///app/a",)),
        obs(3, "s1", cache=5_000, tool_calls=5, error_calls=5),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME


def test_early_error_spin_expires_after_window() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(index, "s1", cache=5_000, tool_calls=5, error_calls=5)
        for index in range(1, 8)
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_within_limits"


# ------------------------------------------------------------- deferral cap


def test_converging_write_deferral_capped() -> None:
    """P0/P1: writes landing above the soft line defer a restart at most
    max_converging_deferrals times -- then the rich handoff is taken instead
    of paying another full reload at the bloated rate."""
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000, tool_calls=3),
        obs(2, "s1", cache=10_000, tool_calls=3),
        obs(3, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w3",)),
        obs(4, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w4",)),
        obs(5, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w5",)),
        # 4th consecutive deferral: falls through to the investment path.
        obs(6, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w6",),
            reads=("file:///app/r6",)),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "v2_cost_investment"


def test_converging_write_under_cap_still_defers() -> None:
    policy = SemanticContextPolicyV2()
    timeline = [
        obs(1, "s1", cache=9_000, tool_calls=3),
        obs(2, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w2",)),
        obs(3, "s1", cache=20_000, tool_calls=3, writes=("file:///app/w3",)),
    ]
    decision = decide(policy, timeline)
    assert decision.action == HarnessContinuationAction.RESUME
    assert decision.reason == "v2_converging_write"


# ------------------------------------------------------------- adaptive slice


def test_adaptive_slice_upscales_quiet_session() -> None:
    """P0: a quiet clean session below half the soft line amortizes reloads
    with a longer slice."""
    policy = SemanticContextPolicyV2()
    current = obs(1, "s1", cache=1_000, tool_calls=3)
    assert policy.recommended_slice_seconds((), current, 75.0) == 150.0


def test_adaptive_slice_never_downscales_by_default() -> None:
    """Regression guard (2026-09-04 dropfix batch): a downscaled slice
    preempts a slow big-context call mid-flight, records zero durable
    events, and the slice no-progress watchdog kills the whole task.
    Downscale stays available for experiments but ships OFF."""
    policy = SemanticContextPolicyV2()
    assert policy.slice_downscale_factor == 1.0
    hot = obs(1, "s1", cache=13_000, tool_calls=3)
    assert policy.recommended_slice_seconds((), hot, 75.0) == 75.0


def test_adaptive_slice_downscales_near_soft_line() -> None:
    policy = SemanticContextPolicyV2(slice_downscale_factor=0.5)
    current = obs(1, "s1", cache=13_000, tool_calls=3)
    assert policy.recommended_slice_seconds((), current, 75.0) == 37.5


def test_adaptive_slice_downscales_on_recent_rejection() -> None:
    policy = SemanticContextPolicyV2(slice_downscale_factor=0.5)
    history = [
        obs(1, "s1", cache=1_000, tool_calls=3),
        obs(2, "s1", cache=1_000, tool_calls=3),
    ]
    current = obs(3, "s1", cache=1_000, tool_calls=3, rejections=1, streak=1)
    assert policy.recommended_slice_seconds(history, current, 75.0) == 37.5


def test_adaptive_slice_base_when_ambiguous() -> None:
    policy = SemanticContextPolicyV2()
    # between the relax and control ratios -> base
    current = obs(1, "s1", cache=10_000, tool_calls=3)
    assert policy.recommended_slice_seconds((), current, 75.0) == 75.0
    # no model calls -> fail closed to base
    silent = obs(1, "s1", cache=0, calls=0)
    assert policy.recommended_slice_seconds((), silent, 75.0) == 75.0


def test_adaptive_slice_clamps_and_fail_closed() -> None:
    policy = SemanticContextPolicyV2()
    quiet = obs(1, "s1", cache=1_000, tool_calls=3)
    assert policy.recommended_slice_seconds((), quiet, 200.0) == 240.0
    hot = obs(1, "s1", cache=60_000, tool_calls=3)
    # downscale-off is an exact no-op even below the floor
    assert policy.recommended_slice_seconds((), hot, 10.0) == 10.0
    downscaling = SemanticContextPolicyV2(slice_downscale_factor=0.5)
    # base below the floor is left alone (the floor never lengthens a slice)
    assert downscaling.recommended_slice_seconds((), hot, 10.0) == 10.0
    # floor applies when the scaled value would dip below it
    assert downscaling.recommended_slice_seconds((), hot, 75.0) == 37.5
    assert downscaling.recommended_slice_seconds((), hot, 40.0) == 30.0
    assert policy.recommended_slice_seconds((), quiet, 0.0) == 0.0