"""Harness-neutral semantic context optimization tests."""

from __future__ import annotations

import pytest

from lhos.integrations.harness import (
    HarnessContinuationAction,
    HarnessPhaseObservation,
    HarnessUsage,
    SemanticContextPolicy,
    build_semantic_handoff,
)


def _phase(
    index: int,
    *,
    session: str = "session-a",
    cache_tokens: int = 1_000,
    model_calls: int = 1,
    event_count: int | None = None,
    reads: tuple[str, ...] = (),
    writes: tuple[str, ...] = (),
    max_tokens: bool = False,
    completed: bool = False,
    unknown_io: bool = False,
) -> HarnessPhaseObservation:
    return HarnessPhaseObservation(
        phase_index=index,
        usage=HarnessUsage(
            cache_read_tokens=cache_tokens,
            model_calls=model_calls,
        ),
        event_count=index + 1 if event_count is None else event_count,
        read_set=reads,
        write_set=writes,
        max_tokens_checkpoint=max_tokens,
        harness_completed=completed,
        unknown_io=unknown_io,
        session_id=session,
        elapsed_ms=10,
    )


def test_policy_does_not_restart_before_minimum_phase_window() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=10_000,
    )
    first = _phase(0, cache_tokens=40_000)
    current = _phase(1, cache_tokens=50_000)

    decision = policy.decide((first,), current, original_instruction="Fix the task")

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "minimum_phase_window_not_reached"
    assert decision.bounded_handoff_items == ()


def test_policy_restarts_compacted_session_on_context_bloat() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=20_000,
        cache_growth_ratio_threshold=2.0,
    )
    history = (
        _phase(0, cache_tokens=8_000),
        _phase(1, cache_tokens=10_000),
    )
    current = _phase(
        2,
        cache_tokens=30_000,
        reads=("workspace://src/input.py",),
        writes=("workspace://src/output.py",),
    )

    decision = policy.decide(history, current, original_instruction="Repair the implementation")

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "context_bloat:cache_tokens_per_call+cache_growth"
    assert decision.cache_tokens_per_call == 30_000
    assert decision.context_score >= 1.5
    assert decision.bounded_handoff_items[0].startswith("instruction_sha256:")
    assert "write_uri:workspace://src/output.py" in decision.bounded_handoff_items


def test_policy_can_restart_on_growth_before_absolute_cache_limit() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=100_000,
        cache_growth_ratio_threshold=2.0,
    )
    history = (
        _phase(0, cache_tokens=4_000, event_count=10),
        _phase(1, cache_tokens=5_000, event_count=20),
    )
    current = _phase(2, cache_tokens=12_000, event_count=22)

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "context_bloat:cache_growth"


def test_policy_defers_marginal_restart_while_event_progress_remains_productive() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=24_000,
        cache_growth_ratio_threshold=1.75,
    )
    history = (
        _phase(0, cache_tokens=9_000, event_count=670),
        _phase(1, cache_tokens=17_000, event_count=1_222),
        _phase(2, cache_tokens=20_000, event_count=2_077),
        _phase(3, cache_tokens=23_000, event_count=2_345),
    )
    current = _phase(4, cache_tokens=24_800, event_count=3_039)

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "context_bloat_deferred:event_progress"


def test_policy_restarts_when_context_is_high_and_event_progress_stalls() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=24_000,
    )
    history = (
        _phase(0, cache_tokens=11_000, event_count=261),
        _phase(1, cache_tokens=27_000, event_count=595),
    )
    current = _phase(2, cache_tokens=25_856, event_count=740)

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == "context_bloat:cache_tokens_per_call"


def test_policy_defers_non_severe_restart_immediately_after_artifact_write() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=24_000,
    )
    history = (
        _phase(0, cache_tokens=10_000, event_count=100),
        _phase(1, cache_tokens=10_000, event_count=200),
    )
    current = _phase(
        2,
        cache_tokens=25_000,
        event_count=210,
        writes=("workspace://src/result.py",),
    )

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "context_bloat_deferred:artifact_write"


def test_severe_context_pressure_overrides_progress_guard() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=3,
        cache_tokens_per_call_threshold=20_000,
        force_restart_context_score=1.5,
    )
    history = (
        _phase(0, cache_tokens=10_000, event_count=100),
        _phase(1, cache_tokens=15_000, event_count=200),
    )
    current = _phase(
        2,
        cache_tokens=35_000,
        event_count=320,
        writes=("workspace://src/result.py",),
    )

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.context_score >= 1.5


def test_verifier_success_never_triggers_destructive_restart() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
    )
    current = _phase(0, cache_tokens=100).model_copy(update={"verifier_passed": True})

    decision = policy.decide((), current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "verifier_passed"


def test_v3_dicom_progress_collapse_restarts_before_legacy_minimum_window() -> None:
    policy = SemanticContextPolicy()
    first = _phase(
        1,
        cache_tokens=91_712,
        model_calls=8,
        event_count=3_245,
        max_tokens=True,
        unknown_io=True,
    )
    current = _phase(
        2,
        cache_tokens=38_336,
        model_calls=3,
        event_count=3_439,
        max_tokens=True,
        unknown_io=True,
    )

    decision = policy.decide(
        (first,),
        current,
        original_instruction="Complete the DICOM audit",
    )

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.reason == (
        "semantic_guard:consecutive_max_tokens+"
        "cumulative_session_cache+semantic_progress_collapse"
    )
    assert decision.guard_triggers == (
        "consecutive_max_tokens",
        "cumulative_session_cache",
        "semantic_progress_collapse",
    )
    assert decision.consecutive_max_tokens == 2
    assert decision.cumulative_session_cache_read_tokens == 130_048
    assert decision.event_progress_ratio == pytest.approx(194 / 3_245, abs=1e-6)


def test_v3_climate_cumulative_cache_guard_is_observable() -> None:
    policy = SemanticContextPolicy(
        # Isolate the cumulative guard in this test. The production default
        # also trips the max-token streak one phase earlier.
        max_consecutive_max_tokens=99,
        cumulative_cache_read_tokens_threshold=128_000,
        cumulative_cache_requires_max_tokens=2,
    )
    history = (
        _phase(
            1,
            cache_tokens=31_168,
            model_calls=4,
            event_count=2_753,
            max_tokens=True,
        ),
        _phase(
            2,
            cache_tokens=51_712,
            model_calls=4,
            event_count=6_524,
            max_tokens=True,
            unknown_io=True,
        ),
    )
    current = _phase(
        3,
        cache_tokens=56_896,
        model_calls=4,
        event_count=9_492,
        max_tokens=True,
        unknown_io=True,
    )

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert decision.guard_triggers == ("cumulative_session_cache",)
    assert decision.cumulative_session_cache_read_tokens == 139_776
    assert decision.event_progress_ratio == pytest.approx(2_968 / 3_771, abs=1e-6)


def test_v3_completed_is_not_verified_but_alp_and_audio_keep_locality() -> None:
    policy = SemanticContextPolicy()
    alp = _phase(
        1,
        cache_tokens=25_280,
        model_calls=3,
        event_count=112,
        completed=True,
    )

    alp_decision = policy.decide((), alp)

    assert alp_decision.action is HarnessContinuationAction.RESUME
    assert alp_decision.reason == "harness_completed_not_verified"
    assert alp_decision.completed_without_verification is True
    assert alp_decision.guard_triggers == ()

    audio_first = _phase(
        1,
        cache_tokens=1_757_696,
        model_calls=51,
        event_count=7_617,
        completed=True,
        writes=("workspace://src/alignment.py",),
    )
    audio_second = _phase(
        2,
        cache_tokens=7_232,
        model_calls=1,
        event_count=10_650,
        max_tokens=True,
    )

    audio_decision = policy.decide((audio_first,), audio_second)

    assert audio_decision.action is HarnessContinuationAction.RESUME
    assert audio_decision.reason == "minimum_phase_window_not_reached"
    assert audio_decision.consecutive_max_tokens == 1
    assert audio_decision.cumulative_session_cache_read_tokens == 1_764_928


def test_usage_without_model_calls_cannot_trigger_cache_rate_restart() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
    )
    current = _phase(0, cache_tokens=1_000_000, model_calls=0)

    decision = policy.decide((), current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "context_within_limits"
    assert decision.cache_tokens_per_call == 0


def test_policy_enforces_restart_cooldown_and_limit_from_session_history() -> None:
    cooldown_policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=3,
    )
    history = (
        _phase(0, session="session-a"),
        _phase(1, session="session-b", cache_tokens=40_000),
    )
    current = _phase(2, session="session-b", cache_tokens=40_000)

    cooldown = cooldown_policy.decide(history, current)

    assert cooldown.action is HarnessContinuationAction.RESUME
    assert cooldown.reason == "restart_cooldown_active"

    limited_policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=0,
        max_restarts=1,
    )
    limited = limited_policy.decide(history, current)
    assert limited.action is HarnessContinuationAction.RESUME
    assert limited.reason == "restart_limit_reached"


def test_equivalent_inputs_produce_identical_decision_hashes() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=2,
        cache_tokens_per_call_threshold=10_000,
    )
    history = (_phase(0, cache_tokens=1_000),)
    current = _phase(1, cache_tokens=20_000)

    first = policy.decide(history, current, original_instruction="Implement parser")
    second = policy.decide(tuple(history), current, original_instruction="Implement parser")
    changed = policy.decide(history, current, original_instruction="Implement compiler")

    assert first == second
    assert first.decision_hash == second.decision_hash
    assert first.decision_hash != changed.decision_hash


def test_handoff_is_bounded_redacted_and_excluded_from_decision_logs() -> None:
    secret = "sk-abcdefghijklmnopqrstuvwxyz123456"
    observations = (
        _phase(
            0,
            reads=(
                f"workspace://src/read.py?token={secret}",
                "not-a-uri tool payload",
            ),
            writes=(f"workspace://src/{secret}.txt",),
        ),
        _phase(
            1,
            reads=("artifact://verified/result.json",),
            writes=("workspace://src/latest.py",),
        ),
    )

    handoff = build_semantic_handoff(
        f"Use api_key={secret} to repair the code",
        observations,
        max_items=4,
        max_chars=240,
        recent_phases=2,
    )

    serialized = "\n".join(handoff)
    assert len(handoff) <= 4
    assert sum(map(len, handoff)) <= 240
    assert secret not in serialized
    assert "[REDACTED]" in serialized
    assert "not-a-uri tool payload" not in serialized
    assert "?token=" not in serialized
    assert "write_uri:workspace://src/[REDACTED].txt" in handoff
    assert "write_uri:workspace://src/latest.py" in handoff

    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
        max_handoff_items=4,
        max_handoff_chars=240,
    )
    decision = policy.decide(
        observations[:-1],
        observations[-1],
        original_instruction=f"Use api_key={secret} to repair the code",
    )
    dumped = decision.model_dump(mode="json")
    assert "bounded_handoff_items" not in dumped
    assert secret not in repr(decision)
    assert secret not in json_text(dumped)


def test_long_instruction_reserves_handoff_budget_for_recent_artifacts() -> None:
    instruction = (
        "Objective: repair the application. "
        + ("Detailed requirement. " * 300)
        + "Final deliverable: verify output/report.json."
    )
    observations = (
        _phase(
            0,
            reads=("workspace://src/old.py",),
            writes=("workspace://output/old.json",),
        ),
        _phase(
            1,
            reads=("workspace://src/current.py",),
            writes=("workspace://output/current.json",),
        ),
    )

    handoff = build_semantic_handoff(
        instruction,
        observations,
        max_items=8,
        max_chars=2048,
        recent_phases=2,
    )

    assert len(handoff[0]) <= 1024
    assert "[TRUNCATED]" in handoff[0]
    assert "Objective: repair the application." in handoff[0]
    assert "Final deliverable: verify output/report.json." in handoff[0]
    assert handoff[1:3] == (
        "write_uri:workspace://output/current.json",
        "write_uri:workspace://output/old.json",
    )
    assert "read_uri:workspace://src/current.py" in handoff
    assert sum(map(len, handoff)) <= 2048


def test_invalid_or_ambiguous_history_fails_closed_to_resume() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
    )
    out_of_order = (_phase(2, cache_tokens=100),)
    current = _phase(1, cache_tokens=100)

    decision = policy.decide(out_of_order, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "invalid_observation_timeline"


def test_decreasing_event_cursor_in_one_session_fails_closed() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
    )
    history = (_phase(0, cache_tokens=100, event_count=20),)
    current = _phase(1, cache_tokens=100, event_count=10)

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "invalid_observation_timeline"


def test_reentering_a_closed_session_fails_closed() -> None:
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=1,
    )
    history = (
        _phase(0, session="session-a"),
        _phase(1, session="session-b"),
    )
    current = _phase(2, session="session-a", cache_tokens=100)

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "invalid_observation_timeline"


def test_restart_unproductive_stall_suppresses_churn() -> None:
    # Two consecutive restarts whose replacement sessions add no new writes
    # (same artifact set each time) must stop the restart loop and let the
    # session run to its natural end instead of churning into AgentTimeout.
    # cache 15K is below the 2x severity threshold (20K), so the unproductive
    # stall still suppresses restarting here (漏洞B fix v2 / C: once the cache
    # is severely bloated >= 2x threshold, restart is forced even without new
    # writes to avoid running to timeout).
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
    )
    history = (
        _phase(0, session="session-a", cache_tokens=15_000, writes=("a.txt",)),
        _phase(1, session="session-b", cache_tokens=15_000, writes=("a.txt",)),
        _phase(2, session="session-b", cache_tokens=15_000, writes=("a.txt",)),
        _phase(3, session="session-c", cache_tokens=15_000, writes=("a.txt",)),
        _phase(4, session="session-c", cache_tokens=15_000, writes=("a.txt",)),
    )
    current = _phase(5, session="session-c", cache_tokens=15_000, writes=("a.txt",))

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "semantic_guard_restart_unproductive"


def test_restart_stall_overridden_when_context_severely_bloated() -> None:
    # 漏洞B fix v2 / C 兜底：cache 已达 2 倍阈值（严重膨胀）时，即使
    # restart_unproductive_stall 触发（无新 write），也必须强制 restart——
    # 否则 context 无限膨胀拖到 timeout（实测 super-mario cache 4.8x、
    # spot 4.6x 被 unproductive/limit 抑制后全灭无 reward）。
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
    )
    history = (
        _phase(0, session="session-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="session-b", cache_tokens=40_000, writes=("a.txt",)),
        _phase(2, session="session-b", cache_tokens=40_000, writes=("a.txt",)),
        _phase(3, session="session-c", cache_tokens=40_000, writes=("a.txt",)),
        _phase(4, session="session-c", cache_tokens=40_000, writes=("a.txt",)),
    )
    current = _phase(5, session="session-c", cache_tokens=40_000, writes=("a.txt",))

    decision = policy.decide(history, current)

    # cache 40K = 4x 阈值（20K）严重膨胀 -> 强制 RESTART_COMPACTED 而非 RESUME
    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED


def test_restart_with_new_writes_is_not_blocked() -> None:
    # A restart whose replacement session grows the write set is productive
    # and must still be allowed (conservative: never block a progressing agent).
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
    )
    history = (
        _phase(0, session="session-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="session-b", cache_tokens=40_000, writes=("a.txt",)),
        _phase(2, session="session-b", cache_tokens=40_000, writes=("b.txt",)),
    )
    current = _phase(3, session="session-b", cache_tokens=40_000, writes=("b.txt",))

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert "restart_unproductive" not in decision.reason


def test_restart_unproductive_requires_threshold() -> None:
    # A single unproductive restart (below the threshold of 2) must not block
    # a restart yet; the counter must be allowed to accumulate.
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
    )
    history = (
        _phase(0, session="session-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="session-b", cache_tokens=40_000, writes=("a.txt",)),
        _phase(2, session="session-b", cache_tokens=40_000, writes=("a.txt",)),
    )
    current = _phase(3, session="session-b", cache_tokens=40_000, writes=("a.txt",))

    decision = policy.decide(history, current)

    # session-b added no new writes (unproductive=1) but threshold is 2,
    # so a guard restart may still happen.
    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED


def test_restart_payoff_stall_suppresses_rebound_churn() -> None:
    # On tool-output heavy tasks the cache rebounds to (or above) its
    # pre-restart level within the payoff window, so each restart only pays
    # the fixed summarisation cost.  Two consecutive failed restarts must stop
    # the churn even though the write set keeps growing (progress is real but
    # restarting cannot shrink the working set).
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
        restart_payoff_window_phases=3,
        restart_payoff_rebloat_ratio=1.0,
        restart_payoff_failures=2,
    )
    history = (
        _phase(0, session="s-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="s-b", cache_tokens=30_000, writes=("a.txt", "b.txt")),
        _phase(2, session="s-b", cache_tokens=45_000, writes=("a.txt", "b.txt")),
        _phase(3, session="s-c", cache_tokens=30_000, writes=("a.txt", "b.txt", "c.txt")),
        _phase(4, session="s-c", cache_tokens=50_000, writes=("a.txt", "b.txt", "c.txt")),
    )
    current = _phase(
        5, session="s-c", cache_tokens=50_000, writes=("a.txt", "b.txt", "c.txt")
    )

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESUME
    assert decision.reason == "semantic_guard_restart_payoff_failure"


def test_restart_with_sustained_low_cache_is_not_blocked() -> None:
    # A restart whose replacement session keeps the cache low (no rebound) is a
    # genuine payoff and must still be allowed.
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
        restart_payoff_window_phases=3,
        restart_payoff_rebloat_ratio=1.0,
        restart_payoff_failures=2,
    )
    history = (
        _phase(0, session="s-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="s-b", cache_tokens=10_000, writes=("a.txt", "b.txt")),
        _phase(2, session="s-b", cache_tokens=12_000, writes=("a.txt", "b.txt")),
        _phase(3, session="s-b", cache_tokens=15_000, writes=("a.txt", "b.txt")),
    )
    current = _phase(4, session="s-b", cache_tokens=15_000, writes=("a.txt", "b.txt"))

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED
    assert "payoff" not in decision.reason


def test_restart_payoff_requires_consecutive_failures() -> None:
    # A single failed restart (below the failure threshold) must not suppress
    # the next restart yet.
    policy = SemanticContextPolicy(
        min_phases_before_restart=1,
        cache_tokens_per_call_threshold=10_000,
        cooldown_phases=2,
        max_restarts=6,
        restart_unproductive_restarts=2,
        restart_payoff_window_phases=3,
        restart_payoff_rebloat_ratio=1.0,
        restart_payoff_failures=2,
    )
    history = (
        _phase(0, session="s-a", cache_tokens=40_000, writes=("a.txt",)),
        _phase(1, session="s-b", cache_tokens=30_000, writes=("a.txt", "b.txt")),
        _phase(2, session="s-b", cache_tokens=45_000, writes=("a.txt", "b.txt")),
        _phase(3, session="s-b", cache_tokens=45_000, writes=("a.txt", "b.txt")),
    )
    current = _phase(4, session="s-b", cache_tokens=45_000, writes=("a.txt", "b.txt"))

    decision = policy.decide(history, current)

    assert decision.action is HarnessContinuationAction.RESTART_COMPACTED


def json_text(value: object) -> str:
    import json

    return json.dumps(value, ensure_ascii=True, sort_keys=True)
