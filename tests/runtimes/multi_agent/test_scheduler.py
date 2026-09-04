"""Scheduler core integration tests — drive MultiAgentScheduler through a
controllable FakeVPG so we can confirm the full scheduling pass
(section 31) without depending on VPG's verification state machine."""

from __future__ import annotations

from datetime import timedelta

from lhos.runtimes.multi_agent import ClaimState
from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.runtimes.multi_agent.models import AgentSnapshot, AttemptState, ComputationCost
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


# ── fixture helpers ──────────────────────────────────────────────────────
def _two_agents():
    return {
        "a1": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
            "cost_weight": 100,
        },
        "a2": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 5,
            "cost_weight": 200,
        },
    }


# ── tests ────────────────────────────────────────────────────────────────
def test_empty_frontier_is_idle():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    res = sch.schedule_once(vpg.graph_id)
    assert res.idle
    assert res.dispatched == []


def test_single_ready_task_dispatched():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", task_kind="code_review", required_specializations=("python",))
    res = sch.schedule_once(vpg.graph_id)
    assert len(res.dispatched) == 1
    assert res.dispatched[0]["task_id"] == "t1"
    # Cheaper agent a1 wins (cost_weight 100 < 200).
    assert res.dispatched[0]["agent_id"] == "a1"


def test_preferred_agent_wins_when_eligible():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task(
        "t1",
        required_specializations=("python",),
        metadata_extra={"sdk": {"agent": "a2"}},
    )

    result = sch.schedule_once(vpg.graph_id)

    assert result.dispatched[0]["agent_id"] == "a2"


def test_ineligible_preferred_agent_does_not_block_fallback():
    vpg = FakeVPG()
    agents = _two_agents()
    agents["a2"]["specializations"] = ("review",)
    sch = fake_scheduler(agents, fake_vpg=vpg)
    vpg.add_ready_task(
        "t1",
        required_specializations=("python",),
        metadata_extra={"sdk": {"agent": "a2"}},
    )

    result = sch.schedule_once(vpg.graph_id)

    assert result.dispatched[0]["agent_id"] == "a1"


def test_max_attempts_is_enforced_per_semantic_epoch():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task(
        "t1",
        required_specializations=("python",),
        metadata_extra={"scheduler": {"max_attempts": 1}},
    )

    first = sch.schedule_once(vpg.graph_id)
    assert first.dispatched
    sch.release_task(vpg.graph_id, "t1")
    second = sch.schedule_once(vpg.graph_id)

    assert second.dispatched == []
    assert "max_attempts exhausted" in second.skipped[0][1]


def test_second_schedule_same_task_skipped():
    """An existing ACTIVE claim means the task is not re-dispatched (D2-I4)."""
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    first = sch.schedule_once(vpg.graph_id)
    assert len(first.dispatched) == 1
    # Bump version so the idempotency key changes; the ACTIVE claim still
    # gates re-dispatch.
    vpg.bump_version()
    vpg.add_ready_task("t1", required_specializations=("python",), version=vpg.current_version)
    second = sch.schedule_once(vpg.graph_id)
    assert second.dispatched == []
    # Exactly one ACTIVE claim after both passes.
    active = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.ACTIVE]
    assert len(active) == 1


def test_unknown_graph_safe_noop():
    """schedule_once against a missing graph must not propagate an exception."""
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    res = sch.schedule_once("does-not-exist")
    assert res.idle
    assert res.dispatched == []


def test_max_claims_bounds_dispatch():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    for i in range(4):
        vpg.add_ready_task(f"t{i}", required_specializations=("python",))
    res = sch.schedule_once(vpg.graph_id, max_claims=2)
    assert len(res.dispatched) == 2


def test_allowed_task_ids_filters_before_claim_without_bypassing_scheduler_checks():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    for task_id in ("a", "b", "c"):
        vpg.add_ready_task(task_id, required_specializations=("python",))

    res = sch.schedule_once(
        vpg.graph_id,
        max_claims=3,
        allowed_task_ids=("b",),
    )

    assert [item["task_id"] for item in res.dispatched] == ["b"]
    assert {item for item in res.skipped if item[1] == "adaptive policy deferred"} == {
        ("a", "adaptive policy deferred"),
        ("c", "adaptive policy deferred"),
    }
    active = [claim for claim in sch.claims if claim.state == ClaimState.ACTIVE]
    assert [claim.task_id for claim in active] == ["b"]


def test_allowed_task_ids_empty_is_fail_closed_and_default_behavior_is_unchanged():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))

    blocked = sch.schedule_once(vpg.graph_id, allowed_task_ids=())
    assert blocked.dispatched == []
    assert blocked.skipped == [("t1", "adaptive policy deferred")]

    legacy = sch.schedule_once(vpg.graph_id)
    assert [item["task_id"] for item in legacy.dispatched] == ["t1"]


def test_schedule_until_idle_terminates():
    """Safety-bound: schedule_until_idle must never loop forever, even when
    tasks keep appearing (frontier grows by 1 per couple of passes)."""
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    for i in range(6):
        vpg.add_ready_task(f"t{i}", required_specializations=("python",))
    results = sch.schedule_until_idle(vpg.graph_id, max_dispatches=50)
    assert len(results) <= 50
    total = sum(len(r.dispatched) for r in results)
    assert total == 6


def test_event_log_emitted_per_dispatch():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    types = [e.event_type for e in sch.events]
    from lhos.runtimes.multi_agent.events import SchedulerEventType

    assert SchedulerEventType.CLAIM_PROPOSED in types
    assert SchedulerEventType.CLAIM_LEASE_ACQUIRED in types
    assert SchedulerEventType.MATCH_DECISION_CREATED in types


def test_agent_snapshot_bind_update_and_stale_cognition_fence():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    claim = sch.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None

    snapshot = AgentSnapshot.from_attempt(
        attempt,
        progress=0.25,
        cost=ComputationCost(input_tokens=10, elapsed_ms=10),
        captured_at=attempt.started_at + timedelta(seconds=1),
    )
    assert sch.bind_agent_snapshot(claim.claim_id, snapshot)
    assert sch.bind_agent_snapshot(claim.claim_id, snapshot)
    assert attempt.agent_snapshot == snapshot

    updated = snapshot.model_copy(
        update={
            "progress": 0.5,
            "captured_at": snapshot.captured_at + timedelta(seconds=1),
            "cost": ComputationCost(input_tokens=20, elapsed_ms=20),
        }
    )
    assert sch.bind_agent_snapshot(claim.claim_id, updated)
    assert attempt.agent_snapshot == updated

    assert sch.mark_stale_cognition(claim.claim_id, "input changed")
    assert attempt.state == AttemptState.STALE_COGNITION
    assert sch.mark_stale_cognition(claim.claim_id, "input changed")
    assert SchedulerEventType.EXECUTION_SNAPSHOT_BOUND in {event.event_type for event in sch.events}
    assert SchedulerEventType.EXECUTION_STALE_COGNITION in {
        event.event_type for event in sch.events
    }

    # A VPG VERIFIED observation cannot cross the stale-cognition commit bar.
    vpg.set_validity("t1", "verified")
    assert sch.observe_vpg(vpg.graph_id)["claims_completed"] == 0
    assert claim.state == ClaimState.ACTIVE

    # Release/failure path preserves the quarantine marker for repair policy.
    assert sch.release_task(
        vpg.graph_id,
        "t1",
        reason="stale_cognition:rebase_required",
        expected_claim_id=claim.claim_id,
    )
    assert attempt.state == AttemptState.STALE_COGNITION


def test_stale_cognition_can_quarantine_operational_success_but_not_verified():
    """The semantic commit window remains invalidatable after executor success."""

    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    claim = sch.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    sch.mark_execution_started(claim.claim_id)
    assert sch.mark_execution_operationally_succeeded(claim.claim_id) is attempt
    assert attempt.state == AttemptState.SUCCEEDED_OPERATIONALLY

    assert sch.mark_stale_cognition(claim.claim_id, "input version advanced")
    assert attempt.state == AttemptState.STALE_COGNITION
    assert not sch.observe_vpg(vpg.graph_id)["claims_completed"]

    # A semantically verified attempt is immutable and cannot be rolled back.
    vpg2 = FakeVPG()
    sch2 = fake_scheduler(_two_agents(), fake_vpg=vpg2)
    vpg2.add_ready_task("t2", required_specializations=("python",))
    sch2.schedule_once(vpg2.graph_id)
    claim2 = sch2.active_claim_for_task("t2", vpg2.graph_id)
    assert claim2 is not None
    attempt2 = sch2.attempt_for_claim(claim2.claim_id)
    assert attempt2 is not None
    sch2.mark_execution_started(claim2.claim_id)
    sch2.mark_execution_operationally_succeeded(claim2.claim_id)
    vpg2.set_validity("t2", "verified")
    assert sch2.observe_vpg(vpg2.graph_id)["claims_completed"] == 1
    assert attempt2.state == AttemptState.VERIFIED_SEMANTICALLY
    assert not sch2.mark_stale_cognition(claim2.claim_id, "too late")


def test_execution_lifecycle_callbacks_do_not_acknowledge_terminal_attempts():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    claim = sch.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None

    # RUNNING start notification is idempotent.
    assert sch.mark_execution_started(claim.claim_id) is attempt
    assert sch.mark_execution_started(claim.claim_id) is attempt
    assert sch.mark_execution_operationally_succeeded(claim.claim_id) is attempt

    # Success is not re-acknowledged, and a later start cannot roll the
    # attempt backwards from its operationally successful state.
    assert sch.mark_execution_operationally_succeeded(claim.claim_id) is None
    assert sch.mark_execution_started(claim.claim_id) is None

    assert sch.mark_stale_cognition(claim.claim_id, "input changed")
    assert sch.mark_execution_started(claim.claim_id) is None
    assert sch.mark_execution_operationally_succeeded(claim.claim_id) is None
    assert attempt.state == AttemptState.STALE_COGNITION


def test_verified_vpg_cannot_complete_failed_attempt():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    claim = sch.active_claim_for_task("t1", vpg.graph_id)
    assert claim is not None
    attempt = sch.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    assert sch.mark_execution_started(claim.claim_id) is attempt
    sch._s._attempts_.mark_failed(
        attempt,
        error="executor failed after partial work",
    )

    vpg.set_validity("t1", "verified")
    assert sch.observe_vpg(vpg.graph_id)["claims_completed"] == 0
    # The direct completion helper is guarded by the same lifecycle gate.
    sch._s.mark_task_completed(claim)

    assert claim.state == ClaimState.ACTIVE
    assert attempt.state == AttemptState.FAILED
    assert SchedulerEventType.EXECUTION_SEMANTICALLY_VERIFIED not in {
        event.event_type for event in sch.events
    }
    assert SchedulerEventType.CLAIM_COMPLETED not in {event.event_type for event in sch.events}


def test_observe_vpg_completes_verified_task():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    active = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.ACTIVE]
    assert len(active) == 1
    # Semantic verification is downstream of an operational success
    # milestone; a bare VPG row must not promote a dispatched attempt.
    assert sch.mark_execution_started(active[0].claim_id) is not None
    assert sch.mark_execution_operationally_succeeded(active[0].claim_id) is not None
    # VPG-derived semantic completion.
    vpg.set_validity("t1", "verified")
    tally = sch.observe_vpg(vpg.graph_id)
    assert tally["claims_completed"] == 1
    completed = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.COMPLETED]
    assert len(completed) == 1


def test_no_eligible_agent_skipped_with_reasons():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    # Requires a specialization nobody has.
    vpg.add_ready_task("t1", required_specializations=("rust",))
    res = sch.schedule_once(vpg.graph_id)
    assert res.dispatched == []
    assert len(res.skipped) == 1
    assert res.skipped[0][0] == "t1"
    assert "no eligible agent" in res.skipped[0][1]


def test_projection_snapshot_has_required_fields():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    snap = sch.projection_snapshot()
    assert "claims" in snap
    assert "attempts" in snap
    assert "match_log" in snap
    assert len(snap["claims"]) == 1
    assert len(snap["match_log"]) >= 1


def test_match_log_decision_hash_populated():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    assert sch.match_log[0].decision_hash
    assert len(sch.match_log[0].decision_hash) == 64  # sha256 hex


def test_run_pass_combines_schedule_observe_reconcile():
    """SchedulerSession.run_pass must advance state coherently."""
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    vpg.add_ready_task("t2", required_specializations=("python",))
    res = sch.run_pass(vpg.graph_id)
    assert len(res.dispatched) >= 1
    # A reconcile pass should be observable via events.
    ev_types = {e.event_type for e in sch.events}
    from lhos.runtimes.multi_agent.events import SchedulerEventType

    assert SchedulerEventType.CLAIM_PROPOSED in ev_types
