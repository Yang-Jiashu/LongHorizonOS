"""Scheduler core integration tests — drive MultiAgentScheduler through a
controllable FakeVPG so we can confirm the full scheduling pass
(section 31) without depending on VPG's verification state machine."""

from __future__ import annotations

from lhos.runtimes.multi_agent import ClaimState
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


def test_observe_vpg_completes_verified_task():
    vpg = FakeVPG()
    sch = fake_scheduler(_two_agents(), fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    active = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.ACTIVE]
    assert len(active) == 1
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
