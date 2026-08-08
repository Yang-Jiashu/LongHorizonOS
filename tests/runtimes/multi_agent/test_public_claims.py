"""Public SDK facade (SchedulerSession)."""

from __future__ import annotations

from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def test_scheduler_session_schedule_and_snapshot():
    vpg = FakeVPG()
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    r = sch.schedule_once(vpg.graph_id)
    assert len(r.dispatched) == 1
    snap = sch.projection_snapshot()
    assert snap["claims"]
    assert sch.claims
    assert sch.match_log


def test_scheduler_session_run_pass_observe_reconcile():
    vpg = FakeVPG()
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",),
                "max_concurrency": 5}},
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    vpg.add_ready_task("t2", required_specializations=("python",))
    sch.run_pass(vpg.graph_id)
    # Claims exist; reconcile returned a result.
    assert len(sch.claims) >= 1
    rec = sch.reconcile()
    assert rec is not None
    assert rec.finished_at is not None


def test_scheduler_session_schedule_until_idle_results():
    vpg = FakeVPG()
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",),
                "max_concurrency": 10}},
        fake_vpg=vpg,
    )
    for i in range(4):
        vpg.add_ready_task(f"t{i}", required_specializations=("python",))
    results = sch.schedule_until_idle(vpg.graph_id, max_dispatches=50)
    assert len(results) >= 1
    total = sum(len(r.dispatched) for r in results)
    assert total == 4


def test_scheduler_session_events_property_returns_list():
    vpg = FakeVPG()
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    assert isinstance(sch.events, list)


def test_active_claim_for_task_helper():
    vpg = FakeVPG()
    sch = fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    claim = sch.active_claim_for_task("t1")
    assert claim is not None
    assert claim.task_id == "t1"
    assert sch.active_claim_for_task("nonexistent") is None
