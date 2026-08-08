"""Section 30 — Scheduler Idempotency.

The Scheduler deduplicates dispatch work via a deterministic
``<graph>:<task>:<version>`` key and must never re-dispatch a work unit it
has already linearised a claim for within the same graph version.
"""

from __future__ import annotations

from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _mk(vpg):
    return fake_scheduler(
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
    )


def test_same_graph_version_not_redispatched():
    vpg = FakeVPG()
    sch = _mk(vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    r1 = sch.schedule_once(vpg.graph_id)
    assert len(r1.dispatched) == 1
    # Same graph version, same task -> idempotency key matches -> skipped.
    r2 = sch.schedule_once(vpg.graph_id)
    assert r2.dispatched == []
    skipped_reasons = [reason for _, reason in r2.skipped]
    assert any(("idempotent" in reason) or ("active claim" in reason)
               for reason in skipped_reasons)


def test_version_change_allows_redispatch():
    """A bump in graph_version changes the idempotency key; but the existing
    ACTIVE claim still gates re-dispatch (D2-I4 + Section 30 interplay)."""
    vpg = FakeVPG()
    sch = _mk(vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    r1 = sch.schedule_once(vpg.graph_id)
    assert len(r1.dispatched) == 1
    vpg.bump_version()
    vpg.add_ready_task("t1", required_specializations=("python",), version=vpg.current_version)
    r2 = sch.schedule_once(vpg.graph_id)
    # Version changed, but ACTIVE claim gates -> still skipped.
    assert r2.dispatched == []


def test_idempotency_key_is_graph_task_version_composite():
    from lhos.runtimes.multi_agent.scheduler import MultiAgentScheduler
    k = MultiAgentScheduler._claim_idempotency_key("g1", "t7", 3)
    assert k == "g1:t7:v3"


def test_idempotent_keys_populated_on_dispatch():
    vpg = FakeVPG()
    sch = _mk(vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    assert any(k.endswith(":t1:v0") for k in sch._s._idempotent_keys)
