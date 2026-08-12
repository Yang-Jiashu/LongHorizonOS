"""Crash/reopen tests for the Scheduler-owned durable projection."""

from __future__ import annotations

import sqlite3

import pytest

from lhos.runtimes.multi_agent.durable_state import (
    SchedulerStateCorruption,
    SchedulerStateStore,
)
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _agents() -> dict[str, dict]:
    return {
        "a1": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 2,
        }
    }


def test_scheduler_reopens_with_claim_attempt_event_and_idempotency(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    first = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    result = first.schedule_once(vpg.graph_id)
    assert len(result.dispatched) == 1
    claim_id = result.dispatched[0]["claim_id"]
    first.mark_execution_started(claim_id)

    reopened = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    assert [c.claim_id for c in reopened.claims] == [claim_id]
    assert reopened.attempt_for_claim(claim_id).state.value == "running"
    assert len(reopened.events) == len(first.events)
    replay = reopened.schedule_once(vpg.graph_id)
    assert replay.dispatched == []
    assert any("active claim" in reason or "idempotent" in reason for _, reason in replay.skipped)


def test_scheduler_event_chain_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE scheduler_events SET event_json = ? WHERE event_seq = 1",
        ('{"event_id":"tampered"}',),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchedulerStateCorruption, match="hash mismatch"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_snapshot_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE scheduler_snapshot SET state_json = ? WHERE snapshot_id = 1",
        ('{"claims":[],"attempts":[],"match_log":[],"idempotent_keys":[]}',),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchedulerStateCorruption, match="snapshot hash"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_store_rolls_back_event_and_snapshot_on_failure(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_store=store)
    vpg.add_ready_task("t1", required_specializations=("python",))

    original = store.append_event

    def fail_once(*args, **kwargs):
        raise RuntimeError("disk full")

    store.append_event = fail_once  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="disk full"):
        scheduler.schedule_once(vpg.graph_id)
    store.append_event = original  # type: ignore[method-assign]

    assert scheduler.claims == []
    assert scheduler.events == []
    assert store.event_count() == 0
    with pytest.raises(SchedulerStateCorruption, match="hash mismatch"):
        # No snapshot is expected for a failed first transaction; force a
        # malformed event row to ensure loading remains fail-closed.
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO scheduler_events "
                "(event_seq,event_id,event_json,event_hash,created_at) "
                "VALUES (1,'orphan','{}','bad','now')"
            )
        store.load()
