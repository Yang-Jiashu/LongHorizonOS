"""Durable cancellation-cleanup marker tests.

Markers are journal-only evidence that an exact Claim/Lease release could not
be completed.  These tests ensure they survive restart, are idempotent, and
cannot be closed against a different ownership epoch.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent.events import SchedulerEventType
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _scheduler_with_claim(tmp_path):
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {"a1": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
        state_path=str(tmp_path / "scheduler.sqlite"),
    )
    vpg.add_ready_task("task-1", required_specializations=("python",))
    result = scheduler.schedule_once(vpg.graph_id)
    assert len(result.dispatched) == 1
    claim_id = result.dispatched[0]["claim_id"]
    attempt = scheduler.attempt_for_claim(claim_id)
    assert attempt is not None
    return scheduler, vpg, claim_id, attempt.attempt_id


def test_cleanup_marker_is_idempotent_and_reopens(tmp_path):
    scheduler, vpg, claim_id, attempt_id = _scheduler_with_claim(tmp_path)
    first = scheduler.record_cleanup_required(
        graph_id=vpg.graph_id,
        task_id="task-1",
        claim_id=claim_id,
        attempt_id=attempt_id,
        reason="cancelled",
        error="release unavailable",
    )
    second = scheduler.record_cleanup_required(
        graph_id=vpg.graph_id,
        task_id="task-1",
        claim_id=claim_id,
        attempt_id=attempt_id,
        reason="cancelled",
        error="release unavailable",
    )
    assert first.event_id == second.event_id
    assert len(scheduler.cleanup_markers) == 1
    marker_id = scheduler.cleanup_markers[0]["marker_id"]
    assert any(
        event.event_type is SchedulerEventType.EXECUTION_CLEANUP_REQUIRED
        and event.metadata["marker_id"] == marker_id
        for event in scheduler.events
    )
    scheduler.close()

    reopened = fake_scheduler(
        {"a1": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
        state_path=str(tmp_path / "scheduler.sqlite"),
    )
    assert [item["marker_id"] for item in reopened.cleanup_markers] == [marker_id]
    reopened.close()


def test_cleanup_resolution_requires_existing_matching_marker(tmp_path):
    scheduler, vpg, claim_id, attempt_id = _scheduler_with_claim(tmp_path)
    with pytest.raises(ValueError, match="unknown cleanup marker"):
        scheduler.record_cleanup_resolution(marker_id="a" * 64)

    marker = scheduler.record_cleanup_required(
        graph_id=vpg.graph_id,
        task_id="task-1",
        claim_id=claim_id,
        attempt_id=attempt_id,
    )
    marker_id = marker.metadata["marker_id"]
    with pytest.raises(ValueError, match="does not match"):
        scheduler.record_cleanup_resolution(
            marker_id=marker_id,
            graph_id=vpg.graph_id,
            task_id="task-1",
            claim_id="replacement-claim",
            attempt_id=attempt_id,
        )
    scheduler.close()


def test_cleanup_reconcile_does_not_resolve_while_lease_is_live(tmp_path):
    scheduler, vpg, claim_id, attempt_id = _scheduler_with_claim(tmp_path)
    marker = scheduler.record_cleanup_required(
        graph_id=vpg.graph_id,
        task_id="task-1",
        claim_id=claim_id,
        attempt_id=attempt_id,
    )
    scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=claim_id,
    )

    # Simulate a terminal Claim whose authoritative Kernel lease is still
    # present.  The marker must remain pending until the lease disappears.
    scheduler._s._lease_lookup_for_claim = lambda claim: object()  # type: ignore[method-assign]
    pending = scheduler.reconcile_cleanup_markers()
    assert pending[0]["status"] == "pending"
    assert pending[0]["reason"] == "terminal_claim_has_live_lease"
    assert scheduler.cleanup_markers

    scheduler._s._lease_lookup_for_claim = lambda claim: None  # type: ignore[method-assign]
    resolved = scheduler.reconcile_cleanup_markers()
    assert resolved[0]["status"] == "resolved"
    assert scheduler.cleanup_markers == []
    assert any(
        event.event_type is SchedulerEventType.EXECUTION_CLEANUP_RESOLVED
        and event.metadata["marker_id"] == marker.metadata["marker_id"]
        for event in scheduler.events
    )
    scheduler.close()


def test_run_pass_cleanup_failure_marker_reopens_and_resolves(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """A failed post-admission compensation is durable and recoverable."""

    scheduler, vpg, seed_claim_id, _seed_attempt_id = _scheduler_with_claim(tmp_path)
    assert scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=seed_claim_id,
        reason="prepare_run_pass_failure",
    )

    def fail_observation(graph_id: str) -> dict[str, int]:
        assert graph_id == vpg.graph_id
        raise RuntimeError("post-admission observation failed")

    def fail_release(*args, **kwargs) -> bool:
        assert args[:2] == (vpg.graph_id, "task-1")
        raise RuntimeError("lease release failed")

    monkeypatch.setattr(scheduler._s, "observe_vpg", fail_observation)
    monkeypatch.setattr(scheduler._s, "release_task", fail_release)

    with pytest.raises(RuntimeError, match="post-admission observation failed") as caught:
        scheduler.run_pass(vpg.graph_id)

    assert len(scheduler.cleanup_markers) == 1
    marker = scheduler.cleanup_markers[0]
    marker_id = marker["marker_id"]
    claim_id = marker["claim_id"]
    attempt_id = marker["attempt_id"]
    assert marker["graph_id"] == vpg.graph_id
    assert marker["task_id"] == "task-1"
    assert claim_id != seed_claim_id
    assert attempt_id
    assert marker["reason"] == "run_pass_post_admission_failed"
    assert marker["metadata"]["lease_id"]
    assert marker["metadata"]["error"] == "RuntimeError: lease release failed"
    assert any(marker_id in note for note in getattr(caught.value, "__notes__", ()))
    scheduler.close()

    reopened = fake_scheduler(
        {"a1": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
        state_path=str(tmp_path / "scheduler.sqlite"),
    )
    try:
        assert [item["marker_id"] for item in reopened.cleanup_markers] == [marker_id]
        assert reopened.release_task(
            vpg.graph_id,
            "task-1",
            expected_claim_id=claim_id,
            reason="recovered_exact_cleanup",
        )
        outcome = reopened.reconcile_cleanup_markers()
        assert outcome == [
            {
                "marker_id": marker_id,
                "graph_id": vpg.graph_id,
                "task_id": "task-1",
                "claim_id": claim_id,
                "status": "resolved",
                "claim_state": "released",
                "reason": "claim_terminal_without_live_lease",
            }
        ]
        assert reopened.cleanup_markers == []
    finally:
        reopened.close()


def test_run_pass_cleanup_marker_keeps_old_claim_identity_when_replaced(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Marker and supersede resolution must not target a replacement owner."""

    scheduler, vpg, seed_claim_id, _seed_attempt_id = _scheduler_with_claim(tmp_path)
    assert scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=seed_claim_id,
        reason="prepare_run_pass_failure",
    )
    original_release = scheduler._s.release_task
    cleanup_claim_id: str | None = None
    replacement_claim_id: str | None = None

    def fail_observation(graph_id: str) -> dict[str, int]:
        assert graph_id == vpg.graph_id
        raise RuntimeError("post-admission observation failed")

    def replace_then_fail(*args, **kwargs) -> bool:
        nonlocal cleanup_claim_id, replacement_claim_id
        cleanup_claim_id = str(kwargs["expected_claim_id"])
        assert original_release(*args, **kwargs)
        replacement = scheduler.schedule_once(vpg.graph_id)
        assert replacement.dispatched
        replacement_claim_id = replacement.dispatched[0]["claim_id"]
        raise RuntimeError("release acknowledgement failed")

    monkeypatch.setattr(scheduler._s, "observe_vpg", fail_observation)
    monkeypatch.setattr(scheduler._s, "release_task", replace_then_fail)

    with pytest.raises(RuntimeError, match="post-admission observation failed"):
        scheduler.run_pass(vpg.graph_id)

    assert replacement_claim_id is not None
    assert cleanup_claim_id is not None
    active = scheduler.active_claim_for_task("task-1", vpg.graph_id)
    assert active is not None
    assert active.claim_id == replacement_claim_id
    marker = scheduler.cleanup_markers[0]
    assert marker["claim_id"] == cleanup_claim_id
    assert marker["claim_id"] != replacement_claim_id
    assert marker["attempt_id"]

    scheduler.record_cleanup_resolution(
        marker_id=marker["marker_id"],
        graph_id=vpg.graph_id,
        task_id="task-1",
        claim_id=cleanup_claim_id,
        attempt_id=marker["attempt_id"],
        status="superseded",
        reason="replacement ownership epoch is authoritative",
    )
    assert scheduler.cleanup_markers == []
    active = scheduler.active_claim_for_task("task-1", vpg.graph_id)
    assert active is not None
    assert active.claim_id == replacement_claim_id

    monkeypatch.setattr(scheduler._s, "release_task", original_release)
    assert scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=replacement_claim_id,
        reason="test_cleanup",
    )
    scheduler.close()


def test_run_pass_marker_write_failure_does_not_mask_root_exception(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    scheduler, vpg, seed_claim_id, _seed_attempt_id = _scheduler_with_claim(tmp_path)
    assert scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=seed_claim_id,
        reason="prepare_run_pass_failure",
    )
    original_release = scheduler._s.release_task
    cleanup_claim_id: str | None = None

    def fail_observation(graph_id: str) -> dict[str, int]:
        assert graph_id == vpg.graph_id
        raise RuntimeError("root observation failure")

    def fail_release(*args, **kwargs) -> bool:
        nonlocal cleanup_claim_id
        cleanup_claim_id = str(kwargs["expected_claim_id"])
        raise RuntimeError("cleanup release failure")

    def fail_marker(**kwargs):
        assert kwargs["claim_id"] == cleanup_claim_id
        raise OSError("scheduler journal unavailable")

    monkeypatch.setattr(scheduler._s, "observe_vpg", fail_observation)
    monkeypatch.setattr(scheduler._s, "release_task", fail_release)
    monkeypatch.setattr(scheduler._s, "record_cleanup_required", fail_marker)

    with pytest.raises(RuntimeError, match="root observation failure") as caught:
        scheduler.run_pass(vpg.graph_id)

    notes = getattr(caught.value, "__notes__", ())
    assert any("durable cleanup marker write failed" in note for note in notes)
    assert any("scheduler journal unavailable" in note for note in notes)
    assert any("exact-claim compensation was incomplete" in note for note in notes)
    assert scheduler.cleanup_markers == []
    assert cleanup_claim_id is not None

    monkeypatch.setattr(scheduler._s, "release_task", original_release)
    assert scheduler.release_task(
        vpg.graph_id,
        "task-1",
        expected_claim_id=cleanup_claim_id,
        reason="test_cleanup",
    )
    scheduler.close()
