"""Regression tests for action resource admission and waiter lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lhos.agent_os.drivers.base import DriverResult
from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import ActionState, Capability, SubmitActionRequest
from lhos.agent_os.sdk.client import create_kernel


class RecordingDriver:
    def __init__(self) -> None:
        self.dispatch_calls: list[str] = []

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, object],
    ) -> DriverResult:
        self.dispatch_calls.append(action_id)
        return DriverResult(status="completed", output={"ok": True})

    async def inspect(self, action_id: str) -> DriverResult:
        return DriverResult(status="unknown")


def _grant_resource_acquire(kernel, pid: str, resource_id: str) -> None:
    kernel._capability_service.grant(
        pid,
        Capability(
            resource_pattern=resource_id,
            operations={"acquire"},
        ),
    )


@pytest.mark.asyncio
async def test_conflicting_submit_is_failed_and_never_dispatched() -> None:
    kernel = create_kernel(":memory:")
    holder = kernel._lease_service.atomic_acquire(
        "holder",
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )
    driver = RecordingDriver()
    kernel.register_driver("tool/mock", driver)

    pid = await kernel.spawn(type("Program", (), {"program_id": "admission-test"})())
    _grant_resource_acquire(kernel, pid, "resource:R1")
    request = SubmitActionRequest(
        pid=pid,
        device_type="tool/mock",
        operation="write",
        resource_claims=[{"resource_id": "resource:R1", "mode": "exclusive"}],
    )

    with pytest.raises(LeaseAcquisitionFailed):
        await kernel._dispatcher.dispatch(request)

    action = kernel._action_service.list_by_pid(pid)[-1]
    assert action.state == ActionState.FAILED
    assert action.lease_ids == []
    assert kernel._lease_service.list_waiters("resource:R1") == []

    await kernel._dispatch_pending_actions()
    assert driver.dispatch_calls == []
    assert kernel._lease_service.get_lease(holder[0].lease_id) is not None


@pytest.mark.asyncio
async def test_claimed_action_with_missing_lease_fails_before_driver_call() -> None:
    kernel = create_kernel(":memory:")
    driver = RecordingDriver()
    kernel.register_driver("tool/recording", driver)

    claim = [{"resource_id": "resource:R1", "mode": "exclusive"}]
    leases = kernel._lease_service.atomic_acquire("p1", claim)
    action = kernel._action_service.submit(
        "p1",
        "tool/recording",
        "write",
        resource_claims=claim,
    )
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(
        action.action_id,
        [leases[0].lease_id],
    )
    kernel._lease_service.release([leases[0].lease_id])

    await kernel._dispatch_pending_actions()

    failed = kernel._action_service.get_action(action.action_id)
    assert failed is not None
    assert failed.state == ActionState.FAILED
    assert failed.error == {
        "reason": "invalid_resource_contract",
        "detail": "lease_missing",
    }
    assert driver.dispatch_calls == []


@pytest.mark.asyncio
async def test_claimed_action_with_expired_lease_fails_before_driver_call() -> None:
    kernel = create_kernel(":memory:")
    driver = RecordingDriver()
    kernel.register_driver("tool/recording", driver)

    claim = [{"resource_id": "resource:R1", "mode": "exclusive"}]
    leases = kernel._lease_service.atomic_acquire("p1", claim)
    action = kernel._action_service.submit(
        "p1",
        "tool/recording",
        "write",
        resource_claims=claim,
    )
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(
        action.action_id,
        [leases[0].lease_id],
    )
    kernel._storage.execute(
        "UPDATE leases_projection SET expires_at = ? WHERE lease_id = ?",
        ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), leases[0].lease_id),
    )

    await kernel._dispatch_pending_actions()

    failed = kernel._action_service.get_action(action.action_id)
    assert failed is not None
    assert failed.state == ActionState.FAILED
    assert failed.error == {
        "reason": "invalid_resource_contract",
        "detail": "lease_expired",
    }
    assert driver.dispatch_calls == []


@pytest.mark.asyncio
async def test_action_without_resource_claims_still_dispatches() -> None:
    kernel = create_kernel(":memory:")
    driver = RecordingDriver()
    kernel.register_driver("tool/recording", driver)
    action = kernel._action_service.submit("p1", "tool/recording", "read")
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(action.action_id, [])

    await kernel._dispatch_pending_actions()

    committed = kernel._action_service.get_action(action.action_id)
    assert committed is not None
    assert committed.state == ActionState.COMMITTED
    assert driver.dispatch_calls == [action.action_id]


def test_successful_retry_clears_all_prior_waiters() -> None:
    kernel = create_kernel(":memory:")
    held = kernel._lease_service.atomic_acquire(
        "holder",
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )
    kernel._lease_service._add_waiter("p2", "resource:stale")
    with pytest.raises(LeaseAcquisitionFailed):
        kernel._lease_service.atomic_acquire(
            "p2",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )

    kernel._lease_service.release([held[0].lease_id])
    kernel._lease_service.atomic_acquire(
        "p2",
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )

    rows = kernel._storage.query_all(
        "SELECT resource_id FROM lease_waiters WHERE pid = ?",
        ("p2",),
    )
    assert rows == []


def test_stale_waiters_are_cleared_on_process_cleanup() -> None:
    kernel = create_kernel(":memory:")
    kernel._lease_service.atomic_acquire(
        "p1",
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )
    kernel._lease_service.atomic_acquire(
        "p2",
        [{"resource_id": "resource:R2", "mode": "exclusive"}],
    )
    kernel._lease_service._add_waiter("p1", "resource:R2")
    kernel._lease_service._add_waiter("p2", "resource:R1")
    assert kernel._lease_service.detect_deadlocks()

    kernel._lease_service.release_all_for_pid("p1")

    assert kernel._lease_service.detect_deadlocks() == []
    assert (
        kernel._storage.query_all(
            "SELECT * FROM lease_waiters WHERE pid = ?",
            ("p1",),
        )
        == []
    )


@pytest.mark.parametrize("mode", ["exclusive", "shared"])
def test_duplicate_resource_claims_are_rejected_without_side_effects(mode: str) -> None:
    kernel = create_kernel(":memory:")

    with pytest.raises(LeaseAcquisitionFailed):
        kernel._lease_service.atomic_acquire(
            "p1",
            [
                {"resource_id": "resource:R1", "mode": mode},
                {"resource_id": "resource:R1", "mode": mode},
            ],
        )

    assert kernel._lease_service.list_all_leases() == []
    assert kernel._lease_service.list_waiters("resource:R1") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["admit", "mark_intent_durable"])
async def test_admission_internal_failure_compensates_action_and_leases(
    monkeypatch,
    failure_point: str,
) -> None:
    kernel = create_kernel(":memory:")
    driver = RecordingDriver()
    kernel.register_driver("tool/mock", driver)
    pid = await kernel.spawn(type("Program", (), {"program_id": "admission-failure"})())
    _grant_resource_acquire(kernel, pid, "resource:R1")
    request = SubmitActionRequest(
        pid=pid,
        device_type="tool/mock",
        operation="write",
        resource_claims=[{"resource_id": "resource:R1", "mode": "exclusive"}],
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError(f"{failure_point} exploded")

    monkeypatch.setattr(kernel._action_service, failure_point, fail)

    with pytest.raises(RuntimeError, match="exploded"):
        await kernel._dispatcher.dispatch(request)

    action = kernel._action_service.list_by_pid(pid)[-1]
    assert action.state == ActionState.FAILED
    assert kernel._lease_service.list_active_leases_for_resource("resource:R1") == []
    assert kernel._lease_service.list_waiters("resource:R1") == []

    await kernel._dispatch_pending_actions()
    assert driver.dispatch_calls == []
