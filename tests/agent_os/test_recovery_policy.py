"""Adversarial tests for durable Action recovery policies."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest

from lhos.agent_os.drivers.base import DriverInspect, DriverResult
from lhos.agent_os.kernel.models import (
    ActionState,
    RecoveryPolicy,
    SideEffectClass,
)
from lhos.agent_os.sdk.client import create_kernel, rebuild_from_journal


class UnknownThenInspectDriver:
    """Return UNKNOWN from dispatch and expose a configurable inspection result."""

    device_type = "test/recovery-policy"

    def __init__(
        self,
        *,
        inspect_status: str = "completed",
        inspect_output: dict[str, Any] | None = None,
    ) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0
        self.inspect_status = inspect_status
        self.inspect_output = inspect_output or {"reconciled": True}

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        return DriverResult(status="unknown")

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.inspect_count += 1
        return DriverInspect(
            status=self.inspect_status,  # type: ignore[arg-type]
            output=self.inspect_output,
        )

    def reset(self) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0


class InspectOnlyDriver:
    """A recovery driver whose inspect call is observable and dispatch is not safe."""

    device_type = "test/recovery-inspect-only"

    def __init__(self) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        return DriverResult(status="completed", output={"blind_dispatch": True})

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.inspect_count += 1
        return DriverInspect(status="completed", output={"observed": True})

    def reset(self) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0


class ExceptionSequenceDriver:
    """Raise on the first dispatch, then expose a controlled retry outcome."""

    device_type = "test/recovery-dispatch-exception"

    def __init__(
        self,
        *,
        retry_status: str = "completed",
        retry_error: dict[str, Any] | None = None,
        on_first_dispatch: Callable[[], None] | None = None,
        on_retry_dispatch: Callable[[], None] | None = None,
    ) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0
        self.retry_status = retry_status
        self.retry_error = retry_error
        self.on_first_dispatch = on_first_dispatch
        self.on_retry_dispatch = on_retry_dispatch

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        if self.dispatch_count == 1:
            if self.on_first_dispatch is not None:
                self.on_first_dispatch()
            raise RuntimeError("first dispatch lost its acknowledgement")
        if self.on_retry_dispatch is not None:
            self.on_retry_dispatch()
        if self.retry_status == "raise":
            raise RuntimeError("retry dispatch lost its acknowledgement")
        return DriverResult(
            status=self.retry_status,  # type: ignore[arg-type]
            output={"retried": True},
            error=self.retry_error,
        )

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.inspect_count += 1
        return DriverInspect(status="unknown")


class CoordinatedRecoveryDriver:
    """Coordinate two Kernel recovery loops around one durable retry."""

    device_type = "test/concurrent-recovery-policy"

    def __init__(
        self,
        *,
        role: str,
        loser_inspecting: asyncio.Event,
        winner_dispatch_started: asyncio.Event,
        allow_winner_completion: asyncio.Event,
    ) -> None:
        self.role = role
        self.loser_inspecting = loser_inspecting
        self.winner_dispatch_started = winner_dispatch_started
        self.allow_winner_completion = allow_winner_completion
        self.dispatch_count = 0
        self.inspect_count = 0

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        if self.role != "winner":
            raise AssertionError("retry reservation loser must not dispatch")
        self.winner_dispatch_started.set()
        await self.allow_winner_completion.wait()
        return DriverResult(status="completed", output={"winner": self.role})

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.inspect_count += 1
        if self.role == "winner":
            await self.loser_inspecting.wait()
        else:
            self.loser_inspecting.set()
            await self.winner_dispatch_started.wait()
        return DriverInspect(status="unknown")


def _admitted_action_with_lease(
    kernel: Any,
    driver: Any,
    *,
    resource_id: str = "resource:recovery-policy/retry",
) -> tuple[Any, Any, Any]:
    """Create an admitted action with a persisted lease/fencing contract."""
    process = kernel._process_service.spawn(program_id="recovery-policy-retry-owner")
    claims = [{"resource_id": resource_id, "mode": "exclusive"}]
    lease = kernel._lease_service.atomic_acquire(process.pid, claims)[0]
    action = kernel._action_service.submit(
        process.pid,
        driver.device_type,
        "retryable-operation",
        side_effect_class=SideEffectClass.PURE,
        recovery_policy=RecoveryPolicy.RETRY,
        resource_claims=claims,
    )
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(
        action.action_id,
        [lease.lease_id],
        fencing_tokens={lease.resource_id: lease.fencing_token},
    )
    return process, action, lease


def _action_signals(kernel: Any, action_id: str) -> list[Any]:
    return [
        event
        for event in kernel._journal.read_all()
        if event.event_type == "SIGNAL_SENT"
        and event.payload.get("payload", {}).get("action_id") == action_id
    ]


@pytest.mark.asyncio
async def test_dispatch_exception_retries_pure_action_once_and_commits() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver(retry_status="completed")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        _, action, lease = _admitted_action_with_lease(kernel, driver)

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.COMMITTED
        assert current.result == {"retried": True}
        assert driver.dispatch_count == 2
        assert driver.inspect_count == 0
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert release_calls == [[lease.lease_id]]

        signals = _action_signals(kernel, action.action_id)
        assert [event.payload["signal_type"] for event in signals] == ["ACTION_COMPLETED"]
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_dispatch_exception_retry_exception_is_uncertain_and_releases_once() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver(retry_status="raise")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        _, action, lease = _admitted_action_with_lease(kernel, driver)

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert current.error is not None
        assert current.error["reason"] == "retry_failed"
        assert current.error["recovery_policy"] == RecoveryPolicy.RETRY.value
        assert current.error["initial_error"]["reason"] == "driver_dispatch_exception"
        assert driver.dispatch_count == 2
        assert driver.inspect_count == 0
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert release_calls == [[lease.lease_id]]

        signals = _action_signals(kernel, action.action_id)
        assert [event.payload["signal_type"] for event in signals] == ["ACTION_UNCERTAIN"]
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_dispatch_exception_retry_unknown_is_uncertain_and_releases_once() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver(retry_status="unknown")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        _, action, lease = _admitted_action_with_lease(kernel, driver)

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert current.error is not None
        assert current.error["reason"] == "retry_unknown"
        assert current.error["recovery_policy"] == RecoveryPolicy.RETRY.value
        assert current.error["initial_error"]["reason"] == "driver_dispatch_exception"
        assert driver.dispatch_count == 2
        assert driver.inspect_count == 0
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert release_calls == [[lease.lease_id]]

        signals = _action_signals(kernel, action.action_id)
        assert [event.payload["signal_type"] for event in signals] == ["ACTION_UNCERTAIN"]
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_dispatch_exception_does_not_retry_after_fencing_superseded() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver(retry_status="completed")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release
    replacement: list[Any] = []

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        process, action, lease = _admitted_action_with_lease(kernel, driver)

        def supersede_old_lease() -> None:
            # Use the original method so the spy observes only the Kernel's
            # single cleanup call for the stale lease bundle.
            assert original_release([lease.lease_id]) == 1
            replacement.extend(
                kernel._lease_service.atomic_acquire(
                    process.pid,
                    action.resource_claims,
                )
            )

        driver.on_first_dispatch = supersede_old_lease
        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert current.error is not None
        assert current.error["reason"] == "retry_fencing_contract_invalid"
        assert current.error["detail"] == "lease_missing"
        assert driver.dispatch_count == 1
        assert driver.inspect_count == 0
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert replacement
        assert kernel._lease_service.get_lease(replacement[0].lease_id) is not None
        assert release_calls == [[lease.lease_id]]

        signals = _action_signals(kernel, action.action_id)
        assert [event.payload["signal_type"] for event in signals] == ["ACTION_UNCERTAIN"]
    finally:
        for item in replacement:
            original_release([item.lease_id])
        kernel.close()


@pytest.mark.asyncio
async def test_dispatch_exception_retry_completion_is_rejected_after_fencing_superseded() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver(retry_status="completed")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release
    replacement: list[Any] = []

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        process, action, lease = _admitted_action_with_lease(kernel, driver)

        def supersede_before_retry_commit() -> None:
            assert original_release([lease.lease_id]) == 1
            replacement.extend(
                kernel._lease_service.atomic_acquire(
                    process.pid,
                    action.resource_claims,
                )
            )

        driver.on_retry_dispatch = supersede_before_retry_commit
        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.FAILED
        assert current.error is not None
        assert current.error["reason"] == "stale_fenced_completion"
        assert current.error["source"] == "dispatch_exception_retry"
        assert driver.dispatch_count == 2
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert replacement
        assert kernel._lease_service.get_lease(replacement[0].lease_id) is not None
        assert release_calls == [[lease.lease_id]]

        signals = _action_signals(kernel, action.action_id)
        assert [event.payload["signal_type"] for event in signals] == ["ACTION_FAILED"]
    finally:
        for item in replacement:
            original_release([item.lease_id])
        kernel.close()


@pytest.mark.asyncio
async def test_unknown_retry_does_not_dispatch_after_reservation_is_cancelled() -> None:
    kernel = create_kernel(":memory:")
    driver = UnknownThenInspectDriver(inspect_status="unknown")
    kernel.register_driver(driver.device_type, driver)
    try:
        process = kernel._process_service.spawn(program_id="cancel-after-reserve")
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "retryable-operation",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.RETRY,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])
        original_reserve = kernel._action_service.reserve_retry_if_fenced

        def reserve_then_cancel(action_id: str, **kwargs: Any):
            result = original_reserve(action_id, **kwargs)
            assert result == (True, None)
            kernel._action_service.cancel(action_id)
            return result

        kernel._action_service.reserve_retry_if_fenced = reserve_then_cancel  # type: ignore[method-assign]

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
        assert current.retry_count == 1
        assert driver.dispatch_count == 1
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_recovery_retry_does_not_dispatch_after_lease_is_superseded() -> None:
    kernel = create_kernel(":memory:")
    driver = UnknownThenInspectDriver(inspect_status="unknown")
    kernel.register_driver(driver.device_type, driver)
    replacement: list[Any] = []
    try:
        process, action, lease = _admitted_action_with_lease(kernel, driver)
        kernel._action_service.dispatch(action.action_id)
        original_reserve = kernel._action_service.reserve_retry_if_fenced

        def reserve_then_supersede(action_id: str, **kwargs: Any):
            result = original_reserve(action_id, **kwargs)
            assert result == (True, None)
            assert kernel._lease_service.release([lease.lease_id]) == 1
            replacement.extend(
                kernel._lease_service.atomic_acquire(
                    process.pid,
                    action.resource_claims,
                )
            )
            return result

        kernel._action_service.reserve_retry_if_fenced = reserve_then_supersede  # type: ignore[method-assign]

        await kernel.recover_incomplete_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert current.retry_count == 1
        assert current.error is not None
        assert current.error["reason"] == "retry_dispatch_contract_invalid"
        assert current.error["detail"] == "lease_missing"
        assert driver.inspect_count == 1
        assert driver.dispatch_count == 0
    finally:
        for item in replacement:
            kernel._lease_service.release([item.lease_id])
        kernel.close()


@pytest.mark.asyncio
async def test_concurrent_recovery_retry_loser_preserves_winner_action_and_lease(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "concurrent-recovery.sqlite")
    winner_kernel = create_kernel(db_path)
    loser_kernel = create_kernel(db_path)
    loser_inspecting = asyncio.Event()
    winner_dispatch_started = asyncio.Event()
    allow_winner_completion = asyncio.Event()
    winner_driver = CoordinatedRecoveryDriver(
        role="winner",
        loser_inspecting=loser_inspecting,
        winner_dispatch_started=winner_dispatch_started,
        allow_winner_completion=allow_winner_completion,
    )
    loser_driver = CoordinatedRecoveryDriver(
        role="loser",
        loser_inspecting=loser_inspecting,
        winner_dispatch_started=winner_dispatch_started,
        allow_winner_completion=allow_winner_completion,
    )
    winner_kernel.register_driver(winner_driver.device_type, winner_driver)
    loser_kernel.register_driver(loser_driver.device_type, loser_driver)
    winner_release_calls: list[list[str]] = []
    loser_release_calls: list[list[str]] = []
    winner_original_release = winner_kernel._lease_service.release
    loser_original_release = loser_kernel._lease_service.release

    def winner_release_spy(lease_ids: list[str]) -> int:
        winner_release_calls.append(list(lease_ids))
        return winner_original_release(lease_ids)

    def loser_release_spy(lease_ids: list[str]) -> int:
        loser_release_calls.append(list(lease_ids))
        return loser_original_release(lease_ids)

    winner_kernel._lease_service.release = winner_release_spy  # type: ignore[method-assign]
    loser_kernel._lease_service.release = loser_release_spy  # type: ignore[method-assign]
    winner_task: asyncio.Task[None] | None = None
    loser_task: asyncio.Task[None] | None = None
    try:
        _, action, lease = _admitted_action_with_lease(
            winner_kernel,
            winner_driver,
            resource_id="resource:recovery-policy/concurrent-retry",
        )
        winner_kernel._action_service.dispatch(action.action_id)

        winner_task = asyncio.create_task(winner_kernel.recover_incomplete_actions())
        loser_task = asyncio.create_task(loser_kernel.recover_incomplete_actions())

        await asyncio.wait_for(winner_dispatch_started.wait(), timeout=2)
        await asyncio.wait_for(loser_task, timeout=2)

        in_flight = loser_kernel._action_service.get_action(action.action_id)
        assert in_flight is not None
        assert in_flight.state == ActionState.RUNNING
        assert in_flight.retry_count == 1
        assert loser_kernel._lease_service.get_lease(lease.lease_id) is not None
        assert loser_driver.inspect_count == 1
        assert loser_driver.dispatch_count == 0
        assert loser_release_calls == []
        assert _action_signals(loser_kernel, action.action_id) == []

        allow_winner_completion.set()
        await asyncio.wait_for(winner_task, timeout=2)

        final = winner_kernel._action_service.get_action(action.action_id)
        assert final is not None
        assert final.state == ActionState.COMMITTED
        assert final.result == {"winner": "winner"}
        assert winner_driver.inspect_count == 1
        assert winner_driver.dispatch_count == 1
        assert winner_release_calls == [[lease.lease_id]]
        assert loser_release_calls == []
        reservations = [
            event
            for event in winner_kernel._journal.read_all()
            if event.event_type == "ACTION_RETRY_RESERVED"
            and event.payload.get("action_id") == action.action_id
        ]
        assert len(reservations) == 1
    finally:
        allow_winner_completion.set()
        pending = [
            task for task in (winner_task, loser_task) if task is not None and not task.done()
        ]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        loser_kernel.close()
        winner_kernel.close()


@pytest.mark.asyncio
async def test_retry_reservation_loser_does_not_release_after_peer_commits(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "peer-commit-before-loser-reread.sqlite")
    owner_kernel = create_kernel(db_path)
    loser_kernel = create_kernel(db_path)
    driver = UnknownThenInspectDriver(inspect_status="unknown")
    loser_kernel.register_driver(driver.device_type, driver)
    inspect_started = asyncio.Event()
    allow_inspect_return = asyncio.Event()
    release_calls: list[list[str]] = []
    original_release = loser_kernel._lease_service.release

    async def blocked_inspect(action_id: str) -> DriverInspect:
        del action_id
        driver.inspect_count += 1
        inspect_started.set()
        await allow_inspect_return.wait()
        return DriverInspect(status="unknown")

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    driver.inspect = blocked_inspect  # type: ignore[method-assign]
    loser_kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    loser_original_reserve = loser_kernel._action_service.reserve_retry_if_fenced
    recovery_task: asyncio.Task[None] | None = None
    lease: Any | None = None
    try:
        _, action, lease = _admitted_action_with_lease(
            owner_kernel,
            driver,
            resource_id="resource:recovery-policy/peer-commit",
        )
        owner_kernel._action_service.dispatch(action.action_id)

        def lose_reservation_then_observe_peer_commit(
            action_id: str,
            **kwargs: Any,
        ) -> tuple[bool, str | None]:
            result = loser_original_reserve(action_id, **kwargs)
            assert result == (False, "retry_budget_exhausted")
            committed, error = owner_kernel._action_service.commit_if_fenced(
                action_id,
                result={"peer": "committed"},
            )
            assert committed is True
            assert error is None
            return result

        loser_kernel._action_service.reserve_retry_if_fenced = (  # type: ignore[method-assign]
            lose_reservation_then_observe_peer_commit
        )
        recovery_task = asyncio.create_task(loser_kernel.recover_incomplete_actions())
        await asyncio.wait_for(inspect_started.wait(), timeout=2)

        reserved, error = owner_kernel._action_service.reserve_retry_if_fenced(action.action_id)
        assert reserved is True
        assert error is None
        allow_inspect_return.set()
        await asyncio.wait_for(recovery_task, timeout=2)

        final = loser_kernel._action_service.get_action(action.action_id)
        assert final is not None
        assert final.state == ActionState.COMMITTED
        assert final.result == {"peer": "committed"}
        assert final.retry_count == 1
        assert loser_kernel._lease_service.get_lease(lease.lease_id) is not None
        assert release_calls == []
        assert driver.dispatch_count == 0
        assert _action_signals(loser_kernel, action.action_id) == []
    finally:
        allow_inspect_return.set()
        if recovery_task is not None and not recovery_task.done():
            await asyncio.gather(recovery_task, return_exceptions=True)
        if lease is not None:
            owner_kernel._lease_service.release([lease.lease_id])
        loser_kernel.close()
        owner_kernel.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reservation_error",
    [
        "retry_budget_exhausted",
        "lease_missing",
        "action_not_running:cancelled",
    ],
)
async def test_retry_reservation_failure_without_live_peer_fails_closed(
    reservation_error: str,
) -> None:
    kernel = create_kernel(":memory:")
    driver = UnknownThenInspectDriver(inspect_status="unknown")
    kernel.register_driver(driver.device_type, driver)
    release_calls: list[list[str]] = []
    original_release = kernel._lease_service.release

    def release_spy(lease_ids: list[str]) -> int:
        release_calls.append(list(lease_ids))
        return original_release(lease_ids)

    kernel._lease_service.release = release_spy  # type: ignore[method-assign]
    try:
        _, action, lease = _admitted_action_with_lease(kernel, driver)
        kernel._action_service.dispatch(action.action_id)

        def reject_reservation(
            action_id: str,
            **kwargs: Any,
        ) -> tuple[bool, str | None]:
            del action_id, kwargs
            return False, reservation_error

        kernel._action_service.reserve_retry_if_fenced = reject_reservation  # type: ignore[method-assign]

        await kernel.recover_incomplete_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert current.retry_count == 0
        assert current.error is not None
        assert current.error["reason"] == "retry_reservation_failed"
        assert current.error["detail"] == reservation_error
        assert kernel._lease_service.get_lease(lease.lease_id) is None
        assert release_calls == [[lease.lease_id]]
        assert driver.inspect_count == 1
        assert driver.dispatch_count == 0
    finally:
        kernel.close()


def _running_action(
    kernel: Any,
    driver: Any,
    *,
    side_effect_class: SideEffectClass,
    recovery_policy: RecoveryPolicy,
) -> Any:
    process = kernel._process_service.spawn(program_id="recovery-policy-owner")
    action = kernel._action_service.submit(
        process.pid,
        driver.device_type,
        "external-operation",
        side_effect_class=side_effect_class,
        recovery_policy=recovery_policy,
    )
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(action.action_id, [])
    kernel._action_service.dispatch(action.action_id)
    return action


@pytest.mark.asyncio
async def test_pure_uncertain_policy_fails_closed_without_inspection() -> None:
    kernel = create_kernel(":memory:")
    driver = InspectOnlyDriver()
    kernel.register_driver(driver.device_type, driver)
    try:
        action = _running_action(
            kernel,
            driver,
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.UNCERTAIN,
        )

        await kernel.recover_incomplete_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert driver.inspect_count == 0
        assert driver.dispatch_count == 0
        assert current.error is not None
        assert current.error["recovery_policy"] == RecoveryPolicy.UNCERTAIN.value
    finally:
        kernel.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "side_effect_class",
    [SideEffectClass.PURE, SideEffectClass.NON_REVERSIBLE],
)
async def test_explicit_inspect_policy_never_redispatches(
    side_effect_class: SideEffectClass,
) -> None:
    kernel = create_kernel(":memory:")
    driver = UnknownThenInspectDriver()
    kernel.register_driver(driver.device_type, driver)
    try:
        action = kernel._action_service.submit(
            kernel._process_service.spawn(program_id="dispatch-owner").pid,
            driver.device_type,
            "unknown-operation",
            side_effect_class=side_effect_class,
            recovery_policy=RecoveryPolicy.INSPECT,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.COMMITTED
        assert driver.dispatch_count == 1
        assert driver.inspect_count == 1
        assert current.result == {"reconciled": True}
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_dead_letter_policy_records_terminal_diagnostic() -> None:
    kernel = create_kernel(":memory:")
    driver = UnknownThenInspectDriver()
    kernel.register_driver(driver.device_type, driver)
    try:
        process = kernel._process_service.spawn(program_id="dead-letter-owner")
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "irreversible-operation",
            side_effect_class=SideEffectClass.NON_REVERSIBLE,
            recovery_policy=RecoveryPolicy.DEAD_LETTER,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])

        await kernel._dispatch_pending_actions()

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.UNCERTAIN
        assert driver.dispatch_count == 1
        assert driver.inspect_count == 0
        assert current.error is not None
        assert current.error["recovery_policy"] == RecoveryPolicy.DEAD_LETTER.value
        assert current.error["recovery_disposition"] == "dead_letter"
    finally:
        kernel.close()


def test_recovery_policy_survives_journal_rebuild(tmp_path) -> None:
    db_path = str(tmp_path / "recovery-policy.db")
    kernel = create_kernel(db_path)
    try:
        process = kernel._process_service.spawn(program_id="durable-policy-owner")
        action = kernel._action_service.submit(
            process.pid,
            "model/mock",
            "durable-operation",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.INSPECT,
        )
        reopened = rebuild_from_journal(db_path)
        restored = reopened._action_service.get_action(action.action_id)
        assert restored is not None
        assert restored.recovery_policy == RecoveryPolicy.INSPECT
        assert restored.side_effect_class == SideEffectClass.PURE
    finally:
        kernel.close()
        if "reopened" in locals():
            reopened.close()


def test_retry_budget_can_only_be_reserved_once() -> None:
    kernel = create_kernel(":memory:")
    driver = ExceptionSequenceDriver()
    try:
        _, action, _ = _admitted_action_with_lease(kernel, driver)
        kernel._action_service.dispatch(action.action_id)

        first = kernel._action_service.reserve_retry_if_fenced(action.action_id)
        second = kernel._action_service.reserve_retry_if_fenced(action.action_id)

        assert first == (True, None)
        assert second == (False, "retry_budget_exhausted")
        restored = kernel._action_service.get_action(action.action_id)
        assert restored is not None
        assert restored.retry_count == 1
        reservations = [
            event
            for event in kernel._journal.read_all()
            if event.event_type == "ACTION_RETRY_RESERVED"
            and event.payload.get("action_id") == action.action_id
        ]
        assert len(reservations) == 1
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_retry_reservation_survives_journal_rebuild_without_redispatch(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "durable-retry-budget.sqlite")
    kernel = create_kernel(db_path)
    driver = ExceptionSequenceDriver()
    _, action, _ = _admitted_action_with_lease(kernel, driver)
    kernel._action_service.dispatch(action.action_id)
    reserved, error = kernel._action_service.reserve_retry_if_fenced(action.action_id)
    assert reserved is True
    assert error is None
    kernel.close()

    reopened = rebuild_from_journal(db_path)
    inspect_driver = InspectOnlyDriver()
    reopened.register_driver(driver.device_type, inspect_driver)
    try:
        restored = reopened._action_service.get_action(action.action_id)
        assert restored is not None
        assert restored.retry_count == 1
        assert reopened._unknown_recovery_mode(restored) == "inspect"

        await reopened.recover_incomplete_actions()

        final = reopened._action_service.get_action(action.action_id)
        assert final is not None
        assert final.state == ActionState.COMMITTED
        assert inspect_driver.inspect_count == 1
        assert inspect_driver.dispatch_count == 0
    finally:
        reopened.close()


def test_legacy_action_event_rebuild_does_not_mint_retry_budget(tmp_path) -> None:
    db_path = str(tmp_path / "legacy-retry-event.sqlite")
    kernel = create_kernel(db_path)
    try:
        process = kernel._process_service.spawn(program_id="legacy-retry-owner")
        action = kernel._action_service.submit(
            process.pid,
            "model/mock",
            "legacy-operation",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.RETRY,
        )
        row = kernel._storage.query_one(
            "SELECT payload_json FROM journal_events "
            "WHERE event_type = 'ACTION_SUBMITTED' AND pid = ?",
            (process.pid,),
        )
        assert row is not None
        payload = json.loads(row["payload_json"])
        payload.pop("retry_count")
        kernel._storage.execute(
            "UPDATE journal_events SET payload_json = ? "
            "WHERE event_type = 'ACTION_SUBMITTED' AND pid = ?",
            (kernel._storage.dumps(payload), process.pid),
        )
    finally:
        kernel.close()

    reopened = rebuild_from_journal(db_path)
    try:
        restored = reopened._action_service.get_action(action.action_id)
        assert restored is not None
        assert restored.retry_count == 1
        assert reopened._unknown_recovery_mode(restored) == "inspect"
    finally:
        reopened.close()


def test_malformed_durable_policy_is_normalized_fail_closed() -> None:
    kernel = create_kernel(":memory:")
    try:
        process = kernel._process_service.spawn(program_id="malformed-policy-owner")
        action = kernel._action_service.submit(
            process.pid,
            "model/mock",
            "malformed-operation",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.RETRY,
        )
        kernel._storage.execute(
            "UPDATE actions_projection SET side_effect_class = ?, recovery_policy = ? "
            "WHERE action_id = ?",
            ("not-a-class", "not-a-policy", action.action_id),
        )

        restored = kernel._action_service.get_action(action.action_id)
        assert restored is not None
        assert restored.side_effect_class == SideEffectClass.UNKNOWN
        assert restored.recovery_policy == RecoveryPolicy.UNCERTAIN
        assert kernel._unknown_recovery_mode(restored) == "uncertain"
    finally:
        kernel.close()
