"""Regression tests for stale asynchronous action completions.

An external driver can finish after an action was cancelled or its lease was
reclaimed.  The completion callback must be conditional: it may release stale
leases, but it must not overwrite the already-terminal action or emit a
contradictory terminal signal.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from lhos.agent_os.drivers.base import DriverInspect, DriverResult
from lhos.agent_os.kernel.models import ActionState, SideEffectClass
from lhos.agent_os.sdk.client import create_kernel


class BlockingCompletionDriver:
    """Hold dispatch until the test performs a concurrent terminal transition."""

    device_type = "race/blocking"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.started.set()
        await self.release.wait()
        return DriverResult(status="completed", output={"late": True})

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        return DriverInspect(status="unknown")

    def reset(self) -> None:
        self.started.clear()
        self.release.clear()


class BlockingUnknownDriver:
    """Return UNKNOWN only after cancellation has had a chance to win."""

    device_type = "race/unknown"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.dispatch_count = 0

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        if self.dispatch_count == 1:
            self.started.set()
            await self.release.wait()
            return DriverResult(status="unknown")
        return DriverResult(status="completed", output={"unexpected_retry": True})

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        return DriverInspect(status="unknown")

    def reset(self) -> None:
        self.started.clear()
        self.release.clear()
        self.dispatch_count = 0


class BlockingUnknownIdempotentDriver:
    """Return UNKNOWN after cancellation and record whether inspect was called."""

    device_type = "race/idempotent-unknown"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.inspect_count = 0

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.started.set()
        await self.release.wait()
        return DriverResult(status="unknown")

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.inspect_count += 1
        return DriverInspect(status="completed", output={"unexpected": True})


class BlockingUnknownInspectDriver:
    """Pause recovery inspection before reporting UNKNOWN."""

    device_type = "race/recovery-unknown"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.dispatch_count = 0

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        del action_id, operation, arguments
        self.dispatch_count += 1
        return DriverResult(status="completed", output={"unexpected_retry": True})

    async def inspect(self, action_id: str) -> DriverInspect:
        del action_id
        self.started.set()
        await self.release.wait()
        return DriverInspect(status="unknown")

    def reset(self) -> None:
        self.started.clear()
        self.release.clear()
        self.dispatch_count = 0


@pytest.mark.asyncio
async def test_stale_driver_completion_after_cancel_is_terminally_idempotent() -> None:
    """A late completion must not raise or emit FAILED/UNCERTAIN after CANCELLED."""

    kernel = create_kernel(":memory:")
    try:
        driver = BlockingCompletionDriver()
        kernel.register_driver(driver.device_type, driver)

        process = kernel._process_service.spawn(program_id="race-owner")
        claims = [{"resource_id": "resource:workspace/race", "mode": "exclusive"}]
        lease = kernel._lease_service.atomic_acquire(process.pid, claims)[0]
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "complete-late",
            side_effect_class=SideEffectClass.PURE,
            resource_claims=claims,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(
            action.action_id,
            [lease.lease_id],
            fencing_tokens={lease.resource_id: lease.fencing_token},
        )

        dispatch_task = asyncio.create_task(kernel._dispatch_pending_actions())
        await asyncio.wait_for(driver.started.wait(), timeout=1.0)

        # Simulate a concurrent cancellation/reclamation while the external
        # side effect is still in flight.
        kernel._action_service.cancel(action.action_id)
        assert kernel._lease_service.release([lease.lease_id]) == 1

        driver.release.set()
        await asyncio.wait_for(dispatch_task, timeout=1.0)

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
        assert kernel._lease_service.get_lease(lease.lease_id) is None

        contradictory_events = [
            event
            for event in kernel._journal.read_all()
            if event.payload.get("action_id") == action.action_id
            and event.event_type in {"ACTION_FAILED", "ACTION_UNCERTAIN"}
        ]
        assert contradictory_events == []

        contradictory_signals = [
            event
            for event in kernel._journal.read_all()
            if event.event_type == "SIGNAL_SENT"
            and event.payload.get("payload", {}).get("action_id") == action.action_id
            and event.payload.get("signal_type") in {"ACTION_FAILED", "ACTION_UNCERTAIN"}
        ]
        assert contradictory_signals == []
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_cancelled_pure_unknown_action_is_not_retried() -> None:
    """Cancellation after UNKNOWN's in-flight call prevents a second dispatch."""

    kernel = create_kernel(":memory:")
    try:
        driver = BlockingUnknownDriver()
        kernel.register_driver(driver.device_type, driver)

        process = kernel._process_service.spawn(program_id="unknown-owner")
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "unknown-then-retry",
            side_effect_class=SideEffectClass.PURE,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])

        dispatch_task = asyncio.create_task(kernel._dispatch_pending_actions())
        await asyncio.wait_for(driver.started.wait(), timeout=1.0)
        kernel._action_service.cancel(action.action_id)
        driver.release.set()
        await asyncio.wait_for(dispatch_task, timeout=1.0)

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
        assert driver.dispatch_count == 1
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_cancelled_idempotent_unknown_action_is_not_inspected() -> None:
    """Cancellation before UNKNOWN returns prevents a stale external inspect."""

    kernel = create_kernel(":memory:")
    try:
        driver = BlockingUnknownIdempotentDriver()
        kernel.register_driver(driver.device_type, driver)

        process = kernel._process_service.spawn(program_id="idempotent-owner")
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "unknown-then-inspect",
            side_effect_class=SideEffectClass.IDEMPOTENT,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])

        dispatch_task = asyncio.create_task(kernel._dispatch_pending_actions())
        await asyncio.wait_for(driver.started.wait(), timeout=1.0)
        kernel._action_service.cancel(action.action_id)
        driver.release.set()
        await asyncio.wait_for(dispatch_task, timeout=1.0)

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
        assert driver.inspect_count == 0

        contradictory_events = [
            event
            for event in kernel._journal.read_all()
            if event.payload.get("action_id") == action.action_id
            and event.event_type in {"ACTION_FAILED", "ACTION_UNCERTAIN"}
        ]
        assert contradictory_events == []

        contradictory_signals = [
            event
            for event in kernel._journal.read_all()
            if event.event_type == "SIGNAL_SENT"
            and event.payload.get("payload", {}).get("action_id") == action.action_id
            and event.payload.get("signal_type") in {"ACTION_FAILED", "ACTION_UNCERTAIN"}
        ]
        assert contradictory_signals == []
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_cancelled_pure_recovery_action_is_not_redispatched() -> None:
    """Cancellation during recovery inspect prevents PURE redispatch."""

    kernel = create_kernel(":memory:")
    try:
        driver = BlockingUnknownInspectDriver()
        kernel.register_driver(driver.device_type, driver)

        process = kernel._process_service.spawn(program_id="recovery-owner")
        action = kernel._action_service.submit(
            process.pid,
            driver.device_type,
            "recover-unknown",
            side_effect_class=SideEffectClass.PURE,
        )
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])
        kernel._action_service.dispatch(action.action_id)

        recovery_task = asyncio.create_task(kernel.recover_incomplete_actions())
        await asyncio.wait_for(driver.started.wait(), timeout=1.0)
        kernel._action_service.cancel(action.action_id)
        driver.release.set()
        await asyncio.wait_for(recovery_task, timeout=1.0)

        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
        assert driver.dispatch_count == 0
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_conditional_terminal_helpers_are_noops_for_cancelled_action() -> None:
    """The conditional service API is safe even when called after cancellation."""

    kernel = create_kernel(":memory:")
    try:
        process = kernel._process_service.spawn(program_id="helper-owner")
        action = kernel._action_service.submit(process.pid, "model/mock", "noop")
        kernel._action_service.admit(action.action_id)
        kernel._action_service.mark_intent_durable(action.action_id, [])
        kernel._action_service.dispatch(action.action_id)
        kernel._action_service.cancel(action.action_id)

        assert (
            kernel._action_service.fail_if_running(
                action.action_id,
                {"reason": "stale"},
            )
            is False
        )
        assert (
            kernel._action_service.mark_uncertain_if_running(
                action.action_id,
                {"reason": "stale"},
            )
            is False
        )
        current = kernel._action_service.get_action(action.action_id)
        assert current is not None
        assert current.state == ActionState.CANCELLED
    finally:
        kernel.close()
