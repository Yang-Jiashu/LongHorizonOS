"""Dispatcher ownership checks for action and lease control syscalls."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import (
    ActionState,
    CancelActionRequest,
    CheckpointRequest,
    InspectActionRequest,
    ReleaseResourceRequest,
    RestoreRequest,
)
from lhos.agent_os.sdk.client import create_kernel


@pytest.fixture
def kernel():
    instance = create_kernel(":memory:")
    try:
        yield instance
    finally:
        instance.close()


@pytest.mark.asyncio
async def test_cross_pid_inspect_is_non_disclosing_but_owner_can_inspect(kernel) -> None:
    acb = kernel._action_service.submit("p1", "model/mock", "generate")
    kernel._action_service.admit(acb.action_id)
    kernel._action_service.mark_intent_durable(acb.action_id, [])
    kernel._action_service.dispatch(acb.action_id)
    kernel._action_service.commit(acb.action_id, result={"secret": "p1-only"})

    denied = await kernel._dispatcher.dispatch(
        InspectActionRequest(pid="p2", action_id=acb.action_id)
    )

    assert denied is not None
    assert denied.event_type == "ACTION_INSPECT_FAILED"
    assert denied.payload == {"action_id": acb.action_id, "reason": "not_found"}
    assert not {"state", "result", "error"} & denied.payload.keys()

    allowed = await kernel._dispatcher.dispatch(
        InspectActionRequest(pid="p1", action_id=acb.action_id)
    )
    assert allowed is not None
    assert allowed.event_type == "ACTION_INSPECTED"
    assert allowed.payload["state"] == ActionState.COMMITTED.value
    assert allowed.payload["result"] == {"secret": "p1-only"}


@pytest.mark.asyncio
async def test_cross_pid_cancel_cannot_change_action_or_release_owner_lease(kernel) -> None:
    claim = {"resource_id": "resource:R1", "mode": "exclusive"}
    lease = kernel._lease_service.atomic_acquire("p1", [claim])[0]
    acb = kernel._action_service.submit(
        "p1",
        "model/mock",
        "generate",
        resource_claims=[claim],
    )
    kernel._action_service.admit(acb.action_id)
    kernel._action_service.mark_intent_durable(acb.action_id, [lease.lease_id])

    denied = await kernel._dispatcher.dispatch(
        CancelActionRequest(pid="p2", action_id=acb.action_id)
    )

    assert denied is not None
    assert denied.event_type == "ACTION_CANCEL_FAILED"
    assert denied.payload == {"action_id": acb.action_id, "reason": "not_found"}
    current = kernel._action_service.get_action(acb.action_id)
    assert current is not None
    assert current.state == ActionState.ADMITTED
    assert kernel._lease_service.get_lease(lease.lease_id) is not None

    allowed = await kernel._dispatcher.dispatch(
        CancelActionRequest(pid="p1", action_id=acb.action_id)
    )
    assert allowed is not None
    assert allowed.event_type == "ACTION_CANCELLED"
    current = kernel._action_service.get_action(acb.action_id)
    assert current is not None
    assert current.state == ActionState.CANCELLED
    assert kernel._lease_service.get_lease(lease.lease_id) is None


@pytest.mark.asyncio
async def test_cross_pid_release_is_all_or_nothing_and_owners_can_release(kernel) -> None:
    p1_lease = kernel._lease_service.atomic_acquire(
        "p1",
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )[0]
    p2_lease = kernel._lease_service.atomic_acquire(
        "p2",
        [{"resource_id": "resource:R2", "mode": "exclusive"}],
    )[0]

    denied = await kernel._dispatcher.dispatch(
        ReleaseResourceRequest(
            pid="p2",
            lease_ids=[p2_lease.lease_id, p1_lease.lease_id],
        )
    )

    assert denied is not None
    assert denied.event_type == "RESOURCE_RELEASE_FAILED"
    assert denied.payload == {"count": 0, "reason": "not_found"}
    assert kernel._lease_service.get_lease(p1_lease.lease_id) is not None
    assert kernel._lease_service.get_lease(p2_lease.lease_id) is not None

    p2_allowed = await kernel._dispatcher.dispatch(
        ReleaseResourceRequest(pid="p2", lease_ids=[p2_lease.lease_id])
    )
    assert p2_allowed is not None
    assert p2_allowed.event_type == "RESOURCE_RELEASED"
    assert p2_allowed.payload["count"] == 1
    assert kernel._lease_service.get_lease(p2_lease.lease_id) is None

    p1_allowed = await kernel._dispatcher.dispatch(
        ReleaseResourceRequest(pid="p1", lease_ids=[p1_lease.lease_id])
    )
    assert p1_allowed is not None
    assert p1_allowed.event_type == "RESOURCE_RELEASED"
    assert p1_allowed.payload["count"] == 1
    assert kernel._lease_service.get_lease(p1_lease.lease_id) is None


@pytest.mark.asyncio
async def test_cross_pid_restore_is_non_disclosing_but_owner_can_restore(kernel) -> None:
    victim = kernel._process_service.spawn(program_id="victim")
    attacker = kernel._process_service.spawn(program_id="attacker")
    created = await kernel._dispatcher.dispatch(CheckpointRequest(pid=victim.pid))
    assert created is not None
    checkpoint_id = created.payload["checkpoint_id"]

    denied = await kernel._dispatcher.dispatch(
        RestoreRequest(pid=attacker.pid, checkpoint_id=checkpoint_id)
    )

    assert denied is not None
    assert denied.event_type == "CHECKPOINT_RESTORE_FAILED"
    assert denied.pid == attacker.pid
    assert denied.payload == {"checkpoint_id": checkpoint_id, "reason": "not_found"}
    assert not {"pid", "journal_offset", "process_sequence"} & denied.payload.keys()

    allowed = await kernel._dispatcher.dispatch(
        RestoreRequest(pid=victim.pid, checkpoint_id=checkpoint_id)
    )
    assert allowed is not None
    assert allowed.event_type == "CHECKPOINT_RESTORED"
    assert allowed.pid == victim.pid
    assert allowed.payload["checkpoint_id"] == checkpoint_id
