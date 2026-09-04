"""Resource capabilities must gate Action admission before any side effect."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import Capability, SubmitActionRequest
from lhos.agent_os.sdk.client import create_kernel


async def _spawn_with_caps(capabilities: list[Capability]) -> tuple[object, str]:
    kernel = create_kernel(":memory:")
    pid = await kernel.spawn(type("Program", (), {"program_id": "capability-test"})())
    cap_set = kernel._capability_service.get_capability_set(pid)
    assert cap_set is not None
    cap_set.capabilities = capabilities
    kernel._capability_service._upsert_capability_set(cap_set)
    return kernel, pid


@pytest.mark.asyncio
async def test_submit_action_denies_undelegated_resource_without_side_effects() -> None:
    kernel, pid = await _spawn_with_caps(
        [
            Capability(
                resource_pattern="device:tool/mock",
                operations={"invoke"},
            )
        ]
    )
    request = SubmitActionRequest(
        pid=pid,
        device_type="tool/mock",
        operation="write",
        resource_claims=[
            {
                "resource_id": "resource:workspace/secret",
                "mode": "exclusive",
            }
        ],
    )

    with pytest.raises(CapabilityDenied):
        await kernel._dispatcher.dispatch(request)

    assert kernel._action_service.list_by_pid(pid) == []
    assert kernel._lease_service.list_all_leases() == []
    assert kernel._lease_service.list_waiters("resource:workspace/secret") == []


@pytest.mark.asyncio
async def test_submit_action_admits_fully_authorized_resource_bundle() -> None:
    kernel, pid = await _spawn_with_caps(
        [
            Capability(
                resource_pattern="device:tool/mock",
                operations={"invoke"},
            ),
            Capability(
                resource_pattern="resource:workspace/allowed",
                operations={"acquire"},
            ),
        ]
    )
    request = SubmitActionRequest(
        pid=pid,
        device_type="tool/mock",
        operation="write",
        resource_claims=[
            {
                "resource_id": "resource:workspace/allowed",
                "mode": "exclusive",
            }
        ],
    )

    event = await kernel._dispatcher.dispatch(request)

    actions = kernel._action_service.list_by_pid(pid)
    leases = kernel._lease_service.list_leases_for_pid(pid)
    assert event.event_type == "ACTION_READY_FOR_DISPATCH"
    assert len(actions) == 1
    assert len(leases) == 1
    assert leases[0].resource_id == "resource:workspace/allowed"
