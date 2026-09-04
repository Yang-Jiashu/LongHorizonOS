"""Test Namespace and Capability Isolation."""

from __future__ import annotations

import contextlib

import pytest

from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.services.capability_service import DEFAULT_CAPABILITIES, CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def storage() -> SQLiteStorage:
    return SQLiteStorage(":memory:")


@pytest.fixture
def journal(storage: SQLiteStorage) -> JournalService:
    return JournalService(storage)


@pytest.fixture
def cap_service(storage: SQLiteStorage, journal: JournalService) -> CapabilityService:
    return CapabilityService(storage, journal)


class TestWorkspaceIsolation:
    def test_p1_cannot_acquire_p2_workspace(self, cap_service: CapabilityService) -> None:
        # P1 has capabilities only for workspace:p1
        p1_caps = [
            Capability(resource_pattern="resource:workspace/p1", operations={"acquire"}),
            Capability(resource_pattern="device:model/mock", operations={"invoke"}),
        ]
        cap_service.create_capability_set("p1", p1_caps)

        # P1 tries to access workspace:p2
        with pytest.raises(CapabilityDenied):
            cap_service.enforce("p1", "resource:workspace/p2", "acquire")

        # Journal should have CAPABILITY_DENIED
        events = [e for e in cap_service._journal.read_all() if e.event_type == "CAPABILITY_DENIED"]
        assert len(events) == 1

    def test_p1_can_access_own_workspace(self, cap_service: CapabilityService) -> None:
        p1_caps = [
            Capability(resource_pattern="resource:workspace/p1", operations={"acquire"}),
        ]
        cap_service.create_capability_set("p1", p1_caps)
        # Should not raise
        cap_service.enforce("p1", "resource:workspace/p1", "acquire")


class TestSignalIsolation:
    def test_unauthorized_signal_denied(self, cap_service: CapabilityService) -> None:
        # P1 has no signal capability
        cap_service.create_capability_set(
            "p1",
            [
                Capability(resource_pattern="device:model/mock", operations={"invoke"}),
            ],
        )

        with pytest.raises(CapabilityDenied):
            cap_service.enforce("p1", "process:signal/p2", "send")

    def test_authorized_signal_allowed(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set(
            "p1",
            [
                Capability(resource_pattern="process:signal/*", operations={"send"}),
            ],
        )
        # Should not raise
        cap_service.enforce("p1", "process:signal/p2", "send")


class TestDeviceActionIsolation:
    def test_unauthorized_device_denied(self, cap_service: CapabilityService) -> None:
        # P1 has no device capability
        cap_service.create_capability_set("p1", [])

        with pytest.raises(CapabilityDenied):
            cap_service.enforce("p1", "device:tool/mock", "invoke")

    def test_authorized_device_allowed(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", DEFAULT_CAPABILITIES["full"])
        cap_service.enforce("p1", "device:tool/mock", "invoke")


class TestAllDenialsHaveJournalEvents:
    """Every capability denial must produce a CAPABILITY_DENIED journal event."""

    def test_denial_event_count_matches(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", [])

        for _ in range(3):
            with contextlib.suppress(CapabilityDenied):
                cap_service.enforce("p1", "device:model/mock", "invoke")

        events = [e for e in cap_service._journal.read_all() if e.event_type == "CAPABILITY_DENIED"]
        assert len(events) == 3
