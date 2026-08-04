"""Test Capability Service."""

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


class TestCapabilityCheck:
    def test_grant_and_check(self, cap_service: CapabilityService) -> None:
        cap = Capability(
            resource_pattern="device:model/mock",
            operations={"invoke"},
        )
        cap_service.create_capability_set("p1", [cap])
        assert cap_service.check("p1", "device:model/mock", "invoke")
        assert not cap_service.check("p1", "device:model/mock", "delete")
        assert not cap_service.check("p1", "device:other", "invoke")

    def test_wildcard_pattern(self, cap_service: CapabilityService) -> None:
        cap = Capability(
            resource_pattern="resource:workspace/*",
            operations={"acquire"},
        )
        cap_service.create_capability_set("p1", [cap])
        assert cap_service.check("p1", "resource:workspace/p1", "acquire")
        assert cap_service.check("p1", "resource:workspace/p2", "acquire")
        assert not cap_service.check("p1", "resource:other", "acquire")


class TestCapabilityEnforce:
    def test_enforce_passes_when_capable(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", DEFAULT_CAPABILITIES["full"])
        # Should not raise
        cap_service.enforce("p1", "device:model/mock", "invoke")

    def test_enforce_raises_when_denied(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", [])
        with pytest.raises(CapabilityDenied):
            cap_service.enforce("p1", "device:model/mock", "invoke")

    def test_denial_produces_journal_event(
        self, storage: SQLiteStorage, cap_service: CapabilityService, journal: JournalService
    ) -> None:
        cap_service.create_capability_set("p1", [])
        with contextlib.suppress(CapabilityDenied):
            cap_service.enforce("p1", "device:model/mock", "invoke")

        events = journal.read_all()
        denial_events = [e for e in events if e.event_type == "CAPABILITY_DENIED"]
        assert len(denial_events) == 1
        assert denial_events[0].payload["resource"] == "device:model/mock"


class TestNamespaceIsolation:
    def test_cross_namespace_denied_by_default(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", DEFAULT_CAPABILITIES["full"])
        # P1 in namespace ns1 cannot access resources in ns2
        assert not cap_service.check_namespace_isolation("p1", "ns1", "workspace:p2", "ns2")

    def test_same_namespace_allowed(self, cap_service: CapabilityService) -> None:
        cap_service.create_capability_set("p1", DEFAULT_CAPABILITIES["full"])
        assert cap_service.check_namespace_isolation("p1", "ns1", "workspace:p1", "ns1")


class TestChildSubset:
    def test_child_must_be_subset(self, cap_service: CapabilityService) -> None:
        parent_caps = DEFAULT_CAPABILITIES["full"]
        cap_service.create_capability_set("parent", parent_caps)

        child_caps = [Capability(resource_pattern="device:model/mock", operations={"invoke"})]
        cap_service.create_capability_set("child", child_caps)
        assert cap_service.verify_child_subset("parent", "child")

    def test_child_not_subset_rejected(self, cap_service: CapabilityService) -> None:
        parent_caps = [Capability(resource_pattern="device:model/mock", operations={"invoke"})]
        cap_service.create_capability_set("parent", parent_caps)

        child_caps = DEFAULT_CAPABILITIES["full"]  # More than parent
        cap_service.create_capability_set("child", child_caps)
        assert not cap_service.verify_child_subset("parent", "child")
