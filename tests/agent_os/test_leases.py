"""Test Lease Service — atomic acquire, release, expiry."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def storage() -> SQLiteStorage:
    return SQLiteStorage(":memory:")


@pytest.fixture
def journal(storage: SQLiteStorage) -> JournalService:
    return JournalService(storage)


@pytest.fixture
def lease_service(storage: SQLiteStorage, journal: JournalService) -> LeaseService:
    return LeaseService(storage, journal)


class TestAtomicAcquire:
    def test_single_acquire(self, lease_service: LeaseService) -> None:
        leases = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        assert len(leases) == 1
        assert leases[0].resource_id == "resource:R1"
        assert leases[0].owner_pid == "p1"

    def test_multi_acquire_atomic(self, lease_service: LeaseService) -> None:
        leases = lease_service.atomic_acquire(
            "p1",
            [
                {"resource_id": "resource:R1", "mode": "exclusive"},
                {"resource_id": "resource:R2", "mode": "exclusive"},
            ],
        )
        assert len(leases) == 2

    def test_atomic_rollback_on_conflict(self, lease_service: LeaseService) -> None:
        # p1 acquires R1
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])

        # p2 tries to acquire R1 and R2 atomically — should fail entirely
        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p2",
                [
                    {"resource_id": "resource:R1", "mode": "exclusive"},  # held by p1
                    {"resource_id": "resource:R2", "mode": "exclusive"},
                ],
            )

        # R2 should NOT be held by p2 (rollback)
        r2_leases = lease_service.list_active_leases_for_resource("resource:R2")
        assert len(r2_leases) == 0

    def test_exclusive_blocks_exclusive(self, lease_service: LeaseService) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])
        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p2", [{"resource_id": "resource:R1", "mode": "exclusive"}]
            )

    def test_shared_allows_shared(self, lease_service: LeaseService) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "shared"}])
        lease_service.atomic_acquire("p2", [{"resource_id": "resource:R1", "mode": "shared"}])
        leases = lease_service.list_active_leases_for_resource("resource:R1")
        assert len(leases) == 2

    def test_shared_blocks_exclusive(self, lease_service: LeaseService) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "shared"}])
        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p2", [{"resource_id": "resource:R1", "mode": "exclusive"}]
            )


class TestRelease:
    def test_release_by_id(self, lease_service: LeaseService) -> None:
        leases = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        count = lease_service.release([leases[0].lease_id])
        assert count == 1
        assert len(lease_service.list_active_leases_for_resource("resource:R1")) == 0

    def test_release_all_for_pid(self, lease_service: LeaseService) -> None:
        lease_service.atomic_acquire(
            "p1",
            [
                {"resource_id": "resource:R1", "mode": "exclusive"},
                {"resource_id": "resource:R2", "mode": "exclusive"},
            ],
        )
        count = lease_service.release_all_for_pid("p1")
        assert count == 2
        assert len(lease_service.list_leases_for_pid("p1")) == 0


class TestExpiry:
    def test_reclaim_expired(self, lease_service: LeaseService) -> None:
        # Acquire with very short TTL
        lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(microseconds=1),
        )
        # Wait a tiny bit
        import time

        time.sleep(0.01)

        reclaimed = lease_service.reclaim_expired(datetime.utcnow())
        assert reclaimed == 1
        assert len(lease_service.list_leases_for_pid("p1")) == 0

    def test_renew_extends_lease(self, lease_service: LeaseService) -> None:
        leases = lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(seconds=1),
        )
        renewed = lease_service.renew(leases[0].lease_id, ttl=timedelta(seconds=60))
        assert renewed is not None
        assert renewed.expires_at > leases[0].expires_at


class TestLeaseJournalEvents:
    def test_acquire_produces_event(
        self, lease_service: LeaseService, journal: JournalService
    ) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])
        events = journal.read_all()
        acquire_events = [e for e in events if e.event_type == "LEASE_ACQUIRED"]
        assert len(acquire_events) == 1

    def test_release_produces_event(
        self, lease_service: LeaseService, journal: JournalService
    ) -> None:
        leases = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        lease_service.release([leases[0].lease_id])
        events = journal.read_all()
        release_events = [e for e in events if e.event_type == "LEASE_RELEASED"]
        assert len(release_events) == 1
