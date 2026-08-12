"""Test Lease Service — atomic acquire, release, expiry."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

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
        first = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "shared"}]
        )[0]
        second = lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R1", "mode": "shared"}]
        )[0]
        leases = lease_service.list_active_leases_for_resource("resource:R1")
        assert len(leases) == 2
        # Concurrent readers form one ownership cohort. A later shared reader
        # must not fence an earlier reader whose lease is still live.
        assert first.fencing_token == second.fencing_token

    def test_new_cohort_advances_fencing_token(self, lease_service: LeaseService) -> None:
        first = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "shared"}]
        )[0]
        lease_service.release([first.lease_id])

        second = lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R1", "mode": "shared"}]
        )[0]

        assert second.fencing_token == first.fencing_token + 1

    def test_exclusive_owner_advances_after_shared_cohort(
        self, lease_service: LeaseService
    ) -> None:
        shared = lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "shared"}]
        )[0]
        lease_service.release([shared.lease_id])

        exclusive = lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )[0]

        assert exclusive.fencing_token == shared.fencing_token + 1

    def test_shared_blocks_exclusive(self, lease_service: LeaseService) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "shared"}])
        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p2", [{"resource_id": "resource:R1", "mode": "exclusive"}]
            )

    def test_conflict_records_waiter_and_event(
        self,
        lease_service: LeaseService,
        storage: SQLiteStorage,
        journal: JournalService,
    ) -> None:
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])

        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p2", [{"resource_id": "resource:R1", "mode": "exclusive"}]
            )

        waiter = storage.query_one(
            "SELECT pid, resource_id FROM lease_waiters WHERE pid = ? AND resource_id = ?",
            ("p2", "resource:R1"),
        )
        assert waiter == {"pid": "p2", "resource_id": "resource:R1"}

        failures = [
            event
            for event in journal.read_all()
            if event.pid == "p2" and event.event_type == "LEASE_ACQUIRE_FAILED"
        ]
        assert len(failures) == 1
        assert failures[0].payload["resource_id"] == "resource:R1"


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

    def test_renew_does_not_revive_expired_lease(self, lease_service: LeaseService) -> None:
        leases = lease_service.atomic_acquire(
            "p1",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(seconds=-1),
        )
        lease_id = leases[0].lease_id

        assert lease_service.renew(lease_id, ttl=timedelta(minutes=5)) is None
        remaining = lease_service.get_lease(lease_id)
        assert remaining is not None
        assert remaining.expires_at == leases[0].expires_at


class TestExpiryConcurrency:
    @staticmethod
    def _open_services(
        db_path: str,
    ) -> tuple[
        SQLiteStorage,
        JournalService,
        LeaseService,
        SQLiteStorage,
        JournalService,
        LeaseService,
    ]:
        renew_storage = SQLiteStorage(db_path)
        renew_journal = JournalService(renew_storage)
        renew_service = LeaseService(renew_storage, renew_journal)
        reclaim_storage = SQLiteStorage(db_path)
        reclaim_journal = JournalService(reclaim_storage)
        reclaim_service = LeaseService(reclaim_storage, reclaim_journal)
        return (
            renew_storage,
            renew_journal,
            renew_service,
            reclaim_storage,
            reclaim_journal,
            reclaim_service,
        )

    def test_renew_first_is_not_reclaimed(self, tmp_path) -> None:
        (
            renew_storage,
            renew_journal,
            renew_service,
            reclaim_storage,
            _reclaim_journal,
            reclaim_service,
        ) = self._open_services(str(tmp_path / "renew-first.db"))
        try:
            lease = renew_service.atomic_acquire(
                "owner",
                [{"resource_id": "resource:R1", "mode": "exclusive"}],
                ttl=timedelta(seconds=60),
            )[0]
            # Use a deliberately stale/future caller cutoff: the lease is live
            # when renew linearizes, but the old implementation had already
            # selected it for deletion before taking the writer lock.
            cutoff = lease.expires_at + timedelta(seconds=1)
            entered_renew = threading.Event()
            allow_renew = threading.Event()
            reclaim_started = threading.Event()
            reclaim_done = threading.Event()
            renew_result: list = []
            reclaim_result: list = []
            errors: list[BaseException] = []

            def pause_renew() -> int:
                entered_renew.set()
                if not allow_renew.wait(5):
                    raise RuntimeError("renew did not receive release permission")
                return 0

            renew_storage.conn.create_function("pause_renew", 0, pause_renew)
            renew_storage.conn.execute(
                "CREATE TRIGGER pause_renew_trigger "
                "BEFORE UPDATE OF expires_at ON leases_projection "
                "BEGIN SELECT pause_renew(); END"
            )

            def renew() -> None:
                try:
                    renew_result.append(renew_service.renew(lease.lease_id, timedelta(minutes=5)))
                except BaseException as exc:
                    errors.append(exc)

            def reclaim() -> None:
                reclaim_started.set()
                try:
                    reclaim_result.append(reclaim_service.reclaim_expired(cutoff))
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    reclaim_done.set()

            renew_thread = threading.Thread(target=renew)
            reclaim_thread = threading.Thread(target=reclaim)
            renew_thread.start()
            assert entered_renew.wait(5)
            reclaim_thread.start()
            assert reclaim_started.wait(5)
            assert not reclaim_done.wait(0.1)
            allow_renew.set()
            renew_thread.join(5)
            reclaim_thread.join(5)

            assert not renew_thread.is_alive()
            assert not reclaim_thread.is_alive()
            assert errors == []
            assert renew_result[0] is not None
            assert reclaim_result == [0]
            assert reclaim_service.get_lease(lease.lease_id) is not None
            lease_events = [
                event
                for event in renew_journal.read_all()
                if event.payload.get("lease_id") == lease.lease_id
            ]
            assert [event.event_type for event in lease_events].count("LEASE_RENEWED") == 1
            assert all(
                event.event_type not in {"LEASE_RELEASED", "LEASE_EXPIRED"}
                for event in lease_events
            )
        finally:
            allow_renew.set()
            renew_storage.close()
            reclaim_storage.close()

    def test_reclaim_first_wins_and_later_renew_returns_none(self, tmp_path) -> None:
        (
            renew_storage,
            _renew_journal,
            renew_service,
            reclaim_storage,
            reclaim_journal,
            reclaim_service,
        ) = self._open_services(str(tmp_path / "reclaim-first.db"))
        try:
            lease = renew_service.atomic_acquire(
                "owner",
                [{"resource_id": "resource:R1", "mode": "exclusive"}],
                ttl=timedelta(seconds=-1),
            )[0]
            with pytest.raises(LeaseAcquisitionFailed):
                renew_service.atomic_acquire(
                    "waiter",
                    [{"resource_id": "resource:R1", "mode": "exclusive"}],
                )
            assert reclaim_service.list_waiters("resource:R1") == ["waiter"]

            entered_reclaim = threading.Event()
            allow_reclaim = threading.Event()
            renew_started = threading.Event()
            renew_done = threading.Event()
            reclaim_result: list = []
            renew_result: list = []
            errors: list[BaseException] = []

            def pause_reclaim() -> int:
                entered_reclaim.set()
                if not allow_reclaim.wait(5):
                    raise RuntimeError("reclaim did not receive release permission")
                return 0

            reclaim_storage.conn.create_function("pause_reclaim", 0, pause_reclaim)
            reclaim_storage.conn.execute(
                "CREATE TRIGGER pause_reclaim_trigger "
                "BEFORE DELETE ON leases_projection "
                "BEGIN SELECT pause_reclaim(); END"
            )

            def reclaim() -> None:
                try:
                    reclaim_result.append(reclaim_service.reclaim_expired(datetime.now(UTC)))
                except BaseException as exc:
                    errors.append(exc)

            def renew() -> None:
                renew_started.set()
                try:
                    renew_result.append(renew_service.renew(lease.lease_id, timedelta(minutes=5)))
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    renew_done.set()

            reclaim_thread = threading.Thread(target=reclaim)
            renew_thread = threading.Thread(target=renew)
            reclaim_thread.start()
            assert entered_reclaim.wait(5)
            renew_thread.start()
            assert renew_started.wait(5)
            assert not renew_done.wait(0.1)
            allow_reclaim.set()
            reclaim_thread.join(5)
            renew_thread.join(5)

            assert not reclaim_thread.is_alive()
            assert not renew_thread.is_alive()
            assert errors == []
            assert reclaim_result == [1]
            assert renew_result == [None]
            assert reclaim_service.get_lease(lease.lease_id) is None
            # Reclaiming the former owner must not erase another process's wait
            # edge; a later successful retry owns that cleanup.
            assert reclaim_service.list_waiters("resource:R1") == ["waiter"]
            lease_events = [
                event
                for event in reclaim_journal.read_all()
                if event.payload.get("lease_id") == lease.lease_id
            ]
            assert [event.event_type for event in lease_events].count("LEASE_RELEASED") == 1
            assert [event.event_type for event in lease_events].count("LEASE_EXPIRED") == 1
        finally:
            allow_reclaim.set()
            renew_storage.close()
            reclaim_storage.close()


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
