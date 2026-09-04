"""Lease renewal durability/atomicity checks."""

from __future__ import annotations

import threading
import time
from datetime import timedelta

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


def test_renew_cannot_revive_lease_that_expires_while_waiting_for_writer(
    tmp_path,
) -> None:
    """The expiry comparison must use a clock sampled after writer admission."""
    db_path = str(tmp_path / "renew-after-writer-wait.sqlite")
    owner_storage = SQLiteStorage(db_path)
    blocker_storage = SQLiteStorage(db_path)
    owner_journal = JournalService(owner_storage)
    service = LeaseService(owner_storage, owner_journal)
    try:
        lease = service.atomic_acquire(
            "worker",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(milliseconds=250),
        )[0]
        original_expiry = lease.expires_at
        blocker_started = threading.Event()

        def hold_writer_past_expiry() -> None:
            with blocker_storage.transaction(immediate=True):
                blocker_started.set()
                time.sleep(0.55)

        holder = threading.Thread(target=hold_writer_past_expiry)
        holder.start()
        assert blocker_started.wait(timeout=2)

        renewed = service.renew(lease.lease_id, ttl=timedelta(minutes=1))
        holder.join(timeout=2)
        assert not holder.is_alive()

        assert renewed is None
        persisted = service.get_lease(lease.lease_id)
        assert persisted is not None
        assert persisted.expires_at == original_expiry
        assert not any(
            event.event_type == "LEASE_RENEWED"
            for event in owner_journal.read_all()
            if event.payload.get("lease_id") == lease.lease_id
        )
    finally:
        owner_storage.close()
        blocker_storage.close()


def test_renew_rolls_back_projection_when_journal_append_fails() -> None:
    """A failed LEASE_RENEWED append must not leave a phantom expiry update."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    service = LeaseService(storage, journal)
    try:
        lease = service.atomic_acquire(
            "worker",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(minutes=1),
        )[0]
        before = service.get_lease(lease.lease_id)
        assert before is not None

        def fail_append(*args, **kwargs):
            raise RuntimeError("simulated journal failure")

        journal.append_events_tx = fail_append  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="simulated journal failure"):
            service.renew(lease.lease_id, ttl=timedelta(minutes=5))

        after = service.get_lease(lease.lease_id)
        assert after is not None
        assert after.expires_at == before.expires_at
        assert not any(
            event.event_type == "LEASE_RENEWED"
            for event in journal.read_all()
            if event.payload.get("lease_id") == lease.lease_id
        )
    finally:
        storage.close()


def test_release_rolls_back_projection_when_journal_append_fails() -> None:
    """A failed LEASE_RELEASED append must not lose the live lease row."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    service = LeaseService(storage, journal)
    try:
        lease = service.atomic_acquire(
            "worker",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
            ttl=timedelta(minutes=1),
        )[0]

        def fail_append(*args, **kwargs):
            raise RuntimeError("simulated journal failure")

        journal.append_events_tx = fail_append  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="simulated journal failure"):
            service.release([lease.lease_id])

        assert service.get_lease(lease.lease_id) is not None
    finally:
        storage.close()


def test_release_all_for_pid_rolls_back_leases_and_waiters_on_journal_failure() -> None:
    """PID cleanup is all-or-nothing, including wait-for edges."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    service = LeaseService(storage, journal)
    try:
        owned = service.atomic_acquire(
            "worker",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )[0]
        service.atomic_acquire(
            "holder",
            [{"resource_id": "resource:R2", "mode": "exclusive"}],
        )
        with pytest.raises(LeaseAcquisitionFailed):
            service.atomic_acquire(
                "worker",
                [{"resource_id": "resource:R2", "mode": "exclusive"}],
            )
        assert service.list_waiters("resource:R2") == ["worker"]

        def fail_append(*args, **kwargs):
            raise RuntimeError("simulated journal failure")

        journal.append_events_tx = fail_append  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="simulated journal failure"):
            service.release_all_for_pid("worker")

        # The lease and waiter edge must both survive the rolled-back cleanup.
        assert service.get_lease(owned.lease_id) is not None
        assert service.list_waiters("resource:R2") == ["worker"]
    finally:
        storage.close()


def test_release_all_for_pid_uses_one_atomic_transaction(monkeypatch) -> None:
    """A PID cleanup must not split SELECT, DELETE, and journal into TOCTOU txns."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    service = LeaseService(storage, journal)
    try:
        lease = service.atomic_acquire(
            "worker",
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )[0]

        original_transaction = storage.transaction
        calls = {"count": 0}

        def at_most_one_transaction(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] > 1:
                raise RuntimeError("cleanup opened a second transaction")
            return original_transaction(*args, **kwargs)

        monkeypatch.setattr(storage, "transaction", at_most_one_transaction)
        assert service.release_all_for_pid("worker") == 1
        assert calls["count"] == 1
        assert service.get_lease(lease.lease_id) is None
    finally:
        storage.close()
