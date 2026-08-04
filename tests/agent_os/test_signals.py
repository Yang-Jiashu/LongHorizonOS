"""Test Signal Service."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import Clock, ProcessState
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def storage() -> SQLiteStorage:
    return SQLiteStorage(":memory:")


@pytest.fixture
def journal(storage: SQLiteStorage) -> JournalService:
    return JournalService(storage)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def process_service(
    storage: SQLiteStorage, journal: JournalService, clock: Clock
) -> ProcessService:
    return ProcessService(storage, journal, clock)


@pytest.fixture
def signal_service(
    storage: SQLiteStorage, journal: JournalService, process_service: ProcessService
) -> SignalService:
    return SignalService(storage, journal, process_service)


class TestSignalSend:
    def test_send_creates_signal(self, signal_service: SignalService) -> None:
        sig = signal_service.send("p1", "ACTION_COMPLETED", source_pid="kernel")
        assert sig.target_pid == "p1"
        assert sig.signal_type == "ACTION_COMPLETED"
        assert not sig.consumed

    def test_send_is_durable(self, signal_service: SignalService, journal: JournalService) -> None:
        signal_service.send("p1", "ACTION_COMPLETED")
        events = journal.read_all()
        sent_events = [e for e in events if e.event_type == "SIGNAL_SENT"]
        assert len(sent_events) == 1


class TestSignalDelivery:
    def test_blocked_process_woken_by_matching_signal(
        self, signal_service: SignalService, process_service: ProcessService
    ) -> None:
        pcb = process_service.spawn("test_prog")
        process_service.transition(pcb.pid, ProcessState.RUNNING)
        process_service.transition(
            pcb.pid,
            ProcessState.BLOCKED,
            wait_condition={"signal_type": "ACTION_COMPLETED"},
        )

        signal_service.send(pcb.pid, "ACTION_COMPLETED")
        delivered = signal_service.deliver_pending()
        assert delivered == 1

        updated = process_service.get_process(pcb.pid)
        assert updated is not None
        assert updated.state == ProcessState.READY

    def test_non_matching_signal_does_not_wake(
        self, signal_service: SignalService, process_service: ProcessService
    ) -> None:
        pcb = process_service.spawn("test_prog")
        process_service.transition(pcb.pid, ProcessState.RUNNING)
        process_service.transition(
            pcb.pid,
            ProcessState.BLOCKED,
            wait_condition={"signal_type": "ACTION_COMPLETED"},
        )

        # Send a non-matching signal
        signal_service.send(pcb.pid, "LEASE_AVAILABLE")
        delivered = signal_service.deliver_pending()
        assert delivered == 0

        updated = process_service.get_process(pcb.pid)
        assert updated is not None
        assert updated.state == ProcessState.BLOCKED

    def test_consumed_signal_not_redelivered(
        self, signal_service: SignalService, process_service: ProcessService
    ) -> None:
        pcb = process_service.spawn("test_prog")
        process_service.transition(pcb.pid, ProcessState.RUNNING)
        process_service.transition(
            pcb.pid,
            ProcessState.BLOCKED,
            wait_condition={"signal_type": "ACTION_COMPLETED"},
        )

        signal_service.send(pcb.pid, "ACTION_COMPLETED")
        signal_service.deliver_pending()

        # Block again
        process_service.transition(pcb.pid, ProcessState.RUNNING)
        process_service.transition(
            pcb.pid,
            ProcessState.BLOCKED,
            wait_condition={"signal_type": "ACTION_COMPLETED"},
        )

        # Deliver again — should not redeliver the old signal
        delivered = signal_service.deliver_pending()
        assert delivered == 0

    def test_action_id_matching(
        self, signal_service: SignalService, process_service: ProcessService
    ) -> None:
        pcb = process_service.spawn("test_prog")
        process_service.transition(pcb.pid, ProcessState.RUNNING)
        process_service.transition(
            pcb.pid,
            ProcessState.BLOCKED,
            wait_condition={"signal_type": "ACTION_COMPLETED", "action_id": "a1"},
        )

        # Wrong action_id
        signal_service.send(pcb.pid, "ACTION_COMPLETED", payload={"action_id": "a2"})
        assert signal_service.deliver_pending() == 0

        # Correct action_id
        signal_service.send(pcb.pid, "ACTION_COMPLETED", payload={"action_id": "a1"})
        assert signal_service.deliver_pending() == 1
