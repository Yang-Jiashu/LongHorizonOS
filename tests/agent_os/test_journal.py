"""Test Journal Service — append, replay, projection rebuild."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import KernelEvent
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def storage() -> SQLiteStorage:
    return SQLiteStorage(":memory:")


@pytest.fixture
def journal(storage: SQLiteStorage) -> JournalService:
    return JournalService(storage)


class TestJournalAppend:
    def test_append_assigns_offset(self, journal: JournalService) -> None:
        ev = KernelEvent(pid="p1", event_type="TEST")
        result = journal.append_event(ev)
        assert result.journal_offset == 0

        ev2 = KernelEvent(pid="p1", event_type="TEST2")
        result2 = journal.append_event(ev2)
        assert result2.journal_offset == 1

    def test_offset_is_monotonic(self, journal: JournalService) -> None:
        offsets = []
        for i in range(100):
            ev = KernelEvent(pid="p1", event_type=f"EV_{i}")
            result = journal.append_event(ev)
            offsets.append(result.journal_offset)
        assert offsets == list(range(100))

    def test_per_pid_sequence_monotonic(self, journal: JournalService) -> None:
        for i in range(10):
            ev = KernelEvent(pid="p1", event_type=f"EV_{i}")
            journal.append_event(ev)
        for i in range(5):
            ev = KernelEvent(pid="p2", event_type=f"EV2_{i}")
            journal.append_event(ev)

        events = journal.read_all()
        p1_seqs = [e.process_sequence for e in events if e.pid == "p1"]
        p2_seqs = [e.process_sequence for e in events if e.pid == "p2"]
        assert p1_seqs == list(range(10))
        assert p2_seqs == list(range(5))

    def test_idempotent_append(self, journal: JournalService) -> None:
        ev = KernelEvent(pid="p1", event_type="TEST", payload={"x": 1})
        result1 = journal.append_event(ev)
        result2 = journal.append_event(ev)  # Same event_id
        assert result1.journal_offset == result2.journal_offset
        assert len(journal.read_all()) == 1

    def test_atomic_append_all_or_nothing(self, journal: JournalService) -> None:
        events = [KernelEvent(pid="p1", event_type=f"EV_{i}") for i in range(5)]
        results = journal.append_events_atomically(events)
        assert len(results) == 5
        assert len(journal.read_all()) == 5


class TestJournalReplay:
    def test_replay_returns_all_events(self, journal: JournalService) -> None:
        for i in range(50):
            journal.append_event(KernelEvent(pid="p1", event_type=f"EV_{i}"))
        events = journal.replay_all()
        assert len(events) == 50

    def test_read_from_offset(self, journal: JournalService) -> None:
        for i in range(20):
            journal.append_event(KernelEvent(pid="p1", event_type=f"EV_{i}"))
        events = journal.read_from_offset(10)
        assert len(events) == 10
        assert events[0].journal_offset == 10


class TestProjectionRebuild:
    def test_rebuild_projections(self, storage: SQLiteStorage, journal: JournalService) -> None:
        # Append some events
        for i in range(10):
            journal.append_event(KernelEvent(pid="p1", event_type=f"EV_{i}"))

        # Add a handler that tracks replayed events
        class Handler:
            def __init__(self) -> None:
                self.events: list[KernelEvent] = []

            def handle_event(self, ev: KernelEvent) -> None:
                self.events.append(ev)

        handler = Handler()
        count = journal.rebuild_projections([handler])
        assert count == 10
        assert len(handler.events) == 10

    def test_rebuild_is_deterministic(
        self, storage: SQLiteStorage, journal: JournalService
    ) -> None:
        for i in range(30):
            journal.append_event(KernelEvent(pid="p1", event_type=f"EV_{i}"))

        class Handler1:
            def __init__(self) -> None:
                self.events: list[str] = []

            def handle_event(self, ev: KernelEvent) -> None:
                self.events.append(ev.event_type)

        h1 = Handler1()
        journal.rebuild_projections([h1])

        h2 = Handler1()
        journal.rebuild_projections([h2])
        assert h1.events == h2.events
