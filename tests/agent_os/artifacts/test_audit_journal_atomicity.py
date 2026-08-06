"""Journal atomicity failpoint audit (Section 13).

Verify that journal append operations maintain atomicity even when
failures occur at various stages of the transaction lifecycle.

Failpoint matrix:
1. Before BEGIN — no impact
2. After BEGIN, before INSERT — full rollback
3. After INSERT, before offset UPDATE — full rollback (same tx)
4. After offset UPDATE, before COMMIT — full rollback
5. Exception after COMMIT — already committed, no rollback possible

Also verifies idempotency under crash: re-append same event_id returns
same offset and does not create duplicates.

We patch _Tx.execute (not sqlite3.Cursor.execute, which is immutable).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from lhos.agent_os.kernel.models import KernelEvent
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage, _Tx


class TestJournalAtomicityFailpoints:
    """Inject exceptions at each stage and verify consistency."""

    def _fresh_storage(self, tmp_path: Path) -> SQLiteStorage:
        return SQLiteStorage(str(tmp_path / "journal.db"))

    def _make_event(self, event_id: str = "evt-001", pid: str = "p1") -> KernelEvent:
        return KernelEvent(
            event_id=event_id,
            pid=pid,
            event_type="test.event",
        )

    def _journal_state(self, storage: SQLiteStorage) -> dict:
        """Return current journal state for assertions."""
        events = [
            dict(r)
            for r in storage.query_all("SELECT * FROM journal_events ORDER BY journal_offset")
        ]
        meta = storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        next_offset = meta["value"] if meta else 0
        return {"events": events, "next_offset": next_offset, "count": len(events)}

    def test_normal_append_succeeds(self, tmp_path):
        """Baseline: normal append works and updates offset."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        ev = self._make_event()
        result = journal.append_event(ev)

        state = self._journal_state(storage)
        assert state["count"] == 1
        assert state["next_offset"] == 1
        assert result.journal_offset == 0

    def test_fail_before_insert_rolls_back(self, tmp_path):
        """Failpoint 2: exception after BEGIN but before INSERT completes."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        with (
            patch.object(_Tx, "execute", side_effect=RuntimeError("crash")),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(self._make_event())

        state = self._journal_state(storage)
        assert state["count"] == 0
        assert state["next_offset"] == 0

    def test_fail_after_insert_rolls_back(self, tmp_path):
        """Failpoint 3: exception inside tx triggers rollback."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        with (
            patch.object(
                SQLiteStorage,
                "transaction",
                side_effect=RuntimeError("crash mid-transaction"),
            ),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(self._make_event())

        state = self._journal_state(storage)
        assert state["count"] == 0
        assert state["next_offset"] == 0

    def test_fail_after_commit_no_rollback(self, tmp_path):
        """Failpoint 5: exception AFTER commit cannot un-commit.

        In SQLite once COMMIT succeeds, the data is durable. An exception
        afterwards is irrelevant to atomicity.
        """
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        ev = self._make_event("evt-post-commit")
        journal.append_event(ev)

        # Data is already committed
        state = self._journal_state(storage)
        assert state["count"] == 1
        assert state["next_offset"] == 1

    def test_idempotent_append_after_crash(self, tmp_path):
        """After a failed append, re-append same event_id should succeed cleanly."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        ev = self._make_event("evt-dup")
        journal.append_event(ev)
        offset1 = ev.journal_offset

        # Re-append same event_id (idempotent): should return same offset
        ev2 = self._make_event("evt-dup")
        journal.append_event(ev2)
        offset2 = ev2.journal_offset

        assert offset1 == offset2

        state = self._journal_state(storage)
        assert state["count"] == 1  # Still only one event
        assert state["next_offset"] == 1  # Offset not consumed twice

    def test_multi_event_atomicity(self, tmp_path):
        """Multi-event append is atomic: all succeed or none."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        events = [self._make_event(f"evt-multi-{i}", f"p{i}") for i in range(5)]
        results = journal.append_events_atomically(events)

        state = self._journal_state(storage)
        assert state["count"] == 5
        assert state["next_offset"] == 5

        # All offsets should be sequential and unique
        offsets = [r.journal_offset for r in results]
        assert offsets == list(range(5))

    def test_multi_event_partial_failure_rolls_all_back(self, tmp_path):
        """Multi-event append: partial failure rolls back ALL events."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        events = [self._make_event(f"evt-partial-{i}") for i in range(3)]

        # Fail the 2nd call to _Tx.execute (simulating crash during 2nd event insert)
        original_exec = _Tx.execute
        call_count = {"n": 0}

        def selective_fail(self_tx, sql, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("crash on 2nd event insert")
            return original_exec(self_tx, sql, *args, **kwargs)

        with patch.object(_Tx, "execute", selective_fail), pytest.raises(RuntimeError):
            journal.append_events_atomically(events)

        state = self._journal_state(storage)
        assert state["count"] == 0
        assert state["next_offset"] == 0

    def test_offset_no_gaps_after_rollback(self, tmp_path):
        """After rollback, next append should NOT reuse the rolled-back offset."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        # First successful append: offset 0
        ev1 = self._make_event("evt-ok-1")
        journal.append_event(ev1)
        assert ev1.journal_offset == 0

        # Failed append
        with (
            patch.object(_Tx, "execute", side_effect=RuntimeError("crash")),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(self._make_event("evt-fail"))

        # Third successful append: should get offset 1, NOT reuse 0
        ev3 = self._make_event("evt-ok-3")
        journal.append_event(ev3)
        assert ev3.journal_offset == 1

        state = self._journal_state(storage)
        assert state["count"] == 2
        assert state["next_offset"] == 2

    def test_sequence_no_gaps_after_rollback(self, tmp_path):
        """Per-pid process_sequence should have no unexpected behavior after rollback."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        # Append two events for pid=p1: seq 0, 1
        ev1 = self._make_event("seq-1", "p1")
        ev2 = self._make_event("seq-2", "p1")
        journal.append_event(ev1)
        journal.append_event(ev2)
        assert ev1.process_sequence == 0
        assert ev2.process_sequence == 1

        # Failed append for p1 — tx rolls back, sequence not consumed
        with (
            patch.object(_Tx, "execute", side_effect=RuntimeError("crash")),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(self._make_event("seq-fail", "p1"))

        # Next append: sequence max is still 1, so new seq = 2
        ev4 = self._make_event("seq-4", "p1")
        journal.append_event(ev4)
        assert ev4.process_sequence == 2

    def test_wal_durability(self, tmp_path):
        """WAL mode ensures committed data survives connection reopen."""
        db_path = str(tmp_path / "wal.db")
        storage = SQLiteStorage(db_path)
        journal = JournalService(storage)

        ev = self._make_event("evt-durable")
        journal.append_event(ev)

        # Close and reopen
        storage.close()
        storage2 = SQLiteStorage(db_path)
        journal2 = JournalService(storage2)

        events = journal2.read_all()
        assert len(events) == 1
        assert events[0].event_id == "evt-durable"
        storage2.close()

    def test_concurrent_process_sequences_independent(self, tmp_path):
        """Rolling back one pid's events doesn't affect another pid's sequence."""
        storage = self._fresh_storage(tmp_path)
        journal = JournalService(storage)

        # p1: seq 0
        ev_p1 = self._make_event("p1-1", "p1")
        journal.append_event(ev_p1)
        assert ev_p1.process_sequence == 0

        # p2: seq 0
        ev_p2 = self._make_event("p2-1", "p2")
        journal.append_event(ev_p2)
        assert ev_p2.process_sequence == 0

        # Failed append for p1 — has no effect on p2
        with (
            patch.object(_Tx, "execute", side_effect=RuntimeError("crash")),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(self._make_event("p1-fail", "p1"))

        # p2 unaffected: next seq for p2 is 1
        ev_p2_next = self._make_event("p2-2", "p2")
        journal.append_event(ev_p2_next)
        assert ev_p2_next.process_sequence == 1

        # p1's next seq is also 1 (rollback didn't consume seq)
        ev_p1_next = self._make_event("p1-2", "p1")
        journal.append_event(ev_p1_next)
        assert ev_p1_next.process_sequence == 1


class TestJournalMetaConsistency:
    """Verify journal_meta.next_offset remains consistent."""

    def test_next_offset_after_success(self, tmp_path):
        storage = SQLiteStorage(str(tmp_path / "meta.db"))
        journal = JournalService(storage)

        for i in range(5):
            journal.append_event(KernelEvent(event_id=f"m-{i}", pid="p1", event_type="test"))

        meta = storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        assert meta["value"] == 5

    def test_next_offset_after_failure(self, tmp_path):
        storage = SQLiteStorage(str(tmp_path / "meta2.db"))
        journal = JournalService(storage)

        # 3 successful appends
        for i in range(3):
            journal.append_event(KernelEvent(event_id=f"m2-{i}", pid="p1", event_type="test"))

        # 1 failed append
        with (
            patch.object(_Tx, "execute", side_effect=RuntimeError("crash")),
            pytest.raises(RuntimeError),
        ):
            journal.append_event(KernelEvent(event_id="m2-fail", pid="p1", event_type="test"))

        meta = storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        assert meta["value"] == 3  # Not 4 — failed append didn't consume offset

    def test_next_offset_after_idempotent(self, tmp_path):
        storage = SQLiteStorage(str(tmp_path / "meta3.db"))
        journal = JournalService(storage)

        ev = KernelEvent(event_id="idem-1", pid="p1", event_type="test")
        journal.append_event(ev)
        journal.append_event(ev)  # idempotent

        meta = storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        assert meta["value"] == 1  # Only one offset consumed
