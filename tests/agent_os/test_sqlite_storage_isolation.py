from __future__ import annotations

import threading

from lhos.agent_os.storage.sqlite import SQLiteStorage


class _RollbackForTest(Exception):
    pass


def test_query_cannot_observe_another_threads_uncommitted_write(tmp_path):
    storage = SQLiteStorage(tmp_path / "isolation.sqlite")
    storage.execute("CREATE TABLE isolation_probe(value INTEGER NOT NULL)")
    storage.execute("INSERT INTO isolation_probe(value) VALUES (0)")

    updated = threading.Event()
    reader_started = threading.Event()
    reader_finished = threading.Event()
    allow_rollback = threading.Event()
    observed: list[int] = []

    def writer() -> None:
        try:
            with storage.transaction() as tx:
                tx.execute("UPDATE isolation_probe SET value = 777")
                updated.set()
                allow_rollback.wait(timeout=5)
                raise _RollbackForTest
        except _RollbackForTest:
            pass

    def reader() -> None:
        assert updated.wait(timeout=5)
        reader_started.set()
        row = storage.query_one("SELECT value FROM isolation_probe")
        observed.append(row["value"] if row is not None else -1)
        reader_finished.set()

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    reader_thread.start()
    assert reader_started.wait(timeout=5)
    assert not reader_finished.wait(timeout=0.2)

    allow_rollback.set()
    writer_thread.join(timeout=5)
    reader_thread.join(timeout=5)
    storage.close()

    assert not writer_thread.is_alive()
    assert not reader_thread.is_alive()
    assert observed == [0]
