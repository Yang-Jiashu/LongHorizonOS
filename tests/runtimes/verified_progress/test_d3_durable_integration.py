"""Durable D3 integration and concurrency invariants.

These tests intentionally exercise the public ``VerifiedProgressRuntime``
surface with a file-backed SQLite store.  D3 is an audit projection, but its
record must be committed in the same transaction as the graph refresh and
must never overwrite an earlier result.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError


def _d3_record(
    graph_id: str,
    base_version: int,
    *,
    record_id: str | None = None,
    result_hash: str | None = None,
    marker: str = "repair",
) -> dict[str, object]:
    """Build the smallest valid durable D3 envelope for a refresh commit."""

    return {
        "record_id": record_id or f"d3:{graph_id}:v{base_version}:{marker}",
        "graph_id": graph_id,
        "base_graph_version": base_version,
        "committed_graph_version": base_version + 1,
        "result_hash": result_hash or (f"{marker}-{base_version}").ljust(64, "0")[:64],
        "stale_nodes": [],
        "preserved_nodes": [],
        "frontier": [],
    }


def _refresh(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
    *,
    expected_version: int,
    d3_record: dict[str, object],
    key: str,
):
    return runtime.refresh_derived_state(
        graph_id,
        author_pid="system",
        idempotency_key=key,
        expected_graph_version=expected_version,
        d3_record=d3_record,
    )


def test_refresh_persists_d3_and_public_runtime_query(tmp_path: Path):
    db_path = str(tmp_path / "d3.sqlite")
    runtime = VerifiedProgressRuntime(db_path)
    try:
        graph_id = runtime.create_graph(owner_pid="p1").graph_id
        record = _d3_record(graph_id, 0)

        result = _refresh(
            runtime,
            graph_id,
            expected_version=0,
            d3_record=record,
            key="refresh-v0",
        )

        assert result.committed_graph_version == 1
        rows = runtime.get_d3_results(graph_id)
        assert len(rows) == 1
        assert rows[0]["record_id"] == record["record_id"]
        assert rows[0]["graph_id"] == graph_id
        assert rows[0]["base_graph_version"] == 0
        assert rows[0]["committed_graph_version"] == 1
        assert rows[0]["result_hash"] == record["result_hash"]
        assert runtime.get_d3_results(graph_id, since_version=1) == rows
        assert runtime.get_d3_results(graph_id, since_version=2) == []
    finally:
        runtime.close()


def test_d3_rows_survive_sqlite_reopen_with_canonical_payload(tmp_path: Path):
    db_path = str(tmp_path / "d3-reopen.sqlite")
    first = VerifiedProgressRuntime(db_path)
    graph_id = first.create_graph(owner_pid="p1").graph_id
    record = _d3_record(graph_id, 0, marker="reopen")
    _refresh(
        first,
        graph_id,
        expected_version=0,
        d3_record=record,
        key="reopen-v0",
    )
    before = first.get_d3_results(graph_id)
    version_before = first.get_graph(graph_id).current_version
    first.close()

    second = VerifiedProgressRuntime(db_path)
    try:
        assert second.get_graph(graph_id).current_version == version_before == 1
        assert second.get_d3_results(graph_id) == before
        # A second runtime must also be able to append a later D3 record.
        later = _d3_record(graph_id, 1, marker="reopen-v1")
        _refresh(
            second,
            graph_id,
            expected_version=1,
            d3_record=later,
            key="reopen-v1",
        )
        assert [row["committed_graph_version"] for row in second.get_d3_results(graph_id)] == [
            1,
            2,
        ]
    finally:
        second.close()


def test_d3_insert_failure_rolls_back_every_graph_side_effect(tmp_path: Path):
    db_path = str(tmp_path / "d3-rollback.sqlite")
    runtime = VerifiedProgressRuntime(db_path)
    try:
        graph_id = runtime.create_graph(owner_pid="p1").graph_id
        # The trigger fires after graph_versions/patches are staged, proving
        # that _immediate_transaction rolls back the entire commit, not just
        # the D3 row.
        runtime.store.conn.executescript(
            """
            CREATE TRIGGER fail_d3_insert
            BEFORE INSERT ON d3_invalidation_results
            BEGIN
                SELECT RAISE(ABORT, 'forced d3 insert failure');
            END;
            """
        )
        baseline_counts = {
            table: runtime.store.conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            for table in (
                "graph_idempotency",
                "graph_patches",
                "graph_versions",
                "graph_events",
                "d3_invalidation_results",
                "graph_projection_snapshots",
            )
        }
        with pytest.raises(sqlite3.IntegrityError, match="forced d3 insert failure"):
            _refresh(
                runtime,
                graph_id,
                expected_version=0,
                d3_record=_d3_record(graph_id, 0, marker="rollback"),
                key="rollback-v0",
            )

        assert runtime.get_graph(graph_id).current_version == 0
        for table in (
            "graph_idempotency",
            "graph_patches",
            "graph_versions",
            "graph_events",
            "d3_invalidation_results",
            "graph_projection_snapshots",
        ):
            count = runtime.store.conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            assert count == baseline_counts[table], table
    finally:
        runtime.close()


def test_d3_record_ids_are_append_only_and_cannot_be_overwritten(tmp_path: Path):
    db_path = str(tmp_path / "d3-append-only.sqlite")
    runtime = VerifiedProgressRuntime(db_path)
    try:
        graph_id = runtime.create_graph(owner_pid="p1").graph_id
        first_record = _d3_record(graph_id, 0, record_id="immutable", marker="first")
        _refresh(
            runtime,
            graph_id,
            expected_version=0,
            d3_record=first_record,
            key="append-v0",
        )
        before = runtime.get_d3_results(graph_id)

        conflicting_record = _d3_record(
            graph_id,
            1,
            record_id="immutable",
            result_hash="different".ljust(64, "x"),
            marker="conflict",
        )
        with pytest.raises(VPGError) as exc:
            _refresh(
                runtime,
                graph_id,
                expected_version=1,
                d3_record=conflicting_record,
                key="append-v1",
            )
        assert exc.value.code == VPGCode.PATCH_REJECTED
        assert runtime.get_graph(graph_id).current_version == 1
        assert runtime.get_d3_results(graph_id) == before
    finally:
        runtime.close()


def test_d3_payload_corruption_fails_closed(tmp_path: Path):
    db_path = str(tmp_path / "d3-corrupt.sqlite")
    runtime = VerifiedProgressRuntime(db_path)
    try:
        graph_id = runtime.create_graph(owner_pid="p1").graph_id
        record = _d3_record(graph_id, 0, marker="corrupt")
        _refresh(
            runtime,
            graph_id,
            expected_version=0,
            d3_record=record,
            key="corrupt-v0",
        )
        runtime.store.conn.execute(
            "UPDATE d3_invalidation_results SET payload_json = ? WHERE record_id = ?",
            (
                json.dumps(
                    {
                        "base_graph_version": 999,
                        "result_hash": record["result_hash"],
                    }
                ),
                record["record_id"],
            ),
        )
        runtime.store.conn.commit()
        with pytest.raises(VPGError) as exc:
            runtime.get_d3_results(graph_id)
        assert exc.value.code == VPGCode.STORAGE_ERROR
    finally:
        runtime.close()


def test_two_d3_refreshes_from_same_graph_version_have_one_winner(tmp_path: Path):
    db_path = str(tmp_path / "d3-cas.sqlite")
    bootstrap = VerifiedProgressRuntime(db_path)
    graph_id = bootstrap.create_graph(owner_pid="p1").graph_id
    bootstrap.close()

    barrier = threading.Barrier(2)
    outcomes: list[object] = [None, None]

    def writer(index: int) -> None:
        runtime = VerifiedProgressRuntime(db_path)
        try:
            barrier.wait(timeout=5)
            outcomes[index] = _refresh(
                runtime,
                graph_id,
                expected_version=0,
                d3_record=_d3_record(graph_id, 0, marker=f"writer-{index}"),
                key=f"cas-{index}",
            )
        except BaseException as exc:  # preserve exact domain error for assertions
            outcomes[index] = exc
        finally:
            runtime.close()

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(outcome is not None for outcome in outcomes)
    winners = [
        outcome
        for outcome in outcomes
        if not isinstance(outcome, BaseException) and outcome.patch_applied
    ]
    losers = [outcome for outcome in outcomes if isinstance(outcome, VPGError)]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0].code == VPGCode.GRAPH_VERSION_CONFLICT

    observer = VerifiedProgressRuntime(db_path)
    try:
        assert observer.get_graph(graph_id).current_version == 1
        assert len(observer.get_d3_results(graph_id)) == 1
    finally:
        observer.close()


def test_old_d3_frontier_is_rejected_after_graph_advances(tmp_path: Path):
    db_path = str(tmp_path / "d3-frontier.sqlite")
    runtime = VerifiedProgressRuntime(db_path)
    try:
        graph_id = runtime.create_graph(owner_pid="p1").graph_id
        old = _d3_record(graph_id, 0, marker="old")
        # Another writer/refresh advances the graph before the old result is
        # submitted.  The expected-version CAS must reject it before any D3
        # row or semantic projection can be written.
        _refresh(
            runtime,
            graph_id,
            expected_version=0,
            d3_record=_d3_record(graph_id, 0, marker="new"),
            key="new-v0",
        )
        with pytest.raises(VPGError) as exc:
            _refresh(
                runtime,
                graph_id,
                expected_version=0,
                d3_record=old,
                key="old-v0",
            )
        assert exc.value.code == VPGCode.GRAPH_VERSION_CONFLICT
        assert runtime.get_graph(graph_id).current_version == 1
        assert [row["record_id"] for row in runtime.get_d3_results(graph_id)] == [
            "d3:" + graph_id + ":v0:new"
        ]
    finally:
        runtime.close()
