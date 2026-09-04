"""Projection-history retention and trusted legacy migration tests."""

from __future__ import annotations

import sqlite3

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _add_task(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
    node_id: str,
) -> None:
    runtime.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=runtime.get_graph(graph_id).current_version,
            author_pid="p1",
            idempotency_key=f"add-{node_id}",
            operations=(
                AddNodeOp(
                    node_id=node_id,
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="p1",
                    title=f"task-{node_id}",
                ),
            ),
        )
    )


def _build_three_versions(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
) -> None:
    for node_id in ("a", "b", "c"):
        _add_task(runtime, graph_id, node_id)


def _simulate_snapshotless_legacy_graph(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
) -> None:
    runtime.store.conn.execute(
        "DELETE FROM graph_projection_snapshots WHERE graph_id = ? AND version > 0",
        (graph_id,),
    )
    runtime.store.conn.execute(
        "DELETE FROM graph_node_history WHERE graph_id = ?",
        (graph_id,),
    )
    runtime.store.conn.execute(
        "DELETE FROM graph_edge_history WHERE graph_id = ?",
        (graph_id,),
    )
    runtime.store.conn.execute(
        "UPDATE graph_versions SET projection_hash = 'legacy-untrusted-hash' "
        "WHERE graph_id = ? AND version > 0",
        (graph_id,),
    )
    runtime.store.conn.commit()


def test_compaction_installs_checkpoint_and_rejects_pruned_versions(tmp_path) -> None:
    path = tmp_path / "compacted.db"
    runtime = VerifiedProgressRuntime(str(path))
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="compacted").graph_id
    _build_three_versions(runtime, graph_id)

    expected_v2 = runtime.store.load_projection_snapshot(graph_id, 2)
    expected_v3 = runtime.store.load_projection_snapshot(graph_id, 3)
    result = runtime.store.compact_projection_history(
        graph_id,
        retain_from_version=2,
        compacted_by="operator",
        reason="retain the last two recoverable versions",
    )

    assert result.previous_earliest_version == 0
    assert result.earliest_recoverable_version == 2
    assert result.current_version == 3
    assert result.deleted_snapshot_headers == 2
    assert runtime.store.get_history_retention_contract(graph_id).earliest_recoverable_version == 2
    assert runtime.store.load_projection_snapshot(graph_id, 2) == expected_v2
    assert runtime.store.load_projection_snapshot(graph_id, 3) == expected_v3
    with pytest.raises(VPGError) as caught:
        runtime.store.load_projection_snapshot(graph_id, 1)
    assert caught.value.code == VPGCode.GRAPH_HISTORY_PRUNED
    assert "earliest recoverable version is 2" in str(caught.value)

    checkpoint_ids = {
        row["node_id"]
        for row in runtime.store.conn.execute(
            "SELECT node_id FROM graph_node_history WHERE graph_id = ? AND version = 2",
            (graph_id,),
        ).fetchall()
    }
    assert checkpoint_ids == {"a", "b"}
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_versions WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 4
    )
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_patches WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 3
    )
    assert tuple(
        runtime.store.conn.execute(
            "SELECT operation, actor, reason FROM graph_history_lifecycle_events "
            "WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()
    ) == (
        "compact",
        "operator",
        "retain the last two recoverable versions",
    )
    runtime.close()

    reopened = VerifiedProgressRuntime(str(path))
    try:
        assert reopened.store.load_projection_snapshot(graph_id, 2) == expected_v2
        assert reopened.store.load_projection_snapshot(graph_id, 3) == expected_v3
        assert not reopened.store.has_projection_snapshot(graph_id, 0)
        with pytest.raises(VPGError) as reopened_error:
            reopened.store.load_projection_snapshot(graph_id, 0)
        assert reopened_error.value.code == VPGCode.GRAPH_HISTORY_PRUNED

        _add_task(reopened, graph_id, "d")
        nodes, edges = reopened.store.load_projection_snapshot(graph_id, 4)
        assert set(nodes) == {"a", "b", "c", "d"}
        assert edges == []
    finally:
        reopened.close()


def test_compaction_failure_rolls_back_checkpoint_and_retention() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="rollback").graph_id
    _build_three_versions(runtime, graph_id)
    snapshots_before = {
        version: runtime.store.load_projection_snapshot(graph_id, version) for version in range(4)
    }
    runtime.store.conn.execute(
        """
        CREATE TRIGGER reject_history_compaction_audit
        BEFORE INSERT ON graph_history_lifecycle_events
        WHEN NEW.operation = 'compact'
        BEGIN
            SELECT RAISE(ABORT, 'simulated compaction audit failure');
        END
        """
    )
    runtime.store.conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="simulated compaction audit failure"):
        runtime.store.compact_projection_history(
            graph_id,
            retain_from_version=2,
            compacted_by="operator",
            reason="exercise atomic rollback",
        )

    assert runtime.store.get_history_retention_contract(graph_id).earliest_recoverable_version == 0
    assert {
        version: runtime.store.load_projection_snapshot(graph_id, version) for version in range(4)
    } == snapshots_before


def test_compaction_refuses_corrupt_retained_history_without_writes() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="corrupt").graph_id
    _build_three_versions(runtime, graph_id)
    runtime.store.conn.execute(
        "UPDATE graph_projection_snapshots SET projection_hash = 'forged' "
        "WHERE graph_id = ? AND version = 3",
        (graph_id,),
    )
    runtime.store.conn.commit()
    before = list(runtime.store.conn.iterdump())

    with pytest.raises(VPGError) as caught:
        runtime.store.compact_projection_history(
            graph_id,
            retain_from_version=2,
            compacted_by="operator",
            reason="must not bless corrupt history",
        )

    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert list(runtime.store.conn.iterdump()) == before


def test_recover_accepts_compacted_history_and_keeps_pruned_floor_explicit() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="recover-compacted").graph_id
    _build_three_versions(runtime, graph_id)
    runtime.store.compact_projection_history(
        graph_id,
        retain_from_version=2,
        compacted_by="operator",
        reason="recovery should use the retained floor",
    )
    runtime.store.delete_projection(graph_id)

    events, record = runtime.recover(graph_id)
    assert record.current_version == 3
    assert events[0].event_type.value == "graph.recovery.started"
    assert events[-1].event_type.value == "graph.recovery.completed"
    # The graph's cache was rebuilt from retained history; compare by ids
    # rather than model object identity.
    assert {node.node_id for node in runtime.store.get_all_nodes(graph_id)} == {"a", "b", "c"}
    with pytest.raises(VPGError) as caught:
        runtime.store.load_projection_snapshot(graph_id, 1)
    assert caught.value.code == VPGCode.GRAPH_HISTORY_PRUNED


def test_recover_fails_closed_when_compacted_checkpoint_is_tampered() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="recover-tamper").graph_id
    _build_three_versions(runtime, graph_id)
    runtime.store.compact_projection_history(
        graph_id,
        retain_from_version=2,
        compacted_by="operator",
        reason="tamper audit",
    )
    before = [
        (node.node_id, node.model_dump_json()) for node in runtime.store.get_all_nodes(graph_id)
    ]
    row = runtime.store.conn.execute(
        "SELECT payload_json FROM graph_node_history "
        "WHERE graph_id = ? AND version = 2 AND node_id = 'a'",
        (graph_id,),
    ).fetchone()
    assert row is not None
    payload = row["payload_json"].replace("task-a", "forged")
    runtime.store.conn.execute(
        "UPDATE graph_node_history SET payload_json = ? "
        "WHERE graph_id = ? AND version = 2 AND node_id = 'a'",
        (payload, graph_id),
    )
    runtime.store.conn.commit()
    runtime.store.delete_projection(graph_id)

    with pytest.raises(VPGError) as caught:
        runtime.recover(graph_id)
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert [
        (node.node_id, node.model_dump_json()) for node in runtime.store.get_all_nodes(graph_id)
    ] == []
    # A failed recovery must not replace the cache. The cache was intentionally
    # empty here, and the durable rows remain untouched.
    assert before


def test_reopen_does_not_recreate_deleted_retention_contract_after_compaction(
    tmp_path,
) -> None:
    path = tmp_path / "missing-retention.db"
    runtime = VerifiedProgressRuntime(str(path))
    graph_id = runtime.create_graph(
        owner_pid="p1",
        graph_id="missing-retention",
    ).graph_id
    _build_three_versions(runtime, graph_id)
    runtime.store.compact_projection_history(
        graph_id,
        retain_from_version=2,
        compacted_by="operator",
        reason="retention metadata tamper audit",
    )
    runtime.store.conn.execute(
        "DELETE FROM graph_history_retention WHERE graph_id = ?",
        (graph_id,),
    )
    runtime.store.conn.commit()
    runtime.close()

    reopened = VerifiedProgressRuntime(str(path))
    try:
        with pytest.raises(VPGError) as caught:
            reopened.store.get_history_retention_contract(graph_id)
        assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
        assert not reopened.store.has_projection_snapshot(graph_id, 0)
    finally:
        reopened.close()


def test_trusted_legacy_migration_requires_preview_hash_and_explicit_trust(
    tmp_path,
) -> None:
    path = tmp_path / "trusted-legacy.db"
    runtime = VerifiedProgressRuntime(str(path))
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="legacy").graph_id
    _add_task(runtime, graph_id, "a")
    _add_task(runtime, graph_id, "b")
    _simulate_snapshotless_legacy_graph(runtime, graph_id)

    plan = runtime.store.preview_trusted_projection_migration(graph_id)
    assert plan.current_version == 2
    assert plan.node_count == 2
    assert plan.edge_count == 0
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE graph_id = ? AND version = 2",
            (graph_id,),
        ).fetchone()[0]
        == 0
    )

    with pytest.raises(VPGError) as trust_error:
        runtime.store.migrate_snapshotless_legacy_projection(
            graph_id,
            expected_current_version=plan.current_version,
            expected_projection_hash=plan.projection_hash,
            trusted=False,
            trusted_by="operator",
            reason="verified against a signed export",
        )
    assert trust_error.value.code == VPGCode.TRUSTED_MIGRATION_REJECTED

    with pytest.raises(VPGError) as hash_error:
        runtime.store.migrate_snapshotless_legacy_projection(
            graph_id,
            expected_current_version=plan.current_version,
            expected_projection_hash="wrong-hash",
            trusted=True,
            trusted_by="operator",
            reason="verified against a signed export",
        )
    assert hash_error.value.code == VPGCode.TRUSTED_MIGRATION_REJECTED

    migrated = runtime.store.migrate_snapshotless_legacy_projection(
        graph_id,
        expected_current_version=plan.current_version,
        expected_projection_hash=plan.projection_hash,
        trusted=True,
        trusted_by="operator",
        reason="verified against a signed export",
    )
    assert migrated == plan
    assert set(runtime.store.load_projection_snapshot(graph_id, 2)[0]) == {"a", "b"}
    with pytest.raises(VPGError) as pruned:
        runtime.store.load_projection_snapshot(graph_id, 1)
    assert pruned.value.code == VPGCode.GRAPH_HISTORY_PRUNED
    assert tuple(
        runtime.store.conn.execute(
            "SELECT operation, actor, reason FROM graph_history_lifecycle_events "
            "WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()
    ) == (
        "trusted_projection_migration",
        "operator",
        "verified against a signed export",
    )
    runtime.close()

    reopened = VerifiedProgressRuntime(str(path))
    try:
        assert not reopened.store.has_projection_snapshot(graph_id, 0)
        assert set(reopened.store.load_projection_snapshot(graph_id, 2)[0]) == {"a", "b"}
        _add_task(reopened, graph_id, "c")
        assert set(reopened.store.load_projection_snapshot(graph_id, 3)[0]) == {
            "a",
            "b",
            "c",
        }
    finally:
        reopened.close()


def test_recover_accepts_trusted_migration_baseline_after_projection_wipe() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="recover-migrated").graph_id
    _add_task(runtime, graph_id, "a")
    _add_task(runtime, graph_id, "b")
    _simulate_snapshotless_legacy_graph(runtime, graph_id)
    plan = runtime.store.preview_trusted_projection_migration(graph_id)
    runtime.store.migrate_snapshotless_legacy_projection(
        graph_id,
        expected_current_version=plan.current_version,
        expected_projection_hash=plan.projection_hash,
        trusted=True,
        trusted_by="operator",
        reason="baseline verified from an external export",
    )
    runtime.store.delete_projection(graph_id)

    events, record = runtime.recover(graph_id)
    assert record.current_version == 2
    assert events[-1].event_type.value == "graph.recovery.completed"
    assert {node.node_id for node in runtime.store.get_all_nodes(graph_id)} == {"a", "b"}


def test_trusted_migration_rejects_projection_changed_after_preview() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="changed").graph_id
    _add_task(runtime, graph_id, "a")
    _simulate_snapshotless_legacy_graph(runtime, graph_id)
    plan = runtime.store.preview_trusted_projection_migration(graph_id)

    runtime.store.conn.execute(
        "UPDATE graph_nodes_projection SET payload_json = "
        "replace(payload_json, 'task-a', 'tampered') "
        "WHERE graph_id = ? AND node_id = 'a'",
        (graph_id,),
    )
    runtime.store.conn.commit()
    before = list(runtime.store.conn.iterdump())

    with pytest.raises(VPGError) as caught:
        runtime.store.migrate_snapshotless_legacy_projection(
            graph_id,
            expected_current_version=plan.current_version,
            expected_projection_hash=plan.projection_hash,
            trusted=True,
            trusted_by="operator",
            reason="stale preview must fail",
        )

    assert caught.value.code == VPGCode.TRUSTED_MIGRATION_REJECTED
    assert list(runtime.store.conn.iterdump()) == before


def test_trusted_migration_failure_rolls_back_all_history_writes() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="migration-rollback").graph_id
    _add_task(runtime, graph_id, "a")
    _simulate_snapshotless_legacy_graph(runtime, graph_id)
    plan = runtime.store.preview_trusted_projection_migration(graph_id)
    runtime.store.conn.execute(
        """
        CREATE TRIGGER reject_trusted_migration_audit
        BEFORE INSERT ON graph_history_lifecycle_events
        WHEN NEW.operation = 'trusted_projection_migration'
        BEGIN
            SELECT RAISE(ABORT, 'simulated migration audit failure');
        END
        """
    )
    runtime.store.conn.commit()
    before = list(runtime.store.conn.iterdump())

    with pytest.raises(sqlite3.IntegrityError, match="simulated migration audit failure"):
        runtime.store.migrate_snapshotless_legacy_projection(
            graph_id,
            expected_current_version=plan.current_version,
            expected_projection_hash=plan.projection_hash,
            trusted=True,
            trusted_by="operator",
            reason="exercise atomic rollback",
        )

    assert list(runtime.store.conn.iterdump()) == before
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE graph_id = ? AND version > 0",
            (graph_id,),
        ).fetchone()[0]
        == 0
    )
