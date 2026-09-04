"""Incremental durable projection-history regression tests."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.events import GraphEventType, ready_frontier_hash
from lhos.runtimes.verified_progress.graph_store import compute_projection_hash
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)


def _submit(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
    key: str,
    operations: tuple[AddNodeOp | AddEdgeOp, ...],
) -> None:
    runtime.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=runtime.get_graph(graph_id).current_version,
            author_pid="p1",
            idempotency_key=key,
            operations=operations,
        )
    )


def _add_task(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
    node_id: str,
    *,
    title: str = "",
) -> None:
    _submit(
        runtime,
        graph_id,
        node_id,
        (
            AddNodeOp(
                node_id=node_id,
                graph_id=graph_id,
                node_type="task",
                created_by_pid="p1",
                title=title,
            ),
        ),
    )


def _materialized_signature(
    runtime: VerifiedProgressRuntime,
    graph_id: str,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    nodes = sorted(
        (node.node_id, node.model_dump_json()) for node in runtime.store.get_all_nodes(graph_id)
    )
    edges = sorted(
        (edge.edge_id, edge.model_dump_json()) for edge in runtime.store.get_all_edges(graph_id)
    )
    return nodes, edges


def test_v2_records_only_changed_entity_revisions() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="incremental").graph_id
    _submit(
        runtime,
        graph_id,
        "structure",
        (
            AddNodeOp(
                node_id="a",
                graph_id=graph_id,
                node_type="task",
                created_by_pid="p1",
            ),
            AddNodeOp(
                node_id="dependency",
                graph_id=graph_id,
                node_type="task",
                created_by_pid="p1",
            ),
            AddEdgeOp(
                edge_id="a-depends-on-dependency",
                edge_type="depends_on",
                source_node_id="a",
                target_node_id="dependency",
                created_by_pid="p1",
            ),
        ),
    )
    _add_task(runtime, graph_id, "b")

    node_rows = runtime.store.conn.execute(
        "SELECT version, node_id FROM graph_node_history "
        "WHERE graph_id = ? ORDER BY version, node_id",
        (graph_id,),
    ).fetchall()
    edge_rows = runtime.store.conn.execute(
        "SELECT version, edge_id FROM graph_edge_history "
        "WHERE graph_id = ? ORDER BY version, edge_id",
        (graph_id,),
    ).fetchall()

    assert [(row["version"], row["node_id"]) for row in node_rows] == [
        (1, "a"),
        (1, "dependency"),
        (2, "b"),
    ]
    assert [(row["version"], row["edge_id"]) for row in edge_rows] == [
        (1, "a-depends-on-dependency")
    ]
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_node_history "
            "WHERE graph_id = ? AND version = 2 AND node_id = 'a'",
            (graph_id,),
        ).fetchone()[0]
        == 0
    )


def test_every_historical_version_reconstructs_and_matches_its_hash() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="historical").graph_id
    for node_id in ("a", "b", "c"):
        _add_task(runtime, graph_id, node_id, title=f"title-{node_id}")

    expected_ids = {
        0: [],
        1: ["a"],
        2: ["a", "b"],
        3: ["a", "b", "c"],
    }
    for version, expected in expected_ids.items():
        nodes, edges = runtime.store.load_projection_snapshot(graph_id, version)
        graph_version = runtime.store.get_version(graph_id, version)
        assert graph_version is not None
        assert sorted(nodes) == expected
        assert edges == []
        assert (
            compute_projection_hash(graph_id, version, nodes.values(), edges)
            == graph_version.projection_hash
        )


def test_legacy_full_history_rows_remain_readable_after_reopen(tmp_path) -> None:
    path = tmp_path / "legacy-full-history.db"
    runtime = VerifiedProgressRuntime(str(path))
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="legacy-full").graph_id
    for node_id in ("a", "b", "c"):
        _add_task(runtime, graph_id, node_id, title=f"title-{node_id}")

    # Emulate the former layout, which copied every unchanged entity into
    # every later version. Snapshot headers and hashes remain authoritative.
    runtime.store.conn.execute(
        "INSERT INTO graph_node_history "
        "(graph_id, version, node_id, node_type, payload_json) "
        "SELECT graph_id, 2, node_id, node_type, payload_json "
        "FROM graph_node_history "
        "WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
        (graph_id,),
    )
    runtime.store.conn.execute(
        "INSERT INTO graph_node_history "
        "(graph_id, version, node_id, node_type, payload_json) "
        "SELECT graph_id, 3, node_id, node_type, payload_json "
        "FROM graph_node_history "
        "WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
        (graph_id,),
    )
    runtime.store.conn.execute(
        "INSERT INTO graph_node_history "
        "(graph_id, version, node_id, node_type, payload_json) "
        "SELECT graph_id, 3, node_id, node_type, payload_json "
        "FROM graph_node_history "
        "WHERE graph_id = ? AND version = 2 AND node_id = 'b'",
        (graph_id,),
    )
    runtime.store.conn.commit()
    assert [
        row[0]
        for row in runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_node_history "
            "WHERE graph_id = ? GROUP BY version ORDER BY version",
            (graph_id,),
        ).fetchall()
    ] == [1, 2, 3]
    runtime.close()

    reopened = VerifiedProgressRuntime(str(path))
    try:
        for version, expected in {
            1: ["a"],
            2: ["a", "b"],
            3: ["a", "b", "c"],
        }.items():
            nodes, edges = reopened.store.load_projection_snapshot(graph_id, version)
            assert sorted(nodes) == expected
            assert edges == []
    finally:
        reopened.close()


@pytest.mark.parametrize("mutation", ["tamper", "delete"])
def test_inherited_revision_tamper_or_loss_fails_closed(mutation: str) -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id=f"corrupt-{mutation}").graph_id
    _add_task(runtime, graph_id, "a", title="trusted")
    _add_task(runtime, graph_id, "b")
    materialized_before = _materialized_signature(runtime, graph_id)

    if mutation == "tamper":
        row = runtime.store.conn.execute(
            "SELECT payload_json FROM graph_node_history "
            "WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
            (graph_id,),
        ).fetchone()
        payload = json.loads(row["payload_json"])
        payload["title"] = "forged"
        runtime.store.conn.execute(
            "UPDATE graph_node_history SET payload_json = ? "
            "WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
            (json.dumps(payload), graph_id),
        )
    else:
        runtime.store.conn.execute(
            "DELETE FROM graph_node_history WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
            (graph_id,),
        )
    runtime.store.conn.commit()

    with pytest.raises(VPGError) as rebuild_error:
        runtime.rebuild_projection(graph_id)
    assert rebuild_error.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert _materialized_signature(runtime, graph_id) == materialized_before

    with pytest.raises(VPGError) as commit_error:
        _add_task(runtime, graph_id, "c")
    assert commit_error.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert runtime.get_graph(graph_id).current_version == 2
    assert runtime.store.has_idempotency(("p1", graph_id, "c")) is None
    assert _materialized_signature(runtime, graph_id) == materialized_before


def test_incremental_history_write_failure_rolls_back_every_commit_plane() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="rollback").graph_id
    _add_task(runtime, graph_id, "a")
    materialized_before = _materialized_signature(runtime, graph_id)
    event_count_before = runtime.store.conn.execute(
        "SELECT COUNT(*) FROM graph_events WHERE graph_id = ?",
        (graph_id,),
    ).fetchone()[0]

    runtime.store.conn.execute(
        """
        CREATE TRIGGER reject_v2_node_revision
        BEFORE INSERT ON graph_node_history
        WHEN NEW.graph_id = 'rollback' AND NEW.version = 2
        BEGIN
            SELECT RAISE(ABORT, 'simulated incremental history failure');
        END
        """
    )
    runtime.store.conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="simulated incremental history failure"):
        _add_task(runtime, graph_id, "b")

    assert runtime.get_graph(graph_id).current_version == 1
    assert _materialized_signature(runtime, graph_id) == materialized_before
    assert runtime.store.has_idempotency(("p1", graph_id, "b")) is None
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_patches WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 1
    )
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_versions WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 2
    )
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 2
    )
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_node_history WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == 1
    )
    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_events WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()[0]
        == event_count_before
    )


def test_incremental_history_is_graph_scoped_even_with_same_local_ids() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_one = runtime.create_graph(owner_pid="p1", graph_id="scope-one").graph_id
    graph_two = runtime.create_graph(owner_pid="p1", graph_id="scope-two").graph_id
    for graph_id, prefix in ((graph_one, "one"), (graph_two, "two")):
        _add_task(runtime, graph_id, "a", title=f"{prefix}-a")
        _add_task(runtime, graph_id, "b", title=f"{prefix}-b")

    for graph_id in (graph_one, graph_two):
        assert (
            runtime.store.conn.execute(
                "SELECT COUNT(*) FROM graph_node_history WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()[0]
            == 2
        )
        assert (
            runtime.store.conn.execute(
                "SELECT COUNT(*) FROM graph_node_history "
                "WHERE graph_id = ? AND version = 2 AND node_id = 'a'",
                (graph_id,),
            ).fetchone()[0]
            == 0
        )

    graph_two_before = _materialized_signature(runtime, graph_two)
    runtime.store.conn.execute(
        "DELETE FROM graph_node_history WHERE graph_id = ? AND version = 1 AND node_id = 'a'",
        (graph_one,),
    )
    runtime.store.conn.commit()

    with pytest.raises(VPGError) as caught:
        runtime.store.load_projection_snapshot(graph_one, 2)
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED

    graph_two_nodes, graph_two_edges = runtime.store.load_projection_snapshot(graph_two, 2)
    assert graph_two_nodes["a"].title == "two-a"
    assert graph_two_nodes["b"].title == "two-b"
    assert graph_two_edges == []
    assert _materialized_signature(runtime, graph_two) == graph_two_before


def test_ready_frontier_event_uses_compact_summary_and_reads_legacy_list() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="frontier-summary").graph_id
    _add_task(runtime, graph_id, "a")
    _add_task(runtime, graph_id, "b")

    row = runtime.store.conn.execute(
        "SELECT event_id, ready_frontier_json FROM graph_events "
        "WHERE graph_id = ? AND event_type = ? ORDER BY graph_version DESC LIMIT 1",
        (graph_id, GraphEventType.READY_FRONTIER_UPDATED.value),
    ).fetchone()
    assert row is not None
    encoded = json.loads(row["ready_frontier_json"])
    assert encoded["encoding"] == "summary-v1"
    assert encoded["count"] == 2
    assert encoded["hash"] == ready_frontier_hash(("a", "b"))

    event = next(
        event for event in runtime.get_events(graph_id) if event.event_id == row["event_id"]
    )
    # New rows expose the audit summary; the authoritative full frontier is
    # recomputed through query_ready_frontier rather than replayed from events.
    assert event.ready_frontier == ()
    assert event.ready_frontier_count == 2
    assert event.ready_frontier_hash == encoded["hash"]

    # Databases written by older versions stored a full ordered JSON list.
    runtime.store.conn.execute(
        "UPDATE graph_events SET ready_frontier_json = ? WHERE event_id = ?",
        (json.dumps(["a", "b"]), row["event_id"]),
    )
    runtime.store.conn.commit()
    legacy_event = next(
        event for event in runtime.get_events(graph_id) if event.event_id == row["event_id"]
    )
    assert legacy_event.ready_frontier == ("a", "b")
    assert legacy_event.ready_frontier_count is None
    assert legacy_event.ready_frontier_hash is None


def test_metadata_key_order_change_writes_a_recoverable_revision() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="metadata-order").graph_id
    _submit(
        runtime,
        graph_id,
        "add-a",
        (
            AddNodeOp(
                node_id="a",
                graph_id=graph_id,
                node_type="task",
                created_by_pid="p1",
                metadata={"nested": {"alpha": 1, "beta": 2}},
            ),
        ),
    )

    parent = runtime.store.get_node(graph_id, "a")
    assert parent is not None
    reordered = parent.model_copy(deep=True)
    reordered.metadata = {"nested": {"beta": 2, "alpha": 1}}
    assert reordered.model_dump() == parent.model_dump()
    assert reordered.model_dump_json() != parent.model_dump_json()

    patch = GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=1,
        author_pid="p1",
        operations=(),
        idempotency_key="reorder-metadata",
    )
    runtime.store.commit_patch(
        patch,
        patch_id=patch.patch_id,
        committed_version=2,
        applied_at=datetime.now(UTC).isoformat(),
        events=(),
        nodes_to_upsert=(("a", reordered),),
        edges_to_upsert=(),
        projection_nodes=(reordered,),
        projection_edges=(),
    )

    assert (
        runtime.store.conn.execute(
            "SELECT COUNT(*) FROM graph_node_history "
            "WHERE graph_id = ? AND version = 2 AND node_id = 'a'",
            (graph_id,),
        ).fetchone()[0]
        == 1
    )
    recovered_nodes, recovered_edges = runtime.store.load_projection_snapshot(graph_id, 2)
    graph_version = runtime.store.get_version(graph_id, 2)
    assert graph_version is not None
    assert list(recovered_nodes["a"].metadata["nested"]) == ["beta", "alpha"]
    assert recovered_edges == []
    assert (
        compute_projection_hash(
            graph_id,
            2,
            recovered_nodes.values(),
            recovered_edges,
        )
        == graph_version.projection_hash
    )


def test_valid_json_materialized_tamper_blocks_next_commit_without_writes() -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id="cache-tamper").graph_id
    _add_task(runtime, graph_id, "a", title="trusted")

    row = runtime.store.conn.execute(
        "SELECT payload_json FROM graph_nodes_projection WHERE graph_id = ? AND node_id = 'a'",
        (graph_id,),
    ).fetchone()
    payload = json.loads(row["payload_json"])
    payload["title"] = "forged"
    runtime.store.conn.execute(
        "UPDATE graph_nodes_projection SET payload_json = ? WHERE graph_id = ? AND node_id = 'a'",
        (json.dumps(payload), graph_id),
    )
    runtime.store.conn.commit()
    database_before_attempt = list(runtime.store.conn.iterdump())

    with pytest.raises(VPGError) as caught:
        _add_task(runtime, graph_id, "b")

    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert list(runtime.store.conn.iterdump()) == database_before_attempt
    assert runtime.get_graph(graph_id).current_version == 1
    assert runtime.store.has_idempotency(("p1", graph_id, "b")) is None
    durable_nodes, durable_edges = runtime.store.load_projection_snapshot(graph_id, 1)
    assert durable_nodes["a"].title == "trusted"
    assert durable_edges == []


@pytest.mark.parametrize("mismatch", ["patch_id", "committed_version"])
def test_low_level_commit_metadata_mismatch_is_rejected_without_writes(
    mismatch: str,
) -> None:
    runtime = VerifiedProgressRuntime(":memory:")
    graph_id = runtime.create_graph(owner_pid="p1", graph_id=f"mismatch-{mismatch}").graph_id
    patch = GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=0,
        author_pid="p1",
        operations=(),
        idempotency_key=f"reject-{mismatch}",
    )
    patch_id = patch.patch_id if mismatch != "patch_id" else f"forged-{patch.patch_id}"
    committed_version = 1 if mismatch != "committed_version" else 2
    database_before_attempt = list(runtime.store.conn.iterdump())

    with pytest.raises(VPGError) as caught:
        runtime.store.commit_patch(
            patch,
            patch_id=patch_id,
            committed_version=committed_version,
            applied_at=datetime.now(UTC).isoformat(),
            events=(),
            nodes_to_upsert=(),
            edges_to_upsert=(),
            projection_nodes=(),
            projection_edges=(),
        )

    assert caught.value.code == VPGCode.PATCH_REJECTED
    assert list(runtime.store.conn.iterdump()) == database_before_attempt
    assert runtime.get_graph(graph_id).current_version == 0
