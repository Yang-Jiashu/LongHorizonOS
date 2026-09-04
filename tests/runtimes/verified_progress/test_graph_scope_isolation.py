"""Graph-local projection keys, migration, and replay-history isolation."""

from __future__ import annotations

import copy
import json
import sqlite3

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.graph_store import GraphStore
from lhos.runtimes.verified_progress.models import (
    EdgeType,
    NodeLifecycle,
    TaskNode,
    VPGEdge,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.projections import rebuild_projection


def _submit_same_ids(rt: VerifiedProgressRuntime, graph_id: str, *, title: str) -> None:
    rt.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="same-key",
            operations=(
                AddNodeOp(
                    node_id="goal",
                    graph_id=graph_id,
                    node_type="goal",
                    created_by_pid="p1",
                    title=title,
                ),
                AddNodeOp(
                    node_id="task",
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="p1",
                    title=title,
                ),
                AddEdgeOp(
                    edge_id="dependency",
                    edge_type="depends_on",
                    source_node_id="goal",
                    target_node_id="task",
                    created_by_pid="p1",
                ),
            ),
        )
    )


def test_same_node_edge_and_idempotency_ids_are_isolated_by_graph() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    first = rt.create_graph(owner_pid="p1", graph_id="graph/one")
    second = rt.create_graph(owner_pid="p1", graph_id="graph/two")

    _submit_same_ids(rt, first.graph_id, title="first")
    _submit_same_ids(rt, second.graph_id, title="second")

    assert rt.inspect_node(first.graph_id, "task").title == "first"
    assert rt.inspect_node(second.graph_id, "task").title == "second"
    assert rt.inspect_edge(first.graph_id, "dependency") is not None
    assert rt.inspect_edge(second.graph_id, "dependency") is not None
    assert rt.get_graph(first.graph_id).current_version == 1
    assert rt.get_graph(second.graph_id).current_version == 1

    rt.store.delete_projection(first.graph_id)
    assert rt.inspect_node(first.graph_id, "task") is None
    assert rt.inspect_node(second.graph_id, "task").title == "second"
    assert rt.inspect_edge(second.graph_id, "dependency") is not None


def test_add_node_graph_id_mismatch_is_rejected_without_side_effects() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    graph_id = rt.create_graph(owner_pid="p1", graph_id="graph-one").graph_id

    with pytest.raises(VPGError) as caught:
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=graph_id,
                expected_graph_version=0,
                author_pid="p1",
                idempotency_key="wrong-graph",
                operations=(
                    AddNodeOp(
                        node_id="task",
                        graph_id="graph-two",
                        node_type="task",
                        created_by_pid="p1",
                    ),
                ),
            )
        )

    assert caught.value.code == VPGCode.EDGE_CROSS_GRAPH
    assert rt.get_graph(graph_id).current_version == 0
    assert rt.store.get_all_nodes(graph_id) == []
    assert rt.store.has_idempotency(("p1", graph_id, "wrong-graph")) is None


def _create_legacy_projection_schema(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE graph_nodes_projection (
            node_id TEXT PRIMARY KEY,
            graph_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE graph_edges_projection (
            edge_id TEXT PRIMARY KEY,
            graph_id TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            source_node_id TEXT NOT NULL,
            target_node_id TEXT NOT NULL,
            created_in_version INTEGER NOT NULL,
            created_by_pid TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    node = TaskNode(
        node_id="task",
        graph_id="legacy-graph",
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
        lifecycle=NodeLifecycle.ADMITTED,
    )
    edge = VPGEdge(
        edge_id="edge",
        graph_id="legacy-graph",
        edge_type=EdgeType.DEPENDS_ON,
        source_node_id="goal",
        target_node_id="task",
        created_in_version=1,
        created_by_pid="p1",
    )
    conn.execute(
        "INSERT INTO graph_nodes_projection VALUES (?, ?, ?, ?)",
        (node.node_id, node.graph_id, node.node_type.value, node.model_dump_json()),
    )
    conn.execute(
        "INSERT INTO graph_edges_projection VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            edge.edge_id,
            edge.graph_id,
            edge.edge_type.value,
            edge.source_node_id,
            edge.target_node_id,
            edge.created_in_version,
            edge.created_by_pid,
            edge.created_at.isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def _primary_key_columns(store: GraphStore, table: str) -> list[str]:
    rows = store.conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [
        str(row["name"])
        for row in sorted((row for row in rows if row["pk"]), key=lambda row: row["pk"])
    ]


def test_legacy_projection_primary_keys_migrate_without_data_loss(tmp_path) -> None:
    db_path = tmp_path / "legacy-vpg.db"
    _create_legacy_projection_schema(str(db_path))

    store = GraphStore(str(db_path))
    try:
        assert _primary_key_columns(store, "graph_nodes_projection") == [
            "graph_id",
            "node_id",
        ]
        assert _primary_key_columns(store, "graph_edges_projection") == [
            "graph_id",
            "edge_id",
        ]
        assert store.get_node("legacy-graph", "task") is not None
        assert store.get_edge("legacy-graph", "edge") is not None

        # The migration is idempotent across reopen and permits the same local
        # ids in another graph without moving the legacy rows.
        store.close()
        store = GraphStore(str(db_path))
        copied = copy.deepcopy(store.get_node("legacy-graph", "task"))
        assert copied is not None
        copied.graph_id = "new-graph"
        store.conn.execute(
            "INSERT INTO graph_nodes_projection VALUES (?, ?, ?, ?)",
            ("task", "new-graph", copied.node_type.value, copied.model_dump_json()),
        )
        store.conn.commit()
        assert store.get_node("legacy-graph", "task") is not None
        assert store.get_node("new-graph", "task") is not None
    finally:
        store.close()


def test_interrupted_projection_migration_recovers_shadow_rows(tmp_path) -> None:
    db_path = tmp_path / "interrupted-vpg.db"
    _create_legacy_projection_schema(str(db_path))

    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        ALTER TABLE graph_nodes_projection
            RENAME TO graph_nodes_projection_legacy_global_id;
        CREATE TABLE graph_nodes_projection (
            node_id TEXT NOT NULL,
            graph_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (graph_id, node_id)
        );

        ALTER TABLE graph_edges_projection
            RENAME TO graph_edges_projection_legacy_global_id;
        CREATE TABLE graph_edges_projection (
            edge_id TEXT NOT NULL,
            graph_id TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            source_node_id TEXT NOT NULL,
            target_node_id TEXT NOT NULL,
            created_in_version INTEGER NOT NULL,
            created_by_pid TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (graph_id, edge_id)
        );
        """
    )
    conn.close()

    store = GraphStore(str(db_path))
    try:
        assert store.get_node("legacy-graph", "task") is not None
        assert store.get_edge("legacy-graph", "edge") is not None
        shadows = store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%_legacy_global_id'"
        ).fetchall()
        assert shadows == []
    finally:
        store.close()


def test_interrupted_projection_migration_rejects_conflicting_rows(tmp_path) -> None:
    db_path = tmp_path / "conflicting-vpg.db"
    _create_legacy_projection_schema(str(db_path))

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        ALTER TABLE graph_nodes_projection
            RENAME TO graph_nodes_projection_legacy_global_id;
        CREATE TABLE graph_nodes_projection (
            node_id TEXT NOT NULL,
            graph_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (graph_id, node_id)
        );
        """
    )
    legacy = conn.execute("SELECT * FROM graph_nodes_projection_legacy_global_id").fetchone()
    payload = json.loads(legacy["payload_json"])
    payload["title"] = "conflicting destination"
    conn.execute(
        "INSERT INTO graph_nodes_projection VALUES (?, ?, ?, ?)",
        (
            legacy["node_id"],
            legacy["graph_id"],
            legacy["node_type"],
            json.dumps(payload),
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="conflicting projection rows"):
        GraphStore(str(db_path))


def test_bulk_node_read_rejects_key_payload_mismatch() -> None:
    store = GraphStore(":memory:")
    node = TaskNode(
        node_id="payload-id",
        graph_id="g",
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
        lifecycle=NodeLifecycle.ADMITTED,
    )
    store.conn.execute(
        "INSERT INTO graph_nodes_projection VALUES (?, ?, ?, ?)",
        ("row-id", "g", node.node_type.value, node.model_dump_json()),
    )
    store.conn.commit()

    with pytest.raises(VPGError, match="key/payload mismatch") as caught:
        store.get_all_nodes("g")
    assert caught.value.code == VPGCode.STORAGE_ERROR


def _patch(graph_id: str, patch_id: str, base_version: int) -> GraphPatchProposal:
    return GraphPatchProposal(
        patch_id=patch_id,
        graph_id=graph_id,
        expected_graph_version=base_version,
        author_pid="p1",
        idempotency_key=patch_id,
    )


@pytest.mark.parametrize(
    ("patches", "message"),
    [
        ([_patch("g", "p0", 0), _patch("g", "p2", 2)], "not contiguous"),
        ([_patch("g", "p0", 0), _patch("g", "p1", 0)], "not contiguous"),
        ([_patch("g", "same", 0), _patch("g", "same", 1)], "patch_id"),
        ([_patch("other", "p0", 0)], "belongs to graph"),
    ],
)
def test_rebuild_rejects_malformed_patch_history(patches, message) -> None:
    with pytest.raises(VPGError, match=message) as caught:
        rebuild_projection("g", patches, {}, {})
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED


def test_rebuild_rejects_projection_history_for_unknown_patch() -> None:
    with pytest.raises(VPGError, match="unknown patch ids") as caught:
        rebuild_projection("g", [_patch("g", "p0", 0)], {"ghost": []}, {})
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED


def test_rebuild_rejects_wrong_history_created_version() -> None:
    node = TaskNode(
        node_id="task",
        graph_id="g",
        created_in_version=99,
        updated_in_version=99,
        created_by_pid="p1",
    )
    with pytest.raises(VPGError, match="created in version") as caught:
        rebuild_projection("g", [_patch("g", "p0", 0)], {}, {"p0": [node]})
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED


def test_inspect_edge_unknown_graph_matches_inspect_node() -> None:
    rt = VerifiedProgressRuntime(":memory:")

    with pytest.raises(VPGError) as caught:
        rt.inspect_edge("unknown", "edge")
    assert caught.value.code == VPGCode.GRAPH_NOT_FOUND


def test_runtime_rebuild_rejects_patch_row_payload_mismatch() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    graph_id = rt.create_graph(owner_pid="p1").graph_id
    result = rt.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="patch",
            operations=(
                AddNodeOp(
                    node_id="task",
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="p1",
                ),
            ),
        )
    )
    row = rt.store.conn.execute(
        "SELECT operations_json FROM graph_patches WHERE patch_id = ?",
        (result.patch_id,),
    ).fetchone()
    payload = json.loads(row["operations_json"])
    payload["patch_id"] = "different-payload-id"
    rt.store.conn.execute(
        "UPDATE graph_patches SET operations_json = ? WHERE patch_id = ?",
        (json.dumps(payload), result.patch_id),
    )
    rt.store.conn.commit()
    before_nodes = [
        (node.node_id, node.model_dump_json()) for node in rt.store.get_all_nodes(graph_id)
    ]
    before_edges = [
        (edge.edge_id, edge.model_dump_json()) for edge in rt.store.get_all_edges(graph_id)
    ]

    with pytest.raises(VPGError, match="does not match payload") as caught:
        rt.rebuild_projection(graph_id)
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert [
        (node.node_id, node.model_dump_json()) for node in rt.store.get_all_nodes(graph_id)
    ] == before_nodes
    assert [
        (edge.edge_id, edge.model_dump_json()) for edge in rt.store.get_all_edges(graph_id)
    ] == before_edges


def test_replace_projection_rolls_back_if_insert_fails() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    graph_id = rt.create_graph(owner_pid="p1").graph_id
    rt.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="patch",
            operations=(
                AddNodeOp(
                    node_id="task",
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="p1",
                ),
            ),
        )
    )
    before = rt.store.get_node(graph_id, "task")
    assert before is not None

    rt.store.conn.execute(
        """
        CREATE TRIGGER reject_projection_insert
        BEFORE INSERT ON graph_nodes_projection
        BEGIN
            SELECT RAISE(ABORT, 'simulated projection write failure');
        END
        """
    )
    rt.store.conn.commit()

    replacement = copy.deepcopy(before)
    replacement.title = "should not commit"
    with pytest.raises(sqlite3.IntegrityError, match="simulated projection write failure"):
        rt.store.replace_projection(
            graph_id,
            expected_graph_version=1,
            nodes=[replacement],
            edges=[],
        )

    after = rt.store.get_node(graph_id, "task")
    assert after is not None
    assert after.model_dump() == before.model_dump()
