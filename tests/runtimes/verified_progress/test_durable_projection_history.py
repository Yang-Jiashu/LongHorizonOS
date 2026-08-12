"""Durable per-version projection history and fail-closed recovery tests."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import GraphVersion
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)


def _add_task(rt: VerifiedProgressRuntime, graph_id: str, key: str = "task") -> None:
    rt.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=rt.get_graph(graph_id).current_version,
            author_pid="p1",
            idempotency_key=key,
            operations=(
                AddNodeOp(
                    node_id=key,
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="p1",
                ),
            ),
        )
    )


def test_snapshot_history_survives_close_reopen_and_projection_wipe(tmp_path) -> None:
    path = tmp_path / "durable.db"
    rt = VerifiedProgressRuntime(str(path))
    gid = rt.create_graph(owner_pid="p1").graph_id
    _add_task(rt, gid)
    expected = rt.store.get_node(gid, "task")
    assert expected is not None
    assert rt.store.has_projection_snapshot(gid, 0)
    assert rt.store.has_projection_snapshot(gid, 1)

    row = rt.store.conn.execute(
        "SELECT projection_hash FROM graph_projection_snapshots "
        "WHERE graph_id = ? AND version = 1",
        (gid,),
    ).fetchone()
    gv = rt.store.get_version(gid, 1)
    assert row is not None and gv is not None
    assert row["projection_hash"] == gv.projection_hash
    rt.close()

    reopened = VerifiedProgressRuntime(str(path))
    assert reopened.store.load_projection_snapshot(gid, 1)[0]["task"] == expected
    reopened.store.delete_projection(gid)
    nodes, edges, _ = reopened.rebuild_projection(gid)
    assert nodes["task"] == expected
    assert edges == []
    assert reopened.store.get_node(gid, "task") == expected
    reopened.close()


def test_snapshot_payload_tamper_fails_before_replacing_materialized_rows() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="p1").graph_id
    _add_task(rt, gid)
    before = rt.store.get_node(gid, "task")
    assert before is not None

    row = rt.store.conn.execute(
        "SELECT payload_json FROM graph_node_history "
        "WHERE graph_id = ? AND version = 1 AND node_id = 'task'",
        (gid,),
    ).fetchone()
    payload = json.loads(row["payload_json"])
    payload["title"] = "forged"
    rt.store.conn.execute(
        "UPDATE graph_node_history SET payload_json = ? "
        "WHERE graph_id = ? AND version = 1 AND node_id = 'task'",
        (json.dumps(payload), gid),
    )
    rt.store.conn.commit()

    with pytest.raises(VPGError) as caught:
        rt.rebuild_projection(gid)
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert rt.store.get_node(gid, "task") == before


def test_snapshot_edge_tamper_is_detected_by_full_edge_hash() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="p1").graph_id
    # Build two nodes and a real dependency edge through the public path.
    rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="setup",
            operations=(
                AddNodeOp(node_id="a", graph_id=gid, node_type="task", created_by_pid="p1"),
                AddNodeOp(node_id="b", graph_id=gid, node_type="task", created_by_pid="p1"),
                AddEdgeOp(
                    edge_id="a-depends-on-b",
                    edge_type="depends_on",
                    source_node_id="a",
                    target_node_id="b",
                    created_by_pid="p1",
                ),
            ),
        )
    )
    edge_count = rt.store.conn.execute(
        "SELECT COUNT(*) AS count FROM graph_edge_history "
        "WHERE graph_id = ? AND version = 1",
        (gid,),
    ).fetchone()["count"]
    assert edge_count == 1

    rt.store.conn.execute(
        "UPDATE graph_edge_history SET edge_type = 'verifies' "
        "WHERE graph_id = ? AND version = 1 AND edge_id = 'a-depends-on-b'",
        (gid,),
    )
    rt.store.conn.commit()

    with pytest.raises(VPGError, match="snapshot hash mismatch"):
        rt.store.load_projection_snapshot(gid, 1)


def test_snapshot_write_failure_rolls_back_patch_version_and_history() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="p1").graph_id
    rt.store.conn.execute(
        """
        CREATE TRIGGER reject_snapshot_history
        BEFORE INSERT ON graph_node_history
        BEGIN
            SELECT RAISE(ABORT, 'simulated snapshot failure');
        END
        """
    )
    rt.store.conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="simulated snapshot failure"):
        _add_task(rt, gid)
    assert rt.get_graph(gid).current_version == 0
    assert rt.store.conn.execute(
        "SELECT COUNT(*) AS count FROM graph_patches WHERE graph_id = ?", (gid,)
    ).fetchone()["count"] == 0
    assert rt.store.conn.execute(
        "SELECT COUNT(*) AS count FROM graph_projection_snapshots "
        "WHERE graph_id = ? AND version = 1",
        (gid,),
    ).fetchone()["count"] == 0
    assert rt.store.get_all_nodes(gid) == []


def test_nonempty_missing_parent_snapshot_rejects_new_commit() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="p1").graph_id
    _add_task(rt, gid)
    rt.store.conn.execute(
        "DELETE FROM graph_projection_snapshots WHERE graph_id = ? AND version = 1",
        (gid,),
    )
    rt.store.conn.execute(
        "DELETE FROM graph_node_history WHERE graph_id = ? AND version = 1",
        (gid,),
    )
    rt.store.conn.commit()

    with pytest.raises(VPGError) as caught:
        _add_task(rt, gid, "task2")
    assert caught.value.code == VPGCode.GRAPH_RECOVERY_FAILED
    assert rt.get_graph(gid).current_version == 1


def test_legacy_split_version_helpers_fail_closed() -> None:
    rt = VerifiedProgressRuntime(":memory:")
    gid = rt.create_graph(owner_pid="p1").graph_id
    before = rt.get_graph(gid)

    with pytest.raises(RuntimeError, match="standalone graph version updates are disabled"):
        rt.store.update_record_version(gid, 1, "forged")
    with pytest.raises(RuntimeError, match="standalone GraphVersion commits are disabled"):
        rt.store.commit_graph_version(
            GraphVersion(
                graph_id=gid,
                version=1,
                parent_version=0,
                patch_id="forged",
                projection_hash="forged",
                committed_by_pid="p1",
                committed_at=datetime.now(UTC),
            )
        )

    assert rt.get_graph(gid) == before
    assert rt.store.get_version(gid, 1) is None
    assert not rt.store.has_projection_snapshot(gid, 1)
