"""Trusted large-patch publication remains atomic.

Ordinary callers keep the per-patch operation ceiling.  The private trusted
flag exists only so the AgentOS composition root can publish one compiled Goal
in a single GraphVersion instead of exposing partially committed batches.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patch_validator import MAX_PATCH_OPS
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _node_ops(graph_id: str, count: int) -> tuple[AddNodeOp, ...]:
    return tuple(
        AddNodeOp(
            node_id=f"node-{index}",
            graph_id=graph_id,
            node_type="task",
            created_by_pid="compiler",
        )
        for index in range(count)
    )


def _proposal(
    graph_id: str,
    operations: tuple[AddNodeOp, ...],
    *,
    key: str,
) -> GraphPatchProposal:
    return GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=0,
        author_pid="compiler",
        idempotency_key=key,
        operations=operations,
    )


def test_ordinary_patch_still_rejects_more_than_max_operations():
    runtime = VerifiedProgressRuntime(":memory:")
    try:
        graph_id = runtime.create_graph(owner_pid="compiler").graph_id
        operations = _node_ops(graph_id, MAX_PATCH_OPS + 1)

        with pytest.raises(VPGError) as exc:
            runtime.submit_patch(_proposal(graph_id, operations, key="ordinary-large"))

        assert exc.value.code == VPGCode.PATCH_TOO_LARGE
        assert runtime.get_graph(graph_id).current_version == 0
        assert runtime.snapshot_projection(graph_id) == ({}, [])
    finally:
        runtime.close()


def test_trusted_large_patch_publishes_in_one_graph_version():
    runtime = VerifiedProgressRuntime(":memory:")
    try:
        graph_id = runtime.create_graph(owner_pid="compiler").graph_id
        operations = _node_ops(graph_id, MAX_PATCH_OPS + 1)

        result = runtime.submit_patch(
            _proposal(graph_id, operations, key="trusted-large"),
            _allow_large_operations=True,
        )

        nodes, edges = runtime.snapshot_projection(graph_id)
        assert result.patch_applied is True
        assert result.committed_graph_version == 1
        assert runtime.get_graph(graph_id).current_version == 1
        assert set(nodes) == {operation.node_id for operation in operations}
        assert edges == []
        assert (
            runtime.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_patches WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 1
        )
        assert (
            runtime.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_versions WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 2  # immutable v0 plus the one atomic publication
        )
    finally:
        runtime.close()


def test_late_validation_failure_exposes_no_partial_large_projection():
    runtime = VerifiedProgressRuntime(":memory:")
    try:
        graph_id = runtime.create_graph(owner_pid="compiler").graph_id
        operations = list(_node_ops(graph_id, MAX_PATCH_OPS))
        # The final operation is invalid only after all prior operations have
        # been admitted into the in-memory candidate.  Validation must abort
        # before any of them become durable or schedulable.
        operations.append(
            AddNodeOp(
                node_id=operations[-1].node_id,
                graph_id=graph_id,
                node_type="task",
                created_by_pid="compiler",
            )
        )

        with pytest.raises(VPGError) as exc:
            runtime.submit_patch(
                _proposal(graph_id, tuple(operations), key="trusted-invalid"),
                _allow_large_operations=True,
            )

        assert exc.value.code == VPGCode.NODE_ALREADY_EXISTS
        assert runtime.get_graph(graph_id).current_version == 0
        assert runtime.snapshot_projection(graph_id) == ({}, [])
        assert (
            runtime.store.conn.execute(
                "SELECT COUNT(*) AS n FROM graph_patches WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            == 0
        )
    finally:
        runtime.close()


def test_storage_failure_rolls_back_entire_trusted_large_patch(tmp_path: Path):
    runtime = VerifiedProgressRuntime(str(tmp_path / "large-atomic.sqlite"))
    try:
        graph_id = runtime.create_graph(owner_pid="compiler").graph_id
        operations = _node_ops(graph_id, MAX_PATCH_OPS + 1)
        runtime.store.conn.executescript(
            f"""
            CREATE TRIGGER fail_late_large_projection
            BEFORE INSERT ON graph_nodes_projection
            WHEN NEW.node_id = 'node-{MAX_PATCH_OPS}'
            BEGIN
                SELECT RAISE(ABORT, 'forced late large-patch failure');
            END;
            """
        )
        tables = (
            "graph_idempotency",
            "graph_patches",
            "graph_versions",
            "graph_events",
            "graph_nodes_projection",
            "graph_node_history",
            "graph_projection_snapshots",
        )
        baseline = {
            table: runtime.store.conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            for table in tables
        }

        with pytest.raises(
            sqlite3.IntegrityError,
            match="forced late large-patch failure",
        ):
            runtime.submit_patch(
                _proposal(graph_id, operations, key="trusted-storage-failure"),
                _allow_large_operations=True,
            )

        assert runtime.get_graph(graph_id).current_version == 0
        assert runtime.snapshot_projection(graph_id) == ({}, [])
        for table in tables:
            count = runtime.store.conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()["n"]
            assert count == baseline[table], table
    finally:
        runtime.close()

