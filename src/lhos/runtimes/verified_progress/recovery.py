"""Crash recovery for the Verified Progress Graph materialized projection.

Patch rows and graph versions are still validated as an audit chain, but the
projection itself is restored from the immutable, per-version snapshot written
atomically with each commit.  Patch operations are not a lossless projection
encoding: timestamps and externally supplied Evidence payloads cannot always be
reconstructed byte-for-byte from ``operations_json`` alone.
"""

from __future__ import annotations

import json

from .errors import VPGCode, VPGError
from .events import GraphEvent, GraphEventType
from .graph_store import GraphStore
from .models import GraphRecord
from .patches import GraphPatchProposal
from .sdk import _normalize_raw_ops


def validate_recovery_history(store: GraphStore, graph_id: str) -> GraphRecord:
    """Validate the GraphVersion/Patch audit chain without touching the cache."""

    record = store.get_record(graph_id)
    if record is None:
        raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
    retention = store.get_history_retention_contract(graph_id)
    earliest_recoverable_version = retention.earliest_recoverable_version

    version_rows = store.conn.execute(
        "SELECT version, parent_version, patch_id, projection_hash "
        "FROM graph_versions WHERE graph_id = ? ORDER BY version",
        (graph_id,),
    ).fetchall()
    actual_versions = [int(row["version"]) for row in version_rows]
    expected_versions = list(range(record.current_version + 1))
    if actual_versions != expected_versions:
        raise VPGError(
            VPGCode.GRAPH_RECOVERY_FAILED,
            "graph version history is not contiguous: "
            f"expected {expected_versions!r}, got {actual_versions!r}",
        )

    for version, row in enumerate(version_rows):
        expected_parent = None if version == 0 else version - 1
        if row["parent_version"] != expected_parent:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"graph version {version} has parent {row['parent_version']!r}, "
                f"expected {expected_parent!r}",
            )
        if version == 0 and row["patch_id"] != "init":
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"graph version 0 references patch {row['patch_id']!r}, expected 'init'",
            )

    # Every *retained* committed version must have an immutable snapshot
    # header. Versions below the explicit retention floor are intentionally
    # inaccessible after compaction/trusted migration and are not required to
    # retain snapshot headers. Header hashes are checked against their
    # corresponding GraphVersion rows for the retained range.
    snapshot_rows = store.conn.execute(
        "SELECT v.version, v.projection_hash AS version_hash, "
        "s.projection_hash AS snapshot_hash "
        "FROM graph_versions AS v "
        "LEFT JOIN graph_projection_snapshots AS s "
        "  ON s.graph_id = v.graph_id AND s.version = v.version "
        "WHERE v.graph_id = ? ORDER BY v.version",
        (graph_id,),
    ).fetchall()
    for row in snapshot_rows:
        if int(row["version"]) < earliest_recoverable_version:
            continue
        if row["snapshot_hash"] is None:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"durable projection snapshot header is missing for graph "
                f"{graph_id!r} version {row['version']}",
            )
        if str(row["snapshot_hash"]) != str(row["version_hash"]):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"projection snapshot header hash does not match GraphVersion "
                f"for graph {graph_id!r} version {row['version']}",
            )
    extra_snapshot = store.conn.execute(
        "SELECT s.version FROM graph_projection_snapshots AS s "
        "LEFT JOIN graph_versions AS v "
        "  ON v.graph_id = s.graph_id AND v.version = s.version "
        "WHERE s.graph_id = ? AND v.version IS NULL LIMIT 1",
        (graph_id,),
    ).fetchone()
    if extra_snapshot is not None:
        raise VPGError(
            VPGCode.GRAPH_RECOVERY_FAILED,
            f"projection snapshot references unknown graph version {extra_snapshot['version']}",
        )
    patch_rows = store.conn.execute(
        "SELECT patch_id, graph_id, committed_version, author_pid, "
        "idempotency_key, operations_json "
        "FROM graph_patches WHERE graph_id = ? ORDER BY committed_version",
        (graph_id,),
    ).fetchall()
    if len(patch_rows) != record.current_version:
        raise VPGError(
            VPGCode.GRAPH_RECOVERY_FAILED,
            f"graph version is {record.current_version}, but patch history "
            f"contains {len(patch_rows)} rows",
        )

    for committed_version, row in enumerate(patch_rows, start=1):
        if int(row["committed_version"]) != committed_version:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "patch history is not contiguous: "
                f"expected committed version {committed_version}, got "
                f"{row['committed_version']}",
            )
        try:
            raw = json.loads(row["operations_json"])
            raw["operations"] = _normalize_raw_ops(raw.get("operations", []))
            patch = GraphPatchProposal(**raw)
        except Exception as exc:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"cannot decode committed patch {row['patch_id']!r}",
            ) from exc

        if patch.patch_id != row["patch_id"]:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"patch row key {row['patch_id']!r} does not match payload "
                f"patch_id {patch.patch_id!r}",
            )
        if patch.graph_id != graph_id or row["graph_id"] != graph_id:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"patch {patch.patch_id!r} belongs to graph "
                f"{patch.graph_id!r}, expected {graph_id!r}",
            )
        if patch.expected_graph_version != committed_version - 1:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"patch {patch.patch_id!r} payload expects base version "
                f"{patch.expected_graph_version}, expected {committed_version - 1}",
            )
        if patch.author_pid != row["author_pid"] or patch.idempotency_key != row["idempotency_key"]:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"patch {patch.patch_id!r} row metadata does not match its payload",
            )
        if version_rows[committed_version]["patch_id"] != patch.patch_id:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"graph version {committed_version} references patch "
                f"{version_rows[committed_version]['patch_id']!r}, "
                f"expected {patch.patch_id!r}",
            )

    return record


def verify_and_recover(
    store: GraphStore,
    graph_id: str,
    *,
    facts_artifact=None,
    facts_kernel=None,
) -> tuple[list[GraphEvent], GraphRecord]:
    """Validate durable history and restore the latest materialized snapshot.

    The facts providers remain accepted for API compatibility.  Recovery does
    not re-evaluate mutable external facts; doing so would manufacture a state
    different from the state committed at the target GraphVersion.
    """
    del facts_artifact, facts_kernel

    record = validate_recovery_history(store, graph_id)

    started = GraphEvent(
        graph_id=graph_id,
        event_type=GraphEventType.GRAPH_RECOVERY_STARTED,
        subject_id=graph_id,
        payload={"current_version": record.current_version},
    )

    previous_node_count = int(
        store.conn.execute(
            "SELECT COUNT(*) AS count FROM graph_nodes_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()["count"]
    )
    previous_edge_count = int(
        store.conn.execute(
            "SELECT COUNT(*) AS count FROM graph_edges_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()["count"]
    )

    # load_projection_snapshot verifies row identity, endpoint closure and both
    # the snapshot-header hash and GraphVersion hash before any cache row is
    # deleted.  replace_projection then performs the delete+insert atomically.
    rebuilt_nodes, rebuilt_edges = store.load_projection_snapshot(
        graph_id,
        record.current_version,
    )
    store.replace_projection(
        graph_id,
        expected_graph_version=record.current_version,
        nodes=rebuilt_nodes.values(),
        edges=rebuilt_edges,
    )

    committed_events = [
        event
        for event in store.get_events(graph_id, record.current_version)
        if event.graph_version == record.current_version
    ]
    completed = GraphEvent(
        graph_id=graph_id,
        event_type=GraphEventType.GRAPH_RECOVERY_COMPLETED,
        subject_id=graph_id,
        payload={
            "current_version": record.current_version,
            "previous_materialized_node_count": previous_node_count,
            "previous_materialized_edge_count": previous_edge_count,
            "materialized_node_count": len(rebuilt_nodes),
            "materialized_edge_count": len(rebuilt_edges),
        },
    )
    return [started, *committed_events, completed], record
