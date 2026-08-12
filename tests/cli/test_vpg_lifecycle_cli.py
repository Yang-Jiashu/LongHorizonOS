"""Operator CLI tests for VPG history lifecycle commands."""

from __future__ import annotations

import json
import sqlite3

from lhos.cli import core
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _add_task(runtime: VerifiedProgressRuntime, graph_id: str, node_id: str) -> None:
    runtime.submit_patch(
        GraphPatchProposal(
            graph_id=graph_id,
            expected_graph_version=runtime.get_graph(graph_id).current_version,
            author_pid="operator",
            idempotency_key=f"add-{node_id}",
            operations=(
                AddNodeOp(
                    node_id=node_id,
                    graph_id=graph_id,
                    node_type="task",
                    created_by_pid="operator",
                    title=f"task-{node_id}",
                ),
            ),
        )
    )


def _build_db(tmp_path, *, graph_id: str = "cli-graph") -> tuple[str, str]:
    db = tmp_path / "vpg.sqlite"
    runtime = VerifiedProgressRuntime(str(db))
    runtime.create_graph(owner_pid="operator", graph_id=graph_id)
    for node_id in ("a", "b", "c"):
        _add_task(runtime, graph_id, node_id)
    runtime.close()
    return str(db), graph_id


def _simulate_snapshotless_legacy(db: str, graph_id: str) -> None:
    conn = sqlite3.connect(db)
    conn.execute(
        "DELETE FROM graph_projection_snapshots WHERE graph_id = ? AND version > 0",
        (graph_id,),
    )
    conn.execute("DELETE FROM graph_node_history WHERE graph_id = ?", (graph_id,))
    conn.execute("DELETE FROM graph_edge_history WHERE graph_id = ?", (graph_id,))
    conn.execute(
        "UPDATE graph_versions SET projection_hash = 'legacy-untrusted-hash' "
        "WHERE graph_id = ? AND version > 0",
        (graph_id,),
    )
    conn.commit()
    conn.close()


def test_vpg_history_json_reports_retention_contract(tmp_path, capsys) -> None:
    db, graph_id = _build_db(tmp_path)

    assert (
        core.main(
            ["vpg", "history", "--db", db, "--graph", graph_id, "--json"],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["graph_id"] == graph_id
    assert payload["current_version"] == 3
    assert payload["earliest_recoverable_version"] == 0
    assert payload["lifecycle_events"] == []


def test_vpg_compact_requires_confirmation_and_emits_json(tmp_path, capsys) -> None:
    db, graph_id = _build_db(tmp_path)

    assert (
        core.main(
            [
                "vpg",
                "compact",
                "--db",
                db,
                "--graph",
                graph_id,
                "--retain-from",
                "2",
                "--actor",
                "operator",
                "--reason",
                "keep recent recovery window",
                "--json",
            ],
        )
        == 2
    )
    error_payload = json.loads(capsys.readouterr().out)
    assert error_payload["code"] == "CLI_ERROR"

    assert (
        core.main(
            [
                "vpg",
                "compact",
                "--db",
                db,
                "--graph",
                graph_id,
                "--retain-from",
                "2",
                "--actor",
                "operator",
                "--reason",
                "keep recent recovery window",
                "--yes",
                "--json",
            ],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["operation"] == "compact"
    assert payload["earliest_recoverable_version"] == 2
    assert payload["deleted_snapshot_headers"] == 2


def test_vpg_migrate_legacy_preview_is_read_only(tmp_path, capsys) -> None:
    db, graph_id = _build_db(tmp_path, graph_id="legacy-cli")
    _simulate_snapshotless_legacy(db, graph_id)

    assert (
        core.main(
            [
                "vpg",
                "migrate-legacy",
                "--db",
                db,
                "--graph",
                graph_id,
                "--json",
            ],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["migrated"] is False
    assert payload["write_required"] is True
    assert payload["current_version"] == 3
    assert payload["node_count"] == 3

    conn = sqlite3.connect(db)
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE graph_id = ? AND version > 0",
            (graph_id,),
        ).fetchone()[0]
        == 0
    )
    conn.close()


def test_vpg_migrate_legacy_requires_all_trust_arguments(tmp_path, capsys) -> None:
    db, graph_id = _build_db(tmp_path, graph_id="legacy-required")
    _simulate_snapshotless_legacy(db, graph_id)

    assert (
        core.main(
            [
                "vpg",
                "migrate",
                "--db",
                db,
                "--graph",
                graph_id,
                "--trust-projection",
                "--json",
            ],
        )
        == 2
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "CLI_ERROR"


def test_vpg_migrate_legacy_trusts_hash_bound_preview(tmp_path, capsys) -> None:
    db, graph_id = _build_db(tmp_path, graph_id="legacy-write")
    _simulate_snapshotless_legacy(db, graph_id)

    assert (
        core.main(
            [
                "vpg",
                "trusted-migration",
                "--db",
                db,
                "--graph",
                graph_id,
                "--json",
            ],
        )
        == 0
    )
    preview = json.loads(capsys.readouterr().out)

    assert (
        core.main(
            [
                "vpg",
                "trusted-migration",
                "--db",
                db,
                "--graph",
                graph_id,
                "--trust-projection",
                "--expected-version",
                str(preview["current_version"]),
                "--expected-hash",
                preview["projection_hash"],
                "--actor",
                "operator",
                "--reason",
                "verified from signed export",
                "--json",
            ],
        )
        == 0
    )
    migrated = json.loads(capsys.readouterr().out)
    assert migrated["migrated"] is True
    assert migrated["write_required"] is False

    assert (
        core.main(
            [
                "vpg",
                "history",
                "--db",
                db,
                "--graph",
                graph_id,
                "--json",
            ],
        )
        == 0
    )
    history = json.loads(capsys.readouterr().out)
    assert history["earliest_recoverable_version"] == preview["current_version"]
    assert history["updated_by"] == "operator"


def test_vpg_commands_fail_without_creating_missing_database(tmp_path, capsys) -> None:
    missing = tmp_path / "does-not-exist.sqlite"

    assert (
        core.main(
            [
                "vpg",
                "history",
                "--db",
                str(missing),
                "--graph",
                "missing",
                "--json",
            ],
        )
        == 2
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "CLI_ERROR"
    assert not missing.exists()
