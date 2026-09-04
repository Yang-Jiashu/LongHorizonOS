from __future__ import annotations

import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts import lhtb_external_checkpoint as checkpoint


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_invocation_active_time_sums_finished_and_running(tmp_path: Path) -> None:
    trial = tmp_path / "trial"
    root = trial / "agent" / "invocations"
    _write_json(
        root / "invocation-0001" / "invocation.json",
        {"invocation": 1, "status": "completed", "elapsed_ms": 1250},
    )
    _write_json(
        root / "invocation-0002" / "invocation.json",
        {
            "invocation": 2,
            "status": "running",
            "started_at": "2026-08-23T00:00:00+00:00",
        },
    )

    active, running, count, completed = checkpoint._invocation_active_time(
        trial,
        observed_at=datetime(2026, 8, 23, 0, 0, 10, tzinfo=UTC),
    )

    assert active == pytest.approx(11.25)
    assert completed == pytest.approx(1.25)
    assert running == 2
    assert count == 2


def test_invocation_active_time_rejects_two_running_records(tmp_path: Path) -> None:
    trial = tmp_path / "trial"
    root = trial / "agent" / "invocations"
    for invocation in (1, 2):
        _write_json(
            root / f"invocation-{invocation:04d}" / "invocation.json",
            {
                "invocation": invocation,
                "status": "running",
                "started_at": "2026-08-23T00:00:00+00:00",
            },
        )

    with pytest.raises(RuntimeError, match="multiple invocations"):
        checkpoint._invocation_active_time(
            trial,
            observed_at=datetime(2026, 8, 23, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("active", "completed", "running", "expected"),
    [
        (99.9, 90.0, 2, ("pending", None)),
        (
            100.0,
            100.0,
            None,
            ("late_unavailable", "cutoff_crossed_without_running_invocation"),
        ),
        (
            105.0,
            101.0,
            3,
            ("late_unavailable", "watcher_missed_cutoff_before_current_invocation"),
        ),
        (100.1, 90.0, 3, ("due", None)),
    ],
)
def test_capture_readiness_only_accepts_cutoff_inside_current_invocation(
    tmp_path: Path,
    active: float,
    completed: float,
    running: int | None,
    expected: tuple[str, str | None],
) -> None:
    trial = checkpoint.ActiveTrial(
        job_root=tmp_path / "job",
        trial_dir=tmp_path / "job" / "trial",
        trial_name="trial",
        active_seconds=active,
        completed_active_seconds=completed,
        running_invocation=running,
        invocation_count=3,
    )

    assert checkpoint._capture_readiness(trial, 100) == expected


def test_capture_readiness_subtracts_prior_instrumentation_freezes(
    tmp_path: Path,
) -> None:
    trial = checkpoint.ActiveTrial(
        job_root=tmp_path / "job",
        trial_dir=tmp_path / "job" / "trial",
        trial_name="trial",
        active_seconds=115.0,
        completed_active_seconds=105.0,
        running_invocation=3,
        invocation_count=3,
    )

    assert checkpoint._capture_readiness(
        trial,
        100,
        excluded_active_seconds=10.0,
        excluded_completed_seconds=10.0,
    ) == ("due", None)
    assert checkpoint._capture_readiness(
        trial,
        110,
        excluded_active_seconds=10.0,
        excluded_completed_seconds=10.0,
    ) == ("pending", None)


def test_freeze_offsets_assign_completed_time_by_invocation(tmp_path: Path) -> None:
    spec = checkpoint.ArmSpec(
        task_name="task",
        arm="dsh_fresh",
        job_name="job",
        config_path=tmp_path / "config.yaml",
        config_sha256="a" * 64,
        docker_image="image:tag",
        docker_image_id="sha256:" + "b" * 64,
        task_root=tmp_path / "task",
        task_content_sha256="c" * 64,
        credential_env="STEPFUN_API_KEY",
        workspace=checkpoint.DEFAULT_WORKSPACE,
    )
    ledger_root = tmp_path / "_capture-freezes" / "task" / "dsh_fresh" / "trial"
    _write_json(
        ledger_root / "old.json",
        {
            "schema_version": checkpoint.FREEZE_SCHEMA,
            "running_invocation": 2,
            "freeze_wall_seconds": 1.5,
        },
    )
    _write_json(
        ledger_root / "current.json",
        {
            "schema_version": checkpoint.FREEZE_SCHEMA,
            "running_invocation": 3,
            "freeze_wall_seconds": 2.5,
        },
    )
    trial = checkpoint.ActiveTrial(
        job_root=tmp_path / "job",
        trial_dir=tmp_path / "job" / "trial",
        trial_name="trial",
        active_seconds=300.0,
        completed_active_seconds=250.0,
        running_invocation=3,
        invocation_count=3,
    )

    assert checkpoint._freeze_offsets(tmp_path, spec, trial) == pytest.approx((4.0, 1.5))


def test_safe_tar_path_rejects_escape_and_normalizes_dot_prefix() -> None:
    assert checkpoint._safe_tar_path("./src/main.py") == "src/main.py"
    assert checkpoint._safe_tar_path(".") == "."
    with pytest.raises(RuntimeError, match="unsafe path"):
        checkpoint._safe_tar_path("../../etc/passwd")
    with pytest.raises(RuntimeError, match="unsafe path"):
        checkpoint._safe_tar_path("/etc/passwd")


def test_workspace_link_validation_rejects_escape_and_nested_members() -> None:
    with pytest.raises(RuntimeError, match="escapes /app"):
        checkpoint._validate_workspace_entries(
            [
                {
                    "path": "link",
                    "type": "symlink",
                    "linkname": "../../etc",
                }
            ]
        )
    with pytest.raises(RuntimeError, match="nested below a symlink"):
        checkpoint._validate_workspace_entries(
            [
                {"path": "target", "type": "directory"},
                {"path": "link", "type": "symlink", "linkname": "target"},
                {"path": "link/payload", "type": "file"},
            ]
        )
    with pytest.raises(RuntimeError, match="unsafe special file"):
        checkpoint._validate_workspace_entries([{"path": "device", "type": "character_device"}])
    with pytest.raises(RuntimeError, match="hardlink cycle"):
        checkpoint._validate_workspace_entries(
            [
                {"path": "a", "type": "hardlink", "linkname": "b"},
                {"path": "b", "type": "hardlink", "linkname": "a"},
            ]
        )


def test_workspace_link_validation_accepts_internal_relative_symlink() -> None:
    checkpoint._validate_workspace_entries(
        [
            {"path": "outputs", "type": "directory"},
            {"path": "outputs/result", "type": "file"},
            {"path": "links", "type": "directory"},
            {
                "path": "links/latest",
                "type": "symlink",
                "linkname": "../outputs/result",
            },
        ]
    )


def test_materialize_snapshot_round_trips_content_addressed_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "checkpoints"
    snapshot = root / "task" / "dsh_fresh" / "active-00003600"
    blobs = root / "_blobs" / "sha256"
    blobs.mkdir(parents=True)
    content = b"checkpoint payload\n"
    digest = hashlib.sha256(content).hexdigest()
    (blobs / digest).write_bytes(content)
    entries = [
        {
            "path": ".",
            "type": "directory",
            "mode": 0o755,
            "uid": 0,
            "gid": 0,
            "uname": "root",
            "gname": "root",
            "mtime": 1.0,
        },
        {
            "path": "outputs/result.txt",
            "type": "file",
            "mode": 0o644,
            "uid": 1000,
            "gid": 1000,
            "uname": "",
            "gname": "",
            "mtime": 2.0,
            "size": len(content),
            "sha256": digest,
        },
        {
            "path": "latest",
            "type": "symlink",
            "mode": 0o777,
            "uid": 1000,
            "gid": 1000,
            "uname": "",
            "gname": "",
            "mtime": 3.0,
            "linkname": "outputs/result.txt",
        },
    ]
    canonical = json.dumps(
        entries,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    tree = hashlib.sha256(canonical).hexdigest()
    workspace = {
        "schema_version": checkpoint.WORKSPACE_SCHEMA,
        "workspace": "/app",
        "entries": entries,
        "tree_sha256": tree,
        "blob_store_relative_to_snapshot": "../../../_blobs/sha256",
    }
    metadata = {
        "schema_version": checkpoint.SNAPSHOT_SCHEMA,
        "snapshot_id": "fixture",
        "checkpoint_root": str(root),
        "workspace_tree_sha256": tree,
    }
    _write_json(snapshot / "workspace-manifest.json", workspace)
    _write_json(snapshot / "metadata.json", metadata)

    archive_path = tmp_path / "workspace.tar"
    result = checkpoint.materialize_snapshot(snapshot, archive_path)

    assert result["archive_sha256"] == checkpoint._sha256_file(archive_path)
    with tarfile.open(archive_path) as archive:
        assert archive.extractfile("./outputs/result.txt").read() == content
        assert archive.getmember("./latest").issym()
        assert archive.getmember("./latest").linkname == "outputs/result.txt"


def test_replay_config_replaces_agent_and_adds_read_only_archive_mount(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original.yaml"
    original.write_text(
        """
job_name: original
n_attempts: 1
environment:
  delete: false
  mounts:
    - type: bind
      source: D:/runtime
      target: /opt/runtime
agents:
  - import_path: original:Agent
    model_name: provider/model
    kwargs: {arm: baseline}
datasets:
  - path: D:/tasks
    task_names: [fixture]
""".strip(),
        encoding="utf-8",
    )
    archive = tmp_path / "workspace.tar"

    replay = checkpoint._replay_config(
        original,
        archive_path=archive,
        archive_sha256="a" * 64,
        job_name="checkpoint-job",
    )

    assert replay["job_name"] == "checkpoint-job"
    assert replay["agents"][0]["import_path"] == checkpoint.RESTORE_AGENT
    assert replay["agents"][0]["env"] == {}
    assert replay["environment"]["mounts"][-1] == {
        "type": "bind",
        "source": archive.resolve().as_posix(),
        "target": "/opt/lhtb-checkpoint/workspace.tar",
        "read_only": True,
        "bind": {"create_host_path": False},
    }


def test_scan_and_store_blob_detects_secret_across_chunk_boundary(
    tmp_path: Path,
) -> None:
    secret = b"sensitive-value"
    prefix = b"x" * (1024 * 1024 - 5)
    payload = prefix + secret + b"tail"

    digest, hit = checkpoint._scan_and_store_blob(
        io.BytesIO(payload),
        blob_root=tmp_path / "blobs",
        expected_size=len(payload),
        secret=secret,
    )

    assert hit is True
    assert not (tmp_path / "blobs" / digest).exists()
