"""Filesystem checkpoint: tar snapshot + manifest with hashes (spec 16.2).

Snapshots a working directory into ``<checkpoint_root>/<run_id>/<id>.tar.gz``
plus a ``<id>.manifest.json`` listing every file's sha256. The database is NOT
copied — the graph and event log recover independently from SQLite.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FilesystemCheckpointManager:
    checkpoint_type = "filesystem"

    def __init__(self, workspace_dir: str, checkpoint_root: str, db=None):  # noqa: ANN001
        self._workspace = Path(workspace_dir)
        self._root = Path(checkpoint_root)
        self._db = db

    def _snapshot_files(self) -> dict[str, str]:
        manifest: dict[str, str] = {}
        if not self._workspace.exists():
            return manifest
        for path in sorted(self._workspace.rglob("*")):
            if path.is_file():
                rel = str(path.relative_to(self._workspace))
                manifest[rel] = _sha256(path)
        return manifest

    def create(self, run_id: str, reason: str) -> str:
        checkpoint_id = f"fs-{uuid4().hex[:12]}"
        run_dir = self._root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        tar_path = run_dir / f"{checkpoint_id}.tar.gz"
        manifest = {
            "checkpoint_id": checkpoint_id,
            "run_id": run_id,
            "reason": reason,
            "workspace": str(self._workspace),
            "files": self._snapshot_files(),
            "created_at": datetime.now().astimezone().isoformat(),
        }
        self._workspace.mkdir(parents=True, exist_ok=True)
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(self._workspace, arcname=".")
        manifest_path = run_dir / f"{checkpoint_id}.manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        self._record(run_id, checkpoint_id, str(tar_path), manifest)
        return checkpoint_id

    def record(self, run_id: str, reason: str) -> str:
        """Pre-execution snapshot: for the filesystem manager this is a full
        tar snapshot, identical to ``create``."""
        return self.create(run_id, reason)

    def restore(self, checkpoint_id: str) -> None:
        matches = list(self._root.glob(f"*/{checkpoint_id}.tar.gz"))
        if not matches:
            raise FileNotFoundError(f"checkpoint {checkpoint_id} not found")
        # Clear the workspace first so files created after the snapshot
        # disappear (true restore semantics, spec 16.2).
        if self._workspace.exists():
            for entry in self._workspace.iterdir():
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
        self._workspace.mkdir(parents=True, exist_ok=True)
        with tarfile.open(matches[0], "r:gz") as tar:
            tar.extractall(self._workspace, filter="data")

    def _record(self, run_id: str, checkpoint_id: str, location: str, manifest: dict) -> None:
        if self._db is None:
            return
        self._db.conn.execute(
            "INSERT INTO checkpoints(id, run_id, checkpoint_type, location, "
            "manifest_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                checkpoint_id,
                run_id,
                self.checkpoint_type,
                location,
                json.dumps(manifest, sort_keys=True),
                manifest["created_at"],
            ),
        )
