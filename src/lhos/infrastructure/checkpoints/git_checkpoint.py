"""Git checkpoint manager (spec 16.2).

- before a node runs: record HEAD;
- after a node verifies: create a commit whose message carries
  run_id / node_id / attempt;
- on failure the runtime can reset to the previous verified commit.

The workspace must be a git repository (``git init`` is run if missing).
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path


class GitCheckpointManager:
    checkpoint_type = "git"

    def __init__(self, workspace_dir: str, db=None):
        self._workspace = Path(workspace_dir)
        self._db = db

    def _git(self, *args: str) -> str:
        proc = subprocess.run(
            ["git", *args],
            cwd=self._workspace,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
        return proc.stdout.strip()

    def _ensure_repo(self) -> None:
        if not (self._workspace / ".git").exists():
            self._workspace.mkdir(parents=True, exist_ok=True)
            self._git("init")
            # Deterministic local identity so commits work in fresh temp repos.
            self._git("config", "user.name", "lhos")
            self._git("config", "user.email", "lhos@localhost")
            self._git("commit", "--allow-empty", "-m", "lhos: init")

    def create(self, run_id: str, reason: str) -> str:
        """Post-verification commit (spec 16.2): message carries
        run_id / node_id / attempt via the reason string."""
        self._ensure_repo()
        self._git("add", "-A")
        # --allow-empty keeps checkpoints meaningful even when nothing changed.
        self._git("commit", "--allow-empty", "-m", f"lhos: run={run_id} {reason}")
        commit = self._git("rev-parse", "HEAD")
        self._record(run_id, commit, reason)
        return commit

    def record(self, run_id: str, reason: str) -> str:
        """Pre-execution snapshot (spec 16.2): record HEAD, do NOT commit."""
        return self.head()

    def restore(self, checkpoint_id: str) -> None:
        self._git("reset", "--hard", checkpoint_id)
        # reset alone leaves untracked files (e.g. written by the failed node);
        # clean makes the worktree match the checkpoint exactly.
        self._git("clean", "-fd")

    def head(self) -> str:
        self._ensure_repo()
        return self._git("rev-parse", "HEAD")

    def _record(self, run_id: str, commit: str, reason: str) -> None:
        if self._db is None:
            return
        self._db.conn.execute(
            "INSERT INTO checkpoints(id, run_id, checkpoint_type, location, "
            "manifest_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                commit,
                run_id,
                self.checkpoint_type,
                str(self._workspace),
                json.dumps({"reason": reason, "commit": commit}),
                datetime.now().astimezone().isoformat(),
            ),
        )
