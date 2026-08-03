"""Phase 7: checkpoint restore wired into the controller (spec 16).

- FilesystemCheckpoint: verification failure with restore_on_failure rolls the
  workspace back to checkpoint_before (files written by the failed node
  disappear) and records CHECKPOINT_RESTORED.
- GitCheckpoint (skipped when git is unavailable): commit per verified node
  with run_id/node_id/attempt in the message; reset --hard on failure.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.events import EventType


def _spec_fail(tmp_name="n1"):
    return {
        "goal": "restore policy test",
        "nodes": [
            {
                "temp_id": "n1",
                "kind": "subtask",
                "title": "write bad file",
                "specification": "write bad.txt with bad content",
                "schedulable": True,
                "max_attempts": 1,
                "verification_spec": {
                    "type": "file_contains",
                    "path": "bad.txt",
                    "substring": "good",
                },
                "metadata": {
                    "script": {
                        "summary": "wrote bad.txt",
                        "produced_artifacts": [{"path": "bad.txt", "content": "bad content"}],
                    }
                },
            }
        ],
        "edges": [],
    }


def test_filesystem_restore_on_verification_failure(tmp_path):
    config = {
        "checkpoint": {"type": "filesystem", "restore_on_failure": True},
        "checkpoint_root": str(tmp_path / "ckpt"),
    }
    workspace = tmp_path / "ws"
    stack = RuntimeStack(tmp_path / "lhos.db", workspace, config=config)
    try:
        run_id = "run-restore"
        spec = _spec_fail()
        stack.graph_store.create_run(run_id, spec["goal"], {})
        stack.initial_builder.build(run_id, spec)
        run = stack.controller.run(run_id)

        assert run.status == "failed"
        node = stack.graph_store.get_node(f"{run_id}:n1")
        assert node.state == NodeState.FAILED

        # The failed node's write was rolled back.
        assert not (workspace / "bad.txt").exists()
        restored = [
            e
            for e in stack.event_store.list_events(run_id)
            if e.event_type == EventType.CHECKPOINT_RESTORED
        ]
        assert len(restored) == 1
        assert restored[0].payload["node_id"] == f"{run_id}:n1"
        assert restored[0].payload["reason"] == "verification failed"
    finally:
        stack.close()


GIT_AVAILABLE = shutil.which("git") is not None


def _git(workspace, *args):
    return subprocess.run(
        ["git", *args], cwd=workspace, capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.mark.skipif(not GIT_AVAILABLE, reason="git is not available")
def test_git_checkpoint_commit_per_verified_node_and_reset_on_failure(tmp_path):
    config = {
        "checkpoint": {
            "type": "git",
            "restore_on_failure": True,
            "after_verified_node": True,
        },
    }
    workspace = tmp_path / "repo"
    stack = RuntimeStack(tmp_path / "lhos.db", workspace, config=config)
    try:
        run_id = "run-git"
        spec = {
            "goal": "git checkpoint test",
            "nodes": [
                {
                    "temp_id": "n1",
                    "kind": "subtask",
                    "title": "good node",
                    "specification": "write a.txt",
                    "schedulable": True,
                    "verification_spec": {"type": "file_exists", "path": "a.txt"},
                    "metadata": {
                        "script": {
                            "summary": "wrote a.txt",
                            "produced_artifacts": [{"path": "a.txt", "content": "alpha"}],
                        }
                    },
                },
                {
                    "temp_id": "n2",
                    "kind": "subtask",
                    "title": "bad node",
                    "specification": "write bad.txt that fails verification",
                    "schedulable": True,
                    "max_attempts": 1,
                    "verification_spec": {
                        "type": "file_contains",
                        "path": "bad.txt",
                        "substring": "good",
                    },
                    "metadata": {
                        "script": {
                            "summary": "wrote bad.txt",
                            "produced_artifacts": [{"path": "bad.txt", "content": "bad"}],
                        }
                    },
                },
            ],
            "edges": [{"source": "n2", "target": "n1", "kind": "depends_on"}],
        }
        stack.graph_store.create_run(run_id, spec["goal"], {})
        stack.initial_builder.build(run_id, spec)
        run = stack.controller.run(run_id)

        assert run.status == "failed"
        assert stack.graph_store.get_node(f"{run_id}:n1").state == NodeState.VERIFIED
        assert stack.graph_store.get_node(f"{run_id}:n2").state == NodeState.FAILED

        # One commit per verified node, message carries run_id/node_id/attempt.
        log = _git(workspace, "log", "--format=%s")
        assert f"run={run_id}" in log
        assert f"verified:{run_id}:n1" in log
        assert "attempt1" in log

        # Failure reset the worktree to the last verified commit: bad.txt is
        # gone, a.txt stays, HEAD is the n1 verified commit.
        assert not (workspace / "bad.txt").exists()
        assert (workspace / "a.txt").exists()
        head = _git(workspace, "rev-parse", "HEAD")
        rows = stack.db.conn.execute(
            "SELECT location, manifest_json FROM checkpoints WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        assert any(head == r["location"] or head in r["manifest_json"] for r in rows)

        restored = [
            e
            for e in stack.event_store.list_events(run_id)
            if e.event_type == EventType.CHECKPOINT_RESTORED
        ]
        assert len(restored) == 1
        assert restored[0].payload["checkpoint_id"] == head
    finally:
        stack.close()
