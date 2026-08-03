"""Crash recovery and resume (spec 16.3, 26.2): kill the runtime mid-run,
resume, and never re-execute verified nodes."""

from __future__ import annotations

import pytest

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.errors import SimulatedCrashError
from lhos.domain.events import EventType


def _spec():
    def node(temp_id, title, script_extra=None):
        script = {
            "summary": f"{title} done",
            "produced_artifacts": [{"path": f"{temp_id}.txt", "content": temp_id}],
        }
        if script_extra:
            script.update(script_extra)
        return {
            "temp_id": temp_id,
            "kind": "subtask",
            "title": title,
            "specification": f"Produce {temp_id}.txt",
            "schedulable": True,
            "progress_weight": 1.0,
            "verification_spec": {"type": "file_exists", "path": f"{temp_id}.txt"},
            "metadata": {"script": script},
        }

    return {
        "goal": "resume test",
        "nodes": [
            node("n1", "First"),
            node("n2", "Second"),
            node("n3", "Third", {"crash_on_attempt": 1}),
        ],
        "edges": [
            {"source": "n2", "target": "n1", "kind": "depends_on"},
            {"source": "n3", "target": "n2", "kind": "depends_on"},
        ],
    }


def _config(tmp_path):
    return {
        "checkpoint": {"type": "filesystem", "after_verified_node": True},
        "checkpoint_root": str(tmp_path / "checkpoints"),
        "runtime": {"lease_seconds": 60},
    }


def _start(stack: RuntimeStack, run_id: str):
    spec = _spec()
    stack.graph_store.create_run(run_id, spec["goal"], {})
    stack.initial_builder.build(run_id, spec)


def test_kill_mid_run_then_resume_does_not_reexecute_verified(tmp_path):
    db_path = tmp_path / "lhos.db"
    workspace = tmp_path / "ws"
    run_id = "run-resume"

    # --- First process: dies while executing n3. ---
    stack = RuntimeStack(db_path, workspace, config=_config(tmp_path))
    _start(stack, run_id)
    with pytest.raises(SimulatedCrashError):
        stack.controller.run(run_id)

    assert stack.graph_store.get_node(f"{run_id}:n1").state == NodeState.VERIFIED
    assert stack.graph_store.get_node(f"{run_id}:n2").state == NodeState.VERIFIED
    # n3 was left mid-flight by the crash.
    assert stack.graph_store.get_node(f"{run_id}:n3").state == NodeState.RUNNING
    stack.close()

    # --- Second process: recover and resume. ---
    stack2 = RuntimeStack(db_path, workspace, config=_config(tmp_path))
    try:
        run = stack2.controller.resume(run_id)
        assert run.status == "completed"

        n1 = stack2.graph_store.get_node(f"{run_id}:n1")
        n2 = stack2.graph_store.get_node(f"{run_id}:n2")
        n3 = stack2.graph_store.get_node(f"{run_id}:n3")
        assert n1.state == NodeState.VERIFIED
        assert n2.state == NodeState.VERIFIED
        assert n3.state == NodeState.VERIFIED

        # Verified nodes were never re-executed (spec 16.3 acceptance).
        assert n1.attempt_count == 1
        assert n2.attempt_count == 1
        assert len(stack2.graph_store.list_executions(run_id, n1.id)) == 1
        assert len(stack2.graph_store.list_executions(run_id, n2.id)) == 1

        started = [
            e
            for e in stack2.event_store.list_events(run_id)
            if e.event_type == EventType.EXECUTION_STARTED and e.payload.get("node_id") == n1.id
        ]
        assert len(started) == 1

        # The crashed node recovered through FAILED and retried (attempt 2
        # succeeded: crash_on_attempt only fires on attempt 1).
        assert n3.attempt_count == 2

        # Recovery was recorded.
        resumed = [
            e
            for e in stack2.event_store.list_events(run_id)
            if e.event_type == EventType.RUN_RESUMED
        ]
        assert len(resumed) == 1
        assert resumed[0].payload["recovery"]["recovered_running_nodes"] == 1

        # Workspace artifacts survived the crash.
        for name in ("n1.txt", "n2.txt", "n3.txt"):
            assert (workspace / name).exists()
    finally:
        stack2.close()
