"""Phase 5 acceptance (spec 15, 27): mid-run invalidation wired into the loop.

- Changing an upstream artifact only affects the downstream subgraph;
- unrelated branches stay VERIFIED;
- affected vs replanned vs re-executed scope is recorded and queryable;
- must-invalidate (必然破坏) is decided deterministically:
  CONSTRAINT_CHANGED with ``invalidates: [...]`` or artifact ``removed: true``
  → INVALIDATED; everything else → STALE.
"""

from __future__ import annotations

import hashlib

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.events import EventType
from lhos.graph.invalidation import invalidation_metrics


def _subtask(temp_id, script, verification=None, extra=None):
    node = {
        "temp_id": temp_id,
        "kind": "subtask",
        "title": temp_id,
        "specification": f"do {temp_id}",
        "schedulable": True,
        "progress_weight": 1.0,
        "verification_spec": verification or {"type": "file_exists", "path": f"{temp_id}.txt"},
        "metadata": {"script": {"summary": f"{temp_id} done", **script}},
    }
    if extra:
        node.update(extra)
    return node


def _write(path, content):
    return {"produced_artifacts": [{"path": path, "content": content}]}


def _artifact(temp_id, path):
    return {
        "temp_id": temp_id,
        "kind": "artifact",
        "title": temp_id,
        "specification": f"artifact {temp_id}",
        "schedulable": False,
        "metadata": {"path": path},
    }


def _constraint(temp_id):
    return {
        "temp_id": temp_id,
        "kind": "constraint",
        "title": temp_id,
        "specification": f"constraint {temp_id}",
        "schedulable": False,
    }


def _start(stack: RuntimeStack, spec: dict, run_id: str):
    stack.graph_store.create_run(run_id, spec["goal"], {})
    stack.initial_builder.build(run_id, spec)
    return stack.controller.run(run_id)


def _events_by_type(stack: RuntimeStack, run_id: str, event_type: str):
    return [
        e for e in stack.event_store.list_events(run_id) if e.event_type == event_type
    ]


def _chain_spec(env_event):
    """p1 --produces--> art <--consumes-- c1 <--depends-- x (env injector);
    u1 is a fully unrelated branch."""
    return {
        "goal": "invalidation loop test",
        "nodes": [
            _artifact("art", "art.txt"),
            _constraint("k"),
            _subtask("p1", _write("art.txt", "v1-content"),
                     verification={"type": "file_exists", "path": "art.txt"}),
            _subtask("c1", _write("c1.txt", "c1"),
                     verification={"type": "file_exists", "path": "c1.txt"}),
            _subtask("x", {**_write("x.txt", "x"), "environment_events": [env_event]},
                     verification={"type": "file_exists", "path": "x.txt"}),
            _subtask("u1", _write("u1.txt", "u1"),
                     verification={"type": "file_exists", "path": "u1.txt"}),
        ],
        "edges": [
            {"source": "p1", "target": "art", "kind": "produces"},
            {"source": "c1", "target": "art", "kind": "consumes"},
            {"source": "c1", "target": "p1", "kind": "depends_on"},
            {"source": "x", "target": "c1", "kind": "depends_on"},
        ],
    }


def test_artifact_change_affects_only_downstream_subgraph(tmp_path):
    """§27 Phase 5: upstream artifact change → only downstream goes STALE;
    the unrelated branch keeps VERIFIED and is never re-executed."""
    run_id = "run-inv1"
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _chain_spec(
            {"type": "artifact_updated", "node_id": f"{run_id}:art",
             "new_hash": "external-v2", "reason": "external edit"}
        )
        run = _start(stack, spec, run_id)
        assert run.status == "completed"

        # Exactly one propagation, scoped to c1 and x — never u1.
        propagations = _events_by_type(stack, run_id, EventType.INVALIDATION_PROPAGATED)
        assert len(propagations) == 1
        payload = propagations[0].payload
        assert set(payload["affected_node_ids"]) == {f"{run_id}:c1", f"{run_id}:x"}
        assert payload["invalidated_node_ids"] == []
        assert payload["replanned_count"] == 0

        # Unrelated branch: verified once, never marked stale, never re-run.
        u1 = stack.graph_store.get_node(f"{run_id}:u1")
        assert u1.state == NodeState.VERIFIED
        assert u1.attempt_count == 1
        stale_marks = _events_by_type(stack, run_id, EventType.NODE_MARKED_STALE)
        assert all(e.payload.get("node_id") != f"{run_id}:u1" for e in stale_marks)

        # Downstream nodes went STALE and re-ran with retry_reason recorded.
        for nid in (f"{run_id}:c1", f"{run_id}:x"):
            node = stack.graph_store.get_node(nid)
            assert node.state == NodeState.VERIFIED
            assert node.attempt_count == 2
        starts = _events_by_type(stack, run_id, EventType.EXECUTION_STARTED)
        retries = [e for e in starts if e.payload.get("retry_reason") == "stale"]
        assert {e.payload["node_id"] for e in retries} == {f"{run_id}:c1", f"{run_id}:x"}

        # Replanning Amplification metrics are queryable (spec 15).
        metrics = invalidation_metrics(stack.event_store, run_id)
        assert len(metrics) == 1
        assert metrics[0]["affected_count"] == 2
        assert metrics[0]["replanned_count"] == 0
        assert metrics[0]["re_executed_count"] == 2
    finally:
        stack.close()


def test_constraint_change_must_invalidates_and_reverification_updates_artifact(tmp_path):
    """Deterministic must-invalidate: CONSTRAINT_CHANGED with invalidates: [p1]
    → p1 INVALIDATED → local replan → re-run; its re-verified artifact gets a
    new version, which staleness-propagates to consumers automatically."""
    run_id = "run-inv2"
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _chain_spec(
            {"type": "constraint_changed", "node_id": f"{run_id}:k",
             "invalidates": [f"{run_id}:p1"], "reason": "policy change"}
        )
        # p1's second attempt produces different content.
        for node in spec["nodes"]:
            if node["temp_id"] == "p1":
                node["metadata"]["script"]["attempts"] = {
                    "2": {"produced_artifacts": [{"path": "art.txt", "content": "v2-content"}]}
                }
        run = _start(stack, spec, run_id)
        assert run.status == "completed"

        # p1 was INVALIDATED (must-invalidate), then replanned to PENDING.
        invalidated_events = _events_by_type(stack, run_id, EventType.NODE_INVALIDATED)
        assert [e.payload["node_id"] for e in invalidated_events] == [f"{run_id}:p1"]
        propagations = _events_by_type(stack, run_id, EventType.INVALIDATION_PROPAGATED)
        assert len(propagations) == 1
        payload = propagations[0].payload
        assert payload["invalidated_node_ids"] == [f"{run_id}:p1"]
        assert payload["replanned_node_ids"] == [f"{run_id}:p1"]
        assert set(payload["affected_node_ids"]) == {
            f"{run_id}:p1", f"{run_id}:c1", f"{run_id}:x",
        }

        # p1 re-ran (retry_reason invalidated) and its new artifact version was
        # tracked: content hash matches the v2 content, version bumped twice
        # (initial production + re-verification change detection).
        art = stack.graph_store.get_node(f"{run_id}:art")
        assert art.metadata["artifact_version"] == 2
        assert art.metadata["content_hash"] == hashlib.sha256(b"v2-content").hexdigest()

        p1 = stack.graph_store.get_node(f"{run_id}:p1")
        assert p1.state == NodeState.VERIFIED
        assert p1.attempt_count == 2
        starts = _events_by_type(stack, run_id, EventType.EXECUTION_STARTED)
        p1_retries = [
            e for e in starts
            if e.payload.get("node_id") == f"{run_id}:p1"
            and e.payload.get("retry_reason") == "invalidated"
        ]
        assert len(p1_retries) == 1

        # Downstream re-ran; unrelated branch untouched.
        assert stack.graph_store.get_node(f"{run_id}:c1").attempt_count == 2
        assert stack.graph_store.get_node(f"{run_id}:x").attempt_count == 2
        u1 = stack.graph_store.get_node(f"{run_id}:u1")
        assert u1.state == NodeState.VERIFIED and u1.attempt_count == 1

        metrics = invalidation_metrics(stack.event_store, run_id)
        assert metrics[0]["replanned_count"] == 1
        assert metrics[0]["re_executed_count"] == 3
    finally:
        stack.close()


def test_removed_artifact_must_invalidates_consumers(tmp_path):
    """Artifact removal is a must-invalidate for direct consumers
    (INVALIDATED, not STALE); their dependents degrade to STALE."""
    run_id = "run-inv3"
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _chain_spec(
            {"type": "artifact_updated", "node_id": f"{run_id}:art",
             "new_hash": None, "removed": True, "reason": "artifact deleted"}
        )
        run = _start(stack, spec, run_id)
        assert run.status == "completed"

        invalidated_events = _events_by_type(stack, run_id, EventType.NODE_INVALIDATED)
        assert [e.payload["node_id"] for e in invalidated_events] == [f"{run_id}:c1"]
        propagations = _events_by_type(stack, run_id, EventType.INVALIDATION_PROPAGATED)
        assert len(propagations) == 1
        payload = propagations[0].payload
        assert payload["invalidated_node_ids"] == [f"{run_id}:c1"]
        assert payload["stale_node_ids"] == [f"{run_id}:x"]
        assert payload["replanned_node_ids"] == [f"{run_id}:c1"]

        c1 = stack.graph_store.get_node(f"{run_id}:c1")
        assert c1.state == NodeState.VERIFIED  # re-planned, re-ran, re-verified
        assert c1.attempt_count == 2
        assert stack.graph_store.get_node(f"{run_id}:x").attempt_count == 2
        u1 = stack.graph_store.get_node(f"{run_id}:u1")
        assert u1.state == NodeState.VERIFIED and u1.attempt_count == 1

        metrics = invalidation_metrics(stack.event_store, run_id)
        assert metrics[0]["replanned_count"] == 1
        assert metrics[0]["re_executed_count"] == 2
    finally:
        stack.close()
