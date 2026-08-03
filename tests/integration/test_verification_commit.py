"""Verification isolation (spec 14, Phase 3 acceptance):

- an agent claim can NEVER directly become VERIFIED;
- a failed verification sends the node to FAILED;
- a passing verification records evidence and then verifies;
- even a worker that outputs ``status: "verified"`` is clamped to the
  claim -> verify flow.
"""

from __future__ import annotations

import pytest

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.errors import EvidenceRequiredError
from lhos.domain.events import EventType


def _spec(tmp_path, verification, script, max_attempts=1):
    return {
        "goal": "verification isolation test",
        "nodes": [
            {
                "temp_id": "n1",
                "kind": "subtask",
                "title": "Do work",
                "specification": "Produce output.txt",
                "schedulable": True,
                "max_attempts": max_attempts,
                "verification_spec": verification,
                "metadata": {"script": script},
            }
        ],
        "edges": [],
    }


def _run(stack: RuntimeStack, spec: dict, run_id: str = "run-v"):
    stack.graph_store.create_run(run_id, spec["goal"], {})
    stack.initial_builder.build(run_id, spec)
    return stack.controller.run(run_id)


def test_failed_verification_sends_node_to_failed(tmp_path):
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _spec(
            tmp_path,
            {"type": "file_exists", "path": "missing.txt"},
            {"summary": "I swear I did it"},  # produces nothing
            max_attempts=1,
        )
        run = _run(stack, spec)
        node = stack.graph_store.get_node("run-v:n1")
        assert node.state == NodeState.FAILED
        assert run.status == "failed"
        event_types = [e.event_type for e in stack.event_store.list_events("run-v")]
        assert EventType.CLAIM_SUBMITTED in event_types
        assert EventType.VERIFICATION_FAILED in event_types
        assert EventType.VERIFICATION_PASSED not in event_types
    finally:
        stack.close()


def test_verified_requires_passing_verifier_and_evidence(tmp_path):
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _spec(
            tmp_path,
            {"type": "file_exists", "path": "output.txt"},
            {
                "summary": "produced output",
                "produced_artifacts": [{"path": "output.txt", "content": "hello"}],
            },
        )
        run = _run(stack, spec)
        node = stack.graph_store.get_node("run-v:n1")
        assert node.state == NodeState.VERIFIED
        assert run.status == "completed"
        # At least one evidence record backs the verification (invariant 2).
        evidence = stack.graph_store.list_evidence("run-v")
        assert len(evidence) >= 1
        assert (tmp_path / "ws" / "output.txt").exists()
    finally:
        stack.close()


def test_worker_claim_of_verified_is_clamped_to_claim_flow(tmp_path):
    stack = RuntimeStack(tmp_path / "lhos.db", tmp_path / "ws", config={})
    try:
        spec = _spec(
            tmp_path,
            {"type": "file_exists", "path": "output.txt"},
            {
                "status": "verified",  # worker tries to self-verify
                "summary": "trust me",
                "produced_artifacts": [{"path": "output.txt", "content": "hello"}],
            },
        )
        _run(stack, spec)
        node = stack.graph_store.get_node("run-v:n1")
        assert node.state == NodeState.VERIFIED
        # ...but only via the gate: the claim and verification events exist.
        event_types = [e.event_type for e in stack.event_store.list_events("run-v")]
        assert EventType.CLAIM_SUBMITTED in event_types
        assert EventType.VERIFICATION_STARTED in event_types
        assert EventType.VERIFICATION_PASSED in event_types
        # The node passed through CLAIMED_DONE; no direct jump to VERIFIED.
        state_events = [
            e
            for e in stack.event_store.list_events("run-v")
            if e.event_type == EventType.NODE_STATE_CHANGED
            and e.payload.get("node_id") == "run-v:n1"
        ]
        states = [e.payload["to_state"] for e in state_events]
        assert "claimed_done" in states
        assert states.index("claimed_done") < states.index("verified")
    finally:
        stack.close()


def test_store_rejects_verified_without_evidence(graph_store, run_id):
    from tests.conftest import make_node

    graph_store.add_node(make_node("n1", run_id=run_id, state=NodeState.CLAIMED_DONE))
    with pytest.raises(EvidenceRequiredError):
        graph_store.set_state("n1", NodeState.VERIFIED, actor="verifier")
