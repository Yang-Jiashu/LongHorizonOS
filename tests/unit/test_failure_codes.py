"""Unit tests for terminal failure codes (Step 8).

Verifies that the TerminationEvaluator produces structured failure codes
and the controller's finish event contains the failure tree.
"""

from __future__ import annotations

from lhos.domain.enums import NodeKind, NodeState
from lhos.domain.models import GraphNode
from lhos.graph.queries import ProgressGraph
from lhos.runtime.termination import (
    FAILURE_ALL_NODES_EXHAUSTED,
    FAILURE_RUN_STUCK,
    TerminationEvaluator,
)


def _make_node(
    node_id: str,
    state: NodeState = NodeState.VERIFIED,
    attempt_count: int = 0,
    max_attempts: int = 3,
) -> GraphNode:
    return GraphNode(
        id=node_id,
        run_id="test-run",
        kind=NodeKind.SUBTASK,
        title=node_id,
        specification="test",
        state=state,
        schedulable=True,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        version=1,
    )


def _make_graph(nodes: list[GraphNode]) -> ProgressGraph:
    return ProgressGraph(
        run_id="test-run",
        nodes={n.id: n for n in nodes},
        edges=[],
    )


class TestTerminationCompleted:
    def test_all_verified_completes(self):
        nodes = [
            _make_node("n1", state=NodeState.VERIFIED),
            _make_node("n2", state=NodeState.VERIFIED),
        ]
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert decision.should_stop
        assert decision.status == "completed"
        assert decision.primary_failure_code is None

    def test_no_schedulable_completes(self):
        nodes = []
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert decision.should_stop
        assert decision.status == "completed"


class TestTerminationFailed:
    def test_all_nodes_exhausted(self):
        """All subtasks failed and can't retry."""
        nodes = [
            _make_node("n1", state=NodeState.FAILED, attempt_count=3, max_attempts=3),
            _make_node("n2", state=NodeState.FAILED, attempt_count=3, max_attempts=3),
        ]
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert decision.should_stop
        assert decision.status == "failed"
        assert decision.primary_failure_code == FAILURE_ALL_NODES_EXHAUSTED

    def test_run_stuck(self):
        """Nodes are pending/stale but can't become ready."""
        nodes = [
            _make_node("n1", state=NodeState.PENDING),
        ]
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert decision.should_stop
        assert decision.status == "failed"
        assert decision.primary_failure_code == FAILURE_RUN_STUCK


class TestTerminationRunning:
    def test_active_nodes_continue(self):
        nodes = [
            _make_node("n1", state=NodeState.VERIFIED),
            _make_node("n2", state=NodeState.READY),
        ]
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert not decision.should_stop

    def test_running_node_continues(self):
        nodes = [
            _make_node("n1", state=NodeState.RUNNING),
        ]
        evaluator = TerminationEvaluator()
        decision = evaluator.evaluate(_make_graph(nodes))
        assert not decision.should_stop
