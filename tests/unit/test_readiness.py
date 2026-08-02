"""Readiness computation (spec section 9)."""

from __future__ import annotations

from lhos.domain.budgets import BudgetLimits, BudgetState
from lhos.domain.enums import EdgeKind, NodeKind, NodeState
from lhos.domain.models import GraphEdge
from lhos.graph.queries import ProgressGraph
from lhos.graph.readiness import EnvironmentSnapshot, ReadinessEvaluator

from tests.conftest import make_node


def _graph(*nodes) -> ProgressGraph:
    return ProgressGraph(run_id="run-test", nodes={n.id: n for n in nodes}, edges=[])


def _evaluator(limits: BudgetLimits | None = None) -> ReadinessEvaluator:
    return ReadinessEvaluator(limits)


def test_pending_with_unverified_dependency_is_not_ready():
    dep = make_node("dep", state=NodeState.RUNNING)
    node = make_node("n", state=NodeState.PENDING)
    graph = _graph(dep, node)
    graph.edges.append(GraphEdge(run_id="run-test", source_node_id="n",
                                 target_node_id="dep", kind=EdgeKind.DEPENDS_ON))
    assert not _evaluator().evaluate(node, graph, EnvironmentSnapshot(), BudgetState())


def test_pending_with_verified_dependencies_is_ready():
    dep = make_node("dep", state=NodeState.VERIFIED)
    node = make_node("n", state=NodeState.PENDING)
    graph = _graph(dep, node)
    graph.edges.append(GraphEdge(run_id="run-test", source_node_id="n",
                                 target_node_id="dep", kind=EdgeKind.DEPENDS_ON))
    assert _evaluator().evaluate(node, graph, EnvironmentSnapshot(), BudgetState())


def test_active_blocks_edge_from_unverified_node_blocks_readiness():
    blocker = make_node("b", state=NodeState.PENDING)
    node = make_node("n", state=NodeState.PENDING)
    graph = _graph(blocker, node)
    graph.edges.append(GraphEdge(run_id="run-test", source_node_id="b",
                                 target_node_id="n", kind=EdgeKind.BLOCKS))
    assert not _evaluator().evaluate(node, graph, EnvironmentSnapshot(), BudgetState())


def test_blocks_edge_from_verified_node_does_not_block():
    blocker = make_node("b", state=NodeState.VERIFIED)
    node = make_node("n", state=NodeState.PENDING)
    graph = _graph(blocker, node)
    graph.edges.append(GraphEdge(run_id="run-test", source_node_id="b",
                                 target_node_id="n", kind=EdgeKind.BLOCKS))
    assert _evaluator().evaluate(node, graph, EnvironmentSnapshot(), BudgetState())


def test_attempt_limit_prevents_readiness():
    node = make_node("n", state=NodeState.FAILED, attempt_count=3, max_attempts=3)
    assert not _evaluator().evaluate(node, _graph(node), EnvironmentSnapshot(), BudgetState())


def test_failed_with_attempts_left_is_ready():
    node = make_node("n", state=NodeState.FAILED, attempt_count=1, max_attempts=3)
    assert _evaluator().evaluate(node, _graph(node), EnvironmentSnapshot(), BudgetState())


def test_stale_is_ready_when_dependencies_verified():
    node = make_node("n", state=NodeState.STALE)
    assert _evaluator().evaluate(node, _graph(node), EnvironmentSnapshot(), BudgetState())


def test_exhausted_budget_prevents_readiness():
    limits = BudgetLimits(max_total_tokens=100)
    budget = BudgetState(input_tokens=60, output_tokens=40)
    node = make_node("n", state=NodeState.PENDING)
    assert not _evaluator(limits).evaluate(node, _graph(node), EnvironmentSnapshot(), budget)


def test_non_subtask_and_non_schedulable_nodes_are_never_ready():
    fact = make_node("f", state=NodeState.PENDING, kind=NodeKind.FACT, schedulable=False)
    subtask = make_node("s", state=NodeState.PENDING, schedulable=False)
    evaluator = _evaluator()
    graph = _graph(fact, subtask)
    assert not evaluator.evaluate(fact, graph, EnvironmentSnapshot(), BudgetState())
    assert not evaluator.evaluate(subtask, graph, EnvironmentSnapshot(), BudgetState())


def test_running_node_is_not_evaluated_ready():
    node = make_node("n", state=NodeState.RUNNING)
    assert not _evaluator().evaluate(node, _graph(node), EnvironmentSnapshot(), BudgetState())
