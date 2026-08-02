"""FIFO and cost-aware schedulers (spec section 11)."""

from __future__ import annotations

from datetime import datetime, timezone

from lhos.domain.budgets import BudgetState
from lhos.domain.enums import EdgeKind, NodeState
from lhos.domain.models import GraphEdge
from lhos.graph.queries import ProgressGraph
from lhos.runtime.cost_aware_scheduler import CostAwareScheduler
from lhos.runtime.fifo_scheduler import FifoScheduler
from lhos.runtime.scheduler import ResourceState

from tests.conftest import make_node

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _ready(node_id: str, ready_at: str, **kwargs):
    metadata = dict(kwargs.pop("metadata", {}))
    metadata["ready_at"] = ready_at
    return make_node(node_id, state=NodeState.READY, metadata=metadata, **kwargs)


def test_fifo_selects_earliest_ready_at():
    nodes = [
        _ready("c", "2026-01-01T11:00:03+00:00"),
        _ready("a", "2026-01-01T11:00:01+00:00"),
        _ready("b", "2026-01-01T11:00:02+00:00"),
    ]
    graph = ProgressGraph(run_id="r", nodes={n.id: n for n in nodes}, edges=[])
    picked = FifoScheduler().select(nodes, graph, BudgetState(), ResourceState())
    assert picked is not None and picked.id == "a"


def test_fifo_empty_returns_none():
    graph = ProgressGraph(run_id="r", nodes={}, edges=[])
    assert FifoScheduler().select([], graph, BudgetState(), ResourceState()) is None


def _chain_graph() -> tuple[ProgressGraph, list]:
    """R1 -> A -> B (remaining chain of 3) plus isolated ready node R2."""
    r1 = _ready("r1", "2026-01-01T11:00:00+00:00")
    r2 = _ready("r2", "2026-01-01T11:00:00+00:00")
    a = make_node("a", state=NodeState.PENDING)
    b = make_node("b", state=NodeState.PENDING)
    edges = [
        GraphEdge(run_id="run-test", source_node_id="a", target_node_id="r1",
                  kind=EdgeKind.DEPENDS_ON),
        GraphEdge(run_id="run-test", source_node_id="b", target_node_id="a",
                  kind=EdgeKind.DEPENDS_ON),
    ]
    nodes = {n.id: n for n in (r1, r2, a, b)}
    return ProgressGraph(run_id="run-test", nodes=nodes, edges=edges), [r1, r2]


def test_cost_aware_is_deterministic():
    graph, ready = _chain_graph()
    scheduler = CostAwareScheduler()
    first = scheduler.select(ready, graph, BudgetState(), ResourceState(), now=NOW)
    second = scheduler.select(ready, graph, BudgetState(), ResourceState(), now=NOW)
    assert first is not None and second is not None
    assert first.id == second.id


def test_cost_aware_prioritizes_critical_path_node():
    """The node on the longest remaining path must outscore an isolated node
    of equal cost (Phase 6 acceptance)."""
    graph, ready = _chain_graph()
    scheduler = CostAwareScheduler()
    picked = scheduler.select(ready, graph, BudgetState(), ResourceState(), now=NOW)
    assert picked is not None and picked.id == "r1"


def test_cost_aware_deprioritizes_high_cost_low_progress_node():
    """A high-cost node with prior failures loses to a cheap fresh node."""
    cheap = _ready("cheap", "2026-01-01T11:00:00+00:00", estimated_token_cost=100)
    expensive = make_node(
        "expensive",
        state=NodeState.READY,
        estimated_token_cost=100000,
        attempt_count=2,
        metadata={"ready_at": "2026-01-01T11:00:00+00:00"},
    )
    nodes = {n.id: n for n in (cheap, expensive)}
    graph = ProgressGraph(run_id="run-test", nodes=nodes, edges=[])
    scheduler = CostAwareScheduler()
    picked = scheduler.select(
        [cheap, expensive], graph, BudgetState(), ResourceState(), now=NOW
    )
    assert picked is not None and picked.id == "cheap"
