"""Local invalidation propagation (spec section 15): only the affected
subgraph is marked stale; unrelated branches keep their verified state."""

from __future__ import annotations

from lhos.domain.enums import EdgeKind, NodeKind, NodeState
from lhos.domain.events import EventType
from lhos.domain.models import GraphEdge
from lhos.graph.invalidation import propagate_invalidation

from tests.conftest import make_node


def _seed(graph_store, run_id):
    artifact = make_node("art", run_id=run_id, kind=NodeKind.ARTIFACT,
                         schedulable=False, state=NodeState.VERIFIED)
    consumer = make_node("consumer", run_id=run_id, state=NodeState.VERIFIED)
    downstream = make_node("downstream", run_id=run_id, state=NodeState.VERIFIED)
    unrelated = make_node("unrelated", run_id=run_id, state=NodeState.VERIFIED)
    claimed = make_node("claimed", run_id=run_id, state=NodeState.CLAIMED_DONE)
    for node in (artifact, consumer, downstream, unrelated, claimed):
        graph_store.add_node(node)
    graph_store.add_edge(GraphEdge(run_id=run_id, source_node_id="consumer",
                                   target_node_id="art", kind=EdgeKind.CONSUMES))
    graph_store.add_edge(GraphEdge(run_id=run_id, source_node_id="claimed",
                                   target_node_id="art", kind=EdgeKind.CONSUMES))
    graph_store.add_edge(GraphEdge(run_id=run_id, source_node_id="downstream",
                                   target_node_id="consumer", kind=EdgeKind.DEPENDS_ON))


def test_invalidation_propagates_along_consumers_and_dependencies(graph_store, run_id):
    _seed(graph_store, run_id)
    report = propagate_invalidation(graph_store, run_id, "art")
    assert set(report["affected_node_ids"]) == {"consumer", "downstream", "claimed"}
    assert graph_store.get_node("consumer").state == NodeState.STALE
    assert graph_store.get_node("downstream").state == NodeState.STALE
    # CLAIMED_DONE nodes are marked stale too (spec 15 pseudocode).
    assert graph_store.get_node("claimed").state == NodeState.STALE


def test_unrelated_branch_stays_verified(graph_store, run_id):
    _seed(graph_store, run_id)
    propagate_invalidation(graph_store, run_id, "art")
    assert graph_store.get_node("unrelated").state == NodeState.VERIFIED


def test_invalidation_is_recorded_as_events(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    propagate_invalidation(graph_store, run_id, "art")
    stale_events = [
        e for e in event_store.list_events(run_id)
        if e.event_type == EventType.NODE_MARKED_STALE
    ]
    assert len(stale_events) == 3
    propagated = [
        e for e in event_store.list_events(run_id)
        if e.event_type == EventType.INVALIDATION_PROPAGATED
    ]
    assert len(propagated) == 1
    assert propagated[0].payload["affected_count"] == 3
