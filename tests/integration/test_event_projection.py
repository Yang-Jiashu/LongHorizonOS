"""Event replay (spec 26.2): run -> delete materialized graph -> rebuild from
events -> the projection is identical."""

from __future__ import annotations

from lhos.cli.replay import _projection_hash
from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType
from lhos.domain.models import EvidenceRef
from lhos.graph.projection import rebuild_projection

from tests.conftest import make_edge, make_node


def _exercise_store(graph_store, run_id):
    n1 = make_node("n1", run_id=run_id)
    n2 = make_node("n2", run_id=run_id)
    n3 = make_node("n3", run_id=run_id)
    for node in (n1, n2, n3):
        graph_store.add_node(node)
    graph_store.add_edge(make_edge(run_id, "n2", "n1"))
    graph_store.add_edge(make_edge(run_id, "n3", "n2"))

    graph_store.set_state("n1", NodeState.READY, actor=ActorType.SYSTEM)
    graph_store.acquire_lease("n1", "worker-1", lease_seconds=60)
    graph_store.set_state("n1", NodeState.RUNNING, actor=ActorType.WORKER)
    graph_store.set_state("n1", NodeState.CLAIMED_DONE, actor=ActorType.WORKER)
    evidence = graph_store.add_evidence(
        EvidenceRef(
            run_id=run_id,
            evidence_type="command_output",
            source_event_id="seed",
            content_hash="deadbeef",
            summary="tests passed",
            metadata={"node_id": "n1"},
        )
    )
    graph_store.set_state(
        "n1", NodeState.VERIFIED, actor=ActorType.VERIFIER,
        evidence_ids=[evidence.id],
    )
    graph_store.release_lease("n1")
    node = graph_store.get_node("n2")
    node.title = "renamed n2"
    graph_store.update_node(node, actor=ActorType.SYSTEM)
    graph_store.set_state("n3", NodeState.ABORTED, actor=ActorType.SYSTEM)


def test_rebuild_from_events_is_identical(graph_store, db, run_id):
    _exercise_store(graph_store, run_id)
    before = _projection_hash(db, run_id)

    with db.transaction():
        db.conn.execute("DELETE FROM nodes WHERE run_id = ?", (run_id,))
        db.conn.execute("DELETE FROM edges WHERE run_id = ?", (run_id,))
        db.conn.execute("DELETE FROM evidence WHERE run_id = ?", (run_id,))

    counts = rebuild_projection(db, run_id)
    after = _projection_hash(db, run_id)

    assert counts == {"nodes": 3, "edges": 2, "evidence": 1}
    assert before == after

    # And the rebuilt projection is semantically correct.
    graph = graph_store.load_graph(run_id)
    assert graph.nodes["n1"].state == NodeState.VERIFIED
    assert graph.nodes["n1"].lease_owner is None
    assert graph.nodes["n2"].title == "renamed n2"
    assert graph.nodes["n3"].state == NodeState.ABORTED
