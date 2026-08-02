"""Context Compiler (spec section 10, Phase 4 acceptance):

- no unrelated siblings;
- no full transcript;
- stable hash for identical dependency versions;
- hash changes when a dependency version changes;
- stale nodes are excluded (weight 0).
"""

from __future__ import annotations

from lhos.domain.enums import NodeKind, NodeState
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.domain.models import EvidenceRef
from lhos.runtime.context_compiler import ContextCompiler, ContextRequest

from tests.conftest import make_edge, make_node


def _seed(graph_store, run_id):
    dep = make_node("dep", run_id=run_id, state=NodeState.VERIFIED,
                    title="Build parser", specification="Build the config parser module")
    sibling = make_node("sibling", run_id=run_id, state=NodeState.VERIFIED,
                        title="Paint bikeshed", specification="UNRELATED_SIBLING_MARKER")
    stale_dep = make_node("stale-dep", run_id=run_id, state=NodeState.VERIFIED,
                          title="Old survey", specification="STALE_DEP_MARKER")
    constraint = make_node("constraint", run_id=run_id, kind=NodeKind.CONSTRAINT,
                           schedulable=False, state=NodeState.VERIFIED,
                           title="Style rule", specification="CONSTRAINT_MARKER: use snake_case")
    target = make_node("target", run_id=run_id, state=NodeState.READY,
                       title="Write tests", specification="Write unit tests for the parser")
    for node in (dep, sibling, stale_dep, constraint, target):
        graph_store.add_node(node)
    graph_store.add_edge(make_edge(run_id, "target", "dep"))
    graph_store.add_edge(make_edge(run_id, "target", "stale-dep"))
    graph_store.add_evidence(
        EvidenceRef(
            run_id=run_id,
            evidence_type="file_hash",
            source_event_id="seed",
            content_hash="abc123",
            summary="parser.py hash",
            metadata={"node_id": "dep"},
        )
    )
    return target


def _request(run_id, node_id="target") -> ContextRequest:
    return ContextRequest(run_id=run_id, node_id=node_id, max_tokens=12000)


def _packet_text(packet) -> str:  # noqa: ANN001
    parts = [packet.global_goal, packet.current_task]
    parts += packet.constraints
    parts += packet.dependency_summaries
    parts += packet.previous_failures
    parts += packet.verification_requirements
    parts += [e.summary or "" for e in packet.artifact_refs]
    return "\n".join(parts)


def test_context_includes_dependencies_constraints_and_goal(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    packet = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    text = _packet_text(packet)
    assert "Build the config parser module" in text
    assert "CONSTRAINT_MARKER" in text
    assert packet.global_goal == "test goal"
    assert any(e.content_hash == "abc123" for e in packet.artifact_refs)


def test_context_excludes_unrelated_siblings(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    packet = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert "UNRELATED_SIBLING_MARKER" not in _packet_text(packet)


def test_context_excludes_full_transcript(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    event_store.append(
        RuntimeEvent(
            run_id=run_id,
            event_type=EventType.TOOL_CALL_COMPLETED,
            actor_type=ActorType.WORKER,
            payload={"node_id": "dep", "result": {"stdout": "RAW_TRANSCRIPT_MARKER"}},
        )
    )
    packet = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert "RAW_TRANSCRIPT_MARKER" not in _packet_text(packet)


def test_stale_dependencies_are_excluded(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    graph_store.set_state("stale-dep", NodeState.STALE, actor=ActorType.SYSTEM)
    packet = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert "STALE_DEP_MARKER" not in _packet_text(packet)


def test_same_dependency_versions_produce_same_hash(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    first = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    # A fresh compiler (empty cache) over the same state must reproduce the hash.
    second = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert first.context_hash == second.context_hash


def test_dependency_version_change_changes_hash(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    before = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    dep = graph_store.get_node("dep")
    dep.specification = "Build the config parser module (v2)"
    graph_store.update_node(dep, actor=ActorType.SYSTEM)
    after = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert before.context_hash != after.context_hash


def test_previous_failures_are_scoped_to_the_node(graph_store, event_store, run_id):
    _seed(graph_store, run_id)
    event_store.append(
        RuntimeEvent(
            run_id=run_id,
            event_type=EventType.VERIFICATION_FAILED,
            actor_type=ActorType.VERIFIER,
            payload={"node_id": "target", "summary": "tests missing"},
        )
    )
    event_store.append(
        RuntimeEvent(
            run_id=run_id,
            event_type=EventType.VERIFICATION_FAILED,
            actor_type=ActorType.VERIFIER,
            payload={"node_id": "dep", "summary": "OTHER_NODE_FAILURE_MARKER"},
        )
    )
    packet = ContextCompiler(graph_store, event_store).compile(_request(run_id))
    assert any("tests missing" in f for f in packet.previous_failures)
    assert "OTHER_NODE_FAILURE_MARKER" not in _packet_text(packet)
