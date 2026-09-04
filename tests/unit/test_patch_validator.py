"""Graph patch validation (spec section 8.2): version conflicts, cycle
rejection, single-transaction atomicity."""

from __future__ import annotations

import pytest

from lhos.domain.enums import NodeState, PatchOperationType
from lhos.domain.errors import CycleError, NodeNotFoundError, VersionConflictError
from lhos.domain.graph_patch import GraphPatchOperation
from lhos.graph.patch_validator import PatchValidator
from tests.conftest import make_edge, make_node


def _seed(graph_store, run_id):
    n1 = make_node("n1", run_id=run_id)
    n2 = make_node("n2", run_id=run_id)
    graph_store.add_node(n1)
    graph_store.add_node(n2)
    graph_store.add_edge(make_edge(run_id, "n2", "n1"))  # n2 depends_on n1
    return n1, n2


def test_version_conflict_rejects_patch(graph_store, run_id):
    _seed(graph_store, run_id)
    validator = PatchValidator(graph_store)
    op = GraphPatchOperation(
        op=PatchOperationType.SET_STATE,
        target_id="n1",
        expected_version=99,  # actual version is 1
        payload={"state": "ready"},
    )
    with pytest.raises(VersionConflictError):
        validator.validate_and_apply(run_id, [op])
    assert graph_store.get_node("n1").state == NodeState.PENDING


def test_dependency_cycle_is_rejected(graph_store, run_id):
    _seed(graph_store, run_id)
    validator = PatchValidator(graph_store)
    op = GraphPatchOperation(
        op=PatchOperationType.ADD_EDGE,
        payload={"source": "n1", "target": "n2", "kind": "depends_on"},
    )
    with pytest.raises(CycleError):
        validator.validate_and_apply(run_id, [op])
    # Nothing was applied: still exactly one edge.
    assert len(graph_store.list_edges(run_id)) == 1


def test_missing_node_is_rejected(graph_store, run_id):
    _seed(graph_store, run_id)
    validator = PatchValidator(graph_store)
    op = GraphPatchOperation(
        op=PatchOperationType.SET_STATE,
        target_id="ghost",
        payload={"state": "ready"},
    )
    with pytest.raises(NodeNotFoundError):
        validator.validate_and_apply(run_id, [op])


def test_failed_patch_leaves_no_partial_update(graph_store, run_id):
    _seed(graph_store, run_id)
    validator = PatchValidator(graph_store)
    ops = [
        GraphPatchOperation(  # valid on its own
            op=PatchOperationType.ADD_NODE,
            payload={
                "temp_id": "n3",
                "kind": "subtask",
                "title": "third",
                "specification": "x",
                "schedulable": True,
            },
        ),
        GraphPatchOperation(  # invalid: cycle
            op=PatchOperationType.ADD_EDGE,
            payload={"source": "n1", "target": "n2", "kind": "depends_on"},
        ),
    ]
    with pytest.raises(CycleError):
        validator.validate_and_apply(run_id, ops)
    # The valid first op must NOT have been applied.
    assert len(graph_store.list_nodes(run_id)) == 2


def test_valid_patch_applies_and_bumps_versions(graph_store, run_id):
    _seed(graph_store, run_id)
    validator = PatchValidator(graph_store)
    ops = [
        GraphPatchOperation(
            op=PatchOperationType.UPDATE_NODE,
            target_id="n1",
            expected_version=1,
            payload={"title": "renamed"},
        ),
        GraphPatchOperation(
            op=PatchOperationType.SET_STATE,
            target_id="n1",
            payload={"state": "ready"},
        ),
    ]
    validator.validate_and_apply(run_id, ops)
    node = graph_store.get_node("n1")
    assert node.title == "renamed"
    assert node.state == NodeState.READY
    assert node.version == 3  # update + set_state


def test_set_state_verified_requires_existing_evidence(graph_store, run_id):
    n1 = make_node("n1", run_id=run_id, state=NodeState.CLAIMED_DONE)
    graph_store.add_node(n1)
    validator = PatchValidator(graph_store)
    op = GraphPatchOperation(
        op=PatchOperationType.SET_STATE,
        target_id="n1",
        payload={"state": "verified", "evidence_ids": ["missing-evidence"]},
    )
    with pytest.raises(Exception, match="evidence"):
        validator.validate_and_apply(run_id, [op])
    assert graph_store.get_node("n1").state == NodeState.CLAIMED_DONE
