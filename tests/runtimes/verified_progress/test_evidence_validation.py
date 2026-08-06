"""Evidence validation semantics.

The EvidenceValidator (verification.validate_evidence) enforces that an
EvidenceNode is only valid when result=="pass", source_action_id points to
a real committed Kernel action, and artifact bindings match. Failures map
to precise VPGCodes.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress.errors import VPGCode
from lhos.runtimes.verified_progress.models import (
    ArtifactRefNode,
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    NodeType,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from lhos.runtimes.verified_progress.verification import validate_evidence


class _FakeKernel:
    def __init__(self, action=None):
        self._action = action

    def get_action(self, action_id):
        return self._action

    def has_event(self, eid):
        return False

    def list_events_for_pid(self, pid):
        return []


class _FakeArtifact:
    def __init__(self, verify=True):
        self._verify = verify

    def artifact_exists(self, pid, uri, ver):
        return True

    def read_hash(self, pid, uri, ver):
        return None

    def verify_binding(self, pid, binding):
        return self._verify

    def can_read(self, pid, aid, ver):
        return True


class _FakeAction:
    def __init__(self, action_id="act1", pid="p1", state="committed"):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {}
        self.artifact_refs = ()


def _evidence(result="pass", source_action_id="act1", artifact_bindings=()):
    return EvidenceNode(
        graph_id="G", node_id="evi1", node_type=NodeType.EVIDENCE,
        evidence_kind="command_result", result=result,
        source_verification_id="v1", source_action_id=source_action_id,
        produced_by_pid="p1", created_in_version=0, updated_in_version=0,
        created_by_pid="p1",
        artifact_bindings=tuple(artifact_bindings),
    )


def _verification_nodes_edges():
    v1 = VerificationNode(
        graph_id="G", node_id="v1", node_type=NodeType.VERIFICATION,
        verification_kind="command_result",
        created_in_version=0, updated_in_version=0, created_by_pid="p1",
    )
    return {"v1": v1}, []


class TestEvidenceResultCodes:
    def test_fail_never_valid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="fail", source_action_id="act1")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_FAIL_REJECTED

    def test_inconclusive_never_valid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="inconclusive", source_action_id="act1")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_INCONCLUSIVE_REJECTED

    def test_pass_with_no_source_action_invalid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="pass", source_action_id="")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND

    def test_pass_with_no_kernel_provider_invalid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="pass", source_action_id="act1")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(None))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND

    def test_pass_action_not_committed_invalid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="pass", source_action_id="act1")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(_FakeAction(state="running")))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_NOT_TERMINAL

    def test_missing_produces_edge_invalid(self):
        nodes, edges = _verification_nodes_edges()
        ev = _evidence(result="pass", source_action_id="act1")
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(), facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_PRODUCES_EDGE_MISSING

    def test_artifact_hash_mismatch_invalid(self):
        v1 = VerificationNode(
            graph_id="G", node_id="v1", node_type=NodeType.VERIFICATION,
            verification_kind="command_result",
            created_in_version=0, updated_in_version=0, created_by_pid="p1",
        )
        ev = EvidenceNode(
            graph_id="G", node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1", created_in_version=0, updated_in_version=0,
            created_by_pid="p1",
            artifact_bindings=(ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h"),),
        )
        nodes = {"v1": v1}
        edges = [VPGEdge(graph_id="G", edge_type=EdgeType.PRODUCES,
                        source_node_id="evi1", target_node_id="v1",
                        created_in_version=0, created_by_pid="p1")]
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(verify=False),
                                facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

    def test_valid_full_chain(self):
        v1 = VerificationNode(
            graph_id="G", node_id="v1", node_type=NodeType.VERIFICATION,
            verification_kind="command_result",
            created_in_version=0, updated_in_version=0, created_by_pid="p1",
        )
        t1 = TaskNode(
            graph_id="G", node_id="t1", node_type=NodeType.TASK,
            created_in_version=0, updated_in_version=0, created_by_pid="p1",
        )
        ar = ArtifactRefNode(
            graph_id="G", node_id="ar1", node_type=NodeType.ARTIFACT_REF,
            canonical_uri="u", artifact_id="a", version=1, content_hash="h",
            created_in_version=0, updated_in_version=0, created_by_pid="p1",
        )
        ev = _evidence(result="pass", source_action_id="act1",
                       artifact_bindings=(ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h"),))
        nodes = {"v1": v1, "t1": t1, "ar1": ar}
        edges = [
            VPGEdge(graph_id="G", edge_type=EdgeType.PRODUCES,
                    source_node_id="v1", target_node_id="evi1",
                    created_in_version=0, created_by_pid="p1"),
            VPGEdge(graph_id="G", edge_type=EdgeType.VERIFIES,
                    source_node_id="v1", target_node_id="t1",
                    created_in_version=0, created_by_pid="p1"),
            VPGEdge(graph_id="G", edge_type=EdgeType.PRODUCES,
                    source_node_id="t1", target_node_id="ar1",
                    created_in_version=0, created_by_pid="p1"),
        ]
        res = validate_evidence(ev, existing_nodes=nodes, existing_edges=edges,
                                facts_artifact=_FakeArtifact(True),
                                facts_kernel=_FakeKernel(_FakeAction()))
        assert res.valid is True
        assert res.code is None
