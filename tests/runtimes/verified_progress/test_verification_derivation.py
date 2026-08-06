"""Task VERIFIED / CLOSED derivation via a wired FACTS provider.

Because the agent cannot AddNode an EvidenceNode, evidence is injected
directly into the materialized projection (simulating out-of-band evidence
creation by the harness/kernel), then attached to a VerificationNode via
AttachEvidenceOp. With a matching FACTS provider, the runtime derives
TASK_VERIFIED_DERIVED + TASK_CLOSED_DERIVED events.

Observed behavior: the runtime writes derived transitions to the EVENTS
store (get_events()); the materialized node projection stays at its last
upserted shape. Test against the event stream which is the canonical
derived-state surface.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    EvidenceNode,
    NodeType,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)


class _Action:
    def __init__(self, action_id="act1", pid="p1", state="committed"):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {}
        self.artifact_refs = ()


class _Facts:
    def __init__(self, action=None):
        self._action = action

    def get_action(self, action_id):
        return self._action

    def has_event(self, eid):
        return False

    def list_events_for_pid(self, pid):
        return []

    def artifact_exists(self, pid, uri, ver):
        return True

    def read_hash(self, pid, uri, ver):
        return None

    def verify_binding(self, pid, binding):
        return True

    def can_read(self, pid, aid, ver):
        return True


def _patch(rt, graph_id, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=rt.get_graph(graph_id).current_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


def _inject_evidence(rt, graph_id, evidence_node_id, verification_id, action_id,
                     artifact_bindings=(), version=None):
    evi = EvidenceNode(
        graph_id=graph_id, node_id=evidence_node_id, node_type=NodeType.EVIDENCE,
        evidence_kind="command_result", result="pass",
        source_verification_id=verification_id, source_action_id=action_id,
        produced_by_pid="p1",
        created_in_version=version if version is not None else rt.get_graph(graph_id).current_version,
        updated_in_version=version if version is not None else rt.get_graph(graph_id).current_version,
        created_by_pid="p1",
        artifact_bindings=tuple(artifact_bindings),
    )
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection (node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
        (evi.node_id, graph_id, "evidence", evi.model_dump_json()),
    )
    rt.store.conn.commit()
    return evi


def _event_types(rt, graph_id):
    return [e.event_type for e in rt.get_events(graph_id)]


class TestNoFactsPath:
    def test_pass_evidence_without_kernel_not_found(self, graph):
        """With no kernel provider, any PASS evidence fails with
        EVIDENCE_SOURCE_ACTION_NOT_FOUND when validated directly."""
        from lhos.runtimes.verified_progress.verification import validate_evidence

        gid, rt = graph
        evi = EvidenceNode(
            graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1", created_in_version=0, updated_in_version=0,
            created_by_pid="p1",
        )
        v1 = __import__("lhos.runtimes.verified_progress.models", fromlist=["VerificationNode"]).VerificationNode(
            graph_id=gid, node_id="v1", node_type=NodeType.VERIFICATION,
            verification_kind="command_result",
            created_in_version=0, updated_in_version=0, created_by_pid="p1",
        )
        res = validate_evidence(
            evi, existing_nodes={"v1": v1}, existing_edges=[],
            facts_artifact=None, facts_kernel=None,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND


class TestFactsPathVerifiedClosed:
    def test_task_verified_and_closed_with_matching_facts(self, graph):
        gid, rt = graph
        facts = _Facts(_Action())
        rt.facts_artifact = facts
        rt.facts_kernel = facts
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="dep", edge_type="depends_on", source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vfy", edge_type="verifies", source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
        ))
        bind = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        _inject_evidence(rt, gid, "evi1", "v1", "act1", artifact_bindings=(bind,))
        _patch(rt, gid, "art", (
            AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1", edge_id="prod"),
        ))
        _patch(rt, gid, "attachev", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1", created_by_pid="p1", edge_id="pe"),
        ))
        evts = _event_types(rt, gid)
        assert GraphEventType.TASK_VERIFIED_DERIVED in evts
        assert GraphEventType.TASK_CLOSED_DERIVED in evts
        assert GraphEventType.GOAL_CLOSED_DERIVED in evts

    def test_evidence_with_hash_mismatch_attach_rejected(self, graph):
        """With a strict artifact facts provider whose verify_binding returns
        False, the binding is rejected at ATTACH time, so the task never
        pins the artifact and never verifies."""
        from lhos.runtimes.verified_progress.errors import VPGError

        gid, rt = graph
        facts_mismatch = _Facts(_Action())
        facts_mismatch.verify_binding = lambda pid, binding: False
        rt.facts_artifact = facts_mismatch
        rt.facts_kernel = _Facts(_Action())
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="dep", edge_type="depends_on", source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vfy", edge_type="verifies", source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
        ))
        bind = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        _inject_evidence(rt, gid, "evi1", "v1", "act1", artifact_bindings=(bind,))
        with pytest.raises(VPGError) as ei:
            _patch(rt, gid, "art", (
                AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1", edge_id="prod"),
            ))
        assert ei.value.code == VPGCode.ARTIFACT_HASH_MISMATCH
        evts = _event_types(rt, gid)
        assert GraphEventType.TASK_VERIFIED_DERIVED not in evts
        assert GraphEventType.GOAL_CLOSED_DERIVED not in evts
