"""Patch commit validation: admission, version bump, evidence-direct rejection."""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import NodeLifecycle
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _patch(rt, graph_id, kid, ops, ev=None):
    v = ev if ev is not None else rt.get_graph(graph_id).current_version
    return rt.submit_patch(GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=v,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


class TestSingleTaskCommit:
    def test_add_single_task_succeeds_version_plus_one(self, graph):
        gid, rt = graph
        r = _patch(rt, gid, "k1", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
        ))
        assert r.patch_applied is True
        assert r.idempotent_replay is False
        assert r.committed_graph_version == 1
        assert rt.get_graph(gid).current_version == 1

    def test_node_lifecycle_becomes_admitted(self, graph):
        gid, rt = graph
        _patch(rt, gid, "k1", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        n = rt.inspect_node(gid, "t1")
        assert n is not None
        assert n.lifecycle == NodeLifecycle.ADMITTED


class TestRejectedPatchNoBump:
    def test_rejected_patch_does_not_bump_version(self, graph):
        gid, rt = graph
        _patch(rt, gid, "good", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        assert rt.get_graph(gid).current_version == 1
        bad = GraphPatchProposal(
            graph_id=gid, expected_graph_version=0, author_pid="p1",
            idempotency_key="stale", operations=(
                AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1"),
            ),
        )
        with pytest.raises(VPGError) as ei:
            rt.submit_patch(bad)
        assert ei.value.code == VPGCode.GRAPH_VERSION_CONFLICT
        assert rt.get_graph(gid).current_version == 1

    def test_rejected_schema_does_not_bump_version(self, graph):
        gid, rt = graph
        _patch(rt, gid, "good", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        with pytest.raises(VPGError):
            _patch(rt, gid, "dup", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
            ))
        assert rt.get_graph(gid).current_version == 1


class TestEvidenceAddNodeRejected:
    def test_evidence_add_node_allowed(self, graph):
        """Agent MAY propose EvidenceNode via AddNode; VERIFIED shortcut is the
        only thing admission forbids (enforced elsewhere)."""
        gid, rt = graph
        _patch(rt, gid, "ev", (
            AddNodeOp(node_id="evi1", graph_id=gid, node_type="evidence",
                       created_by_pid="p1", result="pass",
                       evidence_source_action_id="act1"),
        ))
        n = rt.inspect_node(gid, "evi1")
        assert n is not None
        assert n.node_type.value == "evidence"
        assert n.validity.value == "unverified"

    def test_evidence_add_node_missing_source_action_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError) as ei:
            _patch(rt, gid, "ev-bad", (
                AddNodeOp(node_id="evi-bad", graph_id=gid, node_type="evidence",
                           created_by_pid="p1", result="pass"),
            ))
        # source_action_id is required in admission
        assert rt.inspect_node(gid, "evi-bad") is None
