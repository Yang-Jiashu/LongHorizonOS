"""Admission engine: node schema validation on creation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGError
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _p(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


class TestGoalRequiresTitle:
    def test_goal_without_title_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError):
            _p(rt, gid, "g-no-title", (
                AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title=""),
            ))
        assert rt.inspect_node(gid, "g1") is None

    def test_goal_with_title_admitted(self, graph):
        gid, rt = graph
        _p(rt, gid, "g-with-title", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="Goal"),
        ))
        n = rt.inspect_node(gid, "g1")
        assert n is not None
        assert n.lifecycle.value == "admitted"


class TestTaskRequiredVerificationCount:
    def test_required_verification_count_lt_one_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError):
            _p(rt, gid, "t-bad-rvc", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1",
                           required_verification_count=0),
            ))
        assert rt.inspect_node(gid, "t1") is None

    def test_required_verification_count_one_ok(self, graph):
        gid, rt = graph
        _p(rt, gid, "t-ok-rvc", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1",
                       required_verification_count=1),
        ))
        assert rt.inspect_node(gid, "t1") is not None


class TestTaskExecutionSpecForbidden:
    def test_task_execution_spec_forbidden_key_rejected(self, graph):
        gid, rt = graph
        # Pydantic field-validator rejects forbidden keys at patch CONSTRUCTION
        # time (before submit_patch is reached).
        with pytest.raises(ValidationError):
            GraphPatchProposal(
                graph_id=gid, expected_graph_version=0, author_pid="p1",
                idempotency_key="t-forbid",
                operations=(AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                                      created_by_pid="p1", execution_spec={"callback": "x"}),),
            )

    def test_task_execution_spec_forbidden_path_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(ValidationError):
            GraphPatchProposal(
                graph_id=gid, expected_graph_version=0, author_pid="p1",
                idempotency_key="t-path",
                operations=(AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                                      created_by_pid="p1", execution_spec={"bin": "/usr/local/bin/evil"}),),
            )

    def test_task_execution_spec_clean_accepted(self, graph):
        gid, rt = graph
        _p(rt, gid, "t-clean", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1",
                       execution_spec={"timeout_ms": 5000}),
        ))
        assert rt.inspect_node(gid, "t1") is not None


class TestArtifactRefRequiredFields:
    def test_artifact_ref_missing_uri_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError):
            _p(rt, gid, "ar-nouri", (
                AddNodeOp(node_id="ar1", graph_id=gid, node_type="artifact_ref", created_by_pid="p1",
                           canonical_uri="", artifact_id="a", version=1, content_hash="h"),
            ))

    def test_artifact_ref_missing_artifact_id_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError):
            _p(rt, gid, "ar-noaid", (
                AddNodeOp(node_id="ar1", graph_id=gid, node_type="artifact_ref", created_by_pid="p1",
                           canonical_uri="u", artifact_id="", version=1, content_hash="h"),
            ))

    def test_artifact_ref_missing_hash_rejected(self, graph):
        gid, rt = graph
        with pytest.raises(VPGError):
            _p(rt, gid, "ar-nohash", (
                AddNodeOp(node_id="ar1", graph_id=gid, node_type="artifact_ref", created_by_pid="p1",
                           canonical_uri="u", artifact_id="a", version=1, content_hash=""),
            ))

    def test_artifact_ref_complete_admitted(self, graph):
        gid, rt = graph
        _p(rt, gid, "ar-ok", (
            AddNodeOp(node_id="ar1", graph_id=gid, node_type="artifact_ref", created_by_pid="p1",
                       canonical_uri="u", artifact_id="a", version=1, content_hash="h"),
        ))
        n = rt.inspect_node(gid, "ar1")
        assert n is not None
        assert n.lifecycle.value == "admitted"
