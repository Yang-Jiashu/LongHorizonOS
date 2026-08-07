"""Pydantic model tests for D1 core domain models.

Covers ArtifactVersionBinding, node lifecycle/validity defaults,
TaskDispatchCandidate construction, and invalid-value rejection.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    EdgeType,
    GoalNode,
    GraphRecord,
    GraphVersion,
    NodeLifecycle,
    NodeType,
    NodeValidity,
    ReadinessProof,
    TaskDispatchCandidate,
    TaskNode,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    PatchCommitResult,
)


# ── ArtifactVersionBinding ────────────────────────────────────────────────


class TestArtifactVersionBinding:
    def test_required_fields_present(self):
        b = ArtifactVersionBinding(
            canonical_uri="workspace:///a.md",
            artifact_id="art-1",
            version=3,
            content_hash="deadbeef",
        )
        assert b.canonical_uri == "workspace:///a.md"
        assert b.artifact_id == "art-1"
        assert b.version == 3
        assert b.content_hash == "deadbeef"
        assert b.media_type == "application/octet-stream"

    def test_default_media_type(self):
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        assert b.media_type == "application/octet-stream"

    def test_version_is_int(self):
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=7, content_hash="h")
        assert isinstance(b.version, int)
        assert b.version == 7

    def test_missing_required_field_rejected(self):
        with pytest.raises(ValidationError):
            ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1)  # no content_hash

    def test_hash_field_stored_exactly(self):
        b = ArtifactVersionBinding(
            canonical_uri="u", artifact_id="a", version=1, content_hash="abc123"
        )
        dumped = b.model_dump()
        assert dumped["content_hash"] == "abc123"


# ── Node lifecycle / validity defaults ────────────────────────────────────


class TestNodeDefaults:
    def test_goal_default_proposed_unverified(self):
        n = GoalNode(
            node_id="g1",
            graph_id="G",
            created_in_version=0,
            updated_in_version=0,
            created_by_pid="p1",
            title="Goal",
        )
        assert n.lifecycle == NodeLifecycle.PROPOSED
        assert n.validity == NodeValidity.UNVERIFIED
        assert n.node_type.value == "goal"

    def test_task_default_proposed_unverified(self):
        n = TaskNode(
            node_id="t1",
            graph_id="G",
            created_in_version=0,
            updated_in_version=0,
            created_by_pid="p1",
        )
        assert n.lifecycle == NodeLifecycle.PROPOSED
        assert n.validity == NodeValidity.UNVERIFIED
        assert n.required_verification_count == 1

    def test_graph_record_default_version_zero_not_closed(self):
        rec = GraphRecord(owner_pid="p1", graph_id="G")
        assert rec.current_version == 0
        assert rec.closed is False


# ── TaskDispatchCandidate / ReadinessProof ────────────────────────────────


class TestDispatchCandidate:
    def test_construction(self):
        proof = ReadinessProof(
            graph_id="G",
            graph_version=2,
            task_id="t1",
            lifecycle_ok=True,
            validity_ok=True,
            all_deps_verified=True,
            has_execution_attempt=False,
        )
        cand = TaskDispatchCandidate(
            graph_id="G",
            graph_version=2,
            task_id="t1",
            readiness_proof=proof,
            execution_spec={"cmd": "echo"},
        )
        assert cand.task_id == "t1"
        assert cand.execution_spec == {"cmd": "echo"}
        assert cand.readiness_proof.all_deps_verified is True


# ── Invalid-value rejection ───────────────────────────────────────────────


class TestInvalidValues:
    def test_invalid_lifecycle_string_rejected(self):
        # StrEnum raises ValueError on unknown values; Pydantic wraps it as
        # ValidationError when the field is validated through a model.
        with pytest.raises((ValueError, ValidationError)):
            NodeLifecycle("not_a_lifecycle")

    def test_invalid_validity_string_rejected(self):
        with pytest.raises((ValueError, ValidationError)):
            NodeValidity("banana")

    def test_invalid_edge_type_rejected(self):
        with pytest.raises((ValueError, ValidationError)):
            EdgeType("sideways")

    def test_invalid_lifecycle_via_pydantic(self):
        class Probe(BaseModel):
            lc: NodeLifecycle

        with pytest.raises(ValidationError):
            Probe(lc="not_a_lifecycle")

    def test_invalid_node_type_via_pydantic(self):
        from lhos.runtimes.verified_progress.models import NodeType

        class Probe(BaseModel):
            nt: NodeType

        with pytest.raises(ValidationError):
            Probe(nt="bogus_type")

    def test_vpgerror_carries_code_and_message(self):
        err = VPGError(VPGCode.GRAPH_VERSION_CONFLICT, "msg here")
        assert err.code == VPGCode.GRAPH_VERSION_CONFLICT
        assert err.code.value == "GRAPH_VERSION_CONFLICT"
        assert "msg here" in str(err)

    def test_graph_version_fields(self):
        gv = GraphVersion(
            graph_id="G",
            version=1,
            parent_version=0,
            patch_id="pch",
            projection_hash="hash",
            committed_by_pid="p1",
            committed_at="2024-01-01T00:00:00",
        )
        assert gv.version == 1
        assert gv.parent_version == 0

    def test_patch_commit_result_fields(self):
        r = PatchCommitResult(
            graph_id="G",
            patch_id="p",
            committed_graph_version=3,
            patch_applied=True,
            idempotent_replay=False,
        )
        assert r.committed_graph_version == 3
        assert r.patch_applied is True
        assert r.idempotent_replay is False

    def test_add_node_op_fields(self):
        op = AddNodeOp(node_id="n1", graph_id="G", node_type="task", created_by_pid="p1")
        assert op.op_type.value == "add_node"
        assert op.required_verification_count == 1

    def test_add_edge_op_fields(self):
        op = AddEdgeOp(
            edge_id="e1",
            edge_type="depends_on",
            source_node_id="a",
            target_node_id="b",
            created_by_pid="p1",
        )
        assert op.edge_type == "depends_on"
        assert op.source_node_id == "a"
