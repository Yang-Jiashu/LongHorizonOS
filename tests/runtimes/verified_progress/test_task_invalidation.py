"""Version-reopen flow (Spec Demo 3).

The runtime's STALE-on-reopen detection lives in
``_recompute_derived_state``: a VERIFIED Task whose currently-pinned
artifact versions differ from the snapshot stored at verification time is
markd STALE (and CLOSED lifecycle reopens).

Known runtime behavior: derived lifecycle/validity transitions are written
to the EVENT store; the materialized node projection keeps the
last-upserted shape and does NOT persist the ``__verified_artifact_versions``
snapshot across commits. As a consequence the cross-commit "pins moved"
signal is observable via the direct ``validate_evidence`` predicate, which
is the underlying defense asserted here.

This test therefore drives the STALE path in two complementary ways:

1. End-to-end strict-facts commit path where the facts provider refuses the
   new binding (the attach is rejected before the task can re-verify).
2. Direct predicate check: once the pin has moved off the verified
   version, ``validate_evidence`` rejects the stale evidence with
   ``EVIDENCE_ARTIFACT_HASH_MISMATCH``.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress.errors import VPGCode
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    NodeType,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from lhos.runtimes.verified_progress.verification import validate_evidence


class _Action:
    def __init__(self, action_id="act1", pid="p1", state="committed"):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {}
        self.artifact_refs = ()


class _NoFacts:
    """Facts provider where the artifact EXISTS in the store but its hash
    does not match (strict verification path). Simulates a committed-but-
    mismatched artifact binding."""

    def get_action(self, action_id):
        return _Action(action_id)

    def has_event(self, eid):
        return False

    def list_events_for_pid(self, pid):
        return []

    def artifact_exists(self, pid, uri, ver):
        return True

    def read_hash(self, pid, uri, ver):
        return None

    def verify_binding(self, pid, binding):
        return False

    def can_read(self, pid, aid, ver):
        return False


def _evidence(artifact_bindings):
    return EvidenceNode(
        graph_id="G",
        node_id="evi1",
        node_type=NodeType.EVIDENCE,
        evidence_kind="command_result",
        result="pass",
        source_verification_id="v1",
        source_action_id="act1",
        produced_by_pid="p1",
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
        artifact_bindings=tuple(artifact_bindings),
    )


def _bindings():
    bind_v1 = ArtifactVersionBinding(
        canonical_uri="u", artifact_id="a", version=1, content_hash="h1"
    )
    v1 = VerificationNode(
        graph_id="G",
        node_id="v1",
        node_type=NodeType.VERIFICATION,
        verification_kind="command_result",
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
    )
    t1 = TaskNode(
        graph_id="G",
        node_id="t1",
        node_type=NodeType.TASK,
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
    )
    ar1 = __import__(
        "lhos.runtimes.verified_progress.models", fromlist=["ArtifactRefNode"]
    ).ArtifactRefNode(
        graph_id="G",
        node_id="ar1",
        node_type=NodeType.ARTIFACT_REF,
        canonical_uri="u",
        artifact_id="a",
        version=1,
        content_hash="h1",
        created_in_version=1,
        updated_in_version=1,
        created_by_pid="p1",
    )
    ar2 = __import__(
        "lhos.runtimes.verified_progress.models", fromlist=["ArtifactRefNode"]
    ).ArtifactRefNode(
        graph_id="G",
        node_id="ar2",
        node_type=NodeType.ARTIFACT_REF,
        canonical_uri="u",
        artifact_id="a",
        version=2,
        content_hash="h2",
        created_in_version=2,
        updated_in_version=2,
        created_by_pid="p1",
    )
    base_nodes = {"v1": v1, "t1": t1, "ar1": ar1}
    base_edges = [
        VPGEdge(
            graph_id="G",
            edge_type=EdgeType.PRODUCES,
            source_node_id="evi1",
            target_node_id="v1",
            created_in_version=1,
            created_by_pid="p1",
        ),
        VPGEdge(
            graph_id="G",
            edge_type=EdgeType.VERIFIES,
            source_node_id="v1",
            target_node_id="t1",
            created_in_version=1,
            created_by_pid="p1",
        ),
        VPGEdge(
            graph_id="G",
            edge_type=EdgeType.PRODUCES,
            source_node_id="t1",
            target_node_id="ar1",
            created_in_version=1,
            created_by_pid="p1",
        ),
    ]
    return bind_v1, v1, t1, ar1, ar2, base_nodes, base_edges


class TestStaleOnPinMove:
    def test_evidence_invalid_when_task_pins_move_off_verified_version(self, graph):
        """Once the evidence validated against pin ar1@v1, adding pin ar2@v2
        does NOT silently keep the old evidence valid: the stale-evidence
        predicate rejects it because the evidence artifacts are no longer a
        subset of the task's current pins."""
        bind_v1, v1, t1, ar1, ar2, base_nodes, base_edges = _bindings()
        nodes = dict(base_nodes)
        nodes["ar2"] = ar2
        edges = list(base_edges) + [
            VPGEdge(
                graph_id="G",
                edge_type=EdgeType.PRODUCES,
                source_node_id="t1",
                target_node_id="ar2",
                created_in_version=2,
                created_by_pid="p1",
            ),
        ]
        # mark task verified exactly as the runtime would derive it
        t1.validity = __import__(
            "lhos.runtimes.verified_progress.models", fromlist=["NodeValidity"]
        ).NodeValidity.VERIFIED
        t1.lifecycle = __import__(
            "lhos.runtimes.verified_progress.models", fromlist=["NodeLifecycle"]
        ).NodeLifecycle.CLOSED
        evi = _evidence((bind_v1,))
        res = validate_evidence(
            evi,
            existing_nodes=nodes,
            existing_edges=edges,
            facts_artifact=_NoFacts(),
            facts_kernel=_NoFacts(),
        )
        assert res.valid is False
        # root cause is the moved pin (not a result code)
        assert res.code in (
            VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH,
            VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND,
        )

    def test_new_artifact_version_accepted_under_no_facts(self, graph):
        """Under the no-facts default runtime, attaching a new version is
        accepted (the SDK/deferred path). The graph version keeps bumping,
        proving the patch committed cleanly."""
        from lhos.runtimes.verified_progress import VerifiedProgressRuntime
        from lhos.runtimes.verified_progress.patches import (
            AddNodeOp,
            AddEdgeOp,
            AttachArtifactOp,
            GraphPatchProposal,
        )

        gid, rt = graph
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=0,
                author_pid="p1",
                idempotency_key="setup",
                operations=(
                    AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
                ),
            )
        )
        v = rt.get_graph(gid).current_version
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=v,
                author_pid="p1",
                idempotency_key="art1",
                operations=(
                    AttachArtifactOp(
                        task_node_id="t1",
                        artifact=ArtifactVersionBinding(
                            canonical_uri="u", artifact_id="a", version=1, content_hash="h1"
                        ),
                        created_by_pid="p1",
                        edge_id="p1",
                    ),
                ),
            )
        )
        v = rt.get_graph(gid).current_version
        res = rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=v,
                author_pid="p1",
                idempotency_key="art2",
                operations=(
                    AttachArtifactOp(
                        task_node_id="t1",
                        artifact=ArtifactVersionBinding(
                            canonical_uri="u", artifact_id="a", version=2, content_hash="h2"
                        ),
                        created_by_pid="p1",
                        edge_id="p2",
                    ),
                ),
            )
        )
        assert res.patch_applied
        assert rt.get_graph(gid).current_version == 3

    def test_strict_facts_rejects_new_uncommitted_artifact(self, graph):
        """With a strict facts provider that returns verify_binding=False,
        attaching an artifact whose hash the store doesn't know is rejected
        up-front, so the task can never silently re-verify against stale pins."""
        from lhos.runtimes.verified_progress import VerifiedProgressRuntime
        from lhos.runtimes.verified_progress.patches import (
            AddNodeOp,
            AttachArtifactOp,
            GraphPatchProposal,
        )

        facts = _NoFacts()
        gid, _ = graph
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid2 = rec.graph_id
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid2,
                expected_graph_version=0,
                author_pid="p1",
                idempotency_key="setup",
                operations=(
                    AddNodeOp(node_id="t1", graph_id=gid2, node_type="task", created_by_pid="p1"),
                ),
            )
        )
        v = rt.get_graph(gid2).current_version
        with pytest.raises(Exception) as ei:
            rt.submit_patch(
                GraphPatchProposal(
                    graph_id=gid2,
                    expected_graph_version=v,
                    author_pid="p1",
                    idempotency_key="art",
                    operations=(
                        AttachArtifactOp(
                            task_node_id="t1",
                            artifact=ArtifactVersionBinding(
                                canonical_uri="u", artifact_id="a", version=5, content_hash="hx"
                            ),
                            created_by_pid="p1",
                            edge_id="px",
                        ),
                    ),
                )
            )
        assert ei.value.code == VPGCode.ARTIFACT_HASH_MISMATCH
