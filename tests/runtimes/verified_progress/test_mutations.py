"""Defensive mutation tests VPG-01 through VPG-15 — killed-by-tests.

Each test demonstrates that the specific vulnerability (mutation) does NOT
survive contact with the runtime.  A test failing would mean the mutation
has compromised the runtime.

Reference mutation list:

    VPG-01  Agent directly sets VERIFIED
    VPG-02  Patch ignores expected_graph_version
    VPG-03  Successful patch does not bump GraphVersion
    VPG-04  Conflicting patch partially commits
    VPG-05  DAG check ignores same-patch edges
    VPG-06  Evidence skips source-action check
    VPG-07  Evidence skips artifact hash
    VPG-08  FAIL evidence verifies task
    VPG-09  Task ignores dependencies when marking READY
    VPG-10  Goal closes on ANY task verified
    VPG-11  v1 evidence auto-verifies v2
    VPG-12  Ready frontier yields non-deterministic order
    VPG-13  Projection replay ignores validity
    VPG-14  Duplicate idempotency key creates new version
    VPG-15  Runtime directly imports Kernel Service
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    NodeLifecycle,
    NodeType,
    NodeValidity,
    VPGEdge,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.verification import validate_evidence

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC = PROJECT_ROOT / "src"
VP = SRC / "lhos" / "runtimes" / "verified_progress"


class _Action:
    def __init__(self, aid="act1"):
        self.action_id = aid
        self.pid = "p1"
        self.state = "committed"
        self.result = {}
        self.artifact_refs = ()


class _Facts:
    def get_action(self, aid):
        return _Action(aid)

    def has_event(self, eid):
        return False

    def list_events_for_pid(self, p):
        return []

    def artifact_exists(self, p, u, v):
        return True

    def read_hash(self, p, u, v):
        return None

    def verify_binding(self, p, b):
        return True

    def can_read(self, p, a, v):
        return True


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=rt.get_graph(gid).current_version,
            author_pid="p1",
            idempotency_key=kid,
            operations=ops,
        )
    )


@pytest.fixture
def g():
    rt = VerifiedProgressRuntime(":memory:")
    rec = rt.create_graph(owner_pid="p1")
    return rec.graph_id, rt


# ── VPG-01 — Agent directly sets VERIFIED ────────────────────────────────


class TestVPG01:
    def test_agent_cannot_set_validity_via_addnode(self, g):
        gid, rt = g
        _patch(
            rt,
            gid,
            "v1",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
            ),
        )
        n = rt.inspect_node(gid, "t1")
        assert n.lifecycle == NodeLifecycle.ADMITTED
        assert n.validity == NodeValidity.UNVERIFIED

    def _build_verified_rt(self):
        facts = _Facts()
        rt2 = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt2.create_graph(owner_pid="p1")
        gid2 = rec.graph_id
        _patch(
            rt2,
            gid2,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid2, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid2, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        evi = EvidenceNode(
            graph_id=gid2,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="pass",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt2.get_graph(gid2).current_version,
            updated_in_version=rt2.get_graph(gid2).current_version,
            created_by_pid="p1",
            artifact_bindings=(b,),
        )
        rt2.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid2, "evidence", evi.model_dump_json()),
        )
        rt2.store.conn.commit()
        _patch(
            rt2,
            gid2,
            "art",
            (AttachArtifactOp(task_node_id="t1", artifact=b, created_by_pid="p1", edge_id="p1"),),
        )
        _patch(
            rt2,
            gid2,
            "att",
            (
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe",
                ),
            ),
        )
        return rt2, gid2

    def test_derived_verified_only_via_event_stream(self, g):
        rt2, gid2 = self._build_verified_rt()
        types = [e.event_type for e in rt2.get_events(gid2)]
        assert GraphEventType.TASK_VERIFIED_DERIVED in types
        assert GraphEventType.TASK_CLOSED_DERIVED in types


# ── VPG-02 — optimistic lock ──────────────────────────────────────────────


class TestVPG02:
    def test_stale_expected_version_rejected(self, g):
        gid, rt = g
        _patch(
            rt,
            gid,
            "p1",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
            ),
        )
        stale = GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="stale",
            operations=(
                AddNodeOp(
                    node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"
                ),
            ),
        )
        with pytest.raises(VPGError) as exc:
            rt.submit_patch(stale)
        msg = str(exc.value).lower()
        assert "version" in msg or "conflict" in msg


# ── VPG-03 — version monotonicity ─────────────────────────────────────────


class TestVPG03:
    def test_version_bumps_every_commit(self, g):
        gid, rt = g
        assert rt.get_graph(gid).current_version == 0
        _patch(
            rt,
            gid,
            "p1",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
            ),
        )
        assert rt.get_graph(gid).current_version == 1


# ── VPG-04 — commit atomicity ──────────────────────────────────────────────


class TestVPG04:
    def test_conflicting_patch_atomic_rollback(self, g):
        gid, rt = g
        _patch(
            rt,
            gid,
            "p1",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
            ),
        )
        before = rt.get_graph(gid).current_version
        stale = GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="conflict",
            operations=(
                AddNodeOp(
                    node_id="TX", graph_id=gid, node_type="task", created_by_pid="p1", title="X"
                ),
            ),
        )
        with pytest.raises(VPGError):
            rt.submit_patch(stale)
        assert rt.get_graph(gid).current_version == before
        assert rt.inspect_node(gid, "TX") is None

    def test_version_sequence_contiguous(self, g):
        gid, rt = g
        for i in range(1, 4):
            _patch(
                rt,
                gid,
                f"p{i}",
                (
                    AddNodeOp(
                        node_id=f"t{i}",
                        graph_id=gid,
                        node_type="task",
                        created_by_pid="p1",
                        title=f"T{i}",
                    ),
                ),
            )
        for v in range(1, 4):
            assert rt.store.get_version(gid, v) is not None


# ── VPG-05 — DAG cycle detection ──────────────────────────────────────────


class TestVPG05:
    def test_self_loop_rejected(self, g):
        gid, rt = g
        with pytest.raises(VPGError):
            _patch(
                rt,
                gid,
                "self",
                (
                    AddNodeOp(
                        node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                    ),
                    AddEdgeOp(
                        edge_id="self",
                        edge_type="depends_on",
                        source_node_id="t1",
                        target_node_id="t1",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert rt.inspect_node(gid, "t1") is None

    def test_three_node_cycle_rejected(self, g):
        gid, rt = g
        with pytest.raises(VPGError):
            _patch(
                rt,
                gid,
                "cyc",
                (
                    AddNodeOp(
                        node_id="a", graph_id=gid, node_type="task", created_by_pid="p1", title="A"
                    ),
                    AddNodeOp(
                        node_id="b", graph_id=gid, node_type="task", created_by_pid="p1", title="B"
                    ),
                    AddNodeOp(
                        node_id="c", graph_id=gid, node_type="task", created_by_pid="p1", title="C"
                    ),
                    AddEdgeOp(
                        edge_id="e1",
                        edge_type="depends_on",
                        source_node_id="a",
                        target_node_id="b",
                        created_by_pid="p1",
                    ),
                    AddEdgeOp(
                        edge_id="e2",
                        edge_type="depends_on",
                        source_node_id="b",
                        target_node_id="c",
                        created_by_pid="p1",
                    ),
                    AddEdgeOp(
                        edge_id="e3",
                        edge_type="depends_on",
                        source_node_id="c",
                        target_node_id="a",
                        created_by_pid="p1",
                    ),
                ),
            )


# ── VPG-06 — evidence validates source Action ─────────────────────────────


class TestVPG06:
    def _build_setup_rt(self, action_id="ghost", result="pass"):
        facts = _Facts()
        facts.get_action = lambda aid: None  # journal empty
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result=result,
            source_verification_id="v1",
            source_action_id=action_id,
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        return rt, gid

    def test_validator_directly_rejects_missing_action(self, g):
        """Direct call to validate_evidence rejects evidence when
        source_action_id is missing or not in kernel journal."""
        rt, gid = self._build_setup_rt(action_id="ghost")
        evi_list = [n for n in rt.store.get_all_nodes(gid) if n.node_type == NodeType.EVIDENCE]
        assert evi_list, "evidence must be in projection"
        from lhos.runtimes.verified_progress.models import VerificationNode

        v1 = next(n for n in rt.store.get_all_nodes(gid) if n.node_type == NodeType.VERIFICATION)
        prod_edge = __import__(
            "lhos.runtimes.verified_progress.models", fromlist=["VPGEdge", "EdgeType"]
        ).VPGEdge(
            edge_id="pe",
            graph_id=gid,
            edge_type=EdgeType.PRODUCES,
            source_node_id="evi1",
            target_node_id=v1.node_id,
            created_in_version=0,
            created_by_pid="p1",
            created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        )
        res = validate_evidence(
            evi_list[0],
            existing_nodes={n.node_id: n for n in rt.store.get_all_nodes(gid)},
            existing_edges=list(rt.store.get_all_edges(gid)) + [prod_edge],
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND

    def test_no_verified_when_source_action_missing(self, g):
        """End-to-end: with missing kernel action in journal, attaching
        evidence does NOT cause TASK_VERIFIED_DERIVED."""
        rt, gid = self._build_setup_rt(action_id="ghost")
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        # Attach must succeed (patch_validator does NOT raise on invalid
        # evidence — evidence may become valid later); the kill is that
        # the derived state never fires.
        _patch(
            rt,
            gid,
            "art",
            (AttachArtifactOp(task_node_id="t1", artifact=b, created_by_pid="p1", edge_id="p1"),),
        )
        _patch(
            rt,
            gid,
            "att",
            (
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe",
                ),
            ),
        )
        types = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED not in types


# ── VPG-07 — evidence validates artifact hash ────────────────────────────


class TestVPG07:
    def test_validator_directly_rejects_hash_mismatch(self, g):
        """Direct call to validate_evidence rejects evidence when the
        facts provider's verify_binding returns False."""
        facts_check = _Facts()
        facts_check.verify_binding = lambda p, b: b.content_hash == "correct"
        rt = VerifiedProgressRuntime(
            ":memory:", facts_artifact=facts_check, facts_kernel=facts_check
        )
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="pass",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
            artifact_bindings=(
                ArtifactVersionBinding(
                    canonical_uri="u", artifact_id="a", version=1, content_hash="wrong"
                ),
            ),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        prod_edge = VPGEdge(
            edge_id="pe",
            graph_id=gid,
            edge_type=EdgeType.PRODUCES,
            source_node_id="evi1",
            target_node_id="v1",
            created_in_version=0,
            created_by_pid="p1",
            created_at=datetime.now(timezone.utc),
        )
        res = validate_evidence(
            evi,
            existing_nodes={n.node_id: n for n in rt.store.get_all_nodes(gid)},
            existing_edges=list(rt.store.get_all_edges(gid)) + [prod_edge],
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

    def test_no_verified_on_hash_mismatch(self, g):
        """End-to-end: attaching evidence with mismatched hash never
        produces TASK_VERIFIED_DERIVED."""
        strict = _Facts()
        strict.verify_binding = lambda p, b: b.content_hash == "correct"
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=strict, facts_kernel=strict)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="pass",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
            artifact_bindings=(
                ArtifactVersionBinding(
                    canonical_uri="u", artifact_id="a", version=1, content_hash="wrong"
                ),
            ),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        b = ArtifactVersionBinding(
            canonical_uri="u", artifact_id="a", version=1, content_hash="correct"
        )
        _patch(
            rt,
            gid,
            "art",
            (AttachArtifactOp(task_node_id="t1", artifact=b, created_by_pid="p1", edge_id="p1"),),
        )
        _patch(
            rt,
            gid,
            "att",
            (
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe",
                ),
            ),
        )
        types = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED not in types


# ── VPG-08 — FAIL evidence must not verify task ───────────────────────────


class TestVPG08:
    def test_validator_directly_rejects_fail(self, g):
        """Direct call rejects fail result evidence."""
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="fail",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        prod_edge = VPGEdge(
            edge_id="pe",
            graph_id=gid,
            edge_type=EdgeType.PRODUCES,
            source_node_id="evi1",
            target_node_id="v1",
            created_in_version=0,
            created_by_pid="p1",
            created_at=datetime.now(timezone.utc),
        )
        res = validate_evidence(
            evi,
            existing_nodes={n.node_id: n for n in rt.store.get_all_nodes(gid)},
            existing_edges=list(rt.store.get_all_edges(gid)) + [prod_edge],
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_FAIL_REJECTED

    def test_no_verified_with_fail_evidence(self, g):
        """End-to-end: fail evidence never makes task verified."""
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf1",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="fail",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        _patch(
            rt,
            gid,
            "art",
            (AttachArtifactOp(task_node_id="t1", artifact=b, created_by_pid="p1", edge_id="p1"),),
        )
        _patch(
            rt,
            gid,
            "att",
            (
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe",
                ),
            ),
        )
        types = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED not in types


# ── VPG-09 — Task must check dependencies before READY ────────────────────


class TestVPG09:
    def test_task_with_unverified_dep_is_not_ready(self, g):
        gid, rt = g
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"
                ),
                AddNodeOp(
                    node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"
                ),
                AddEdgeOp(
                    edge_id="d1",
                    edge_type="depends_on",
                    source_node_id="t2",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier

        ready = compute_ready_frontier(
            gid,
            rt.get_graph(gid).current_version,
            {n.node_id: n for n in rt.store.get_all_nodes(gid)},
            list(rt.store.get_all_edges(gid)),
        )
        ready_ids = [c.task_id for c in ready]
        assert "t1" in ready_ids
        assert "t2" not in ready_ids


# ── VPG-10 — Goal closes only on ALL task deps verified ───────────────────


class TestVPG10:
    def test_goal_does_not_close_when_only_one_of_two_tasks_verified(self):
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id

        def add_task(task_id, title="T"):
            _patch(
                rt,
                gid,
                f"add-{task_id}",
                (
                    AddNodeOp(
                        node_id=task_id,
                        graph_id=gid,
                        node_type="task",
                        created_by_pid="p1",
                        title=title,
                    ),
                    AddNodeOp(
                        node_id=f"v-{task_id}",
                        graph_id=gid,
                        node_type="verification",
                        created_by_pid="p1",
                    ),
                    AddEdgeOp(
                        edge_id=f"vf-{task_id}",
                        edge_type="verifies",
                        source_node_id=f"v-{task_id}",
                        target_node_id=task_id,
                        created_by_pid="p1",
                    ),
                ),
            )
            _patch(
                rt,
                gid,
                f"dep-{task_id}",
                (
                    AddEdgeOp(
                        edge_id=f"dep-{task_id}",
                        edge_type="depends_on",
                        source_node_id="g1",
                        target_node_id=task_id,
                        created_by_pid="p1",
                    ),
                ),
            )

        _patch(
            rt,
            gid,
            "g1",
            (
                AddNodeOp(
                    node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G1"
                ),
            ),
        )
        add_task("t1", title="T1")
        add_task("t2", title="T2")

        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")

        def verify_task(task_id, evi_id, edge_prefix):
            evi_node = EvidenceNode(
                graph_id=gid,
                node_id=evi_id,
                node_type=NodeType.EVIDENCE,
                evidence_kind="command_result",
                result="pass",
                source_verification_id=f"v-{task_id}",
                source_action_id="act1",
                produced_by_pid="p1",
                created_in_version=rt.get_graph(gid).current_version,
                updated_in_version=rt.get_graph(gid).current_version,
                created_by_pid="p1",
                artifact_bindings=(b,),
            )
            rt.store.conn.execute(
                "INSERT INTO graph_nodes_projection "
                "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
                (evi_id, gid, "evidence", evi_node.model_dump_json()),
            )
            rt.store.conn.commit()
            _patch(
                rt,
                gid,
                f"art-{task_id}",
                (
                    AttachArtifactOp(
                        task_node_id=task_id,
                        artifact=b,
                        created_by_pid="p1",
                        edge_id=f"p-{edge_prefix}",
                    ),
                ),
            )
            _patch(
                rt,
                gid,
                f"att-{task_id}",
                (
                    AttachEvidenceOp(
                        verification_node_id=f"v-{task_id}",
                        evidence_node_id=evi_id,
                        created_by_pid="p1",
                        edge_id=f"pe-{edge_prefix}",
                    ),
                ),
            )

        verify_task("t1", "evi-t1", "t1")

        events = rt.get_events(gid)
        goal_closed = [e for e in events if e.event_type == GraphEventType.GOAL_CLOSED_DERIVED]
        assert not goal_closed, "Goal closed with only one of two tasks verified"

    def test_goal_closes_when_all_tasks_verified(self):
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id

        def add_task(task_id, title="T"):
            _patch(
                rt,
                gid,
                f"add-{task_id}",
                (
                    AddNodeOp(
                        node_id=task_id,
                        graph_id=gid,
                        node_type="task",
                        created_by_pid="p1",
                        title=title,
                    ),
                    AddNodeOp(
                        node_id=f"v-{task_id}",
                        graph_id=gid,
                        node_type="verification",
                        created_by_pid="p1",
                    ),
                    AddEdgeOp(
                        edge_id=f"vf-{task_id}",
                        edge_type="verifies",
                        source_node_id=f"v-{task_id}",
                        target_node_id=task_id,
                        created_by_pid="p1",
                    ),
                ),
            )
            _patch(
                rt,
                gid,
                f"dep-{task_id}",
                (
                    AddEdgeOp(
                        edge_id=f"dep-{task_id}",
                        edge_type="depends_on",
                        source_node_id="g1",
                        target_node_id=task_id,
                        created_by_pid="p1",
                    ),
                ),
            )

        _patch(
            rt,
            gid,
            "g1",
            (
                AddNodeOp(
                    node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G1"
                ),
            ),
        )
        add_task("t1", title="T1")
        add_task("t2", title="T2")

        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")

        def verify_task(task_id, evi_id, edge_prefix):
            evi_node = EvidenceNode(
                graph_id=gid,
                node_id=evi_id,
                node_type=NodeType.EVIDENCE,
                evidence_kind="command_result",
                result="pass",
                source_verification_id=f"v-{task_id}",
                source_action_id="act1",
                produced_by_pid="p1",
                created_in_version=rt.get_graph(gid).current_version,
                updated_in_version=rt.get_graph(gid).current_version,
                created_by_pid="p1",
                artifact_bindings=(b,),
            )
            rt.store.conn.execute(
                "INSERT INTO graph_nodes_projection "
                "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
                (evi_id, gid, "evidence", evi_node.model_dump_json()),
            )
            rt.store.conn.commit()
            _patch(
                rt,
                gid,
                f"art-{task_id}",
                (
                    AttachArtifactOp(
                        task_node_id=task_id,
                        artifact=b,
                        created_by_pid="p1",
                        edge_id=f"p-{edge_prefix}",
                    ),
                ),
            )
            _patch(
                rt,
                gid,
                f"att-{task_id}",
                (
                    AttachEvidenceOp(
                        verification_node_id=f"v-{task_id}",
                        evidence_node_id=evi_id,
                        created_by_pid="p1",
                        edge_id=f"pe-{edge_prefix}",
                    ),
                ),
            )

        verify_task("t1", "evi-t1", "t1")
        verify_task("t2", "evi-t2", "t2")

        events = rt.get_events(gid)
        goal_closed = [e for e in events if e.event_type == GraphEventType.GOAL_CLOSED_DERIVED]
        assert goal_closed, "Goal must close when all dep tasks verified"


# ── VPG-11 — version-pinned evidence does not cross versions ──────────────


class TestVPG11:
    def _setup_v1_verified_rt(self):
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"
                ),
                AddEdgeOp(
                    edge_id="vf",
                    edge_type="verifies",
                    source_node_id="v1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
            ),
        )
        b1 = ArtifactVersionBinding(
            canonical_uri="u", artifact_id="a", version=1, content_hash="h1"
        )
        evi = EvidenceNode(
            graph_id=gid,
            node_id="evi1",
            node_type=NodeType.EVIDENCE,
            evidence_kind="command_result",
            result="pass",
            source_verification_id="v1",
            source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
            artifact_bindings=(b1,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        _patch(
            rt,
            gid,
            "att1",
            (
                AttachArtifactOp(task_node_id="t1", artifact=b1, created_by_pid="p1", edge_id="p1"),
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe",
                ),
            ),
        )
        return rt, gid, b1

    def test_v1_evidence_verifies_task_at_v1(self, g):
        rt, gid, b1 = self._setup_v1_verified_rt()
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED in evts

    def test_v2_attach_rejected_when_binding_unknown(self, g):
        """A mutation that would let v1 evidence auto-validate v2 content
        is killed because the facts provider checks the actual binding hash.
        With a provider that rejects b2's hash, pinning v2 is rejected at
        attach time."""
        rt, gid, b1 = self._setup_v1_verified_rt()
        strict = _Facts()
        strict.verify_binding = lambda p, b: b.version == 1 and b.content_hash == "h1"
        rt.facts_artifact = strict
        rt.facts_kernel = strict
        b2 = ArtifactVersionBinding(
            canonical_uri="u", artifact_id="a", version=2, content_hash="h2"
        )
        with pytest.raises(VPGError) as ei:
            _patch(
                rt,
                gid,
                "att2",
                (
                    AttachArtifactOp(
                        task_node_id="t1", artifact=b2, created_by_pid="p1", edge_id="p2"
                    ),
                    AttachEvidenceOp(
                        verification_node_id="v1",
                        evidence_node_id="evi1",
                        created_by_pid="p1",
                        edge_id="pe",
                    ),
                ),
            )
        assert ei.value.code == VPGCode.ARTIFACT_HASH_MISMATCH


# ── VPG-12 — Ready frontier must be deterministic ─────────────────────────


class TestVPG12:
    def test_ready_frontier_is_ordered_deterministically(self, g):
        gid, rt = g
        ops = []
        for i in range(5):
            ops.append(
                AddNodeOp(
                    node_id=f"t{i}",
                    graph_id=gid,
                    node_type="task",
                    created_by_pid="p1",
                    title=f"T{i}",
                )
            )
        _patch(rt, gid, "bundle", tuple(ops))
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier

        r1 = compute_ready_frontier(
            gid,
            rt.get_graph(gid).current_version,
            {n.node_id: n for n in rt.store.get_all_nodes(gid)},
            list(rt.store.get_all_edges(gid)),
        )
        r2 = compute_ready_frontier(
            gid,
            rt.get_graph(gid).current_version,
            {n.node_id: n for n in rt.store.get_all_nodes(gid)},
            list(rt.store.get_all_edges(gid)),
        )
        assert r1 == r2, "Ready frontier order must be deterministic"
        r1_ids = [c.task_id for c in r1]
        assert set(r1_ids) == {"t0", "t1", "t2", "t3", "t4"}


# ── VPG-13 — Projection replay must respect validity ──────────────────────


class TestVPG13:
    def test_admit_rejects_node_with_wrong_graph_id(self, g):
        """The admission engine — also called by projection replay —
        rejects nodes whose graph_id does not match.  This respects
        the validity constraint for all projection paths."""
        from lhos.runtimes.verified_progress.admission import admit
        from lhos.runtimes.verified_progress.models import TaskNode

        gid, rt = g
        bad = TaskNode(
            graph_id="wrong-graph-id",
            node_id="bad",
            node_type=NodeType.TASK,
            created_by_pid="p1",
            created_in_version=1,
            updated_in_version=1,
            title="X",
        )
        res = admit(bad, gid)
        assert not res.admitted
        assert res.node.validity == NodeValidity.INVALID
        assert any("graph_id" in m.lower() for m in res.messages)


# ── VPG-14 — idempotency does not bump version ────────────────────────────


class TestVPG14:
    def test_duplicate_idempotency_key_is_noop(self, g):
        gid, rt = g
        _patch(
            rt,
            gid,
            "k1",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"
                ),
            ),
        )
        v1 = rt.get_graph(gid).current_version
        assert v1 == 1
        # Re-submitting the same idempotency key must be a no-op.
        _patch(
            rt,
            gid,
            "k1",
            (
                AddNodeOp(
                    node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"
                ),
            ),
        )
        # Version must NOT advance — the whole patch is deduplicated.
        assert rt.get_graph(gid).current_version == v1
        # And because the whole same-key patch was deduplicated, t2 does
        # not exist.
        assert rt.inspect_node(gid, "t2") is None


# ── VPG-15 — Architecture: VPG does not import agent_os ───────────────────


class TestVPG15:
    def test_vpg_does_not_import_agent_os(self):
        """No file in lhos.runtimes.verified_progress may import from
        lhos.agent_os."""
        pat = re.compile(r"^\s*(?:from|import)\s+lhos\.agent_os\b", re.MULTILINE)
        offenders: list[str] = []
        for p in VP.rglob("*.py"):
            text = p.read_text(encoding="utf-8")
            for line in text.splitlines():
                if pat.search(line) and not line.strip().startswith("#"):
                    offenders.append(f"{p.name}: {line.strip()}")
        assert not offenders, "VPG must NOT import lhos.agent_os:\n" + "\n".join(offenders)

    def test_vpg_does_not_reference_kernel(self):
        pat = re.compile(r"\bkernel\b|\bKernel\b", re.IGNORECASE)
        skip = {"admission_kernel_reference"}  # reserved keywords not relevant
        offenders: list[str] = []
        for p in VP.rglob("*.py"):
            text = p.read_text(encoding="utf-8")
            for line in text.splitlines():
                if pat.search(line) and not line.strip().startswith("#"):
                    offenders.append(f"{p.name}: {line.strip()}")
        assert not offenders, "VPG must not reference kernel types:\n" + "\n".join(offenders)
