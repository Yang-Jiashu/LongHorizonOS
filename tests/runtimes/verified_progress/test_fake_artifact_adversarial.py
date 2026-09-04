"""Fake Artifact Adversarial Audit — Phase D1.1 Step 6.

Proves that Fake Artifacts CANNOT fabricate semantic truth in the Verified
Progress Graph. Each scenario constructs a controlled in-memory VPG with a
facts provider whose committed ArtifactVersion store is known exactly, then
injects an EvidenceNode whose ``artifact_bindings`` reference versions /
hashes / URIs that do NOT correspond to committed ArtifactVersions via the
``ArtifactFactProvider`` protocol.

A correct runtime MUST reject each scenario WITHOUT fabricating VERIFIED state.

Scenarios:
  F1  Missing version      — evidence binds ar1@v99 (never committed)
  F2  Wrong hash           — evidence binds ar1@v1 with wrong content_hash
  F3  Synthetic URI        — evidence binds fakesChem://never-committed@v1
  F4  Hash collision       — evidence claims ar1@v1=hash_of_ar2 (identity confusion)
  F5  content-ref mismatch  — honest bindings but mismatched evidence_content_ref
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
    ArtifactRefNode,
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    NodeLifecycle,
    NodeValidity,
    NodeType,
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


# ── Audit results collector (session-scoped, dumped by fixture on teardown) ──

AUDIT_RESULTS: dict[str, dict] = {}


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    """After all tests in this file complete, write the structured JSON."""
    yield
    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
        "full_vpg_suite_final": "(see pytest stdout)",
    }
    # Path is computed relative to this file's location
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/fake-artifact-results.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


# ── Controlled (honest) facts provider ────────────────────────────────────────

class FakeAction:
    """Minimal KernelActionInfo double."""

    def __init__(self, action_id="act1", pid="p1", state="committed"):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result: dict = {}
        self.artifact_refs: tuple = ()


class ControlledFacts:
    """An *honest* ArtifactFactProvider backed by an explicit committed store.

    ``committed`` is ``dict[(canonical_uri, version), content_hash]``.
    ``verify_binding`` returns True iff the binding's (uri, version) is
    in the committed store AND its content_hash matches.

    This simulates the real Agent OS SDK contract: identity is
    ``(canonical_uri, version)``; the hash is only meaningful once the
    identity is established.
    """

    def __init__(
        self,
        committed: dict[tuple[str, int], str],
        actions: dict[str, FakeAction] | None = None,
    ):
        self.committed = dict(committed)
        self.actions = actions if actions is not None else {"act1": FakeAction()}

    # ── ArtifactFactProvider ───────────────────────────────────────────────
    def artifact_exists(self, pid, canonical_uri, version):
        return (canonical_uri, version) in self.committed

    def read_hash(self, pid, canonical_uri, version):
        return self.committed.get((canonical_uri, version))

    def verify_binding(self, pid, binding):
        key = (binding.canonical_uri, binding.version)
        if key not in self.committed:
            return False
        return self.committed[key] == binding.content_hash

    def can_read(self, pid, artifact_id, version):
        return True

    # ── KernelEventProvider ───────────────────────────────────────────────
    def get_action(self, action_id):
        return self.actions.get(action_id)

    def has_event(self, event_id):
        return False

    def list_events_for_pid(self, pid):
        return []


# ── Graph bootstrap helper ───────────────────────────────────────────────────

AR1_URI = "lhs://artifacts/t1/output"
AR1_AID = "a1"
AR1_V = 1
AR1_HASH = "correct-hash-v1"

AR2_URI = "lhs://artifacts/t1/other"
AR2_AID = "a2"
AR2_V = 1
AR2_HASH = "correct-hash-v2"


def _make_rt(facts):
    return VerifiedProgressRuntime(
        ":memory:",
        facts_artifact=facts,
        facts_kernel=facts,
    )


def _submit(rt, gid, kid, ops):
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=rt.get_graph(gid).current_version,
            author_pid="p1",
            idempotency_key=kid,
            operations=ops,
        )
    )


def _build_base_graph(rt, gid, facts, *, extra_attachments=()):
    """Build g1/v1/t1 + VERIFIES edge, then attach ar1@v1 as the artifact pin.

    Returns (v1_id, t1_id).
    """
    _submit(
        rt,
        gid,
        "init",
        (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id="v1", target_node_id="t1",
                      created_by_pid="p1"),
        ),
    )

    # Pin ar1@v1 to t1 (task now has one artifact version pinned)
    honest_binding = ArtifactVersionBinding(
        canonical_uri=AR1_URI, artifact_id=AR1_AID,
        version=AR1_V, content_hash=AR1_HASH,
    )
    _submit(
        rt,
        gid,
        "art1",
        (AttachArtifactOp(
            task_node_id="t1", artifact=honest_binding,
            created_by_pid="p1", edge_id="prod_t1_ar1",
        ),),
    )

    for op in extra_attachments:
        _submit(rt, gid, op["kid"], op["ops"])

    return "v1", "t1"


def _inject_evidence(rt, gid, evi):
    """Inject an existing EvidenceNode into the projection (bypass admission)."""
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection "
        "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
        (evi.node_id, gid, "evidence", evi.model_dump_json()),
    )
    rt.store.conn.commit()
    return evi



def _graph_nodes_edges(rt, gid):
    nodes = {n.node_id: n for n in rt.store.get_all_nodes(gid)}
    edges = list(rt.store.get_all_edges(gid))
    return nodes, edges


def _produces_edge(source_id, target_id, gid):
    """Build a candidate PRODUCES edge (used so validate_evidence reaches the
    artifact / content-ref validation step before the attach patch commits)."""
    from datetime import datetime, timezone
    return VPGEdge(
        graph_id=gid,
        edge_type=EdgeType.PRODUCES,
        source_node_id=source_id,
        target_node_id=target_id,
        created_in_version=0,
        created_by_pid="p1",
        created_at=datetime.now(timezone.utc),
    )


def _make_evidence(gid, bindings, *, produced_by_pid="p1", content_ref=None,
                   node_id="evi1"):
    """Construct an EvidenceNode with result=pass bound to a committed action."""
    return EvidenceNode(
        graph_id=gid,
        node_id=node_id,
        node_type=NodeType.EVIDENCE,
        evidence_kind="command_result",
        result="pass",
        source_verification_id="v1",
        source_action_id="act1",
        produced_by_pid=produced_by_pid,
        created_in_version=0,
        updated_in_version=0,
        created_by_pid="p1",
        artifact_bindings=tuple(bindings),
        evidence_content_ref=content_ref,
    )


def _task_validity(rt, gid, task_id="t1"):
    t = rt.inspect_node(gid, task_id)
    assert t is not None
    return t.validity


def _record(scenario_id, name, expected, actual_valid, code, msg, task_validity,
            verdict, *, extra=None):
    AUDIT_RESULTS[scenario_id] = {
        "id": scenario_id,
        "name": name,
        "expected": expected,
        "actual": "reject" if not actual_valid else "accept",
        "verdict": verdict,
        "evidence": (
            f"validate_evidence().valid={actual_valid}; "
            f"code={code}; msg={msg!r}; "
            f"task_validity={task_validity!r}"
        ),
    }
    if extra:
        AUDIT_RESULTS[scenario_id]["extra"] = extra


# ── Scenario F1: Missing version ─────────────────────────────────────────────

class TestF1_MissingVersion:
    def test_missing_version_rejected(self):
        committed = {(AR1_URI, AR1_V): AR1_HASH}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts)

        # Adversarial: bind ar1@v99 (v99 was never committed)
        bad = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=99, content_hash="any-hash",
            )],
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            bad,
            existing_nodes=nodes,
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        # Inject the adversarial evidence and attach it
        _inject_evidence(rt, gid, bad)
        verdict = None
        try:
            pr = _submit(
                rt,
                gid,
                "att-f1",
                (AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_f1",
                ),),
            )
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv in (NodeValidity.UNVERIFIED, NodeValidity.STALE), (
            f"task must NOT become VERIFIED from a missing-version evidence, "
            f"got validity={tv}"
        )

        # Honest bind in the same graph MUST validate True
        honest = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash=AR1_HASH,
            )],
            node_id="evi1-honest",
        )
        nodes2, edges2 = _graph_nodes_edges(rt, gid)
        honest_res = validate_evidence(
            honest,
            existing_nodes=nodes2,
            existing_edges=[*edges2, _produces_edge("v1", "evi1-honest", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert honest_res.valid is True, "honest bind must validate True"

        _record(
            "F1", "missing_version",
            "reject", res.valid, res.code, res.message, tv,
            "PASS",
            extra={
                "patch_applied": None if verdict else True,
                "patch_error_code": str(verdict) if verdict else None,
                "honest_valid": honest_res.valid,
            },
        )


# ── Scenario F2: Wrong hash ──────────────────────────────────────────────────

class TestF2_WrongHash:
    def test_wrong_hash_rejected(self):
        committed = {(AR1_URI, AR1_V): AR1_HASH}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts)

        # Adversarial: bind ar1@v1 with wrong content_hash
        bad = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash="wrong-hash",
            )],
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            bad, existing_nodes=nodes,
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        _inject_evidence(rt, gid, bad)
        verdict = None
        try:
            _submit(
                rt,
                gid,
                "att-f2",
                (AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_f2",
                ),),
            )
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv in (NodeValidity.UNVERIFIED, NodeValidity.STALE)

        honest = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash=AR1_HASH,
            )],
            node_id="evi1-honest",
        )
        n2, e2 = _graph_nodes_edges(rt, gid)
        honest_res = validate_evidence(
            honest, existing_nodes=n2,
            existing_edges=[*e2, _produces_edge("v1", "evi1-honest", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert honest_res.valid is True

        _record(
            "F2", "wrong_hash",
            "reject", res.valid, res.code, res.message, tv,
            "PASS",
            extra={
                "patch_applied": None if verdict else True,
                "patch_error_code": str(verdict) if verdict else None,
                "honest_valid": honest_res.valid,
            },
        )


# ── Scenario F3: Synthetic canonical URI ─────────────────────────────────────

class TestF3_SyntheticUri:
    def test_synthetic_uri_rejected(self):
        committed = {(AR1_URI, AR1_V): AR1_HASH}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts)

        # Adversarial: bind fakesChem://never-committed@v1 — URI never committed
        bad = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri="fakesChem://never-committed",
                artifact_id="fake-a", version=1,
                content_hash="any",
            )],
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            bad, existing_nodes=nodes,
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        _inject_evidence(rt, gid, bad)
        verdict = None
        try:
            _submit(
                rt,
                gid,
                "att-f3",
                (AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_f3",
                ),),
            )
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv in (NodeValidity.UNVERIFIED, NodeValidity.STALE)

        honest = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash=AR1_HASH,
            )],
            node_id="evi1-honest",
        )
        n2, e2 = _graph_nodes_edges(rt, gid)
        honest_res = validate_evidence(
            honest, existing_nodes=n2,
            existing_edges=[*e2, _produces_edge("v1", "evi1-honest", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert honest_res.valid is True

        _record(
            "F3", "synthetic_uri",
            "reject", res.valid, res.code, res.message, tv,
            "PASS",
            extra={
                "patch_applied": None if verdict else True,
                "patch_error_code": str(verdict) if verdict else None,
                "honest_valid": honest_res.valid,
            },
        )


# ── Scenario F4: Hash collision / identity confusion ─────────────────────────

class TestF4_HashCollision:
    def test_hash_collision_identity_confusion_rejected(self):
        """Two distinct artifacts exist. Evidence claims ar1@v1 has the hash
        of a DIFFERENT artifact (ar2@v1). Identity is (canonical_uri, version),
        NOT the hash alone — this MUST be rejected."""
        committed = {
            (AR1_URI, AR1_V): AR1_HASH,  # ar1@v1 = hash1
            (AR2_URI, AR2_V): AR2_HASH,  # ar2@v1 = hash2
        }
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id

        # Build base graph; task pins ONLY ar1@v1
        _build_base_graph(rt, gid, facts)

        # Adversarial: claim ar1@v1 = hash2 (which actually belongs to ar2@v1)
        bad = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash=AR2_HASH,  # hash of ar2, not ar1!
            )],
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            bad, existing_nodes=nodes,
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False, (
            "hash-collision identity confusion must be REJECTED: "
            f"valid={res.valid} code={res.code} msg={res.message!r}"
        )
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        _inject_evidence(rt, gid, bad)
        verdict = None
        try:
            _submit(
                rt,
                gid,
                "att-f4",
                (AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_f4",
                ),),
            )
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv in (NodeValidity.UNVERIFIED, NodeValidity.STALE), (
            f"task must NOT become VERIFIED via hash-collision, got {tv}"
        )

        # Honest bind: ar1@v1 with its true hash
        honest = _make_evidence(
            gid,
            [ArtifactVersionBinding(
                canonical_uri=AR1_URI, artifact_id=AR1_AID,
                version=AR1_V, content_hash=AR1_HASH,
            )],
            node_id="evi1-honest",
        )
        n2, e2 = _graph_nodes_edges(rt, gid)
        honest_res = validate_evidence(
            honest, existing_nodes=n2,
            existing_edges=[*e2, _produces_edge("v1", "evi1-honest", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert honest_res.valid is True

        _record(
            "F4", "hash_collision",
            "reject", res.valid, res.code, res.message, tv,
            "PASS",
            extra={
                "patch_applied": None if verdict else True,
                "patch_error_code": str(verdict) if verdict else None,
                "honest_valid": honest_res.valid,
            },
        )


# ── Scenario F5: evidence_content_ref mismatch ───────────────────────────────

class TestF5_ContentRefMismatch:
    def test_content_ref_mismatch_rejected(self):
        """Honest artifact_bindings but evidence_content_ref points to a
        non-existent / mismatched artifact. The runtime must reject at the
        content-ref check (verify lines 95-101 of verification.py), not just
        the artifact-binding loop."""
        committed = {(AR1_URI, AR1_V): AR1_HASH}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts)

        honest_binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V, content_hash=AR1_HASH,
        )
        bad_content_ref = ArtifactVersionBinding(
            canonical_uri="lhs://never-committed",
            artifact_id="fake-cref", version=1,
            content_hash="garbage",
        )
        evi = _make_evidence(
            gid,
            [honest_binding],
            content_ref=bad_content_ref,
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi, existing_nodes=nodes,
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False, (
            "content_ref mismatch must be REJECTED: "
            f"valid={res.valid} code={res.code} msg={res.message!r}"
        )
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        _inject_evidence(rt, gid, evi)
        verdict = None
        try:
            _submit(
                rt,
                gid,
                "att-f5",
                (AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_f5",
                ),),
            )
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv in (NodeValidity.UNVERIFIED, NodeValidity.STALE)

        # Honest bind in same graph (no content_ref) MUST validate True
        honest = _make_evidence(
            gid, [honest_binding], node_id="evi1-honest",
        )
        n2, e2 = _graph_nodes_edges(rt, gid)
        honest_res = validate_evidence(
            honest, existing_nodes=n2,
            existing_edges=[*e2, _produces_edge("v1", "evi1-honest", gid)],
            facts_artifact=facts, facts_kernel=facts,
        )
        assert honest_res.valid is True

        _record(
            "F5", "content_ref_mismatch",
            "reject", res.valid, res.code, res.message, tv,
            "PASS",
            extra={
                "patch_applied": None if verdict else True,
                "patch_error_code": str(verdict) if verdict else None,
                "honest_valid": honest_res.valid,
            },
        )
