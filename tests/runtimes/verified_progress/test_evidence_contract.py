"""Evidence-Contract Adversarial Audit — Phase D1.1 Steps 7-10.

Proves that the evidence contract enforces strict semantic constraints:

  S7  Action COMMITTED != Task VERIFIED
       Only result=pass + exact-bindings authorizes Task.VERIFIED.
  S8  Evidence Version Exactness
       Repinning an artifact to a new version invalidates previously-VERIFIED
       evidence; only evidence matching the new pin re-verifies.
  S9  Evidence Immutability
       Committed evidence cannot be deleted, mutated, or rebound. Re-playing
       a committed evidence patch is a no-op idempotent replay.
  S10 Patch Atomicity
       A patch with N operations where any k-th op fails commits 0 of the N
       ops — no prefix commit, version unchanged.

Each scenario constructs a controlled in-memory VPG with a known facts
provider, then proves the evidence contract enforces the mandated behavior.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import (
    ArtifactRefNode,
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    NodeLifecycle,
    NodeValidity,
    NodeType,
    TaskNode,
    VerificationNode,
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
        "steps_covered": [
            "S7_action_committed_neq_verified",
            "S8_evidence_version_exactness",
            "S9_evidence_immutability",
            "S10_patch_atomicity",
        ],
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
        "full_vpg_suite_after": "(see pytest stdout)",
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/evidence-contract-results.json"
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

    ``actions`` is ``dict[action_id, FakeAction]`` (kernel journal).
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


# ── Graph bootstrap + helpers ─────────────────────────────────────────────────

AR1_URI = "lhs://artifacts/t1/output"
AR1_AID = "a1"
AR1_V1 = 1
AR1_HASH_V1 = "correct-hash-v1"
AR1_V2 = 2
AR1_HASH_V2 = "correct-hash-v2"


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


def _build_base_graph(rt, gid, facts, *, pin=True, v1_id="v1", t1_id="t1"):
    """Build: g1 -> v1 VERIFIES t1, optionally pin ar1@v1 to t1."""
    _submit(
        rt,
        gid,
        "init",
        (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id=v1_id, graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddNodeOp(node_id=t1_id, graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id=v1_id, target_node_id=t1_id,
                      created_by_pid="p1"),
        ),
    )
    if pin:
        _submit(
            rt,
            gid,
            "art1",
            (AttachArtifactOp(
                task_node_id=t1_id,
                artifact=ArtifactVersionBinding(
                    canonical_uri=AR1_URI, artifact_id=AR1_AID,
                    version=AR1_V1, content_hash=AR1_HASH_V1,
                ),
                created_by_pid="p1",
                edge_id="prod_t1_ar1",
            ),),
        )
    return v1_id, t1_id


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
    return VPGEdge(
        graph_id=gid,
        edge_type=EdgeType.PRODUCES,
        source_node_id=source_id,
        target_node_id=target_id,
        created_in_version=0,
        created_by_pid="p1",
        created_at=datetime.now(timezone.utc),
    )


def _evidence_with_result(gid, result, source_action_id, bindings=(),
                          *, produced_by_pid="p1", node_id="evi1"):
    return EvidenceNode(
        graph_id=gid,
        node_id=node_id,
        node_type=NodeType.EVIDENCE,
        evidence_kind="command_result",
        result=result,
        source_verification_id="v1",
        source_action_id=source_action_id,
        produced_by_pid=produced_by_pid,
        created_in_version=0,
        updated_in_version=0,
        created_by_pid="p1",
        artifact_bindings=tuple(bindings),
    )


def _task_validity(rt, gid, task_id="t1"):
    t = rt.inspect_node(gid, task_id)
    assert t is not None
    return t.validity


def _record(scenario_id, step, name, description, expected, actual_valid,
            code, msg, task_validity, verdict, *, extra=None):
    AUDIT_RESULTS[scenario_id] = {
        "id": scenario_id,
        "step": step,
        "name": name,
        "description": description,
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


# ══════════════════════════════════════════════════════════════════════════════
# Step 7 — Action COMMITTED ≠ Task VERIFIED
# ══════════════════════════════════════════════════════════════════════════════

class TestS7_ActionCommittedNotVerified:
    """Only result=pass + exact-bindings authorizes Task.VERIFIED."""

    def _setup(self, committed, actions=None, *, pin=True):
        facts = ControlledFacts(committed, actions)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts, pin=pin)
        return rt, gid, facts

    # ── S7a: committed action, evidence result=fail → reject ───────────────
    def test_S7a_committed_action_rejects_fail_result(self):
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed)
        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )

        evi = _evidence_with_result(
            gid, "fail", "act1", (binding,),
        )
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_FAIL_REJECTED

        # Inject + attach (patch commits even on fail-result evidence,
        # but derivation must NOT authorize Task.VERIFIED).
        _inject_evidence(rt, gid, evi)
        verdict = None
        try:
            _submit(rt, gid, "att-S7a", (
                AttachEvidenceOp(
                    verification_node_id="v1",
                    evidence_node_id="evi1",
                    created_by_pid="p1",
                    edge_id="pe_S7a",
                ),))
        except VPGError as e:
            verdict = e.code

        tv = _task_validity(rt, gid)
        assert tv != NodeValidity.VERIFIED, (
            f"FAIL-result evidence must never authorize VERIFIED; got {tv}"
        )

        _record(
            "S7a", 7, "committed_action_rejects_fail_result",
            "Committed-kernel action with evidence result=fail must NOT "
            "authorize Task.VERIFIED.",
            "reject", res.valid, res.code, res.message, tv, "PASS",
            extra={"attach_error": str(verdict) if verdict else None,
                   "attach_succeeded": verdict is None},
        )

    # ── S7b: pass but wrong artifact hash → reject with HASH_MISMATCH ──────
    def test_S7b_pass_wrong_artifact_hash(self):
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed)
        bad_binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash="WRONG-HASH",
        )
        evi = _evidence_with_result(gid, "pass", "act1", (bad_binding,))
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH

        _inject_evidence(rt, gid, evi)
        try:
            _submit(rt, gid, "att-S7b", (
                AttachEvidenceOp(verification_node_id="v1",
                                evidence_node_id="evi1",
                                created_by_pid="p1", edge_id="pe_S7b"),
            ))
        except VPGError:
            pass

        tv = _task_validity(rt, gid)
        assert tv != NodeValidity.VERIFIED
        _record(
            "S7b", 7, "pass_wrong_artifact_hash",
            "Committed action + pass result but wrong artifact hash must "
            "be rejected with EVIDENCE_ARTIFACT_HASH_MISMATCH.",
            "reject", res.valid, res.code, res.message, tv, "PASS",
        )

    # ── S7c: pass + exact pins → accept ────────────────────────────────────
    def test_S7c_pass_exact_pins_accepted(self):
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed)
        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi = _evidence_with_result(gid, "pass", "act1", (binding,))
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is True
        assert res.code is None

        _inject_evidence(rt, gid, evi)
        _submit(rt, gid, "att-S7c", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S7c"),
        ))
        tv = _task_validity(rt, gid)
        assert tv == NodeValidity.VERIFIED, (
            f"pass + exact pins MUST authorize VERIFIED; got {tv}"
        )
        _record(
            "S7c", 7, "pass_exact_pins_accepted",
            "Committed action + pass + exact pins MUST authorize Task.VERIFIED "
            "(positive control).",
            "accept", res.valid, res.code, res.message, tv, "PASS",
        )

    # ── S7d: result != "pass" (inconclusive) → reject ─────────────────────
    def test_S7d_non_pass_result_rejected(self):
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed)
        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        # result="inconclusive" — the closest analog to "no explicit pass";
        # EvidenceNode.result is typed as EvidenceResult enum (not nullable),
        # so None is rejected at Pydantic construction.
        evi = _evidence_with_result(gid, "inconclusive", "act1", (binding,))
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_INCONCLUSIVE_REJECTED
        _record(
            "S7d", 7, "non_pass_result_rejected",
            "Committed action with result != pass (inconclusive) must NOT "
            "produce valid=True.",
            "reject", res.valid, res.code, res.message, "n/a", "PASS",
        )

    # ── S7e: missing artifact binding — RISK scenarios ─────────────────────
    def test_S7e_empty_binding_task_no_pins(self):
        """Task pins NOTHING; evidence has 0 bindings → empty==empty → valid.

        This is the safe case mandated by the D1 exact-match contract.
        """
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed, pin=False)
        evi = _evidence_with_result(gid, "pass", "act1", ())
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )
        assert res.valid is True, (
            "empty-evidence-binding with task that pins nothing MUST be "
            "valid (empty==empty exact match)"
        )
        _record(
            "S7e_no_pins", 7, "empty_binding_task_no_pins",
            "Task pins nothing + evidence has 0 bindings → valid=True "
            "(empty==empty exact match).",
            "accept", res.valid, res.code, res.message, "n/a", "PASS",
        )

    def test_S7eR_empty_binding_task_has_pins(self):
        """RISK: Task pins ar1@v1=hash1, but evidence has 0 bindings.

        The D1 spec line 8 says: 'Task currently pins E's exact artifact
        versions (no silent cross-version validation)'.  Under strict
        reading, an evidence with NO bindings should NOT verify a task that
        DOES pin artifacts — the bind-sets differ ({pins} vs {}).

        The current implementation treats empty evidence_versions as a
        vacuous-match (the `if evidence_versions !=` guard skips when empty),
        which means an empty-binding evidence can verify ANY task regardless
        of its pins.  This is documented here as a surviving risk.
        """
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        rt, gid, facts = self._setup(committed, pin=True)
        evi = _evidence_with_result(gid, "pass", "act1", ())
        nodes, edges = _graph_nodes_edges(rt, gid)
        res = validate_evidence(
            evi,
            existing_nodes={**nodes, "evi1": evi},
            existing_edges=[*edges, _produces_edge("v1", "evi1", gid)],
            facts_artifact=facts,
            facts_kernel=facts,
        )

        # Under strict D1 reading, this should be REJECT (HASH_MISMATCH).
        # The current code ACCEPTS it.  Document as a surviving RISK.
        if res.valid is True:
            verdict = "RISK"
            evidence_str = (
                "validate_evidence REJECTED=expected-but-got-Accept; "
                "empty-binding evidence validated against task WITH pins; "
                "silent vacuous-match permits cross-version bypass"
            )
        else:
            verdict = "PASS"
            evidence_str = (
                "validate_evidence correctly rejected empty-binding evidence "
                "when task pins artifacts"
            )
        _record(
            "S7eR_has_pins", 7, "empty_binding_task_has_pins",
            "RISK: Task pins artifacts but evidence has 0 bindings — strict "
            "D1 reading says REJECT; current code may vacuously ACCEPT.",
            "reject", res.valid, res.code, res.message, "n/a", verdict,
            extra={"verbatim": evidence_str},
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 8 — Evidence Version Exactness
# ══════════════════════════════════════════════════════════════════════════════

class TestS8_EvidenceVersionExactness:
    """Prove v2 pin invalidates previously-VERIFIED v1 evidence."""

    def _setup(self):
        committed = {
            (AR1_URI, AR1_V1): AR1_HASH_V1,
            (AR1_URI, AR1_V2): AR1_HASH_V2,
        }
        actions = {
            "act1": FakeAction("act1"),
            "act2": FakeAction("act2"),
        }
        facts = ControlledFacts(committed, actions)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts, pin=True)
        return rt, gid, facts

    # ── S8a: prior verified ────────────────────────────────────────────────
    def test_S8a_prior_verified(self):
        rt, gid, facts = self._setup()
        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi1 = _evidence_with_result(gid, "pass", "act1", (binding,))
        _inject_evidence(rt, gid, evi1)
        _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S8a"),
        ))
        tv = _task_validity(rt, gid)
        assert tv == NodeValidity.VERIFIED, f"v1 evidence must verify; got {tv}"
        _record(
            "S8a", 8, "prior_verified",
            "v1 evidence attached → task becomes VERIFIED (baseline).",
            "accept", True, None, "ok", tv, "PASS",
        )

    # ── S8b: v2 pin invalidates t1 ─────────────────────────────────────────
    def test_S8b_v2_invalidates_t1(self):
        rt, gid, facts = self._setup()
        # First verify with v1
        binding_v1 = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi1 = _evidence_with_result(gid, "pass", "act1", (binding_v1,))
        _inject_evidence(rt, gid, evi1)
        _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S8b1"),
        ))
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED

        # Repin to v2
        _submit(rt, gid, "repin-v2", (
            AttachArtifactOp(
                task_node_id="t1",
                artifact=ArtifactVersionBinding(
                    canonical_uri=AR1_URI, artifact_id=AR1_AID,
                    version=AR1_V2, content_hash=AR1_HASH_V2,
                ),
                created_by_pid="p1",
                edge_id="prod_t1_ar1_v2",
            ),))
        tv = _task_validity(rt, gid)
        assert tv != NodeValidity.VERIFIED, (
            f"v2 repin must invalidate previously-VERIFIED v1 task; got {tv}"
        )
        _record(
            "S8b", 8, "v2_invalidates_t1",
            "Repinning artifact to v2 MUST invalidate previously-VERIFIED "
            "v1 evidence; task must leave VERIFIED.",
            "reject", False, None, "v2 pin != v1 evidence", tv, "PASS",
            extra={"post_repin_validity": str(tv)},
        )

    # ── S8c: v2 evidence reverifies ────────────────────────────────────────
    def test_S8c_v2_evidence_reverifies(self):
        rt, gid, facts = self._setup()
        # Verify with v1
        binding_v1 = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi1 = _evidence_with_result(gid, "pass", "act1", (binding_v1,))
        _inject_evidence(rt, gid, evi1)
        _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S8c1"),
        ))
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED

        # Repin to v2
        _submit(rt, gid, "repin-v2", (
            AttachArtifactOp(
                task_node_id="t1",
                artifact=ArtifactVersionBinding(
                    canonical_uri=AR1_URI, artifact_id=AR1_AID,
                    version=AR1_V2, content_hash=AR1_HASH_V2,
                ),
                created_by_pid="p1",
                edge_id="prod_t1_ar1_v2",
            ),))
        assert _task_validity(rt, gid) != NodeValidity.VERIFIED

        # Submit v2 evidence via act2
        binding_v2 = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V2, content_hash=AR1_HASH_V2,
        )
        evi2 = _evidence_with_result(
            gid, "pass", "act2", (binding_v2,), node_id="evi2")
        _inject_evidence(rt, gid, evi2)
        _submit(rt, gid, "evi2", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi2",
                            created_by_pid="p1", edge_id="pe_S8c2"),
        ))
        tv = _task_validity(rt, gid)
        assert tv == NodeValidity.VERIFIED, (
            f"v2 evidence must re-verify task after v2 pin; got {tv}"
        )
        _record(
            "S8c", 8, "v2_evidence_reverifies",
            "New evidence binding v2 pin re-verifies the task after repin.",
            "accept", True, None, "v2 evidence matches v2 pin", tv, "PASS",
        )

    # ── Idempotency: re-submitting evi1 by same key does NOT revert state ──
    def test_S8d_idempotent_evi1_no_revert(self):
        rt, gid, facts = self._setup()
        # Verify with v1, repin to v2
        binding_v1 = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi1 = _evidence_with_result(gid, "pass", "act1", (binding_v1,))
        _inject_evidence(rt, gid, evi1)
        _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S8d"),
        ))
        _submit(rt, gid, "repin-v2", (
            AttachArtifactOp(
                task_node_id="t1",
                artifact=ArtifactVersionBinding(
                    canonical_uri=AR1_URI, artifact_id=AR1_AID,
                    version=AR1_V2, content_hash=AR1_HASH_V2,
                ),
                created_by_pid="p1",
                edge_id="prod_t1_ar1_v2",
            ),))
        tv_after_repin = _task_validity(rt, gid)

        # Re-play evi1 with the SAME idempotency_key
        re = _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S8d"),
        ))
        assert re.idempotent_replay is True
        assert re.patch_applied is False

        tv_after_replay = _task_validity(rt, gid)
        assert tv_after_replay == tv_after_repin, (
            f"Replay must NOT revert state; after_repin={tv_after_repin} "
            f"after_replay={tv_after_replay}"
        )
        _record(
            "S8d", 8, "idempotent_evi1_no_revert",
            "Re-playing committed evidence patch with the same idempotency "
            "key is a no-op and must NOT revert task state.",
            "reject", False, None, "idempotent replay", tv_after_replay, "PASS",
            extra={"idempotent_replay": True,
                   "patch_applied": False,
                   "state_unchanged": tv_after_replay == tv_after_repin},
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 9 — Evidence Immutability
# ══════════════════════════════════════════════════════════════════════════════

class TestS9_EvidenceImmutability:
    """Prove old evidence cannot be deleted, modified, or rebound."""

    def _setup(self):
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts, pin=True)

        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi1 = _evidence_with_result(gid, "pass", "act1", (binding,))
        _inject_evidence(rt, gid, evi1)
        _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S9"),
        ))
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED
        return rt, gid, facts

    # ── S9a: no mutation API exists ────────────────────────────────────────
    def test_S9a_no_mutation_api(self):
        import lhos.runtimes.verified_progress.patches as patches_mod

        # No DeleteNodeOp, UpdateNodeOp, SetTaskPinsOp, or similar exist.
        assert not hasattr(patches_mod, "DeleteNodeOp"), (
            "DeleteNodeOp must NOT exist — evidence cannot be deleted"
        )
        assert not hasattr(patches_mod, "UpdateNodeOp"), (
            "UpdateNodeOp must NOT exist — committed evidence cannot be mutated"
        )
        assert not hasattr(patches_mod, "SetTaskPinsOp"), (
            "SetTaskPinsOp must NOT exist — pins mutate only via AttachArtifactOp"
        )

        # Sanity: exactly the 4 expected ops exist
        from lhos.runtimes.verified_progress.patches import PatchOpType
        assert set(PatchOpType) == {
            "add_node", "add_edge", "attach_artifact", "attach_evidence",
        }

        _record(
            "S9a", 9, "no_mutation_api",
            "Runtime exposes NO public op for deleting, updating, or "
            "rebinding a committed EvidenceNode; PatchOpType has exactly "
            "the 4 expected variants.",
            "reject", False, None, "no mutation ops", "n/a", "PASS",
        )

    # ── S9b: re-binding / re-creating committed evidence node is illegal ───
    def test_S9b_evidence_node_immutable(self):
        rt, gid, facts = self._setup()

        # B1: AddNodeOp("evi1") again — node id already committed → reject.
        with pytest.raises(VPGError) as excinfo:
            _submit(rt, gid, "dup-evi1", (
                AddNodeOp(node_id="evi1", graph_id=gid, node_type="evidence",
                          created_by_pid="p1",
                          source_verification_id="v1",
                          produced_by_pid="p1"),
            ))
        assert excinfo.value.code == VPGCode.NODE_ALREADY_EXISTS

        # B2: AttachEvidenceOp pointing at a non-existent evidence node → reject.
        with pytest.raises(VPGError) as excinfo2:
            _submit(rt, gid, "ghost-evi", (
                AttachEvidenceOp(verification_node_id="v1",
                                evidence_node_id="ghost-evidence",
                                created_by_pid="p1",
                                edge_id="pe_ghost"),
            ))
        assert excinfo2.value.code == VPGCode.NODE_NOT_FOUND

        # B3: the committed EvidenceNode's artifact_bindings and result are
        # frozen — inspect the node to prove they remain exactly
        # result=pass + AR1_URI@v1=hash1, unchanged.
        evi_after = rt.inspect_node(gid, "evi1")
        assert isinstance(evi_after, EvidenceNode)
        assert evi_after.result.value == "pass"
        assert len(evi_after.artifact_bindings) == 1
        b = evi_after.artifact_bindings[0]
        assert b.canonical_uri == AR1_URI and b.version == AR1_V1
        assert b.content_hash == AR1_HASH_V1

        _record(
            "S9b", 9, "evidence_node_immutable",
            "Re-creating committed EvidenceNode id raises NODE_ALREADY_EXISTS; "
            "AttachEvidenceOp pointing at non-existent evidence raises "
            "NODE_NOT_FOUND; the committed EvidenceNode's result + bindings "
            "remain exactly as committed (no mutation API exists).",
            "reject", False, None, "node uniqueness + frozen fields", "n/a",
            "PASS",
            extra={"duplicate_node_code": VPGCode.NODE_ALREADY_EXISTS.value,
                   "ghost_edge_code": VPGCode.NODE_NOT_FOUND.value,
                   "frozen_result": evi_after.result.value,
                   "frozen_binding": f"{b.canonical_uri}@{b.version}={b.content_hash}"},
        )

    # ── S9d (FIXED): AttachEvidenceOp.edge_id is honored ──────────────────
    def test_S9d_edge_id_honored(self):
        """FIXED: patch_validator now passes edge_id=op.edge_id when building
        the PRODUCES edge for AttachEvidenceOp (both the validate_evidence
        preview and the committed edge).

        Consequences after fix:
          1. The caller-supplied edge_id IS written to the committed edge.
          2. The EDGE_ALREADY_EXISTS guard on AttachEvidenceOp is now live.
          3. Re-submitting the SAME evidence with a NEW idempotency key
             raises EDGE_ALREADY_EXISTS — structural duplicates are blocked.
        """
        # Build a fresh graph WITHOUT the pre-attached evi1 so this test
        # controls the committed edge_id for the evidence under test.
        committed = {(AR1_URI, AR1_V1): AR1_HASH_V1}
        facts = ControlledFacts(committed)
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_base_graph(rt, gid, facts, pin=True)

        # Inject a SEPARATE evidence node (evi_edge) for the edge_id probe.
        binding = ArtifactVersionBinding(
            canonical_uri=AR1_URI, artifact_id=AR1_AID,
            version=AR1_V1, content_hash=AR1_HASH_V1,
        )
        evi_edge = _evidence_with_result(gid, "pass", "act1", (binding,),
                                         node_id="evi_edge")
        _inject_evidence(rt, gid, evi_edge)

        caller_edge_id = "my-stable-edge-id"
        pr1 = _submit(rt, gid, "edge-first", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi_edge",
                            created_by_pid="p1",
                            edge_id=caller_edge_id),
        ))
        assert pr1.patch_applied is True

        edges = rt.store.get_all_edges(gid)
        produces_edges = [e for e in edges if e.source_node_id == "v1"
                          and e.target_node_id == "evi_edge"]
        committed_edge_id = produces_edges[0].edge_id
        assert committed_edge_id == caller_edge_id, (
            f"caller edge_id must be honored; got {committed_edge_id!r}"
        )

        # Re-attach the SAME evidence with a DIFFERENT idempotency key —
        # the EDGE_ALREADY_EXISTS guard now fires because committed edge_id
        # equals the caller-supplied edge_id.
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "edge-second", (
                AttachEvidenceOp(verification_node_id="v1",
                                evidence_node_id="evi_edge",
                                created_by_pid="p1",
                                edge_id=caller_edge_id),
            ))
        assert ei.value.code == VPGCode.EDGE_ALREADY_EXISTS, (
            f"expected EDGE_ALREADY_EXISTS; got {ei.value.code}"
        )

        # GraphVersion must NOT advance on the rejected duplicate.
        ver_after = rt.get_graph(gid).current_version
        assert ver_after == pr1.committed_graph_version, (
            f"rejected duplicate attach must not bump graph version; "
            f"before={pr1.committed_graph_version} after={ver_after}"
        )

        # Exactly one edge v1→evi_edge exists.
        dup_count = sum(1 for e in rt.store.get_all_edges(gid)
                        if e.source_node_id == "v1"
                        and e.target_node_id == "evi_edge")
        assert dup_count == 1, f"expected exactly 1 produces edge; got {dup_count}"

        _record(
            "S9d", 9, "edge_id_honored_PASS",
            "FIXED: AttachEvidenceOp.edge_id is honored (passed to VPGEdge). "
            "EDGE_ALREADY_EXISTS guard is live: re-attach with new idempotency "
            "key raises EDGE_ALREADY_EXISTS and graph version is unchanged. "
            "Exactly one produces edge exists.",
            "reject", False, VPGCode.EDGE_ALREADY_EXISTS,
            "edge_id honored + guard live",
            "n/a", "PASS",
            extra={
                "edge_id_honored": True,
                "committed_edge_id": committed_edge_id,
                "duplicate_edge_created": False,
                "produces_edge_count_v1_evi_edge": dup_count,
            },
        )

    # ── S9c: same idempotency_key replay is a no-op ────────────────────────
    def test_S9c_idempotent_replay(self):
        rt, gid, facts = self._setup()
        ver_before = rt.get_graph(gid).current_version

        pr = _submit(rt, gid, "evi1", (
            AttachEvidenceOp(verification_node_id="v1",
                            evidence_node_id="evi1",
                            created_by_pid="p1", edge_id="pe_S9"),
        ))
        assert pr.patch_applied is False
        assert pr.idempotent_replay is True
        ver_after = rt.get_graph(gid).current_version
        assert ver_after == ver_before, (
            f"idempotency replay must NOT bump version; before={ver_before} "
            f"after={ver_after}"
        )

        _record(
            "S9c", 9, "idempotent_replay",
            "Re-submitting committed evidence patch by same idempotency_key "
            "returns patch_applied=False + idempotent_replay=True; graph "
            "version unchanged.",
            "reject", False, None, "idempotent replay", "n/a", "PASS",
            extra={"patch_applied": False, "idempotent_replay": True,
                   "version_unchanged": ver_after == ver_before},
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 10 — Patch Atomicity (no prefix commit)
# ══════════════════════════════════════════════════════════════════════════════

class TestS10_PatchAtomicity:
    """A patch with any failing op commits NONE of its ops."""

    def _fresh_rt(self):
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        return rt, rec.graph_id

    def test_S10a_last_op_invalid(self):
        """Patch = [AddNodeOp(t1), AddEdgeOp(invalid→broken)]. Edge fails."""
        rt, gid = self._fresh_rt()
        ver_before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as excinfo:
            _submit(rt, gid, "patch10a", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T1"),
                AddEdgeOp(edge_id="e_bad10a", edge_type="verifies",
                          source_node_id="nonexistent-v",
                          target_node_id="t1",
                          created_by_pid="p1"),
            ))
        assert excinfo.value.code == VPGCode.EDGE_SOURCE_NOT_FOUND

        ver_after = rt.get_graph(gid).current_version
        assert ver_after == ver_before, (
            f"failed patch must NOT bump version; before={ver_before} "
            f"after={ver_after}"
        )
        assert rt.inspect_node(gid, "t1") is None, (
            "AddNodeOp(t1) in a failed patch must NOT persist (no prefix commit)"
        )
        _record(
            "S10a", 10, "last_op_invalid",
            "Patch with last op invalid commits 0 ops: graph version "
            "unchanged, t1 not persisted.",
            "reject", False, None, "atomic rollback", "n/a", "PASS",
            extra={"error_code": VPGCode.EDGE_SOURCE_NOT_FOUND.value,
                   "version_unchanged": ver_after == ver_before,
                   "t1_not_persisted": rt.inspect_node(gid, "t1") is None},
        )

    def test_S10b_middle_op_invalid(self):
        """Patch = [AddNodeOp(t1), AddEdgeOp(invalid), AddNodeOp(t2)]."""
        rt, gid = self._fresh_rt()
        ver_before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as excinfo:
            _submit(rt, gid, "patch10b", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T1"),
                AddEdgeOp(edge_id="e_bad10b", edge_type="verifies",
                          source_node_id="nonexistent-v",
                          target_node_id="t1",
                          created_by_pid="p1"),
                AddNodeOp(node_id="t2", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T2"),
            ))
        assert excinfo.value.code == VPGCode.EDGE_SOURCE_NOT_FOUND

        ver_after = rt.get_graph(gid).current_version
        assert ver_after == ver_before
        assert rt.inspect_node(gid, "t1") is None, (
            "t1 (prefix) must NOT persist when middle op fails"
        )
        assert rt.inspect_node(gid, "t2") is None, (
            "t2 (suffix) must NOT persist when middle op fails"
        )
        _record(
            "S10b", 10, "middle_op_invalid",
            "Patch with middle op invalid commits 0 ops: t1 and t2 both "
            "absent, version unchanged.",
            "reject", False, None, "atomic rollback", "n/a", "PASS",
            extra={"t1_absent": rt.inspect_node(gid, "t1") is None,
                   "t2_absent": rt.inspect_node(gid, "t2") is None},
        )

    def test_S10c_first_op_invalid(self):
        """Patch = [AddEdgeOp(invalid), AddNodeOp(t1), AddNodeOp(t2)]."""
        rt, gid = self._fresh_rt()
        ver_before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as excinfo:
            _submit(rt, gid, "patch10c", (
                AddEdgeOp(edge_id="e_bad10c", edge_type="verifies",
                          source_node_id="nonexistent-v",
                          target_node_id="nonexistent-t",
                          created_by_pid="p1"),
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T1"),
                AddNodeOp(node_id="t2", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T2"),
            ))
        assert excinfo.value.code == VPGCode.EDGE_SOURCE_NOT_FOUND

        ver_after = rt.get_graph(gid).current_version
        assert ver_after == ver_before
        assert rt.inspect_node(gid, "t1") is None
        assert rt.inspect_node(gid, "t2") is None
        _record(
            "S10c", 10, "first_op_invalid",
            "Patch with first op invalid commits 0 ops: t1 and t2 both "
            "absent, version unchanged.",
            "reject", False, None, "atomic rollback", "n/a", "PASS",
        )
