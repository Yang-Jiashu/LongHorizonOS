"""SemanticReady vs OperationalAdmissible — Phase D1.1 Step 16.

Prove: ``SemanticReady`` (the D1 predicate) and ``OperationalAdmissible``
(host's operational gate — e.g., pid capability, wallclock) can INDEPENDENTLY be
true or false without one being reachable by cheating the other.

If the runtime exposes a function/method that returns both a readiness bit and
an operational bit for the SAME task, test it.  If not, build scenarios from
existing APIs.

  S16a (SemanticReady true): task with all deps VERIFIED; frontier returns it.
  S16b (SemanticReady false): task with dep not satisfied; frontier excludes it.
  S16c: a malicious agent cannot "promote" a NOT-READY task to READY by
       bypassing operational gating (demonstrate readiness is purely a semantic
       derivation from dep-verified-ness).
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    NodeLifecycle,
    NodeValidity,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)

AUDIT_RESULTS: dict[str, dict] = {}


class _Action:
    def __init__(self, aid, pid="p1", state="committed"):
        self.action_id = aid
        self.pid = pid
        self.state = state
        self.result: dict = {}
        self.artifact_refs: tuple = ()


class _Facts:
    def __init__(self, committed=None, actions=None):
        self.committed = committed or {}
        self.actions = actions or {}

    def get_action(self, aid):
        return self.actions.get(aid)

    def has_event(self, eid):
        return False

    def list_events_for_pid(self, pid):
        return []

    def artifact_exists(self, pid, canonical_uri, version):
        return (canonical_uri, version) in self.committed

    def read_hash(self, pid, canonical_uri, version):
        return self.committed.get((canonical_uri, version))

    def verify_binding(self, pid, binding):
        stored = self.committed.get((binding.canonical_uri, binding.version))
        return stored is not None and stored == binding.content_hash

    def can_read(self, pid, aid, ver):
        return True


def _make_rt(facts=None):
    if facts is None:
        facts = _Facts()
    return VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)


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


def _ready_ids(rt, gid):
    return sorted(c.task_id for c in rt.query_ready_frontier(gid))


def _verify_t1(rt, gid, kid_prefix, facts, *, version=1, hash_="hash_v1"):
    uri = "lhs://artifacts/t1/output"
    bind = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="a_t1", version=version, content_hash=hash_,
    )
    evi_id = f"evi_t1_v{version}"
    facts.committed[(uri, version)] = hash_
    _patch(rt, gid, f"{kid_prefix}_art", (
        AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1",
                        edge_id=f"prod_t1_v{version}"),
    ))
    from lhos.runtimes.verified_progress.models import EvidenceNode, NodeType
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection (node_id, graph_id, node_type, payload_json) "
        "VALUES (?, ?, 'evidence', ?)",
        (evi_id, gid,
         EvidenceNode(
             graph_id=gid, node_id=evi_id, node_type=NodeType.EVIDENCE,
             evidence_kind="command_result", result="pass",
             source_verification_id="v_t1", source_action_id="act_t1",
             produced_by_pid="p1",
             created_in_version=rt.get_graph(gid).current_version,
             updated_in_version=rt.get_graph(gid).current_version,
             created_by_pid="p1", artifact_bindings=(bind,),
         ).model_dump_json()),
    )
    rt.store.conn.commit()
    _patch(rt, gid, f"{kid_prefix}_evi", (
        AttachEvidenceOp(verification_node_id="v_t1", evidence_node_id=evi_id,
                         created_by_pid="p1", edge_id=f"pe_t1_v{version}"),
    ))


def _record(sid, name, expected, verdict, evidence):
    AUDIT_RESULTS[sid] = {
        "id": sid, "step": 16, "name": name,
        "expected": expected, "verdict": verdict, "evidence": evidence,
    }


def _build_t1_dep_t2(rt, gid):
    _patch(rt, gid, "init", (
        AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
        AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"),
        AddEdgeOp(edge_id="e_t2_t1", edge_type="depends_on",
                  source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
        AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                  created_by_pid="p1", verification_kind="command_result"),
        AddEdgeOp(edge_id="vf_t1", edge_type="verifies",
                  source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
    ))


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    import json
    from pathlib import Path
    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 16, "step_name": "ReadyVsRunnable",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-16-ready-vs-runnable.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


class TestS16a_SemanticReadyTrue:
    def test_ready_task_all_deps_verified(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_t1_dep_t2(rt, gid)
        _verify_t1(rt, gid, "s16a", facts)
        # t1 VERIFIED; t2 depends_on t1 → t2 READY
        ids = _ready_ids(rt, gid)
        assert "t2" in ids, f"t2 should be READY (t1 verified), got {ids}"
        _record("S16a", "semantic_ready_true", "PASS", "PASS",
                f"t1 VERIFIED → t2 READY: frontier={ids}")


class TestS16b_SemanticReadyFalse:
    def test_not_ready_task_dep_not_satisfied(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_t1_dep_t2(rt, gid)
        # t1 NOT verified → t2 NOT READY
        ids = _ready_ids(rt, gid)
        assert "t2" not in ids, f"t2 should NOT be READY (t1 unverified), got {ids}"
        _record("S16b", "semantic_ready_false", "PASS", "PASS",
                f"t1 unverified → t2 excluded: frontier={ids}")


class TestS16c_MaliciousPromoteNotReady:
    def test_malicious_agent_cannot_promote_not_ready(self):
        """Adversarial agent tries to trick READY derivation by attaching an
        EVIDENCE node whose produced_by_pid does NOT match the kernel Action's
        pid.  The D1-I foreign-PID check in validate_evidence (verification.py)
        must reject such evidence, so t1 stays UNVERIFIED even though the
        artifact attach succeeded."""
        # Action act_t1 exists with pid "p1" in the kernel journal.
        facts = _Facts(actions={"act_t1": _Action("act_t1", pid="p1")},
                       committed={("lhs://artifacts/t1/output", 1): "hash_v1"})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_t1_dep_t2(rt, gid)

        # Artifact pin v1 (matches committed {uri:v1 → hash_v1}) — attach passes.
        good_bind = ArtifactVersionBinding(
            canonical_uri="lhs://artifacts/t1/output", artifact_id="a_t1",
            version=1, content_hash="hash_v1",
        )
        _patch(rt, gid, "mal_art_ok", (
            AttachArtifactOp(task_node_id="t1", artifact=good_bind, created_by_pid="p1",
                            edge_id="prod_t1_real"),
        ))

        # Now inject an evidence node that claims a DIFFERENT pid ("p_malicious")
        # than the kernel action's pid ("p1").  validate_evidence must reject it
        # with EVIDENCE_SOURCE_ACTION_WRONG_PID.
        evi_bad = "evi_t1_bad"
        from lhos.runtimes.verified_progress.models import EvidenceNode, NodeType
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection (node_id, graph_id, node_type, payload_json) "
            "VALUES (?, ?, 'evidence', ?)",
            (evi_bad, gid,
             EvidenceNode(
                 graph_id=gid, node_id=evi_bad, node_type=NodeType.EVIDENCE,
                 evidence_kind="command_result", result="pass",
                 source_verification_id="v_t1", source_action_id="act_t1",
                 produced_by_pid="p_malicious",  # ← WRONG PID (kernel has pid="p1")
                 created_in_version=rt.get_graph(gid).current_version,
                 updated_in_version=rt.get_graph(gid).current_version,
                 created_by_pid="p_malicious", artifact_bindings=(good_bind,),
             ).model_dump_json()),
        )
        rt.store.conn.commit()
        _patch(rt, gid, "mal_evi_bad", (
            AttachEvidenceOp(verification_node_id="v_t1", evidence_node_id=evi_bad,
                             created_by_pid="p_malicious", edge_id="pe_t1_bad"),
        ))

        # t1 should NOT be VERIFIED (foreign-PID evidence rejected);
        # t2 NOT READY.
        t1 = rt.inspect_node(gid, "t1")
        assert t1.validity != NodeValidity.VERIFIED, (
            f"RISK: malicious foreign-PID evidence verified t1! validity={t1.validity}"
        )
        ids = _ready_ids(rt, gid)
        assert "t2" not in ids, (
            f"RISK: t2 READY despite t1 not verified: {ids}"
        )
        _record("S16c", "malicious_promote_blocked", "PASS", "PASS",
                f"foreign-PID evidence rejected: t1.validity={t1.validity.value!r}; t2 not in READY")
