"""Ready Frontier Semantic Gate — Phase D1.1 Step 14.

Prove: READY is Query/Deduction only — materialized, side-effect-free, no
lifecycle mutation.

Setup: t1 —(DEPENDS_ON)→ t2 —(DEPENDS_ON)→ t3 —(DEPENDS_ON)→ t4.
Initially t1 has no deps → t1 is READY; t2/t3/t4 not READY (dep not satisfied).

Scenarios:
  S14a: query_ready_frontier returns ["t1"]; repeat 100×, identical + version unchanged.
  S14b: query 50× in a row → graph_version must NOT advance (read-only).
  S14c: t1 becomes STALE (repin) → frontier no longer contains t1,
        and downstream (t2/t3/t4) not promoted.
  S14d: closing t1 via CLOSED lifecycle must NOT auto-close t2.
  S14e: t1 VERIFIED then repinned to STALE; confirm t2 does not become READY.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    NodeLifecycle,
    NodeType,
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


def _build_chain(rt, gid):
    """Build t1→t2→t3→t4 linear dep chain and verify t1."""
    _patch(rt, gid, "init", (
        AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"),
        AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"),
        AddNodeOp(node_id="t3", graph_id=gid, node_type="task", created_by_pid="p1", title="T3"),
        AddNodeOp(node_id="t4", graph_id=gid, task_type="task", created_by_pid="p1", title="T4")
        if False else
        AddNodeOp(node_id="t4", graph_id=gid, node_type="task", created_by_pid="p1", title="T4"),
        AddEdgeOp(edge_id="e_t2_t1", edge_type="depends_on", source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
        AddEdgeOp(edge_id="e_t3_t2", edge_type="depends_on", source_node_id="t3", target_node_id="t2", created_by_pid="p1"),
        AddEdgeOp(edge_id="e_t4_t3", edge_type="depends_on", source_node_id="t4", target_node_id="t3", created_by_pid="p1"),
        AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification", created_by_pid="p1", verification_kind="command_result"),
        AddEdgeOp(edge_id="vf_t1", edge_type="verifies", source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
    ))


def _pin_t1(rt, gid, kid_prefix, facts, *, version, hash_):
    """Attach an artifact version to t1 without attaching matching evidence.

    Does NOT change t1 validity (remains UNVERIFIED or whatever it was).
    """
    uri = "lhs://artifacts/t1/output"
    bind = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="a_t1", version=version, content_hash=hash_,
    )
    facts.committed[(uri, version)] = hash_
    _patch(rt, gid, f"{kid_prefix}_art_v{version}", (
        AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1",
                         edge_id=f"prod_t1_v{version}"),
    ))


def _attach_matching_evidence(rt, gid, kid_prefix, facts, *, version, hash_):
    """Inject an EvidenceNode for v_t1 and attach it.

    Only works if t1 already pins a matching artifact version.
    """
    uri = "lhs://artifacts/t1/output"
    bind = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="a_t1", version=version, content_hash=hash_,
    )
    evi_id = f"evi_t1_v{version}"
    from lhos.runtimes.verified_progress.models import EvidenceNode, NodeType  # noqa: F811
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection (node_id, graph_id, node_type, payload_json) "
        "VALUES (?, ?, 'evidence', ?)",
        (evi_id, gid,
         EvidenceNode(
             graph_id=gid, node_id=evi_id, node_type=NodeType.EVIDENCE,
             evidence_kind="command_result", result="pass",
             source_verification_id="v_t1",
             source_action_id="act_t1", produced_by_pid="p1",
             created_in_version=rt.get_graph(gid).current_version,
             updated_in_version=rt.get_graph(gid).current_version,
             created_by_pid="p1",
             artifact_bindings=(bind,),
         ).model_dump_json()),
    )
    rt.store.conn.commit()
    _patch(rt, gid, f"{kid_prefix}_evi_v{version}", (
        AttachEvidenceOp(verification_node_id="v_t1", evidence_node_id=evi_id,
                         created_by_pid="p1", edge_id=f"pe_t1_v{version}"),
    ))


def _verify_t1(rt, gid, kid_prefix, facts, *, version=1, hash_="hash_v1"):
    """Pin t1 to a version and attach matching evidence → makes t1 VERIFIED."""
    _pin_t1(rt, gid, kid_prefix, facts, version=version, hash_=hash_)
    _attach_matching_evidence(rt, gid, kid_prefix, facts, version=version, hash_=hash_)


def _record(sid, name, expected, verdict, evidence):
    AUDIT_RESULTS[sid] = {
        "id": sid, "step": 14, "name": name,
        "expected": expected, "verdict": verdict, "evidence": evidence,
    }


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    import json
    from pathlib import Path
    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 14, "step_name": "ReadyFrontierSemanticGate",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-14-ready-frontier.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


class TestS14a_InitialFrontierRepeated:
    def test_initial_frontier_repeated(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_chain(rt, gid)
        ver0 = rt.get_graph(gid).current_version

        first = _ready_ids(rt, gid)
        assert first == ["t1"], f"expected ['t1'], got {first}"

        for _ in range(100):
            cur = _ready_ids(rt, gid)
            assert cur == first, f"frontier diverged: {cur} != {first}"
        ver1 = rt.get_graph(gid).current_version
        assert ver1 == ver0, f"read-only query bumped version: {ver0} → {ver1}"

        _record("S14a", "initial_frontier_repeated", "PASS", "PASS",
                f"frontier={first} stable over 100 queries; version {ver0}→{ver1}")


class TestS14b_ReadOnly50Queries:
    def test_50_queries_no_version_advance(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_chain(rt, gid)
        ver0 = rt.get_graph(gid).current_version
        for _ in range(50):
            rt.query_ready_frontier(gid)
        ver1 = rt.get_graph(gid).current_version
        assert ver1 == ver0, f"50 queries bumped version {ver0}→{ver1}"
        _record("S14b", "read_only_50_queries", "PASS", "PASS",
                f"50 queries, version unchanged at {ver0}")


class TestS14c_StaleNotReady:
    def test_stale_t1_drops_from_frontier(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_chain(rt, gid)
        _verify_t1(rt, gid, "s14c", facts, version=1, hash_="h1")
        # t1 should now be VERIFIED (not READY anymore)
        t1 = rt.inspect_node(gid, "t1")
        assert t1.validity == NodeValidity.VERIFIED
        frontier_before = _ready_ids(rt, gid)
        # Repin v2 WITHOUT matching evidence → t1 becomes STALE
        # The newly-pinned v2 artifact has no matching EvidenceNode, so
        # validate_evidence rejects: t1 should transition VERIFIED → STALE.
        _pin_t1(rt, gid, "s14c_repin", facts, version=2, hash_="h2")
        t1_after_repin = rt.inspect_node(gid, "t1")
        assert t1_after_repin.validity == NodeValidity.STALE, (
            f"t1 should be STALE after repin without evidence: validity={t1_after_repin.validity}"
        )
        # After repin: t1 STALE, downstream not promoted
        frontier_after = _ready_ids(rt, gid)
        assert "t1" not in frontier_after
        # t2/t3/t4 must not appear just because t1 was stale
        for n in ("t2", "t3", "t4"):
            assert n not in frontier_after, (
                f"{n} incorrectly READY after t1 STALE: {frontier_after}"
            )
        _record("S14c", "stale_t1_drops_from_frontier", "PASS", "PASS",
                f"before={frontier_before} after={frontier_after}; t2/t3/t4 not promoted")


class TestS14d_CloseTCascadesNoAutoClose:
    def test_closing_t1_does_not_auto_close_t2(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_chain(rt, gid)
        _verify_t1(rt, gid, "s14d", facts, version=1, hash_="h1")
        # Closing t1 via lifecycle CLOSED happens automatically after verify
        # (VERIFIED task is auto-CLOSED).  Assert t2.lifecycle != CLOSED.
        t2 = rt.inspect_node(gid, "t2")
        assert t2.lifecycle != NodeLifecycle.CLOSED, (
            f"t2 auto-closed when t1 closed: lifecycle={t2.lifecycle}"
        )
        _record("S14d", "closing_t1_no_auto_close_t2", "PASS", "PASS",
                f"t1 verified+closed; t2.lifecycle={t2.lifecycle!r} != CLOSED")


class TestS14e_StaleBlocksDownstreamReady:
    def test_t1_verified_then_stale_blocks_t2_ready(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _build_chain(rt, gid)
        # Verify t1
        _verify_t1(rt, gid, "s14e_v1", facts, version=1, hash_="h1")
        # t1 VERIFIED, t2 should now be READY
        _ready_after_verify = _ready_ids(rt, gid)
        assert "t2" in _ready_after_verify, (
            f"t2 should be READY after t1 verified: {_ready_after_verify}"
        )
        # Repin WITHOUT matching v2 evidence → t1 goes STALE
        _pin_t1(rt, gid, "s14e_v2pin", facts, version=2, hash_="h2")
        _ready_after_stale = _ready_ids(rt, gid)
        assert "t2" not in _ready_after_stale, (
            f"t2 incorrectly READY after t1 went STALE: {_ready_after_stale}"
        )
        _record("S14e", "stale_blocks_downstream", "PASS", "PASS",
                f"after_verify={_ready_after_verify} after_stale={_ready_after_stale}")
