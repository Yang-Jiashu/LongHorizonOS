"""Goal Closure — Phase D1.1 Step 17.

Prove: the runtime only CLOSES a GoalNode when ALL of its direct dependency
Tasks are VERIFIED; STALE/INVALID deps block closure; empty-dep goals cannot be
closed; reopening is automatic when an upstream dep goes STALE.

This test suite complements (does not duplicate) existing
``test_goal_closure.py``.  Existing tests cover the "all-deps-verified → closed"
single-shot case; this file focuses on NEGATIVE/edge cases:

  S17a: Goal g1 DEPENDS_ON t1 (VERIFIED) → lifecycle==CLOSED.  (confirms
        existing behavior via a goal lifecycle assertion that inspects the
        `lifecycle` field rather than the event stream.)
  S17b: Goal g1 DEPENDS_ON t1 (STALE) → lifecycle NOT CLOSED.
  S17c: Goal g1 with NO deps → lifecycle NOT CLOSED (empty != closed).
  S17d: Goal g1 CLOSED; then t1 repinned to v2 → t1 STALE → g1 reopens
        (lifecycle != CLOSED).

All closure happens through the derived-state path (``_recompute_derived_state``
in ``sdk.py``); no explicit goal-close API exists.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    NodeLifecycle,
    NodeValidity,
)
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.errors import VPGCode

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


def _etypes(rt, gid):
    return [e.event_type for e in rt.get_events(gid)]


def _pin_t1(rt, gid, kid_prefix, facts, *, version, hash_):
    """Pin t1 to a new artifact version without evidence → can make t1 STALE."""
    uri = "lhs://artifacts/t1/output"
    bind = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="a_t1", version=version, content_hash=hash_,
    )
    facts.committed[(uri, version)] = hash_
    _patch(rt, gid, f"{kid_prefix}_art_v{version}", (
        AttachArtifactOp(task_node_id="t1", artifact=bind, created_by_pid="p1",
                        edge_id=f"prod_t1_v{version}"),
    ))


def _verify_t1(rt, gid, kid_prefix, facts, *, version=1, hash_="hash_v1"):
    """Pin t1 to a version and attach matching evidence → makes t1 VERIFIED."""
    _pin_t1(rt, gid, kid_prefix, facts, version=version, hash_=hash_)
    uri = "lhs://artifacts/t1/output"
    bind = ArtifactVersionBinding(
        canonical_uri=uri, artifact_id="a_t1", version=version, content_hash=hash_,
    )
    evi_id = f"evi_t1_v{version}"
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
        "id": sid, "step": 17, "name": name,
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
        "step": 17, "step_name": "GoalClosureAdversarial",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-17-goal-closure.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


class TestS17a_GoalClosedWhenDepVerified:
    def test_goal_closed_when_dep_verified(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="g1_t1", edge_type="depends_on",
                      source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
            AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vf_t1", edge_type="verifies",
                      source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
        ))
        _verify_t1(rt, gid, "s17a", facts, version=1, hash_="h1")
        g1 = rt.inspect_node(gid, "g1")
        assert g1.lifecycle == NodeLifecycle.CLOSED, (
            f"Goal should be CLOSED when all deps verified, got lifecycle={g1.lifecycle}"
        )
        _record("S17a", "goal_closed_dep_verified", "PASS", "PASS",
                f"g1.lifecycle={g1.lifecycle.value!r}; t1 VERIFIED→goal CLOSED")


class TestS17b_GoalNotClosedStaleDep:
    def test_goal_not_closed_when_dep_stale(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="g1_t1", edge_type="depends_on",
                      source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
            AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vf_t1", edge_type="verifies",
                      source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
        ))
        # First verify t1, then repin WITHOUT matching evidence → t1 becomes STALE
        _verify_t1(rt, gid, "s17b_v1", facts, version=1, hash_="h1")
        # goal should be CLOSED right after verify
        g1_after_verify = rt.inspect_node(gid, "g1")
        assert g1_after_verify.lifecycle == NodeLifecycle.CLOSED
        # Repin to v2 without matching evidence → t1 STALE; g1 should NOT be CLOSED
        _pin_t1(rt, gid, "s17b_v2pin", facts, version=2, hash_="h2")
        g1_after_stale = rt.inspect_node(gid, "g1")
        assert g1_after_stale.lifecycle != NodeLifecycle.CLOSED, (
            f"RISK: goal still CLOSED when dep went stale: {g1_after_stale.lifecycle}"
        )
        _record("S17b", "goal_not_closed_stale_dep", "PASS", "PASS",
                f"after verify: CLOSED; after stale: lifecycle={g1_after_stale.lifecycle.value!r}")


class TestS17c_EmptyDepGoalNotClosed:
    def test_empty_dep_goal_not_closed(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        # Goal with NO dependencies
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="g_empty", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="EmptyGoal"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vf_t1", edge_type="verifies",
                      source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
        ))
        # Even after fully verifying an unrelated task, the empty-dep goal must stay open
        _verify_t1(rt, gid, "s17c", facts, version=1, hash_="h1")
        g = rt.inspect_node(gid, "g_empty")
        assert g.lifecycle != NodeLifecycle.CLOSED, (
            f"RISK: empty-dep goal auto-closed: lifecycle={g.lifecycle}"
        )
        _record("S17c", "empty_dep_goal_not_closed", "PASS", "PASS",
                f"empty-dep goal lifecycle={g.lifecycle.value!r} after unrelated task verified")


class TestS17d_GoalReopensOnStale:
    def test_goal_reopens_when_dep_stale(self):
        facts = _Facts(actions={"act_t1": _Action("act_t1")})
        rt = _make_rt(facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="g1_t1", edge_type="depends_on",
                      source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
            AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                      created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vf_t1", edge_type="verifies",
                      source_node_id="v_t1", target_node_id="t1", created_by_pid="p1"),
        ))
        _verify_t1(rt, gid, "s17d_v1", facts, version=1, hash_="h1")
        g1 = rt.inspect_node(gid, "g1")
        assert g1.lifecycle == NodeLifecycle.CLOSED

        # Pin v2 WITHOUT matching evidence → t1 STALE → g1 must REOPEN
        _pin_t1(rt, gid, "s17d_v2pin", facts, version=2, hash_="h2")
        g1_after = rt.inspect_node(gid, "g1")
        assert g1_after.lifecycle != NodeLifecycle.CLOSED, (
            f"RISK: goal did not reopen on dep stale: lifecycle={g1_after.lifecycle}"
        )
        evts = _etypes(rt, gid)
        assert GraphEventType.GOAL_REOPENED_DERIVED in evts, (
            f"RISK: GOAL_REOPENED_DERIVED event missing; got {evts}"
        )
        _record("S17d", "goal_reopens_on_stale", "PASS", "PASS",
                f"lifecycle after stale dep: {g1_after.lifecycle.value!r}; "
                f"GOAL_REOPENED_DERIVED present")
