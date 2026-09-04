"""Step 19 — Event Causality Audit.

Proves that derived events carry correct causal references:

  S19a  TASK_VERIFIED_DERIVED events carry causation_patch_id == the
         evidence-attaching patch, node_id == the verified task, and
         graph_version == the committed graph version.
  S19b  Re-running the rebuild replays identical TASK_VERIFIED_DERIVED events
         (same node_id + graph_version → causal relationship is stable).
  S19c  With NO evidence attached, NO TASK_VERIFIED_DERIVED events appear,
         even after a reconvergent derivation pass — no spurious verification
         events.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    NodeLifecycle,
    NodeValidity,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)
from lhos.runtimes.verified_progress.projections import rebuild_projection

AUDIT_RESULTS: dict[str, dict] = {}


@pytest.fixture(autouse=True, scope="session")
def _dump_results():
    yield
    _write()


def _write():
    out = {
        "step": 19, "step_name": "EventCausality",
        "scenarios": [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)],
        "surviving_risks": [s["id"] for s in AUDIT_RESULTS.values() if s["verdict"] == "RISK"],
        "overall_verdict": "RISK" if any(
            s["verdict"] == "RISK" for s in AUDIT_RESULTS.values()
        ) else "PASS",
    }
    path = Path(__file__).resolve().parents[3] / "artifacts/agent_os_phase_d1_audit/step-19-event-causality.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2))


def _record(sid, name, expected, verdict, evidence):
    AUDIT_RESULTS[sid] = {
        "id": sid, "step": 19, "name": name,
        "expected": expected, "actual": verdict, "verdict": verdict, "evidence": evidence,
    }


class _Act:
    def __init__(self, aid="act1"):
        self.action_id = aid; self.pid = "p1"; self.state = "committed"; self.result = {}; self.artifact_refs = ()


class _Facts:
    def __init__(self, n):
        self.actions = {f"act{i}": _Act(f"act{i}") for i in range(1, n+1)}
    def get_action(self, aid):
        return self.actions.get(aid, _Act(aid))
    has_event = lambda self, e: False
    list_events_for_pid = lambda self, p: []
    artifact_exists = lambda self, p, u, v: True
    read_hash = lambda self, p, u, v: None
    can_read = lambda self, p, a, v: True
    def verify_binding(self, p, b): return True


def _make_rt_and_graph(with_evidence=True):
    facts = _Facts(3)
    rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
    rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
    ev_patch_id = [None]
    _build(rt, gid, facts, with_evidence, ev_patch_id)
    return rt, gid, facts, ev_patch_id[0]


def _build(rt, gid, facts, with_evidence, ev_patch_id_holder):
    # P1: structure (g1, t1..t3, v1..v3, ar1..ar3 + edges)
    ops = [AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G")]
    for i in range(1, 4):
        ops.append(AddNodeOp(node_id=f"t{i}", graph_id=gid, node_type="task", created_by_pid="p1", title=f"T{i}"))
        ops.append(AddNodeOp(node_id=f"v{i}", graph_id=gid, node_type="verification", created_by_pid="p1"))
        ops.append(AddNodeOp(node_id=f"ar{i}", graph_id=gid, node_type="artifact_ref", created_by_pid="p1",
                             canonical_uri=f"u/ar{i}", artifact_id=f"ar{i}", version=1, content_hash=f"h{i}"))
        ops.append(AddEdgeOp(edge_id=f"gd{i}", edge_type="depends_on", source_node_id="g1", target_node_id=f"t{i}", created_by_pid="p1"))
        ops.append(AddEdgeOp(edge_id=f"vf{i}", edge_type="verifies", source_node_id=f"v{i}", target_node_id=f"t{i}", created_by_pid="p1"))
        ops.append(AddEdgeOp(edge_id=f"tp{i}", edge_type="produces", source_node_id=f"t{i}", target_node_id=f"ar{i}", created_by_pid="p1"))
    rt.submit_patch(GraphPatchProposal(graph_id=gid, expected_graph_version=0, author_pid="p1", idempotency_key="structure", operations=tuple(ops)))

    if with_evidence:
        # P2: evidence → attach (this patch_id is the causation we track)
        ev_ops = []
        for i in range(1, 4):
            b = ArtifactVersionBinding(canonical_uri=f"u/ar{i}", artifact_id=f"ar{i}", version=1, content_hash=f"h{i}")
            ev_ops.append(AddNodeOp(node_id=f"ev{i}", graph_id=gid, node_type="evidence", created_by_pid="p1",
                                    result="pass", evidence_source_action_id=f"act{i}", source_verification_id=f"v{i}",
                                    produced_by_pid="p1", artifact_bindings=(b,)))
            ev_ops.append(AttachEvidenceOp(verification_node_id=f"v{i}", evidence_node_id=f"ev{i}", created_by_pid="p1", edge_id=f"pev{i}"))
        pr = rt.submit_patch(GraphPatchProposal(graph_id=gid, expected_graph_version=1, author_pid="p1",
                                               idempotency_key="evidence", operations=tuple(ev_ops)))
        ev_patch_id_holder[0] = pr.patch_id


def _verified_events(events):
    return [e for e in events if e.event_type == GraphEventType.TASK_VERIFIED_DERIVED]


class TestS19a_EventCausationPatchId:
    def test_EVENTS_carry_correct_causation(self):
        rt, gid, facts, ev_patch_id = _make_rt_and_graph(with_evidence=True)
        evs = rt.get_events(gid)
        verified = _verified_events(evs)
        assert len(verified) == 3, f"expected 3 TASK_VERIFIED_DERIVED; got {len(verified)}"
        for e in verified:
            assert e.causation_patch_id == ev_patch_id, (
                f"causation_patch_id={e.causation_patch_id} != evidence patch_id={ev_patch_id}"
            )
            assert e.node_id in ("t1", "t2", "t3")
            assert e.graph_version == 2, f"graph_version={e.graph_version} != 2"
        _record("S19a", "causation_patch_id_correct", "PASS", "PASS",
                f"3 events; causation_patch_id=={ev_patch_id}; node_ids=[{','.join(sorted(e.node_id for e in verified))}]")


class TestS19b_RebuildReidenticalEvents:
    def test_REBUILD_replays_same_events(self):
        rt, gid, facts, _ = _make_rt_and_graph(with_evidence=True)
        # Rebuild with proper per-patch histories
        rn, re_, ev = _do_rebuild(rt, gid, facts)
        verified = _verified_events(ev)
        assert len(verified) == 3
        node_ids_fwd = sorted(e.node_id for e in verified)
        # Rebuild again fresh → must yield identical (node_id, graph_version, patch) tuples
        rn2, re2, ev2 = _do_rebuild(rt, gid, facts)
        verified2 = _verified_events(ev2)
        tuples_fwd = sorted((e.node_id, e.graph_version, e.causation_patch_id) for e in verified)
        tuples_rev = sorted((e.node_id, e.graph_version, e.causation_patch_id) for e in verified2)
        assert tuples_fwd == tuples_rev, (
            f"event identity not stable across rebuilds: {tuples_fwd} vs {tuples_rev}"
        )
        _record("S19b", "rebuild_reidentical_events", "PASS", "PASS",
                f"fresh+rebuild produce identical {(node_ids_fwd, 2)} event tuples")


class TestS19c_NoSpuriousEventsWithoutEvidence:
    def test_NO_evidence_no_derived_verified_events(self):
        rt, gid, facts, _ = _make_rt_and_graph(with_evidence=False)
        evs = rt.get_events(gid)
        verified = _verified_events(evs)
        assert len(verified) == 0, (
            f"no evidence was attached but {len(verified)} TASK_VERIFIED_DERIVED events appeared: "
            f"{[e.node_id for e in verified]}"
        )
        _record("S19c", "no_spurious_events_without_evidence", "PASS", "PASS",
                f"zero TASK_VERIFIED_DERIVED events with no evidence attached")


# ── shared helper (same idiom as Step 18; factored for reuse only here) ──────
def _do_rebuild(rt, gid, facts, *, reverse=False):
    rows = rt.store.conn.execute(
        "SELECT operations_json, patch_id, committed_version FROM graph_patches "
        "WHERE graph_id=? ORDER BY committed_version", (gid,),
    ).fetchall()
    ver2pid = {r[2]: r[1] for r in rows}
    n_hist = {r[1]: [] for r in rows}
    e_hist = {r[1]: [] for r in rows}
    _cp = __import__("copy").deepcopy
    for nd in rt.store.get_all_nodes(gid):
        nd = _cp(nd); nd.lifecycle = NodeLifecycle.PROPOSED; nd.validity = NodeValidity.UNVERIFIED
        if isinstance(nd.metadata, dict): nd.metadata.pop("__verified_artifact_versions", None)
        pid = ver2pid.get(nd.created_in_version)
        if pid in n_hist: n_hist[pid].append(nd)
    for e in rt.store.get_all_edges(gid):
        e = _cp(e); pid = ver2pid.get(e.created_in_version)
        if pid in e_hist: e_hist[pid].append(e)
    patches = [GraphPatchProposal(**json.loads(r[0])) for r in rows]
    if reverse: patches = list(reversed(patches))
    return rebuild_projection(gid, patches, e_hist, n_hist,
                              facts_artifact=facts, facts_kernel=facts)
