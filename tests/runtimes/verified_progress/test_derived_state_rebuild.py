"""Step 18 — Derived-State Deterministic Rebuild Audit.

Adversarial claim: the Verified Progress Graph projection is FULLY DERIVABLE
from committed patch history and must be byte-identical across:

  (a) verify_projection() succeeds on the freshly committed graph
  (b) after delete_projection() (SIGKILL simulation), verify_projection() fails
  (c) internal ``projections.rebuild_projection()`` with proper per-patch
      histories rebuilds the projection deterministically
  (d) three successive fresh rebuilds yield byte-identical projection hashes
      (idempotency: repeated crash converges)
  (e) rebuilding patches in REVERSE order still yields byte-identical projection
      (order-independence of the deterministic derivation)

Projection hash idiom: projection_fields_hash over
  node_id:node_type:lifecycle:validity and edge_id:edge_type:source:target
(sorted, timestamps excluded).

Graph: 1 Goal + 50 Tasks + 50 ArtifactRef + 50 Verification + 50 Evidence = 201
nodes; goal depends_on every task; tasks chain via depends_on; each
verification verifies exactly one task, produces exactly one evidence node, and
the task produces exactly one artifact_ref. Evidence binds ar_i@v1=hash_i.

BUILD-IDIO: evidence AddNodeOp and AttachEvidenceOp are put in the SAME
commit patch; otherwise _ops_to_nodes_edges(projection replay) constructs a
placeholder EvidenceNode without artifact_bindings — a known gap the audit
tracks but does not patch.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

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

# ── Audit results collector ──────────────────────────────────────────────────
AUDIT_RESULTS: dict[str, dict] = {}


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    _write_results()


def _write_results() -> None:
    import json
    from pathlib import Path

    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 18,
        "step_name": "DerivedStateRebuild",
        "nodes_in_graph": 201,
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "surviving_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-18-derived-state-rebuild.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


def _record(sid, name, expected, verdict, evidence, **extra):
    row = {
        "id": sid,
        "step": 18,
        "name": name,
        "expected": expected,
        "actual": verdict,
        "verdict": verdict,
        "evidence": evidence,
    }
    row.update(extra)
    AUDIT_RESULTS[sid] = row


# ── Lightweight Kernel/Artifact facts (no Agent OS SDK, pure-graph path) ────
class _FakeAction:
    def __init__(self, action_id="act1", pid="p1"):
        self.action_id = action_id
        self.pid = pid
        self.state = "committed"
        self.result: dict = {}
        self.artifact_refs: tuple = ()


class _Facts:
    """Serves every source_action_id as ``committed``; accepts every binding."""

    def __init__(self, n):
        self.actions = {f"act{i}": _FakeAction(f"act{i}") for i in range(1, n + 1)}

    def get_action(self, aid):
        if aid in self.actions:
            return self.actions[aid]
        return _FakeAction(aid)

    def has_event(self, eid):
        return False

    list_events_for_pid = lambda self, p: []
    artifact_exists = lambda self, p, u, v: True
    read_hash = lambda self, p, u, v: None
    verify_binding = lambda self, p, b: True
    can_read = lambda self, p, a, v: True


# ── Graph bootstrap ─────────────────────────────────────────────────────────
N_TASKS = 50


def _build_graph(rt, gid, facts):
    """Structure patch: goal + tasks + verifications + artifact_refs + edges."""
    ops = [
        AddNodeOp(
            node_id="g1", graph_id=gid, node_type="goal",
            created_by_pid="p1", title="Root goal",
        ),
    ]
    for i in range(1, N_TASKS + 1):
        ops.append(
            AddNodeOp(
                node_id=f"t{i}", graph_id=gid, node_type="task",
                created_by_pid="p1", title=f"T{i}",
            )
        )
        ops.append(
            AddNodeOp(
                node_id=f"v{i}", graph_id=gid, node_type="verification",
                created_by_pid="p1", verification_kind="command_result",
            )
        )
        ops.append(
            AddNodeOp(
                node_id=f"ar{i}", graph_id=gid, node_type="artifact_ref",
                created_by_pid="p1", canonical_uri=f"u/ar{i}",
                artifact_id=f"ar{i}", version=1, content_hash=f"hash-{i}",
            )
        )
        # g1 DEPENDS_ON t_i
        ops.append(
            AddEdgeOp(
                edge_id=f"g1-dep-t{i}", edge_type="depends_on",
                source_node_id="g1", target_node_id=f"t{i}",
                created_by_pid="p1",
            )
        )
        # v_i VERIFIES t_i
        ops.append(
            AddEdgeOp(
                edge_id=f"vf-{i}", edge_type="verifies",
                source_node_id=f"v{i}", target_node_id=f"t{i}",
                created_by_pid="p1",
            )
        )
        # t_i PRODUCES ar_i
        ops.append(
            AddEdgeOp(
                edge_id=f"tp-{i}", edge_type="produces",
                source_node_id=f"t{i}", target_node_id=f"ar{i}",
                created_by_pid="p1",
            )
        )
        # task chain: t_i DEPENDS_ON t_{i-1}
        if i > 1:
            ops.append(
                AddEdgeOp(
                    edge_id=f"dep-{i}-{i-1}", edge_type="depends_on",
                    source_node_id=f"t{i}", target_node_id=f"t{i-1}",
                    created_by_pid="p1",
                )
            )
    rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid, expected_graph_version=0, author_pid="p1",
            idempotency_key="s1-structure", operations=tuple(ops),
        )
    )

    # Evidence patches: per-task evidence AddNodeOp + AttachEvidenceOp co-located.
    # Split into patches of 25 tasks to stay well under MAX_PATCH_OPS=500.
    for batch_start in (1, N_TASKS // 2 + 1):
        batch_end = batch_start + N_TASKS // 2
        ev_ops = []
        for i in range(batch_start, batch_end):
            b = ArtifactVersionBinding(
                canonical_uri=f"u/ar{i}", artifact_id=f"ar{i}",
                version=1, content_hash=f"hash-{i}",
            )
            ev_ops.append(
                AddNodeOp(
                    node_id=f"ev{i}", graph_id=gid, node_type="evidence",
                    created_by_pid="p1", evidence_kind="command_result",
                    result="pass", evidence_source_action_id=f"act{i}",
                    source_verification_id=f"v{i}", produced_by_pid="p1",
                    artifact_bindings=(b,),
                )
            )
            ev_ops.append(
                AttachEvidenceOp(
                    verification_node_id=f"v{i}", evidence_node_id=f"ev{i}",
                    created_by_pid="p1", edge_id=f"pev-{i}",
                )
            )
        rt.submit_patch(
            GraphPatchProposal(
                graph_id=gid,
                expected_graph_version=rt.get_graph(gid).current_version,
                author_pid="p1",
                idempotency_key=f"s2-evidence-{batch_start}",
                operations=tuple(ev_ops),
            )
        )


def _make_rt_and_graph():
    facts = _Facts(N_TASKS)
    rt = VerifiedProgressRuntime(
        ":memory:", facts_artifact=facts, facts_kernel=facts,
    )
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id
    _build_graph(rt, gid, facts)
    return rt, gid, facts


# ── Helpers ──────────────────────────────────────────────────────────────────
def projection_hash(nodes, edges) -> str:
    """SHA-256 over the SIGNIFICANT fields of the projection (no timestamps)."""
    h = hashlib.sha256()
    for n_id in sorted(nodes):
        n = nodes[n_id]
        h.update(
            f"{n.node_id}:{n.node_type.value}:{n.lifecycle.value}:{n.validity.value}".encode()
        )
        h.update(b"|")
    for e in sorted(edges, key=lambda x: x.edge_id):
        h.update(
            f"{e.edge_id}:{e.edge_type.value}:{e.source_node_id}:{e.target_node_id}".encode()
        )
        h.update(b"|")
    return h.hexdigest()


def verify_projection(store, graph_id: str) -> bool:
    """Projection consistent iff non-empty and the control task t1 is VERIFIED."""
    ns = store.get_all_nodes(graph_id)
    es = store.get_all_edges(graph_id)
    if not ns or not es:
        return False
    t1 = next((n for n in ns if n.node_id == "t1"), None)
    return t1 is not None and t1.validity.value == "verified"


def _rebuild(rt, gid, facts, *, reverse=False):
    """Rebuild projection with FRESH per-patch histories (rebuild mutates nodes).

    SQL row layout:  r[0]=operations_json, r[1]=patch_id, r[2]=committed_version
    """
    rows = rt.store.conn.execute(
        "SELECT operations_json, patch_id, committed_version "
        "FROM graph_patches WHERE graph_id=? ORDER BY committed_version",
        (gid,),
    ).fetchall()
    ver2pid = {r[2]: r[1] for r in rows}        # version -> patch_id
    n_hist: dict[str, list] = {r[1]: [] for r in rows}  # patch_id -> [nodes]
    e_hist: dict[str, list] = {r[1]: [] for r in rows}  # patch_id -> [edges]
    _copy = __import__("copy").deepcopy
    for nd in rt.store.get_all_nodes(gid):
        nd = _copy(nd)
        nd.lifecycle = NodeLifecycle.PROPOSED
        nd.validity = NodeValidity.UNVERIFIED
        # Strip cached derived-state metadata — rebuild derives VALIDITY
        # deterministically from the graph structure, not from a prior cache.
        if isinstance(nd.metadata, dict):
            nd.metadata.pop("__verified_artifact_versions", None)
        pid = ver2pid.get(nd.created_in_version)
        if pid in n_hist:
            n_hist[pid].append(nd)
    for e in rt.store.get_all_edges(gid):
        e = _copy(e)
        pid = ver2pid.get(e.created_in_version)
        if pid in e_hist:
            e_hist[pid].append(e)
    patches = [GraphPatchProposal(**json.loads(r[0])) for r in rows]
    if reverse:
        patches = list(reversed(patches))
    rn, re, ev = rebuild_projection(
        gid, patches, e_hist, n_hist,
        facts_artifact=facts, facts_kernel=facts,
    )
    return rn, re, ev


# ══════════════════════════════════════════════════════════════════════════════
# Step 18a — verify_projection succeeds on freshly committed graph
# ══════════════════════════════════════════════════════════════════════════════
class TestS18a_VerifyProjectionOK:
    def test_verify_projection_passes_on_committed_graph(self):
        rt, gid, facts = _make_rt_and_graph()
        ok1 = verify_projection(rt.store, gid)
        t1 = rt.inspect_node(gid, "t1")
        g1 = rt.inspect_node(gid, "g1")
        t50 = rt.inspect_node(gid, "t50")
        assert ok1 is True
        assert t1.validity.value == "verified"
        assert t1.lifecycle.value == "closed"
        assert g1.lifecycle.value == "closed"
        assert t50.validity.value == "verified"
        _record(
            "S18a", "verify_projection_ok", "PASS", "PASS",
            f"verify_projection=True; t1={t1.validity.value}/{t1.lifecycle.value}; "
            f"g1={g1.lifecycle.value}; t50={t50.validity.value}",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 18b — delete_projection (SIGKILL simulation) → projection inconsistent
# ══════════════════════════════════════════════════════════════════════════════
class TestS18b_DeleteProjectionInconsistent:
    def test_after_delete_verify_fails(self):
        rt, gid, facts = _make_rt_and_graph()
        # Pre-condition: projection consistent
        assert verify_projection(rt.store, gid) is True
        # SIGKILL: wipe materialized projection
        rt.store.delete_projection(gid)
        rt.store.conn.commit()
        nodes_after = rt.store.get_all_nodes(gid)
        assert len(nodes_after) == 0, "projection must be empty post-delete"
        ok_post = verify_projection(rt.store, gid)
        assert ok_post is False, (
            "verify_projection MUST return False after projection wipe"
        )
        _record(
            "S18b", "delete_then_inconsistent", "PASS", "PASS",
            f"post-delete node_count=0; verify_projection={ok_post}",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 18c — full rebuild captures deterministic projection_hash
# ══════════════════════════════════════════════════════════════════════════════
class TestS18c_RebuildDeterministic:
    def test_full_rebuild_is_deterministic(self):
        rt, gid, facts = _make_rt_and_graph()
        rn, re, ev = _rebuild(rt, gid, facts)
        h = projection_hash(rn, re)
        verified_cnt = sum(
            1 for n in rn.values()
            if n.node_id.startswith("t") and n.validity.value == "verified"
        )
        assert verified_cnt == N_TASKS, (
            f"rebuild must verify all {N_TASKS} tasks; got {verified_cnt}"
        )
        assert rn["g1"].lifecycle.value == "closed"
        assert any(e.event_type == GraphEventType.TASK_VERIFIED_DERIVED for e in ev)
        assert any(e.event_type == GraphEventType.GOAL_CLOSED_DERIVED for e in ev)
        _record(
            "S18c", "rebuild_deterministic", "PASS", "PASS",
            f"projection_hash={h}; verified_tasks={verified_cnt}; "
            f"t1={rn['t1'].validity.value}/{rn['t1'].lifecycle.value}; "
            f"g1={rn['g1'].lifecycle.value}",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 18d — three more fresh rebuilds byte-identical (idempotency)
# ══════════════════════════════════════════════════════════════════════════════
class TestS18d_RebuildIdempotent:
    def test_three_rebuilds_byte_identical(self):
        rt, gid, facts = _make_rt_and_graph()
        hashes = []
        for _ in range(4):
            rn, re, ev = _rebuild(rt, gid, facts)
            hashes.append(projection_hash(rn, re))
        unique = len(set(hashes))
        assert unique == 1, (
            f"all 4 rebuilds must be byte-identical; got {unique} distinct hashes"
        )
        _record(
            "S18d", "three_rebuilds_identical", "PASS", "PASS",
            f"4 rebuilds produced 1 unique hash ({hashes[0][:16]}...)",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Step 18e — reverse patch order still byte-identical
# ══════════════════════════════════════════════════════════════════════════════
class TestS18e_ReverseOrderIdentical:
    def test_reverse_order_rebuild_matches_forward(self):
        rt, gid, facts = _make_rt_and_graph()
        rn_fwd, re_fwd, ev_fwd = _rebuild(rt, gid, facts, reverse=False)
        h_fwd = projection_hash(rn_fwd, re_fwd)
        rn_rev, re_rev, ev_rev = _rebuild(rt, gid, facts, reverse=True)
        h_rev = projection_hash(rn_rev, re_rev)
        assert h_fwd == h_rev, (
            f"forward vs reverse hashes differ: {h_fwd[:16]}... vs {h_rev[:16]}..."
        )
        # Reverse-order derivation must still verify all tasks + close the goal
        verified_rev = sum(
            1 for n in rn_rev.values()
            if n.node_id.startswith("t") and n.validity.value == "verified"
        )
        assert verified_rev == N_TASKS, (
            f"reverse-order rebuild must verify all tasks; got {verified_rev}"
        )
        assert rn_rev["g1"].lifecycle.value == "closed"
        _record(
            "S18e", "reverse_order_identical", "PASS", "PASS",
            f"forward={h_fwd[:16]}... reverse={h_rev[:16]}... identical=True; "
            f"verified_tasks={verified_rev}; g1={rn_rev['g1'].lifecycle.value}",
        )
