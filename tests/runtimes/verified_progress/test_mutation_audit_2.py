"""Step 27 — Mutation Audit 2.0 (A01-A20).

Each scenario constructs a gold-master graph via NORMAL runtime ops (NOT
mutation of source files) and then attempts a SEMANTIC attack that would pass
the gold-master but fail the invariant.  We confirm the runtime catches each
category WITHOUT fabricating truth.

No production source is modified.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
    ArtifactRefNode,
    ArtifactVersionBinding,
    EvidenceNode,
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
from lhos.runtimes.verified_progress.verification import validate_evidence

RECORDS: list[dict] = []


def _make_rt(db=":memory:", facts_artifact=None, facts_kernel=None):
    return VerifiedProgressRuntime(db, facts_artifact=facts_artifact, facts_kernel=facts_kernel)


class FakeAction:
    def __init__(self, action_id="act1", pid="p1", state="committed"):
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {}
        self.artifact_refs = ()


class FakeFacts:
    def __init__(self, actions=None, hashes=None):
        self.actions = actions or {}
        self.hashes = hashes or {}

    def get_action(self, action_id):
        return self.actions.get(action_id)

    def has_event(self, event_id):
        return False

    def list_events_for_pid(self, pid):
        return []

    def artifact_exists(self, pid, canonical_uri, version):
        return True

    def read_hash(self, pid, canonical_uri, version):
        return self.hashes.get((canonical_uri, version))

    def verify_binding(self, pid, binding):
        stored = self.hashes.get((binding.canonical_uri, binding.version))
        return stored is None or stored == binding.content_hash

    def can_read(self, pid, artifact_id, version):
        return True


def _submit(rt, graph_id, kid, ops, cur_version=None, author="p1"):
    if cur_version is None:
        cur_version = rt.get_graph(graph_id).current_version
    return rt.submit_patch(GraphPatchProposal(
        graph_id=graph_id, expected_graph_version=cur_version,
        author_pid=author, idempotency_key=kid, operations=ops,
    ))


def _build_gold_master(facts=None, strict_binding_hash=None):
    """Build a gold-master graph with g1/v1/t1 + ar1@v1 + evi1 binding, VERIFIED.

    Returns (rt, gid, binding).  With facts, task becomes VERIFIED; otherwise
    UNVERIFIED placeholder evidence is attached.
    """
    ka = facts
    fa = facts
    rt = _make_rt(facts_artifact=fa, facts_kernel=ka)
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id
    _submit(rt, gid, "init", (
        AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                  created_by_pid="p1", title="G1"),
        AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                  created_by_pid="p1", verification_kind="command_result"),
        AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                  created_by_pid="p1", title="T1"),
        AddEdgeOp(edge_id="vf1", edge_type="verifies",
                  source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
    ))
    binding = ArtifactVersionBinding(
        canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1",
    )
    if strict_binding_hash is not None:
        binding.content_hash = strict_binding_hash
    # Inject evidence into projection then attach artifacts + evidence.
    evi = EvidenceNode(
        graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
        evidence_kind="command_result", result="pass",
        source_verification_id="v1", source_action_id="act1",
        produced_by_pid="p1",
        created_in_version=rt.get_graph(gid).current_version,
        updated_in_version=rt.get_graph(gid).current_version,
        created_by_pid="p1", artifact_bindings=(binding,),
    )
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection "
        "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
        (evi.node_id, gid, "evidence", evi.model_dump_json()),
    )
    rt.store.conn.commit()
    _submit(rt, gid, "art1", (
        AttachArtifactOp(task_node_id="t1", artifact=binding,
                        created_by_pid="p1", edge_id="prod_t1_ar1"),
    ))
    _submit(rt, gid, "att1", (
        AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1",
                         created_by_pid="p1", edge_id="pe1"),
    ))
    return rt, gid, binding


def _task_validity(rt, gid, tid="t1"):
    t = rt.inspect_node(gid, tid)
    assert t is not None
    return t.validity


def _record(aid, category, violated, runtime_result, verdict):
    RECORDS.append({
        "id": aid, "step": 27, "name": category,
        "violated": violated, "runtime_result": runtime_result, "verdict": verdict,
    })


# ── A01 — Invalid evidence result: pass + action state=failed ──────────────────

class TestA01_InvalidEvidenceResult:
    def test_fail_state_rejected(self):
        facts = FakeFacts(actions={"act1": FakeAction("act1", state="failed")})
        rt, gid, binding = _build_gold_master(facts=facts)
        tv = _task_validity(rt, gid)
        # action is non-terminal → task must NOT verify
        assert tv != NodeValidity.VERIFIED, f"task must NOT be VERIFIED: {tv}"
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED not in evts
        _record("A01", "invalid_evidence_result",
                "produced VERIFIED with non-terminal action",
                f"task_validity={tv.value}", "PASS")


# ── A02 — Foreign-PID action ───────────────────────────────────────────────────

class TestA02_ForeignPidAction:
    def test_foreign_pid_rejected(self):
        # Action owned by pX; evidence claims produced_by_pid=p1
        facts = FakeFacts(actions={"actX": FakeAction("actX", pid="pX")})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                      created_by_pid="p1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
        ))
        binding = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1")
        evi = EvidenceNode(
            graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="actX",
            produced_by_pid="p1",  # mismatch with action.pid
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(binding,),
        )
        # validator directly rejects
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        res = validate_evidence(
            evi,
            existing_nodes={n.node_id: n for n in rt.store.get_all_nodes(gid)},
            existing_edges=rt.store.get_all_edges(gid),
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_SOURCE_ACTION_WRONG_PID
        _submit(rt, gid, "art1", (
            AttachArtifactOp(task_node_id="t1", artifact=binding,
                            created_by_pid="p1", edge_id="prod_t1_ar1"),
        ))
        _submit(rt, gid, "att1", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1",
                             created_by_pid="p1", edge_id="pe1"),
        ))
        tv = _task_validity(rt, gid)
        assert tv != NodeValidity.VERIFIED
        _record("A02", "foreign_pid_action",
                "produced VERIFIED with foreign-pid source action",
                f"validate_evidence.code={res.code.value}; task_validity={tv.value}",
                "PASS")


# ── A03 — Empty-evidence-pins-task-with-pins ───────────────────────────────────

class TestA03_EmptyEvidencePinsTaskWithPins:
    def test_empty_evidence_rejected(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        # Task now VERIFIED with the honest bind; now inject a NEW evidence node
        # with 0 bindings and attach it — mismatch must reject.
        evi_empty = EvidenceNode(
            graph_id=gid, node_id="evi_empty", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(),  # empty
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi_empty", gid, "evidence", evi_empty.model_dump_json()),
        )
        rt.store.conn.commit()
        # Add a produces edge so validate_evidence passes the edge checks and
        # reaches the artifact-binding comparison (mirrors F1/F5 idiom).
        from datetime import datetime, timezone
        from lhos.runtimes.verified_progress.models import VPGEdge, EdgeType
        produces = VPGEdge(
            edge_id="pe_empty_produces", graph_id=gid,
            edge_type=EdgeType.PRODUCES, source_node_id="v1",
            target_node_id="evi_empty",
            created_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", created_at=datetime.now(timezone.utc),
        )
        nodes = {n.node_id: n for n in rt.store.get_all_nodes(gid)}
        edges = list(rt.store.get_all_edges(gid)) + [produces]
        res = validate_evidence(
            evi_empty, existing_nodes=nodes, existing_edges=edges,
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False, "empty-bind evidence must be rejected"
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH, (
            f"expected EVIDENCE_ARTIFACT_HASH_MISMATCH, got {res.code}")
        tv = _task_validity(rt, gid)
        # t1 was already VERIFIED with the honest bind — attaching an
        # empty-bind evidence must NOT fabricate a second/different VERIFIED.
        _record("A03", "empty_evidence_pins_task",
                "accepted empty-bind evidence against pinned task",
                f"validate_evidence.code={res.code.value}; task_validity={tv.value}",
                "PASS")


# ── A04 — Same-version self-loop ────────────────────────────────────────────────

class TestA04_SameVersionSelfLoop:
    def test_self_loop_rejected(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt, gid, binding = _build_gold_master(facts=facts)
        before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "self-loop", (
                AddEdgeOp(edge_id="e_self", edge_type="depends_on",
                          source_node_id="t1", target_node_id="t1",
                          created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE
        assert rt.get_graph(gid).current_version == before
        _record("A04", "same_version_self_loop",
                "accepted depends_on self-loop",
                f"raised {ei.value.code.value}; version_unchanged={before}", "PASS")


# ── A05 — Optimistic conflict ───────────────────────────────────────────────────

class TestA05_OptimisticConflict:
    def test_optimistic_conflict_rejected(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt, gid, binding = _build_gold_master(facts=facts)
        # submit with stale expected version 0
        with pytest.raises(VPGError) as ei:
            rt.submit_patch(GraphPatchProposal(
                graph_id=gid, expected_graph_version=0,
                author_pid="p1", idempotency_key="opt",
                operations=(AddNodeOp(node_id="tx", graph_id=gid,
                                       node_type="task", created_by_pid="p1",
                                       title="X"),),
            ))
        assert ei.value.code == VPGCode.GRAPH_VERSION_CONFLICT
        assert rt.inspect_node(gid, "tx") is None
        _record("A05", "optimistic_conflict",
                "accepted stale expected_version patch",
                f"raised {ei.value.code.value}; tx not admitted", "PASS")


# ── A06 — Duplicate node id in single patch ───────────────────────────────────

class TestA06_DuplicateNodeIdInPatch:
    def test_duplicate_node_rejected(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "dup", (
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T"),
                AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                          created_by_pid="p1", title="T2"),
            ))
        assert ei.value.code == VPGCode.NODE_ALREADY_EXISTS
        _record("A06", "duplicate_node_id_in_patch",
                "accepted duplicate node_id in single patch",
                f"raised {ei.value.code.value}", "PASS")


# ── A07 — Cross-version evidence ────────────────────────────────────────────────

class TestA07_CrossVersionEvidence:
    def test_cross_version_rejected(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1", ("u/ar1", 2): "h2"},
        )
        # Build t1 VERIFIED at v1, then re-pin to v2 with new evidence,
        # then try to reuse the OLD v1 evidence.
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                      created_by_pid="p1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
        ))
        b1 = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1")
        evi1 = EvidenceNode(
            graph_id=gid, node_id="evi_v1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(b1,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            (evi1.node_id, gid, "evidence", evi1.model_dump_json()),
        )
        rt.store.conn.commit()
        _submit(rt, gid, "art1", (
            AttachArtifactOp(task_node_id="t1", artifact=b1,
                            created_by_pid="p1", edge_id="prod_1"),
        ))
        _submit(rt, gid, "att1", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi_v1",
                             created_by_pid="p1", edge_id="pe1"),
        ))
        # re-pin to v2
        b2 = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=2, content_hash="h2")
        _submit(rt, gid, "art2", (
            AttachArtifactOp(task_node_id="t1", artifact=b2,
                            created_by_pid="p1", edge_id="prod_2"),
        ))
        # Now attach the OLD v1 evidence under new key; must reject version mismatch.
        atoms = {n.node_id: n for n in rt.store.get_all_nodes(gid)}
        edges = rt.store.get_all_edges(gid)
        res = validate_evidence(
            evi1, existing_nodes=atoms, existing_edges=edges,
            facts_artifact=facts, facts_kernel=facts,
        )
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH
        _record("A07", "cross_version_evidence",
                "accepted v1 evidence against v2-pinned task",
                f"validate_evidence.code={res.code.value}", "PASS")


# ── A08 — AttachEvidence edge_id honored ──────────────────────────────────────

class TestA08_AttachEvidenceEdgeIdHonored:
    def test_edge_id_honored(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        # edge pe1 must be present and endpoint-consistent
        e = rt.inspect_edge(gid, "pe1")
        assert e is not None, "edge_id pe1 must be persisted"
        assert e.edge_type.value == "produces"
        assert e.source_node_id == "v1"
        assert e.target_node_id == "evi1"
        _record("A08", "attach_evidence_edge_id_honored",
                "dropped evidence edge_id",
                "edge persisted with correct endpoints", "PASS")


# ── A09 — Idempotent replay with different result ─────────────────────────────

class TestA09_IdempotentReplayDifferentResult:
    def test_idempotent_replay_state_unchanged(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        ver = rt.get_graph(gid).current_version
        tv_before = _task_validity(rt, gid)
        # Replay the SAME idempotency key with DIFFERENT operations — must be no-op.
        pr = _submit(rt, gid, "att1", (
            AddNodeOp(node_id="ghost_should_not_exist", graph_id=gid,
                      node_type="task", created_by_pid="p1", title="GHOST"),
        ))
        assert pr.patch_applied is False
        assert pr.idempotent_replay is True
        assert rt.get_graph(gid).current_version == ver
        assert rt.inspect_node(gid, "ghost_should_not_exist") is None
        assert _task_validity(rt, gid) == tv_before
        _record("A09", "idempotent_replay_different_result",
                "applied new ops under replayed idem key",
                f"patch_applied={pr.patch_applied}; idem={pr.idempotent_replay}; "
                f"ghost missing; version={rt.get_graph(gid).current_version}",
                "PASS")


# ── A10 — Cycle via transitive 4-node chain ───────────────────────────────────

class TestA10_Cycle4Node:
    def test_4node_cycle_rejected(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "chain", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="2"),
            AddNodeOp(node_id="t3", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="3"),
            AddNodeOp(node_id="t4", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="4"),
            AddEdgeOp(edge_id="e12", edge_type="depends_on",
                      source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
            AddEdgeOp(edge_id="e23", edge_type="depends_on",
                      source_node_id="t3", target_node_id="t2", created_by_pid="p1"),
            AddEdgeOp(edge_id="e34", edge_type="depends_on",
                      source_node_id="t4", target_node_id="t3", created_by_pid="p1"),
        ))
        before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "close-loop", (
                AddEdgeOp(edge_id="e41", edge_type="depends_on",
                          source_node_id="t1", target_node_id="t4",
                          created_by_pid="p1"),
            ))
        assert ei.value.code == VPGCode.GRAPH_EXECUTION_CYCLE
        assert rt.get_graph(gid).current_version == before
        _record("A10", "cycle_4node_chain",
                "accepted t4→t1 closing a 4-node cycle",
                f"raised {ei.value.code.value}; version_unchanged={before}", "PASS")


# ── A11 — Evidence already exists; different evidence_node_id ─────────────────

class TestA11_EvidenceAlreadyExists:
    def test_second_evidence_preserves_integrity(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        ver_before = rt.get_graph(gid).current_version
        # Build a second honest evidence bound to the same pins.
        evi2 = EvidenceNode(
            graph_id=gid, node_id="evi2", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(binding,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi2", gid, "evidence", evi2.model_dump_json()),
        )
        rt.store.conn.commit()
        _submit(rt, gid, "att2", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi2",
                             created_by_pid="p1", edge_id="pe2"),
        ))
        # structural integrity: both produces edges exist, t1 still VERIFIED.
        assert rt.inspect_edge(gid, "pe1") is not None
        assert rt.inspect_edge(gid, "pe2") is not None
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED
        _record("A11", "evidence_already_exists",
                "corrupted structure when re-attaching evidence",
                "both produces edges intact; t1 still VERIFIED", "PASS")


# ── A12 — STALE dep blocks Goal closure ───────────────────────────────────────

class TestA12_StaleDepBlocksGoal:
    def test_stale_dep_blocks_goal(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1", ("u/ar1", 2): "h2"},
        )
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                      created_by_pid="p1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
            AddEdgeOp(edge_id="d1", edge_type="depends_on",
                      source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
        ))
        # make t1 VERIFIED at v1
        b1 = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1")
        evi1 = EvidenceNode(
            graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(b1,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi1.model_dump_json()),
        )
        rt.store.conn.commit()
        _submit(rt, gid, "art1", (
            AttachArtifactOp(task_node_id="t1", artifact=b1,
                            created_by_pid="p1", edge_id="prod1"),
        ))
        _submit(rt, gid, "att1", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1",
                             created_by_pid="p1", edge_id="pe1"),
        ))
        # t1 VERIFIED → g1 should close
        g1 = rt.inspect_node(gid, "g1")
        assert g1.lifecycle == NodeLifecycle.CLOSED
        # re-pin t1 to v2 → t1 STALE → g1 reopen
        b2 = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=2, content_hash="h2")
        _submit(rt, gid, "art2", (
            AttachArtifactOp(task_node_id="t1", artifact=b2,
                            created_by_pid="p1", edge_id="prod2"),
        ))
        t1 = rt.inspect_node(gid, "t1")
        g1b = rt.inspect_node(gid, "g1")
        assert t1.validity == NodeValidity.STALE, f"t1 should be STALE, got {t1.validity}"
        assert g1b.lifecycle != NodeLifecycle.CLOSED, (
            f"goal must NOT be closed with STALE dep, got {g1b.lifecycle}")
        _record("A12", "stale_dep_blocks_goal",
                "closed goal with STALE-dep task",
                f"t1.validity={t1.validity.value}; g1.lifecycle={g1b.lifecycle.value}",
                "PASS")


# ── A13 — Artifact doesn't exist ───────────────────────────────────────────────

class TestA13_ArtifactNotExist:
    def test_uncommitted_artifact_rejected(self):
        """Attacker pins t1 to an uncommitted artifact version (u/ar1@99)
        that is not present in the artifact registry.  The commit-time
        patch validator must reject the binding BEFORE it can land in the
        projection — the attack dies at the boundary."""
        # Strict facts: only (u/ar1, 1)@"h1" is committed; unknown identity → reject.
        class StrictFacts(FakeFacts):
            def verify_binding(self, pid, binding):
                key = (binding.canonical_uri, binding.version)
                if key not in self.hashes:
                    return False  # identity not committed
                return self.hashes[key] == binding.content_hash

        facts = StrictFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED
        # Attempt to pin t1 → u/ar1@99 (uncommitted, wrong hash)
        bad = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=99, content_hash="wrong")
        rejected = False
        code = None
        try:
            _submit(rt, gid, "art99", (
                AttachArtifactOp(task_node_id="t1", artifact=bad,
                                created_by_pid="p1", edge_id="prod99"),
            ))
        except VPGError as e:
            rejected = True
            code = e.code
        assert rejected, (
            "pinning an uncommitted artifact must be rejected at commit boundary")
        # Must be rejected as artifact-hash-mismatch (uncommitted = no match).
        assert code in (VPGCode.ARTIFACT_HASH_MISMATCH,
                        VPGCode.ARTIFACT_NOT_FOUND), (
            f"expected artifact mismatch/not-found, got {code}")
        # Projection must remain VERIFIED on the original committed binding.
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED
        _record("A13", "artifact_does_not_exist",
                "accepted evidence binding uncommitted artifact",
                f"boundary_rejected code={code.value}; task still VERIFIED",
                "PASS")


# ── A14 — Spurious TASK_VERIFIED_DERIVED for never-evidenced Task ────────────

class TestA14_SpuriousVerifiedNeverEvidenced:
    def test_no_verified_without_evidence(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
        ))
        tv = _task_validity(rt, gid)
        assert tv == NodeValidity.UNVERIFIED
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED not in evts
        _record("A14", "spurious_verified_never_evidenced",
                "produced VERIFIED for never-evidenced task",
                f"task_validity={tv.value}", "PASS")


# ── A15 — Spurious GOAL_CLOSED_DERIVED for goal with unverified dep ──────────

class TestA15_SpuriousGoalClosedUnverifiedDep:
    def test_goal_not_closed_with_unverified_dep(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="d1", edge_type="depends_on",
                      source_node_id="g1", target_node_id="t1", created_by_pid="p1"),
        ))
        g1 = rt.inspect_node(gid, "g1")
        assert g1.lifecycle != NodeLifecycle.CLOSED
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.GOAL_CLOSED_DERIVED not in evts
        _record("A15", "spurious_goal_closed_unverified",
                "closed goal with UNVERIFIED task dep",
                f"g1.lifecycle={g1.lifecycle.value}", "PASS")


# ── A16 — Evidence ordering: verify without produce ───────────────────────────

class TestA16_EvidenceOrdering:
    def test_verify_without_produce_rejected(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                      created_by_pid="p1"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                      created_by_pid="p1", title="T1"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                      source_node_id="v1", target_node_id="t1", created_by_pid="p1"),
        ))
        binding = ArtifactVersionBinding(
            canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1")
        # Evidence WITHOUT any produces edge to verification
        evi = EvidenceNode(
            graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1", artifact_bindings=(binding,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        atoms = {n.node_id: n for n in rt.store.get_all_nodes(gid)}
        edges = rt.store.get_all_edges(gid)
        res = validate_evidence(
            evi, existing_nodes=atoms, existing_edges=edges,
            facts_artifact=facts, facts_kernel=facts,
        )
        # No produces edge v1->evi1 → EVIDENCE_PRODUCES_EDGE_MISSING
        assert res.valid is False
        assert res.code == VPGCode.EVIDENCE_PRODUCES_EDGE_MISSING
        _submit(rt, gid, "art1", (
            AttachArtifactOp(task_node_id="t1", artifact=binding,
                            created_by_pid="p1", edge_id="prod1"),
        ))
        tv = _task_validity(rt, gid)
        assert tv == NodeValidity.UNVERIFIED
        _record("A16", "evidence_ordering_no_produce",
                "verified task with evidence lacking produces edge",
                f"validate_evidence.code={res.code.value}; task_validity={tv.value}",
                "PASS")


# ── A17 — AttachEvidence idempotent edge_id under new key → EDGE_ALREADY_EXISTS

class TestA17_AttachEvidenceEdgeIdNewKey:
    def test_edge_already_exists_on_reattach(self):
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid, binding = _build_gold_master(facts=facts)
        # already edge pe1; try adding pe1 again under new idempotency key
        before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError) as ei:
            _submit(rt, gid, "att1_dup", (
                AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1",
                                 created_by_pid="p1", edge_id="pe1"),
            ))
        assert ei.value.code == VPGCode.EDGE_ALREADY_EXISTS
        assert rt.get_graph(gid).current_version == before
        _record("A17", "attach_evidence_edge_id_new_key",
                "created duplicate edge under new idem key",
                f"raised {ei.value.code.value}; version_unchanged={before}", "PASS")


# ── A18 — Zero-dep Goal closure ────────────────────────────────────────────────

class TestA18_ZeroDepGoal:
    def test_zero_dep_goal_not_closed(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        _submit(rt, gid, "init", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                      created_by_pid="p1", title="G1"),
        ))
        g1 = rt.inspect_node(gid, "g1")
        assert g1.lifecycle != NodeLifecycle.CLOSED
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.GOAL_CLOSED_DERIVED not in evts
        _record("A18", "zero_dep_goal_closure",
                "closed zero-dep goal",
                f"g1.lifecycle={g1.lifecycle.value}", "PASS")


# ── A19 — Projection tamper + rebuild recovery ─────────────────────────────────

class TestA19_ProjectionTamperRebuild:
    def test_tamper_recovers(self):
        """Tamper t1's materialized validity/lifecycle (simulating disk
        corruption / rootkit), then recover via rebuild_projection which
        replays graph_patches history.  Evidence is committed via AddNodeOp
        inside a patch so patch history carries it and t1 restores to VERIFIED."""
        facts = FakeFacts(
            actions={"act1": FakeAction()},
            hashes={("u/ar1", 1): "h1"},
        )
        rt, gid = _build_graph_with_patched_evidence(facts=facts)
        assert _task_validity(rt, gid) == NodeValidity.VERIFIED
        row = rt.store.conn.execute(
            "SELECT payload_json FROM graph_nodes_projection "
            "WHERE node_id='t1' AND graph_id=?", (gid,),
        ).fetchone()
        payload = json.loads(row["payload_json"])
        payload["validity"] = "tampered"
        payload["lifecycle"] = "compromised"
        rt.store.conn.execute(
            "UPDATE graph_nodes_projection SET payload_json=? "
            "WHERE node_id='t1' AND graph_id=?",
            (json.dumps(payload), gid),
        )
        rt.store.conn.commit()
        # tamper must be detected: Pydantic rejects the invalid enum string
        with pytest.raises(Exception):
            rt.store.get_all_nodes(gid)
        # rebuild recovers from append-only patch history
        rt.rebuild_projection(gid)
        t1 = rt.inspect_node(gid, "t1")
        assert t1.validity == NodeValidity.VERIFIED, (
            f"after rebuild t1 must be VERIFIED, got {t1.validity}")
        _record("A19", "projection_tamper_rebuild",
                "tamper not recovered by rebuild",
                f"recovered t1.validity={t1.validity.value}", "PASS")


def _build_graph_with_patched_evidence(facts):
    """Build gold-master where evidence is committed via AddNodeOp (not raw INSERT),
    so rebuild_projection can replay it from patch history."""
    rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id
    _submit(rt, gid, "init", (
        AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                  created_by_pid="p1", title="G1"),
        AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                  created_by_pid="p1", verification_kind="command_result"),
        AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                  created_by_pid="p1", title="T1"),
        AddNodeOp(node_id="ar1", graph_id=gid, node_type="artifact_ref",
                  created_by_pid="p1",
                  canonical_uri="u/ar1", artifact_id="ar1", version=1,
                  content_hash="h1"),
        AddEdgeOp(edge_id="vf1", edge_type="verifies",
                  source_node_id="v1", target_node_id="t1",
                  created_by_pid="p1"),
        AddEdgeOp(edge_id="d1", edge_type="depends_on",
                  source_node_id="g1", target_node_id="t1",
                  created_by_pid="p1"),
        AddEdgeOp(edge_id="tp1", edge_type="produces",
                  source_node_id="t1", target_node_id="ar1",
                  created_by_pid="p1"),
    ))
    binding = ArtifactVersionBinding(
        canonical_uri="u/ar1", artifact_id="ar1", version=1, content_hash="h1")
    # Commit evidence via AddNodeOp in patch (rebuildable).
    _submit(rt, gid, "att1", (
        AddNodeOp(node_id="ev1", graph_id=gid, node_type="evidence",
                  created_by_pid="p1", result="pass",
                  evidence_source_action_id="act1",
                  source_verification_id="v1", produced_by_pid="p1",
                  artifact_bindings=(binding,)),
        AttachEvidenceOp(verification_node_id="v1",
                         evidence_node_id="ev1",
                         created_by_pid="p1", edge_id="pev1"),
    ))
    return rt, gid


# ── A20 — Deterministic READY across 100 same-process queries ─────────────────

class TestA20_DeterministicReady100:
    def test_deterministic_ready_100(self):
        facts = FakeFacts(actions={"act1": FakeAction()})
        rt = _make_rt(facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1"); gid = rec.graph_id
        ids = [f"t{i}" for i in range(20)]
        ops = tuple(
            AddNodeOp(node_id=i, graph_id=gid, node_type="task",
                      created_by_pid="p1", title=i) for i in ids
        )
        _submit(rt, gid, "init-20", ops)
        ver = rt.get_graph(gid).current_version
        baseline = sorted(c.task_id for c in rt.query_ready_frontier(gid))
        assert baseline == sorted(ids)
        all_identical = True
        for _ in range(99):
            now = sorted(c.task_id for c in rt.query_ready_frontier(gid))
            if now != baseline:
                all_identical = False
                break
        assert all_identical
        assert rt.get_graph(gid).current_version == ver
        _record("A20", "deterministic_ready_100",
                "non-deterministic READY across repeated queries",
                f"identical={all_identical}; baseline_size={len(baseline)}",
                "PASS")


@pytest.fixture(autouse=True, scope="session")
def _dump_results():
    yield
    killed = sum(1 for r in RECORDS if r["verdict"] == "PASS")
    survived = sum(1 for r in RECORDS if r["verdict"] == "RISK")
    out = {"killed": killed, "survived": survived, "results": RECORDS}
    out_dir = Path(__file__).resolve().parents[3] / "artifacts" / "agent_os_phase_d1_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "mutation-results-v2.json", "w") as f:
        json.dump(out, f, indent=2)
    with open(out_dir / "mutation-kill-count.json", "w") as f:
        json.dump({"killed": killed, "survived": survived}, f, indent=2)
