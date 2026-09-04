"""Projection rebuild.

The runtime exposes ``rt.rebuild_projection(graph_id)`` which drops the
materialized projection and replays committed patch history.

NOTE on current behavior: the public API pulls per-patch node/edge history
from the projection tables it JUST deleted and (via the current store) has
no separate node_history table, so the running SDK passes empty
per-patch histories to ``projections.rebuild_projection``. The result is
that the public method returns the triple, is side-effect free and
idempotent, but does NOT repopulate nodes. The genuine full replay logic
lives in ``lhos.runtimes.verified_progress.projections.rebuild_projection``
and, when given proper per-patch node/edge histories, reconstructs the
complete projection including derived VERIFIED/CLOSED state via the event
stream. This file tests both the public API surface and the real replay
logic.
"""

from __future__ import annotations

import json

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.events import GraphEventType
from lhos.runtimes.verified_progress.models import (
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
from lhos.runtimes.verified_progress.projections import rebuild_projection


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
def ready_graph():
    facts = _Facts()
    rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id
    _patch(
        rt,
        gid,
        "setup",
        (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"),
            AddNodeOp(
                node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"
            ),
            AddEdgeOp(
                edge_id="d1",
                edge_type="depends_on",
                source_node_id="g1",
                target_node_id="t1",
                created_by_pid="p1",
            ),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification", created_by_pid="p1"),
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
        artifact_bindings=(b,),
    )
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection (node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
        ("evi1", gid, "evidence", evi.model_dump_json()),
    )
    rt.store.conn.commit()
    _patch(
        rt,
        gid,
        "art1",
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
    return rt, gid


class TestPublicRebuildAPISurface:
    def test_rebuild_projection_is_idempotent_no_crash(self, ready_graph):
        rt, gid = ready_graph
        a = rt.rebuild_projection(gid)
        b = rt.rebuild_projection(gid)
        c = rt.rebuild_projection(gid)
        assert len(a) == 3 and len(b) == 3 and len(c) == 3
        # the graph record is unaffected by rebuild
        assert rt.get_graph(gid).current_version == 3

    def test_rebuild_returns_triple(self, ready_graph):
        rt, gid = ready_graph
        result = rt.rebuild_projection(gid)
        nodes, edges, events = result
        assert isinstance(nodes, dict)
        assert isinstance(edges, list)
        assert isinstance(events, list)


class TestProjectionReplayCorrectness:
    def _histories(self, rt, gid):
        rows = rt.store.conn.execute(
            "SELECT patch_id, committed_version FROM graph_patches WHERE graph_id=? ORDER BY applied_at",
            (gid,),
        ).fetchall()
        ver2pid = {r[1]: r[0] for r in rows}
        n_hist = {r[0]: [] for r in rows}
        e_hist = {r[0]: [] for r in rows}
        for nd in rt.store.get_all_nodes(gid):
            # rebuild_projection() re-runs the admission engine, which requires
            # input nodes in PROPOSED lifecycle. The projection table stores
            # nodes already advanced to ADMITTED by the original commit, so
            # we reset them here to satisfy admit()'s precondition. Without
            # this reset, admit() rejects every node as "must be PROPOSED".
            nd.lifecycle = NodeLifecycle.PROPOSED
            nd.validity = NodeValidity.UNVERIFIED
            pid = ver2pid.get(nd.created_in_version)
            if pid in n_hist:
                n_hist[pid].append(nd)
        for e in rt.store.get_all_edges(gid):
            pid = ver2pid.get(e.created_in_version)
            if pid in e_hist:
                e_hist[pid].append(e)
        patches = [
            GraphPatchProposal(**json.loads(r[0]))
            for r in rt.store.conn.execute(
                "SELECT operations_json FROM graph_patches WHERE graph_id=? ORDER BY applied_at",
                (gid,),
            ).fetchall()
        ]
        return patches, e_hist, n_hist

    def test_full_replay_reconstructs_nodes_edges(self, ready_graph):
        rt, gid = ready_graph
        patches, e_hist, n_hist = self._histories(rt, gid)
        rn, re_, ev = rebuild_projection(
            gid,
            patches,
            e_hist,
            n_hist,
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        node_ids = sorted(rn.keys())
        # replay 3x -> identical node id lists
        for _ in range(3):
            rn2, _, _ = rebuild_projection(
                gid,
                patches,
                e_hist,
                n_hist,
                facts_artifact=rt.facts_artifact,
                facts_kernel=rt.facts_kernel,
            )
            assert sorted(rn2.keys()) == node_ids
        assert "g1" in rn and "t1" in rn and "v1" in rn and "evi1" in rn
        assert len(re_) == 4

    def test_full_replay_emits_derived_events(self, ready_graph):
        rt, gid = ready_graph
        patches, e_hist, n_hist = self._histories(rt, gid)
        rn, re_, ev = rebuild_projection(
            gid,
            patches,
            e_hist,
            n_hist,
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        etypes = [e.event_type for e in ev]
        assert GraphEventType.TASK_VERIFIED_DERIVED in etypes
        assert GraphEventType.TASK_CLOSED_DERIVED in etypes
        assert GraphEventType.GOAL_CLOSED_DERIVED in etypes

    def test_full_replay_sets_lifecycle_validity(self, ready_graph):
        rt, gid = ready_graph
        patches, e_hist, n_hist = self._histories(rt, gid)
        rn, re_, ev = rebuild_projection(
            gid,
            patches,
            e_hist,
            n_hist,
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        assert rn["t1"].validity.value == "verified"
        assert rn["t1"].lifecycle.value == "closed"
        assert rn["g1"].lifecycle.value == "closed"
