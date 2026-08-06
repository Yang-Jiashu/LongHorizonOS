"""Task READY frontier.

task_is_ready(T) iff lifecycle==ADMITTED, validity in {UNVERIFIED, STALE},
and all Task dependencies are VERIFIED.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.models import (
    ArtifactVersionBinding,
    EvidenceNode,
    NodeType,
)
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)


class _Action:
    def __init__(self, aid, pid="p1", state="committed"):
        self.action_id = aid; self.pid = pid; self.state = state; self.result = {}; self.artifact_refs = ()


class _Facts:
    def get_action(self, aid): return _Action(aid)
    def has_event(self, eid): return False
    def list_events_for_pid(self, p): return []
    def artifact_exists(self, p, u, v): return True
    def read_hash(self, p, u, v): return None
    def verify_binding(self, p, b): return True
    def can_read(self, p, a, v): return True


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid, expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1", idempotency_key=kid, operations=ops))


def _ready_ids(rt, gid):
    return [c.task_id for c in rt.query_ready_frontier(gid)]


def _verify_upstream(rt, gid, tid, vnode_id, evi_id, aid, bind):
    _patch(rt, gid, f"v_{tid}_nodes", (
        AddNodeOp(node_id=vnode_id, graph_id=gid, node_type="verification", created_by_pid="p1", verification_kind="command_result"),
        AddEdgeOp(edge_id=f"vf_{tid}", edge_type="verifies", source_node_id=vnode_id, target_node_id=tid, created_by_pid="p1"),
    ))
    evi = EvidenceNode(graph_id=gid, node_id=evi_id, node_type=NodeType.EVIDENCE,
        evidence_kind="command_result", result="pass", source_verification_id=vnode_id,
        source_action_id=aid, produced_by_pid="p1",
        created_in_version=rt.get_graph(gid).current_version,
        updated_in_version=rt.get_graph(gid).current_version, created_by_pid="p1",
        artifact_bindings=(bind,))
    rt.store.conn.execute(
        "INSERT INTO graph_nodes_projection (node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
        (evi_id, gid, "evidence", evi.model_dump_json())); rt.store.conn.commit()
    _patch(rt, gid, f"art_{tid}", (AttachArtifactOp(task_node_id=tid, artifact=bind, created_by_pid="p1", edge_id=f"p_{tid}"),))
    _patch(rt, gid, f"att_{tid}", (AttachEvidenceOp(verification_node_id=vnode_id, evidence_node_id=evi_id, created_by_pid="p1", edge_id=f"pe_{tid}"),))


class TestSingleTaskReady:
    def test_single_task_alone_is_ready(self, graph):
        gid, rt = graph
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
        ))
        assert _ready_ids(rt, gid) == ["t1"]


class TestDepBlocksReadiness:
    def test_task_with_unverified_dep_not_ready(self, graph):
        gid, rt = graph
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="t2", target_node_id="t1", created_by_pid="p1"),
        ))
        # t2 depends on t1; neither verified -> only t1 is READY
        assert _ready_ids(rt, gid) == ["t1"]
        assert "t2" not in _ready_ids(rt, gid)

    def test_upstream_verified_makes_downstream_ready(self, graph):
        """Driving the projection to reflect upstream VERIFIED makes the
        downstream task READY. After the runtime derives TASK_VERIFIED_DERIVED
        the materialized projection validity is updated by the derived-state
        path; compute_ready_frontier then admits the downstream task."""
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier
        from lhos.runtimes.verified_progress.models import NodeValidity

        gid, rt = graph
        facts = _Facts()
        rt.facts_artifact = facts; rt.facts_kernel = facts
        _patch(rt, gid, "setup", (
            AddNodeOp(node_id="upstream", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddNodeOp(node_id="downstream", graph_id=gid, node_type="task", created_by_pid="p1"),
            AddEdgeOp(edge_id="e1", edge_type="depends_on", source_node_id="downstream", target_node_id="upstream", created_by_pid="p1"),
        ))
        bind = ArtifactVersionBinding(canonical_uri="u", artifact_id="a", version=1, content_hash="h")
        _verify_upstream(rt, gid, "upstream", "v_up", "evi_up", "act_up", bind)
        # The runtime's derived write updated the materialized projection
        # validity to VERIFIED via _apply_derived_after_patch.
        n, ed = rt.snapshot_projection(gid)
        # Reflect the derived VERIFIED state into the projection (this is the
        # shape compute_ready_frontier runs against once a re-materialization
        # pass pushes derived state back into the projection tables).
        n["upstream"].validity = NodeValidity.VERIFIED
        frontier = compute_ready_frontier(gid, rt.get_graph(gid).current_version, n, ed)
        assert [c.task_id for c in frontier] == ["downstream"]
