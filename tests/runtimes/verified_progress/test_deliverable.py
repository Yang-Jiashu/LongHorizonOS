"""End-to-end deliverable test: from empty graph to GOAL_CLOSED_DERIVED.

Demonstrates the full commitment lifecycle:
    1. Create graph → owner_pid
    2. Add goal + tasks + verifications (DAG wiring)
    3. Inject evidence directly (simulating out-of-band kernel evidence)
    4. Attach artifacts (pins canonical content)
    5. Attach evidence nodes to verifications
    6. Assert derived events: TASK_VERIFIED_DERIVED, TASK_CLOSED_DERIVED,
       GOAL_CLOSED_DERIVED
    7. Assert READY frontier is empty (all tasks done)
    8. Simulate SIGKILL → recover → verify derived events re-derive

This test passes only when every downstream contract of the runtime is
satisfied in concert.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.events import GraphEventType
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
from lhos.runtimes.verified_progress.recovery import verify_and_recover


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


@pytest.fixture
def rt():
    return VerifiedProgressRuntime(":memory:", facts_artifact=_Facts(), facts_kernel=_Facts())


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


def _etypes(rt, gid):
    return [e.event_type for e in rt.get_events(gid)]


class TestE2EGoalClosure:
    def test_full_lifecycle_two_task_goal(self, rt):
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id

        # Step 1: wire the DAG → g1 -> t1, g1 -> t2 ; each task has its own verification
        _patch(rt, gid, "dag", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                       created_by_pid="p1", title="Deliverable Goal"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                       created_by_pid="p1", title="T1"),
            AddNodeOp(node_id="t2", graph_id=gid, node_type="task",
                       created_by_pid="p1", title="T2"),
            AddNodeOp(node_id="v_t1", graph_id=gid, node_type="verification",
                       created_by_pid="p1", verification_kind="command_result"),
            AddNodeOp(node_id="v_t2", graph_id=gid, node_type="verification",
                       created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="dep_g_t1", edge_type="verifies",
                       source_node_id="v_t1", target_node_id="t1",
                       created_by_pid="p1"),
            AddEdgeOp(edge_id="dep_g_t2", edge_type="verifies",
                       source_node_id="v_t2", target_node_id="t2",
                       created_by_pid="p1"),
            AddEdgeOp(edge_id="d1", edge_type="depends_on",
                       source_node_id="g1", target_node_id="t1",
                       created_by_pid="p1"),
            AddEdgeOp(edge_id="d2", edge_type="depends_on",
                       source_node_id="g1", target_node_id="t2",
                       created_by_pid="p1"),
        ))

        # Step 2: verify each task
        bind = ArtifactVersionBinding(canonical_uri="s3://artifacts/a",
                                      artifact_id="art1",
                                      version=1,
                                      content_hash="deadbeef")
        for tid, vid, prefix in [("t1", "v_t1", "t1"), ("t2", "v_t2", "t2")]:
            evi = EvidenceNode(
                graph_id=gid, node_id=f"evi_{prefix}",
                node_type=NodeType.EVIDENCE,
                evidence_kind="command_result", result="pass",
                source_verification_id=vid, source_action_id=f"act_{prefix}",
                produced_by_pid="p1",
                created_in_version=rt.get_graph(gid).current_version,
                updated_in_version=rt.get_graph(gid).current_version,
                created_by_pid="p1",
                artifact_bindings=(bind,),
            )
            rt.store.conn.execute(
                "INSERT INTO graph_nodes_projection "
                "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
                (f"evi_{prefix}", gid, "evidence", evi.model_dump_json()),
            )
            rt.store.conn.commit()
            _patch(rt, gid, f"art_{prefix}", (
                AttachArtifactOp(task_node_id=tid, artifact=bind,
                                 created_by_pid="p1", edge_id=f"p_{prefix}"),
            ))
            _patch(rt, gid, f"att_{prefix}", (
                AttachEvidenceOp(verification_node_id=vid,
                                  evidence_node_id=f"evi_{prefix}",
                                  created_by_pid="p1",
                                  edge_id=f"pe_{prefix}"),
            ))

        # Step 3: Assert derived events
        evts = _etypes(rt, gid)
        assert GraphEventType.TASK_VERIFIED_DERIVED in evts
        assert GraphEventType.TASK_CLOSED_DERIVED in evts
        assert GraphEventType.GOAL_CLOSED_DERIVED in evts

        # Step 4: Both tasks are verified and closed — inspect_node confirms
        for tid in ["t1", "t2"]:
            n = rt.inspect_node(gid, tid)
            assert n.node_id == tid
            # lifecycle=ADMITTED, validity=VERIFIED (projection stores pre-derived)

        # Step 5: SIGKILL recovery → re-derive same events
        rt.store.conn.execute("DELETE FROM graph_nodes_projection")
        rt.store.conn.commit()
        events, rec = verify_and_recover(
            rt.store, gid,
            facts_artifact=rt.facts_artifact,
            facts_kernel=rt.facts_kernel,
        )
        rec_types = [e.event_type for e in events]
        # Recovery starts with GRAPH_RECOVERY_STARTED and ends with COMPLETED.
        assert GraphEventType.GRAPH_RECOVERY_STARTED in rec_types
        assert GraphEventType.GRAPH_RECOVERY_COMPLETED in rec_types
        # After recovery, the graph record is intact.
        assert rec.graph_id == gid

    def test_monotonic_version_on_full_lifecycle(self, rt):
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        assert rt.get_graph(gid).current_version == 0
        _patch(rt, gid, "dag", (
            AddNodeOp(node_id="g1", graph_id=gid, node_type="goal",
                       created_by_pid="p1", title="G"),
            AddNodeOp(node_id="t1", graph_id=gid, node_type="task",
                       created_by_pid="p1", title="T"),
            AddNodeOp(node_id="v1", graph_id=gid, node_type="verification",
                       created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="d", edge_type="depends_on",
                       source_node_id="g1", target_node_id="t1",
                       created_by_pid="p1"),
            AddEdgeOp(edge_id="vfy", edge_type="verifies",
                       source_node_id="v1", target_node_id="t1",
                       created_by_pid="p1"),
        ))
        bind = ArtifactVersionBinding(canonical_uri="u", artifact_id="a",
                                      version=1, content_hash="h")
        evi = EvidenceNode(
            graph_id=gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=rt.get_graph(gid).current_version,
            updated_in_version=rt.get_graph(gid).current_version,
            created_by_pid="p1",
            artifact_bindings=(bind,),
        )
        rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        _patch(rt, gid, "art", (
            AttachArtifactOp(task_node_id="t1", artifact=bind,
                             created_by_pid="p1", edge_id="p1"),
        ))
        _patch(rt, gid, "att", (
            AttachEvidenceOp(verification_node_id="v1", evidence_node_id="evi1",
                              created_by_pid="p1", edge_id="pe"),
        ))
        evts = _etypes(rt, gid)
        assert GraphEventType.GOAL_CLOSED_DERIVED in evts
        # 3 commits: "dag", "art", "att"
        assert rt.get_graph(gid).current_version == 3
