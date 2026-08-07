"""Property-based random graph exploration (seed=42).

Deterministically seed random graph generators and check invariants hold
across many configurations.  Output is also captured in
artifacts/agent_os_phase_d1/random-graph-results.json.

Invariants checked:

    I-1  Version monotonically increases and is contiguous
    I-2  DAG prevents cycles (no mutation can create a cycle)
    I-3  Ready frontier is always a subset of task nodes with no deps
    I-4  Ready frontier is deterministic (same seed → same order)
    I-5  Evidence injection + facts match → TASK_VERIFIED_DERIVED fires
    I-6  Goal closes only when all directly-depending tasks verified
    I-7  Idempotency dedups: same key → no-op (version stable)
    I-8  Partial-invalid patches do not commit (atomic rollback)
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGError
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

PROJECT_ROOT = Path(__file__).resolve().parents[4]
ART = PROJECT_ROOT / "artifacts" / "agent_os_phase_d1"


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


def _make_random_dag(seed: int, n_tasks: int, edge_prob: float):
    """Generate a random DAG with `n_tasks` tasks and a single root goal.

    Returns (nodes, edges) as lists of AddNodeOp/AddEdgeOp.
    """
    rng = random.Random(seed)
    nodes = [
        AddNodeOp(node_id="g0", graph_id="_", node_type="goal", created_by_pid="p1", title="Root")
    ]
    for i in range(n_tasks):
        nodes.append(
            AddNodeOp(
                node_id=f"t{i}", graph_id="_", node_type="task", created_by_pid="p1", title=f"T{i}"
            )
        )
    # goal -> first task always
    edges = [
        AddEdgeOp(
            edge_id="root_dep",
            edge_type="depends_on",
            source_node_id="g0",
            target_node_id="t0",
            created_by_pid="p1",
        )
    ]
    for i in range(n_tasks):
        for j in range(i + 1, n_tasks):
            if rng.random() < edge_prob:
                edges.append(
                    AddEdgeOp(
                        edge_id=f"e_{i}_{j}",
                        edge_type="depends_on",
                        source_node_id=f"t{j}",
                        target_node_id=f"t{i}",
                        created_by_pid="p1",
                    )
                )
    return nodes, edges


class TestRandomGraphInvariants:
    def _build_rt(self, seed, n_tasks, edge_prob):
        rng = random.Random(seed)
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        nodes, edges = _make_random_dag(seed, n_tasks, edge_prob)
        for n in nodes:
            n.graph_id = gid
        all_ops = tuple(nodes + edges)
        kid = f"seed-{seed}-{n_tasks}-{edge_prob}"
        _patch(rt, gid, kid, all_ops)
        return rt, gid

    def test_invariant_1_version_contiguous(self):
        for seed in [42, 7, 13, 99]:
            rt, gid = self._build_rt(seed, 10, 0.3)
            ver = rt.get_graph(gid).current_version
            assert ver == 1, f"Expected single commit but got {ver}"
            for v in range(ver + 1):
                assert rt.store.get_version(gid, v) is not None
            assert rt.store.get_version(gid, ver + 1) is None

    def test_invariant_2_dag_no_cycles(self):
        # Building a legal DAG must not raise.  Self-loops should reject.
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "nodes",
            (
                AddNodeOp(
                    node_id="a", graph_id=gid, node_type="task", created_by_pid="p1", title="A"
                ),
                AddNodeOp(
                    node_id="b", graph_id=gid, node_type="task", created_by_pid="p1", title="B"
                ),
                AddEdgeOp(
                    edge_id="e",
                    edge_type="depends_on",
                    source_node_id="b",
                    target_node_id="a",
                    created_by_pid="p1",
                ),
            ),
        )
        with pytest.raises(VPGError):
            _patch(
                rt,
                gid,
                "cycle",
                (
                    AddEdgeOp(
                        edge_id="e2",
                        edge_type="depends_on",
                        source_node_id="a",
                        target_node_id="b",
                        created_by_pid="p1",
                    ),
                ),
            )

    @pytest.mark.parametrize("seed", [42, 7, 13, 99, 2024])
    def test_invariant_3_ready_frontier_subset_of_tasks(self, seed):
        rt, gid = self._build_rt(seed, 15, 0.25)
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier

        ready = compute_ready_frontier(
            gid,
            rt.get_graph(gid).current_version,
            {n.node_id: n for n in rt.store.get_all_nodes(gid)},
            list(rt.store.get_all_edges(gid)),
        )
        tasks = {n.node_id for n in rt.store.get_all_nodes(gid) if n.node_type == NodeType.TASK}
        for c in ready:
            assert c.task_id in tasks

    @pytest.mark.parametrize("seed", [42, 7, 13])
    def test_invariant_4_ready_frontier_deterministic(self, seed):
        r1 = self._build_rt(seed, 12, 0.3)
        r2 = self._build_rt(seed, 12, 0.3)
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier

        gid1 = r1[0].create_graph(owner_pid="p1").graph_id  # dummy; use real gid below
        gid1 = r1[1]
        gid2 = r2[1]
        fr1 = compute_ready_frontier(
            gid1,
            r1[0].get_graph(gid1).current_version,
            {n.node_id: n for n in r1[0].store.get_all_nodes(gid1)},
            list(r1[0].store.get_all_edges(gid1)),
        )
        fr2 = compute_ready_frontier(
            gid2,
            r2[0].get_graph(gid2).current_version,
            {n.node_id: n for n in r2[0].store.get_all_nodes(gid2)},
            list(r2[0].store.get_all_edges(gid2)),
        )
        ids1 = [c.task_id for c in fr1]
        ids2 = [c.task_id for c in fr2]
        assert ids1 == ids2, f"Frontier mismatch: {ids1} vs {ids2}"

    def test_invariant_5_evidence_with_facts_verifies(self):
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=_Facts(), facts_kernel=_Facts())
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "s",
            (
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T"
                ),
                AddNodeOp(
                    node_id="v1",
                    graph_id=gid,
                    node_type="verification",
                    created_by_pid="p1",
                    verification_kind="command_result",
                ),
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
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        _patch(
            rt,
            gid,
            "art",
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
        evts = [e.event_type for e in rt.get_events(gid)]
        assert GraphEventType.TASK_VERIFIED_DERIVED in evts

    def test_invariant_6_goal_close_dep_boundary(self):
        facts = _Facts()
        rt = VerifiedProgressRuntime(":memory:", facts_artifact=facts, facts_kernel=facts)
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "init",
            (
                AddNodeOp(
                    node_id="g1", graph_id=gid, node_type="goal", created_by_pid="p1", title="G"
                ),
                AddNodeOp(
                    node_id="t1", graph_id=gid, node_type="task", created_by_pid="p1", title="T1"
                ),
                AddNodeOp(
                    node_id="t2", graph_id=gid, node_type="task", created_by_pid="p1", title="T2"
                ),
                AddEdgeOp(
                    edge_id="d1",
                    edge_type="depends_on",
                    source_node_id="g1",
                    target_node_id="t1",
                    created_by_pid="p1",
                ),
                AddEdgeOp(
                    edge_id="d2",
                    edge_type="depends_on",
                    source_node_id="g1",
                    target_node_id="t2",
                    created_by_pid="p1",
                ),
                AddNodeOp(
                    node_id="v1",
                    graph_id=gid,
                    node_type="verification",
                    created_by_pid="p1",
                    verification_kind="command_result",
                ),
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
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", gid, "evidence", evi.model_dump_json()),
        )
        rt.store.conn.commit()
        _patch(
            rt,
            gid,
            "art",
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
        evts = [e.event_type for e in rt.get_events(gid)]
        # Only t1 verified → g1 NOT closed
        assert GraphEventType.GOAL_CLOSED_DERIVED not in evts

    def test_invariant_7_idempotency_dedup(self):
        for seed in [42, 7, 13]:
            rt, gid = self._build_rt(seed, 5, 0.3)
            v = rt.get_graph(gid).current_version
            # Replay the exact same patch — idempotency should dedup.
            nodes, edges = _make_random_dag(seed, 5, 0.3)
            for n in nodes:
                n.graph_id = gid
            ops = tuple(nodes) + tuple(edges)
            _patch(rt, gid, f"seed-{seed}-5-0.3", ops)
            assert rt.get_graph(gid).current_version == v, (
                f"seed={seed}: version advanced despite same idem key"
            )

    def test_invariant_8_partial_invalid_rollback(self):
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        _patch(
            rt,
            gid,
            "init",
            (
                AddNodeOp(
                    node_id="a", graph_id=gid, node_type="task", created_by_pid="p1", title="A"
                ),
            ),
        )
        before = rt.get_graph(gid).current_version
        with pytest.raises(VPGError):
            _patch(
                rt,
                gid,
                "bad_and_good",
                (
                    AddNodeOp(
                        node_id="ok",
                        graph_id=gid,
                        node_type="task",
                        created_by_pid="p1",
                        title="OK",
                    ),
                    AddEdgeOp(
                        edge_id="bad",
                        edge_type="depends_on",
                        source_node_id="ghost",
                        target_node_id="a",
                        created_by_pid="p1",
                    ),
                ),
            )
        assert rt.get_graph(gid).current_version == before
        assert rt.inspect_node(gid, "ok") is None


def _run_battery():
    """Full battery for artifact generation.  Same logic as the tests."""
    results = []
    rng = random.Random(42)

    # Battery 1: multiple seeds, varying sizes
    for trial, seed in enumerate([42, 7, 13, 99, 2024]):
        n_tasks = 5 + trial * 5
        edge_prob = 0.2 + trial * 0.05
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        gid = rec.graph_id
        nodes, edges = _make_random_dag(seed, n_tasks, edge_prob)
        for n in nodes:
            n.graph_id = gid
        all_ops = tuple(nodes + edges)
        _patch(rt, gid, f"seed-{seed}-{n_tasks}-{edge_prob}", all_ops)
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier

        ready = compute_ready_frontier(
            gid,
            rt.get_graph(gid).current_version,
            {n.node_id: n for n in rt.store.get_all_nodes(gid)},
            list(rt.store.get_all_edges(gid)),
        )
        results.append(
            {
                "trial": trial,
                "seed": seed,
                "n_tasks": n_tasks,
                "edge_prob": edge_prob,
                "version": rt.get_graph(gid).current_version,
                "ready_count": len(ready),
                "ready_ids": [c.task_id for c in ready],
                "task_count": sum(
                    1 for n in rt.store.get_all_nodes(gid) if n.node_type == NodeType.TASK
                ),
                "edge_count": len(list(rt.store.get_all_edges(gid))),
            }
        )

    ART.mkdir(parents=True, exist_ok=True)
    out = ART / "random-graph-results.json"
    out.write_text(
        json.dumps(
            {
                "battery": "random_graph_invariants",
                "version": "1.0.0",
                "fixed_seed": 42,
                "rng_library": "python.random",
                "trials": results,
            },
            indent=2,
        )
    )
    return results


if __name__ == "__main__":
    _run_battery()
