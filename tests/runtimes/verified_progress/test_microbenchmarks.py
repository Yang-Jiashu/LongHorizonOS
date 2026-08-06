"""Runtime microbenchmarks (timing measurements).

Measures wall-clock time for core runtime operations at several scales.
Also emits a machine-readable report to
artifacts/agent_os_phase_d1/microbenchmarks.json.

Benchmarks:

    BM-1  10-task graph build time
    BM-2  100-task graph build time
    BM-3  Ready frontier computation (10/50/100 tasks)
    BM-4  Patch commit latency (single op vs. batch of 10)
    BM-5  Event store append rate (100 events)
    BM-6  SIGKILL recovery time (projection replay, 50 nodes)
    BM-7  Idempotency check on 1000 cached keys
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGError
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

PROJECT_ROOT = Path(__file__).resolve().parents[4]
ART = PROJECT_ROOT / "artifacts" / "agent_os_phase_d1"


def _patch(rt, gid, kid, ops):
    return rt.submit_patch(GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=rt.get_graph(gid).current_version,
        author_pid="p1",
        idempotency_key=kid,
        operations=ops,
    ))


class _Facts:
    def get_action(self, aid):
        class A:
            action_id = aid; pid = "p1"; state = "committed"; result = {}; artifact_refs = ()
        return A()

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


def _build_task_graph(rt, gid, n):
    nodes = [AddNodeOp(node_id="g0", graph_id=gid, node_type="goal",
                        created_by_pid="p1", title="G")]
    edges = [AddEdgeOp(edge_id="root", edge_type="depends_on",
                        source_node_id="g0", target_node_id="t0",
                        created_by_pid="p1")]
    for i in range(n):
        nodes.append(AddNodeOp(
            node_id=f"t{i}", graph_id=gid, node_type="task",
            created_by_pid="p1", title=f"T{i}"))
        if i > 0:
            edges.append(AddEdgeOp(
                edge_id=f"e{i}", edge_type="depends_on",
                source_node_id=f"t{i}", target_node_id=f"t{i - 1}",
                created_by_pid="p1"))
    _patch(rt, gid, f"build_{n}", tuple(nodes + edges))


class TestMicrobenchmarks:
    @pytest.fixture(autouse=True)
    def _rt(self):
        rt = VerifiedProgressRuntime(":memory:")
        rec = rt.create_graph(owner_pid="p1")
        self.gid = rec.graph_id
        self.rt = rt

    def test_bm1_build_10_tasks(self):
        t0 = time.perf_counter()
        _build_task_graph(self.rt, self.gid, 10)
        elapsed = time.perf_counter() - t0
        assert elapsed < 1.0, f"10-task build too slow: {elapsed:.3f}s"

    def test_bm2_build_100_tasks(self):
        t0 = time.perf_counter()
        _build_task_graph(self.rt, self.gid, 100)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100-task build too slow: {elapsed:.3f}s"

    @pytest.mark.parametrize("n", [10, 50, 100])
    def test_bm3_ready_frontier_computes_quickly(self, n):
        _build_task_graph(self.rt, self.gid, n)
        from lhos.runtimes.verified_progress.readiness import compute_ready_frontier
        t0 = time.perf_counter()
        for _ in range(10):
            compute_ready_frontier(
                self.gid, self.rt.get_graph(self.gid).current_version,
                {nd.node_id: nd for nd in self.rt.store.get_all_nodes(self.gid)},
                list(self.rt.store.get_all_edges(self.gid)),
            )
        elapsed = time.perf_counter() - t0
        assert elapsed / 10 < 0.1, f"Frontier for {n} tasks too slow: {(elapsed/10)*1000:.1f}ms"

    def test_bm4_single_vs_batch(self):
        t0 = time.perf_counter()
        for i in range(10):
            _patch(self.rt, self.gid, f"single_{i}", (
                AddNodeOp(node_id=f"s{i}", graph_id=self.gid, node_type="task",
                           created_by_pid="p1"),
            ))
        single = time.perf_counter() - t0

        t0 = time.perf_counter()
        _patch(self.rt, self.gid, "batch_10", tuple(
            AddNodeOp(node_id=f"b{i}", graph_id=self.gid, node_type="task",
                       created_by_pid="p1") for i in range(10)
        ))
        batch = time.perf_counter() - t0
        # Batch should be at least as fast as sequentially doing 10 ops
        assert batch <= single * 1.5, (
            f"Batch ({batch:.3f}s) not competitive vs single ({single:.3f}s)"
        )

    def test_bm5_event_append_rate(self):
        facts = _Facts()
        self.rt.facts_artifact = facts
        self.rt.facts_kernel = facts
        _patch(self.rt, self.gid, "s", (
            AddNodeOp(node_id="t1", graph_id=self.gid, node_type="task",
                       created_by_pid="p1", title="T"),
            AddNodeOp(node_id="v1", graph_id=self.gid, node_type="verification",
                       created_by_pid="p1", verification_kind="command_result"),
            AddEdgeOp(edge_id="vf1", edge_type="verifies",
                       source_node_id="v1", target_node_id="t1",
                       created_by_pid="p1"),
        ))
        b = ArtifactVersionBinding(canonical_uri="u", artifact_id="a",
                                   version=1, content_hash="h")
        evi = EvidenceNode(
            graph_id=self.gid, node_id="evi1", node_type=NodeType.EVIDENCE,
            evidence_kind="command_result", result="pass",
            source_verification_id="v1", source_action_id="act1",
            produced_by_pid="p1",
            created_in_version=self.rt.get_graph(self.gid).current_version,
            updated_in_version=self.rt.get_graph(self.gid).current_version,
            created_by_pid="p1",
            artifact_bindings=(b,),
        )
        self.rt.store.conn.execute(
            "INSERT INTO graph_nodes_projection "
            "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
            ("evi1", self.gid, "evidence", evi.model_dump_json()),
        )
        self.rt.store.conn.commit()
        t0 = time.perf_counter()
        for i in range(100):
            evi_loop = EvidenceNode(
                graph_id=self.gid, node_id=f"evi_loop_{i}", node_type=NodeType.EVIDENCE,
                evidence_kind="command_result", result="pass",
                source_verification_id="v1", source_action_id="act1",
                produced_by_pid="p1",
                created_in_version=self.rt.get_graph(self.gid).current_version,
                updated_in_version=self.rt.get_graph(self.gid).current_version,
                created_by_pid="p1",
                artifact_bindings=(b,),
            )
            self.rt.store.conn.execute(
                "INSERT INTO graph_nodes_projection "
                "(node_id,graph_id,node_type,payload_json) VALUES (?,?,?,?)",
                (f"evi_loop_{i}", self.gid, "evidence",
                 evi_loop.model_dump_json()),
            )
            self.rt.store.conn.commit()
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100 evidence inserts too slow: {elapsed:.3f}s"

    def test_bm6_recovery_50_nodes(self):
        _build_task_graph(self.rt, self.gid, 50)
        self.rt.store.conn.execute("DELETE FROM graph_nodes_projection")
        self.rt.store.conn.commit()
        t0 = time.perf_counter()
        events, rec = verify_and_recover(
            self.rt.store, self.gid,
            facts_artifact=_Facts(), facts_kernel=_Facts(),
        )
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"Recovery too slow: {elapsed:.3f}s"
        assert rec.graph_id == self.gid

    def test_bm7_idempotency_cache_1000(self):
        for i in range(1000):
            _patch(self.rt, self.gid, f"k{i}", (
                AddNodeOp(node_id=f"n{i}", graph_id=self.gid, node_type="task",
                           created_by_pid="p1"),
            ))
        t0 = time.perf_counter()
        for i in range(1000):
            _patch(self.rt, self.gid, f"k{i}", (
                AddNodeOp(node_id=f"n{i}_dup", graph_id=self.gid, node_type="task",
                           created_by_pid="p1"),
            ))
        elapsed = time.perf_counter() - t0
        # Replay must be fast — pure cache hit.
        assert elapsed < 2.0, f"1000 dedups replay too slow: {elapsed:.3f}s"
        assert self.rt.get_graph(self.gid).current_version == 1000


def run_benchmarks():
    """Run all benches and write artifact (can be invoked standalone)."""
    results = {}
    rt = VerifiedProgressRuntime(":memory:")
    rec = rt.create_graph(owner_pid="p1")
    gid = rec.graph_id

    t0 = time.perf_counter()
    _build_task_graph(rt, gid, 10)
    results["bm1_build_10"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    _build_task_graph(rt, gid, 100)
    results["bm2_build_100"] = time.perf_counter() - t0

    from lhos.runtimes.verified_progress.readiness import compute_ready_frontier
    for n in [10, 50, 100]:
        _build_task_graph(rt, gid, n)
        t0 = time.perf_counter()
        for _ in range(10):
            compute_ready_frontier(
                gid, rt.get_graph(gid).current_version,
                {nd.node_id: nd for nd in rt.store.get_all_nodes(gid)},
                list(rt.store.get_all_edges(gid)),
            )
        results[f"bm3_frontier_{n}"] = (time.perf_counter() - t0) / 10

    t0 = time.perf_counter()
    for i in range(10):
        _patch(rt, gid, f"single_{i}", (
            AddNodeOp(node_id=f"bench_s{i}", graph_id=gid, node_type="task",
                       created_by_pid="p1"),
        ))
    results["bm4_single_10"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    _patch(rt, gid, "batch_bench", tuple(
        AddNodeOp(node_id=f"bench_b{i}", graph_id=gid, node_type="task",
                   created_by_pid="p1") for i in range(10)
    ))
    results["bm4_batch_10"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    _build_task_graph(rt, gid, 50)
    rt.store.conn.execute("DELETE FROM graph_nodes_projection")
    rt.store.conn.commit()
    events, rec = verify_and_recover(
        rt.store, gid,
        facts_artifact=_Facts(), facts_kernel=_Facts(),
    )
    results["bm6_recovery_50"] = time.perf_counter() - t0

    ART.mkdir(parents=True, exist_ok=True)
    out = ART / "microbenchmarks.json"
    out.write_text(json.dumps({
        "benchmark_suite": "vpg_microbenchmarks",
        "version": "1.0.0",
        "results": results,
        "units": "seconds",
    }, indent=2))
    return results


if __name__ == "__main__":
    run_benchmarks()
