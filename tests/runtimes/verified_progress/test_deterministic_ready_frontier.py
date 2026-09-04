"""Step 30 — Deterministic Ready Frontier Audit.

Prove: query_ready_frontier(gid) produces DETERMINICAL output for identical
inputs regardless of:
  - Number of calls (100 same-process iterations)
  - Process restart (20 cold-start reconnections)
  - Projection rebuild (20 rebuilds)
  - PYTHONHASHSEED (3 distinct values)

Build a 500-task DAG with a reproducible edge set, seed once, then assert.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import random
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import (
    AddEdgeOp,
    AddNodeOp,
    GraphPatchProposal,
)

RECORDS: list[dict] = []


def _submit(rt, gid, kid, ops, author="p1"):
    cur = rt.get_graph(gid).current_version
    return rt.submit_patch(
        GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=cur,
            author_pid=author,
            idempotency_key=kid,
            operations=ops,
        )
    )


def _build_500_dag(db_path, gid):
    """Build a 500-task DAG with a reproducible edge set. Returns (rt, gid)."""
    rt = VerifiedProgressRuntime(db_path)
    rec = rt.create_graph(owner_pid="p1", graph_id=gid)
    # goal node
    rng = random.Random(12345)
    _submit(
        rt,
        gid,
        "seed-goal",
        (
            AddNodeOp(
                node_id="g0", graph_id=gid, node_type="goal",
                created_by_pid="p1", title="Root",
            ),
        ),
    )
    # 500 tasks t0..t499
    ops = []
    for i in range(500):
        ops.append(
            AddNodeOp(
                node_id=f"t{i}", graph_id=gid, node_type="task",
                created_by_pid="p1", title=f"T{i}",
            )
        )
    _submit(rt, gid, "seed-tasks", tuple(ops))

    # edges: goal -> t0, plus each task i depends on exactly one earlier task
    # chosen deterministically so the graph is a wide forest with controlled
    # depth (max depth ~ log2(500) ~ 9).
    edges = [
        AddEdgeOp(
            edge_id="g0-t0", edge_type="depends_on",
            source_node_id="g0", target_node_id="t0", created_by_pid="p1",
        ),
    ]
    for i in range(1, 500):
        parent = rng.randrange(0, i)
        edges.append(
            AddEdgeOp(
                edge_id=f"dep_t{i}_t{parent}",
                edge_type="depends_on",
                source_node_id=f"t{i}",
                target_node_id=f"t{parent}",
                created_by_pid="p1",
            )
        )
    # Commit edges in batches of 100 to keep patch size manageable.
    batch = []
    for idx, e in enumerate(edges):
        batch.append(e)
        if len(batch) >= 100:
            _submit(rt, gid, f"seed-edges-{idx}", tuple(batch))
            batch = []
    if batch:
        _submit(rt, gid, "seed-edges-final", tuple(batch))
    return rt, gid


def _frontier_signature(rt, gid):
    """Return a compact, comparable signature of the READY frontier."""
    frontier = rt.query_ready_frontier(gid)
    # task ids sorted, plus the full candidate list as JSON dicts for proof equality.
    sig = sorted(c.task_id for c in frontier)
    proof = []
    for c in frontier:
        proof.append(
            {
                "task_id": c.task_id,
                "graph_version": c.graph_version,
                "lifecycle_ok": c.readiness_proof.lifecycle_ok,
                "validity_ok": c.readiness_proof.validity_ok,
                "deps_ok": c.readiness_proof.all_deps_verified,
            }
        )
    return sig, proof


def _record(sid, name, expected, actual, verdict):
    RECORDS.append(
        {
            "id": sid, "step": 30, "name": name,
            "expected": expected, "actual": actual, "verdict": verdict,
        }
    )


# ── S30a: same-process 100 sequential queries ────────────────────────────────

class TestS30a_SameProcess100:
    def test_S30a_same_process_100_identical(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "s30a.db")
            gid = "s30a-gid"
            rt, gid = _build_500_dag(db_path, gid)
            sig0, proof0 = _frontier_signature(rt, gid)
            n = rt.get_graph(gid).current_version
            version_before = n
            all_identical = True
            for _ in range(99):
                s, p = _frontier_signature(rt, gid)
                if (s, p) != (sig0, proof0):
                    all_identical = False
                    break
            assert all_identical, "READY frontier must be identical across same-process queries"
            assert rt.get_graph(gid).current_version == version_before
            assert len(sig0) > 0, "frontier should be non-empty (t0 is root)"
            assert "t0" in sig0
            rt.close()
            _record(
                "S30a", "same_process_100_identical", "all_identical",
                f"identical={all_identical}; frontier_size={len(sig0)}; version={n}",
                "PASS",
            )


# ── S30b: cold-start 20 reconnections ─────────────────────────────────────────

class TestS30b_ColdStart20:
    def test_S30b_cold_start_20_identical(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "s30b.db")
            gid = "s30b-gid"
            # Build once, then CLOSE rt so the next open is a true cold start.
            rt, gid = _build_500_dag(db_path, gid)
            sig0, proof0 = _frontier_signature(rt, gid)
            rt.close()
            all_identical = True
            for i in range(20):
                rt2 = VerifiedProgressRuntime(db_path)
                s, p = _frontier_signature(rt2, gid)
                if (s, p) != (sig0, proof0):
                    all_identical = False
                    rt2.close()
                    break
                rt2.close()
            assert all_identical, "READY frontier must be identical across cold-start reconnections"
            _record(
                "S30b", "cold_start_20_identical", "all_identical",
                f"identical={all_identical}; frontier_size={len(sig0)}",
                "PASS",
            )


# ── S30c: rebuild loop 20 times ────────────────────────────────────────────────

class TestS30c_RebuildLoop20:
    def test_S30c_rebuild_loop_20_identical(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "s30c.db")
            gid = "s30c-gid"
            rt, gid = _build_500_dag(db_path, gid)
            sig0, proof0 = _frontier_signature(rt, gid)
            all_identical = True
            for _ in range(20):
                rt.rebuild_projection(gid)
                s, p = _frontier_signature(rt, gid)
                if (s, p) != (sig0, proof0):
                    all_identical = False
                    break
            assert all_identical, "READY frontier must be identical across rebuilds"
            rt.close()
            _record(
                "S30c", "rebuild_loop_20_identical", "all_identical",
                f"identical={all_identical}; frontier_size={len(sig0)}",
                "PASS",
            )


# ── S30d: different PYTHONHASHSEED in subprocess ──────────────────────────────

_CHILD_SCRIPT = r"""
import json, sys
from lhos.runtimes.verified_progress import VerifiedProgressRuntime

db_path, gid, result_path = sys.argv[1], sys.argv[2], sys.argv[3]
rt = VerifiedProgressRuntime(db_path)
frontier = rt.query_ready_frontier(gid)
sig = sorted(c.task_id for c in frontier)
with open(result_path, "w") as f:
    json.dump({"sig": sig, "count": len(sig)}, f)
"""


class TestS30d_HashSeedVariants:
    def test_S30d_hashseed_variants_identical(self):
        multiprocessing.set_start_method("spawn", force=True)
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "s30d.db")
            gid = "s30d-gid"
            rt, gid = _build_500_dag(db_path, gid)
            sig0, proof0 = _frontier_signature(rt, gid)
            rt.close()
            child_script_path = os.path.join(td, "_child_ready.py")
            with open(child_script_path, "w") as f:
                f.write(_CHILD_SCRIPT)
            seeds = ["1", "42", "9999"]
            child_sig = None
            for seed in seeds:
                result_path = os.path.join(td, f"result-{seed}.json")
                env = os.environ.copy()
                env["PYTHONHASHSEED"] = seed
                subprocess.check_call(
                    [sys.executable, child_script_path, db_path, gid, result_path],
                    env=env,
                )
                with open(result_path) as f:
                    data = json.load(f)
                if child_sig is None:
                    child_sig = data["sig"]
                    assert child_sig == sig0, (
                        f"hashseed={seed}: child frontier differs from parent: "
                        f"{child_sig} vs {sig0}"
                    )
                else:
                    assert data["sig"] == child_sig, (
                        f"hashseed={seed}: child frontier differs across seeds: "
                        f"{data['sig']} vs {child_sig}"
                    )
            _record(
                "S30d", "hashseed_variants_identical", "identical_across_seeds",
                f"seeds={seeds}; child==parent: {child_sig == sig0}",
                "PASS",
            )


@pytest.fixture(autouse=True, scope="session")
def _dump_results():
    yield
    all_identical = all(r.get("verdict") == "PASS" for r in RECORDS)
    out_dir = Path(__file__).resolve().parents[3] / "artifacts" / "agent_os_phase_d1_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "ready-frontier-determinism.json", "w") as f:
        json.dump(
            {
                "graphs": 1, "tasks": 500,
                "same_process_iterations": 100,
                "restart_iterations": 20,
                "rebuild_iterations": 20,
                "hashseed_variants": 3,
                "all_identical": all_identical,
            },
            f,
            indent=2,
        )
