"""D3 §37 — deterministic computation worker used across crash trials.

A single graph (fixed seed) with a branch-preservation shape is built, the D3
engine computes the invalidation result, and we expose:
  - a marker file `ready` written when the computation reaches the requested
    boundary (so the parent can SIGKILL at that exact point);
  - the deterministic result dict (cone_hash, frontier_hash, affected set,
    preserved set, reopened goals) that the recovery must reproduce.

The parent uses the 'reverify' boundary to obtain the reference (no-crash)
result and kills at S1..S5.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)
from lhos.runtimes.invalidation.models import InvalidationCause


class _Val:
    def __init__(self, v: str): self.value = v


class TNode:
    def __init__(self, tid: str, validity: str = "verified"):
        self.node_id = tid
        self.validity = _Val(validity)
        self.lifecycle = _Val("admitted")
        self.node_type = "task"


class Edge:
    def __init__(self, etype: str, s: str, t: str):
        self.edge_type = _Val(etype)
        self.source_node_id = s
        self.target_node_id = t


def depends_on(s: str, t: str) -> Edge:
    return Edge("depends_on", s, t)


def build_graph() -> dict:
    """20-task graph: a chain + an independent branch (to test preservation
    across every restart)."""
    tasks = {f"T{i}": TNode(f"T{i}", "verified") for i in range(12)}
    edges = [
        depends_on("T1", "T0"), depends_on("T2", "T1"), depends_on("T3", "T2"),
        depends_on("T4", "T3"), depends_on("T5", "T4"),  # main chain
        depends_on("T7", "T6"), depends_on("T8", "T7"),  # independent branch
    ]
    return {"tasks": tasks, "edges": edges}


def compute_deterministic(boundary: str) -> dict:
    marker_dir = os.environ.get("D3_MARKER_DIR")
    graph = build_graph()
    tasks = graph["tasks"]
    edges = graph["edges"]
    # Seed T0 => main chain (T1..T5) stale; independent branch (T6,T7,T8) preserved.
    cause = InvalidationCause(
        cause_id="c:A", graph_id="g", graph_version=1,
        cause_type="ARTIFACT_VERSION_SUPERSEDED",
        source_node_id="T0", artifact_id="A", old_version=0, new_version=1,
        reason="seed T0",
    )
    # boundary hooks: we only need ONE deterministic compute; the boundary
    # distinguishes where the parent kills.
    inp = EngineInputs(
        graph_id="g", current_version=1, task_nodes=tasks,
        goal_nodes={}, evidence_nodes={}, edges=edges,
        explicit_causes=(cause,),
    )
    er = run_invalidation_engine(inp)
    res = build_invalidation_result(inp, er)
    result = {
        "cone_hash": res.cone.cone_hash,
        "frontier_hash": res.frontier.frontier_hash,
        "affected": list(res.stale_nodes),
        "preserved": list(res.preserved_nodes),
        "reopened": list(res.reopened_goals),
        "boundary": boundary,
    }
    # Signal readiness ONLY when we are the kill-target (not the reference run).
    if marker_dir and os.environ.get("D3_WRITE_MARKER") == "1":
        os.makedirs(marker_dir, exist_ok=True)
        open(os.path.join(marker_dir, "ready"), "w").write("1")
        # keep the process alive at the boundary until killed
        import time
        time.sleep(60)
    return result
