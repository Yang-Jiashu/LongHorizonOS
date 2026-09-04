"""Phase D3.1 §7/§8 — 1000-node Over- and Under-Invalidation audits with an
INDEPENDENT reference algorithm.

We build large (1000-task) DAGs with 20+ independent branches, chains,
diamonds, fan-out/fan-in, then:

  REF (independent): the "semantic descendants" of a seed = closure over
  DEPENDS_ON edges from the seed (reverse direction), as THE authoritative
  expected-affected set, computed WITHOUT calling the D3 cone/engine.

  D3: run the real D3 engine.

  over-invalidation  := nodes that D3 marked stale but REF says are preserved
                        (false positives)  -> must be ZERO
  under-invalidation := nodes that REF says must be stale (depend on seed &
                        were verified) but D3 left VERIFIED  -> must be ZERO

Over-invalidation audit: §7, 1000 trials.
Under-invalidation audit: §8, 1000 trials.
"""

# ruff: noqa
from __future__ import annotations

import json
import random
import sys
from collections import deque
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)
from lhos.runtimes.invalidation.models import InvalidationCause


class _Val:
    def __init__(self, v):
        self.value = v


class TNode:
    def __init__(self, tid, validity="verified"):
        self.node_id = tid
        self.validity = _Val(validity)
        self.lifecycle = _Val("admitted")
        self.node_type = "task"


class Edge:
    def __init__(self, etype, s, t):
        self.edge_type = _Val(etype)
        self.source_node_id = s
        self.target_node_id = t


def depends_on(s, t):
    return Edge("depends_on", s, t)


def _cause(gid, ver, tid):
    return InvalidationCause(
        cause_id=f"c:{tid}",
        graph_id=gid,
        graph_version=ver,
        cause_type="ARTIFACT_VERSION_SUPERSEDED",
        source_node_id=tid,
        artifact_id="A",
        old_version=0,
        new_version=1,
        reason=f"seed {tid}",
    )


def reference_expected(seed, task_ids, edges):
    """Independent expected-affected set = all tasks that depend on the seed
    through DEPENDS_ON edges, PLUS the seed itself."""
    # reverse-deps: node -> tasks that depend on it (source depends on target means
    # when target is stale, source is affected)
    rev = {tid: set() for tid in task_ids}
    for e in edges:
        if e.edge_type.value == "depends_on":
            rev[e.target_node_id].add(e.source_node_id)
    affected = set()
    q = deque([seed])
    while q:
        u = q.popleft()
        if u in affected:
            continue
        affected.add(u)
        for v in rev.get(u, ()):
            q.append(v)
    return affected


def build_large_dag(rng, n=1000):
    """1000-task DAG with chains, diamonds, fan-out, fan-in, 20+ branches."""
    ids = [f"T{i}" for i in range(n)]
    tasks = {tid: TNode(tid, "verified") for tid in ids}
    edges = []
    # several heavy chains
    for c in range(20):
        start = c * 50
        for i in range(start, min(start + 49, n - 1)):
            edges.append(depends_on(ids[i + 1], ids[i]))
    # diamonds: T(a) and T(b) both depend on T(seed); T(join) depends on both
    for d in range(30):
        seed = rng.randrange(200, 600)
        a = rng.randrange(600, 700)
        b = rng.randrange(700, 800)
        j = rng.randrange(900, n)
        edges.append(depends_on(ids[a], ids[seed]))
        edges.append(depends_on(ids[b], ids[seed]))
        edges.append(depends_on(ids[j], ids[a]))
        edges.append(depends_on(ids[j], ids[b]))
    # fan-in to a sink
    sink = n - 2
    for i in range(500):
        src = rng.randrange(1, 500)
        edges.append(depends_on(ids[sink], ids[src]))
    # dedup
    seen = set()
    dedup = []
    for e in edges:
        k = (e.source_node_id, e.target_node_id, e.edge_type.value)
        if k not in seen:
            seen.add(k)
            dedup.append(e)
    return {"ids": ids, "tasks": tasks, "edges": dedup}


def run_one(graph, seed):
    inp = EngineInputs(
        graph_id="gbig",
        current_version=1,
        task_nodes=graph["tasks"],
        goal_nodes={},
        evidence_nodes={},
        edges=graph["edges"],
        explicit_causes=(_cause("gbig", 1, seed),),
    )
    r = run_invalidation_engine(inp)
    res = build_invalidation_result(inp, r)
    affected = set(res.stale_nodes)
    return affected, res


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(0xD31)

    # Build 20 distinct 1000-node graphs and fold over trials.
    graphs = [build_large_dag(rng, 1000) for _ in range(20)]

    over_results = {"trials": 0, "false_positives": 0, "details": []}
    under_results = {"trials": 0, "false_negatives": 0, "details": []}

    for t in range(1000):
        g = rng.choice(graphs)
        seed = rng.choice(g["ids"])
        expected = reference_expected(seed, g["ids"], g["edges"])
        affected, _ = run_one(g, seed)

        # over-invalidation: D3 stale but NOT in expected and seed-independent
        false_positive = affected - expected
        # under-invalidation: expected must-stale (seed-dependent verified) not
        # in D3 affected
        must_stale = expected - {seed}
        false_negative = set()
        for node in must_stale:
            if node not in affected and g["tasks"][node].validity.value == "verified":
                false_negative.add(node)

        over_results["trials"] += 1
        under_results["trials"] += 1
        if false_positive:
            over_results["false_positives"] += len(false_positive)
            over_results["details"].append({"trial": t, "fp": sorted(false_positive)[:20]})
        if false_negative:
            under_results["false_negatives"] += len(false_negative)
            under_results["details"].append({"trial": t, "fn": sorted(false_negative)[:20]})

    over_results["pass"] = over_results["false_positives"] == 0
    under_results["pass"] = under_results["false_negatives"] == 0

    # §15 minimality: T1->T2->T3->T4->T5 all stale -> frontier [T1] then [T2]...
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    mind_ids = [f"T{i}" for i in range(1, 6)]
    mind_tasks = {tid: TNode(tid, "verified") for tid in mind_ids}
    mind_edges = [
        depends_on("T2", "T1"),
        depends_on("T3", "T2"),
        depends_on("T4", "T3"),
        depends_on("T5", "T4"),
    ]
    # T1 seeded stale -> all 5 stale; frontier = [T1]
    inp = EngineInputs(
        graph_id="m",
        current_version=1,
        task_nodes=mind_tasks,
        goal_nodes={},
        evidence_nodes={},
        edges=mind_edges,
        explicit_causes=(_cause("m", 1, "T1"),),
    )
    r = run_invalidation_engine(inp)
    res = build_invalidation_result(inp, r)
    f1 = [c.task_id for c in res.frontier.candidates]
    # after T1 reverified -> frontier=[T2]
    dv = {"T1": "verified", "T2": "stale", "T3": "stale", "T4": "stale", "T5": "stale"}
    f2 = [
        c.task_id
        for c in compute_repair_frontier(
            "m",
            2,
            mind_tasks,
            mind_edges,
            stale_or_unverified={"T2", "T3", "T4", "T5"},
            derived_validity=dv,
        ).candidates
    ]
    minimality_pass = f1 == ["T1"] and f2 == ["T2"]

    # §14 frontier exactness: expected == D3 frontier (1000 random states)
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    frontier_mismatch = 0
    for t in range(1000):
        g = rng.choice(graphs)
        # random partial stale state
        stale_set = set()
        for tid in g["ids"]:
            if rng.random() < 0.2:
                stale_set.add(tid)
        derived = {}
        for tid, node in g["tasks"].items():
            derived[tid] = "stale" if tid in stale_set else node.validity.value
        fr = compute_repair_frontier(
            "gf", 1, g["tasks"], g["edges"], stale_or_unverified=stale_set, derived_validity=derived
        )
        actual = {c.task_id for c in fr.candidates}
        # expected: stale tasks whose all deps are verified
        fwd = {tid: [] for tid in g["ids"]}
        for e in g["edges"]:
            if e.edge_type.value == "depends_on":
                fwd[e.source_node_id].append(e.target_node_id)
        expected_fr = set()
        for tid in stale_set:
            deps = fwd.get(tid, [])
            if all(derived.get(d, "missing") == "verified" for d in deps):
                expected_fr.add(tid)
        if actual != expected_fr:
            frontier_mismatch += 1

    frontier_exactness_pass = frontier_mismatch == 0

    result = {
        "spec_section": "§7/§8/§14/§15",
        "over_invalidation": over_results,
        "under_invalidation": under_results,
        "frontier_exactness": {
            "trials": 1000,
            "mismatches": frontier_mismatch,
            "pass": frontier_exactness_pass,
        },
        "frontier_minimality": {"pass": minimality_pass, "f1": f1, "f2": f2},
    }

    # distinct artifacts
    (out_dir / "over-invalidation-audit.json").write_text(
        json.dumps(
            {
                "spec_section": "§7",
                "trials": 1000,
                "false_positives": over_results["false_positives"],
                "pass": over_results["pass"],
                "details": over_results["details"][:3],
            },
            indent=2,
        )
    )
    (out_dir / "under-invalidation-audit.json").write_text(
        json.dumps(
            {
                "spec_section": "§8",
                "trials": 1000,
                "false_negatives": under_results["false_negatives"],
                "pass": under_results["pass"],
                "details": under_results["details"][:3],
            },
            indent=2,
        )
    )
    (out_dir / "repair-frontier-exactness.json").write_text(
        json.dumps(
            {
                "spec_section": "§14",
                "trials": 1000,
                "mismatches": frontier_mismatch,
                "pass": frontier_exactness_pass,
            },
            indent=2,
        )
    )

    print(
        "over:",
        over_results["false_positives"],
        "false-positives /",
        over_results["trials"],
        "trials",
    )
    print(
        "under:",
        under_results["false_negatives"],
        "false-negatives /",
        under_results["trials"],
        "trials",
    )
    print("frontier exactness mismatches:", frontier_mismatch)
    print("frontier minimality:", minimality_pass, f1, f2)
    ok = (
        over_results["pass"]
        and under_results["pass"]
        and frontier_exactness_pass
        and minimality_pass
    )
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
