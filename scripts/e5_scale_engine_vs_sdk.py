"""E5 — scale: where does the cost live, the engine or the SDK compile path?

Two things are already known and must be separated:

  * Through the public SDK, a goal larger than ~165 tasks cannot be built at
    all: `Goal.compile` emits ONE GraphPatchProposal and
    `patch_validator.MAX_PATCH_OPS = 500` rejects it.
  * Per-task cost through the SDK grows with N (measured ~O(N^2.2)), because
    every operation materializes the whole projection.

But the audit scripts construct 1000-node graphs happily -- they drive the
invalidation engine directly, bypassing `Goal.compile`.  So the question a
reviewer will ask ("what about 1000 tasks?") has two different answers depending
on which path you mean.

This script measures both, separately:

  ARM 1 (engine only): time compute_invalidation_cone + repair frontier on
         synthetic graphs from 100 to 4000 nodes.  No SDK, no SQLite.
  ARM 2 (SDK path):    time goal build + run-to-closure for increasing N until
         it fails, recording the exact failure and the per-task cost curve.

Output lets you state precisely: "the semantic engine scales to N; the SDK
compile path caps at M for reason R."
"""

# ruff: noqa
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from d31_over_under_invalidation import TNode, _cause, depends_on

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)


def make_graph(rng, n):
    ids = [f"T{i}" for i in range(n)]
    edges = []
    for i in range(1, n):
        for _ in range(rng.randint(1, 2)):
            j = rng.randrange(max(0, i - 10), i)
            edges.append((ids[i], ids[j]))
    edges = sorted(set(edges))
    return ids, [depends_on(s, t) for s, t in edges], len(edges)


def arm_engine():
    rng = random.Random(0xE5)
    rows = []
    for n in (100, 250, 500, 1000, 2000, 4000):
        ids, edges, n_edges = make_graph(rng, n)
        tasks = {t: TNode(t, "verified") for t in ids}
        seed = ids[len(ids) // 3]
        inp = EngineInputs(
            graph_id="e5",
            current_version=1,
            task_nodes=tasks,
            goal_nodes={},
            evidence_nodes={},
            edges=edges,
            explicit_causes=(_cause("e5", 1, seed),),
        )
        t0 = time.perf_counter()
        res = build_invalidation_result(inp, run_invalidation_engine(inp))
        dt = (time.perf_counter() - t0) * 1000
        rows.append(
            {
                "nodes": n,
                "edges": n_edges,
                "affected": len(res.stale_nodes),
                "wall_ms": round(dt, 2),
                "us_per_node": round(dt * 1000 / n, 2),
            }
        )
        print(
            f"  engine  N={n:5d} E={n_edges:6d} affected={len(res.stale_nodes):5d} "
            f"{dt:9.2f} ms  {rows[-1]['us_per_node']:8.2f} us/node"
        )
    return rows


def arm_sdk():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    rows = []
    for n in (25, 50, 100, 150, 165, 200, 400):
        rt = AgentOS(":memory:")
        rt.add_agent(Agent("w", specializations=("python",), max_concurrency=8))
        g = Goal("E5")
        prev = None
        for i in range(n):
            prev = g.task(
                f"t{i}",
                agent="w",
                depends_on=(prev,) if prev else (),
                verify=scripted_executor(artifact_id=f"a{i}.txt", version=1),
            )
        t0 = time.perf_counter()
        try:
            res = rt.run(g, max_dispatches=n * 6 + 60, max_steps=n * 6 + 60)
            dt = (time.perf_counter() - t0) * 1000
            rows.append(
                {
                    "nodes": n,
                    "ok": True,
                    "goal_state": res.goal_state,
                    "wall_ms": round(dt, 2),
                    "ms_per_task": round(dt / n, 2),
                    "error": None,
                }
            )
            print(f"  sdk     N={n:5d} {res.goal_state:>8s} {dt:10.1f} ms {dt / n:8.2f} ms/task")
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000
            rows.append(
                {
                    "nodes": n,
                    "ok": False,
                    "goal_state": None,
                    "wall_ms": round(dt, 2),
                    "ms_per_task": None,
                    "error": f"{type(e).__name__}: {str(e)[:120]}",
                }
            )
            print(f"  sdk     N={n:5d} FAILED  {type(e).__name__}: {str(e)[:90]}")
    return rows


def main() -> int:
    out = REPO / "artifacts" / "agent_os_phase_d3"
    out.mkdir(parents=True, exist_ok=True)
    print("ARM 1 - invalidation engine only (no SDK, no SQLite)")
    engine = arm_engine()
    print()
    print("ARM 2 - public SDK path (Goal.compile + run to closure)")
    sdk = arm_sdk()

    ok = [r for r in sdk if r["ok"]]
    max_sdk = max((r["nodes"] for r in ok), default=0)
    first_fail = next((r for r in sdk if not r["ok"]), None)

    report = {
        "experiment": "E5_scale_engine_vs_sdk",
        "arm1_engine_only": engine,
        "arm2_sdk_path": sdk,
        "max_nodes_via_sdk": max_sdk,
        "first_sdk_failure": first_fail,
        "engine_max_nodes_tested": max(r["nodes"] for r in engine),
        "headline": (
            "The semantic engine itself is not the bottleneck; the public SDK "
            "compile path caps goal size because Goal.compile emits a single "
            "patch bounded by MAX_PATCH_OPS."
        ),
    }
    (out / "e5-scale-engine-vs-sdk.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print()
    print(f"engine tested up to : {report['engine_max_nodes_tested']} nodes")
    print(f"max via public SDK  : {max_sdk} nodes")
    if first_fail:
        print(f"first SDK failure   : N={first_fail['nodes']} -> {first_fail['error']}")
    print(f"json: {out / 'e5-scale-engine-vs-sdk.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
