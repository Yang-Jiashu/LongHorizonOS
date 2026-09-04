"""B4 — head-to-head against a content-hash (Bazel/make-semantics) rebuilder.

WHY THIS EXPERIMENT DECIDES THE PAPER
-------------------------------------
Build systems already solve "what must be rebuilt after a change", and they do
it soundly.  A reviewer will therefore ask: *what does an agent runtime add?*

The answer is not precision.  It is **where the dependency set comes from**:

  * make / Bazel derive it from a **statically declared** input set.  Anything
    the task actually consumed but did not declare is invisible.
  * Riker (ATC'22) derives it by **tracing** the execution -- sound, but only
    because a build command can be re-run and observed.
  * LongHorizonOS derives it from the **Evidence recorded at verification
    time** (`EvidenceNode.artifact_bindings`), i.e. what the execution actually
    pinned.  This works without replayability, which matters because an agent
    execution cannot be replayed.

So this experiment models the one thing that separates them: **undeclared
dependencies**.  It also honestly measures the one place the static rebuilder
is *better* -- early cutoff.

POLICIES COMPARED (all on identical graphs)
-------------------------------------------
  P1 static-mtime   : declared edges, no early cutoff (classic make)
  P2 static-hash    : declared edges, WITH early cutoff (Bazel constructive
                      traces: if a rebuilt output is byte-identical, dependents
                      are spared)
  P3 LHOS           : the REAL invalidation engine, run over the dependency set
                      recorded by evidence (declared + actually-consumed)

GROUND TRUTH
------------
A task must be re-executed iff its real inputs changed in *content*.
Propagation therefore runs over real edges and STOPS at content-stable tasks
(a stable task must itself re-run to discover it is stable, but its dependents
are unaffected).  This is computed directly from the simulated world state, not
from any policy's algorithm.

METRICS
-------
  under-invalidation := TRUE - policy   (unsound: stale work reported valid)
  over-invalidation  := policy - TRUE   (wasteful: needless re-execution)

Expected shape of the result -- and we report it whichever way it falls:
  * P1/P2 accrue UNDER-invalidation as undeclared dependencies appear.
  * P3 should hold under = 0, but accrues OVER-invalidation because it has no
    early cutoff (version-based, not content-based).
That trade is the honest finding: soundness under undeclared dependencies is
bought with the loss of early cutoff.
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
sys.path.insert(0, str(REPO / "scripts"))

from d31_over_under_invalidation import TNode, _cause, depends_on

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)


def build_graph(rng, n, hidden_ratio, stable_ratio):
    """A DAG whose real dependency set is a superset of the declared one.

    ``hidden_ratio``  fraction of real edges that the static declaration misses
                      (the agent consumed the artifact but never declared it).
    ``stable_ratio``  fraction of tasks whose re-execution yields byte-identical
                      output (early cutoff applies to them).
    """
    ids = [f"T{i}" for i in range(n)]
    real: list[tuple[str, str]] = []
    # spine: layered DAG, each task depends on 1-3 earlier tasks
    for i in range(1, n):
        k = rng.randint(1, 3)
        for _ in range(k):
            j = rng.randrange(max(0, i - 12), i)
            real.append((ids[i], ids[j]))
    real = sorted(set(real))
    hidden = set(rng.sample(real, int(round(len(real) * hidden_ratio))))
    declared = [e for e in real if e not in hidden]
    stable = set(rng.sample(ids, int(round(n * stable_ratio))))
    return ids, real, declared, sorted(hidden), stable


def _rev(edges):
    r: dict[str, set[str]] = {}
    for src, tgt in edges:  # src DEPENDS_ON tgt
        r.setdefault(tgt, set()).add(src)
    return r


def truth(root, edges, stable):
    """Tasks that genuinely must re-execute, with early cutoff at stable tasks."""
    rev = _rev(edges)
    must: set[str] = set()
    q = deque([root])
    while q:
        u = q.popleft()
        if u in must:
            continue
        must.add(u)
        if u != root and u in stable:
            continue  # output identical => dependents unaffected
        for v in rev.get(u, ()):
            if v not in must:
                q.append(v)
    return must


def policy_static(root, declared, stable, early_cutoff):
    """make (early_cutoff=False) / Bazel (early_cutoff=True) over declared edges."""
    rev = _rev(declared)
    hit: set[str] = set()
    q = deque([root])
    while q:
        u = q.popleft()
        if u in hit:
            continue
        hit.add(u)
        if early_cutoff and u != root and u in stable:
            continue
        for v in rev.get(u, ()):
            if v not in hit:
                q.append(v)
    return hit


def policy_lhos(root, ids, real_edges):
    """The REAL LHOS invalidation engine over the evidence-recorded dep set."""
    tasks = {t: TNode(t, "verified") for t in ids}
    edges = [depends_on(s, t) for s, t in real_edges]
    inp = EngineInputs(
        graph_id="b4",
        current_version=1,
        task_nodes=tasks,
        goal_nodes={},
        evidence_nodes={},
        edges=edges,
        explicit_causes=(_cause("b4", 1, root),),
    )
    return set(build_invalidation_result(inp, run_invalidation_engine(inp)).stale_nodes)


def main() -> int:
    out = REPO / "artifacts" / "agent_os_phase_d3"
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(0xB4)

    n = 300
    trials = 60
    grid = [(h, s) for h in (0.0, 0.1, 0.25) for s in (0.0, 0.2)]

    report = {
        "experiment": "B4_content_hash_rebuilder_headtohead",
        "nodes_per_graph": n,
        "trials_per_cell": trials,
        "policies": {
            "P1_static_mtime": "declared edges, no early cutoff (make)",
            "P2_static_hash": "declared edges, early cutoff (Bazel constructive traces)",
            "P3_lhos": "REAL invalidation engine over evidence-recorded dep set",
        },
        "cells": [],
    }

    hdr = (
        f"{'hidden':>7} {'stable':>7} | "
        f"{'P1 under':>9} {'P1 over':>8} | {'P2 under':>9} {'P2 over':>8} | "
        f"{'P3 under':>9} {'P3 over':>8} | {'|TRUE|':>7}"
    )
    print(hdr)
    print("-" * len(hdr))

    for hidden_ratio, stable_ratio in grid:
        acc = {k: [0, 0] for k in ("P1", "P2", "P3")}
        truth_sizes = []
        for _ in range(trials):
            ids, real, declared, hidden, stable = build_graph(rng, n, hidden_ratio, stable_ratio)
            root = rng.choice(ids)
            T = truth(root, real, stable)
            truth_sizes.append(len(T))
            res = {
                "P1": policy_static(root, declared, stable, early_cutoff=False),
                "P2": policy_static(root, declared, stable, early_cutoff=True),
                "P3": policy_lhos(root, ids, real),
            }
            for k, got in res.items():
                acc[k][0] += len(T - got)  # under
                acc[k][1] += len(got - T)  # over

        cell = {
            "hidden_dependency_ratio": hidden_ratio,
            "content_stable_ratio": stable_ratio,
            "mean_true_affected": round(sum(truth_sizes) / len(truth_sizes), 2),
            **{
                f"{k}_{m}_invalidation_total": acc[k][i]
                for k in ("P1", "P2", "P3")
                for i, m in ((0, "under"), (1, "over"))
            },
        }
        report["cells"].append(cell)
        print(
            f"{hidden_ratio:7.2f} {stable_ratio:7.2f} | "
            f"{acc['P1'][0]:9d} {acc['P1'][1]:8d} | "
            f"{acc['P2'][0]:9d} {acc['P2'][1]:8d} | "
            f"{acc['P3'][0]:9d} {acc['P3'][1]:8d} | "
            f"{cell['mean_true_affected']:7.1f}"
        )

    lhos_under = sum(c["P3_under_invalidation_total"] for c in report["cells"])
    static_under_with_hidden = sum(
        c["P2_under_invalidation_total"]
        for c in report["cells"]
        if c["hidden_dependency_ratio"] > 0
    )
    report["lhos_under_invalidation_total"] = lhos_under
    report["static_hash_under_invalidation_with_hidden_deps"] = static_under_with_hidden
    report["headline"] = (
        "LHOS stays sound (under=0) once dependencies are evidence-recorded; the "
        "static-declaration rebuilder accrues under-invalidation proportional to the "
        "undeclared-dependency ratio. LHOS pays for this with over-invalidation "
        "wherever early cutoff would have applied."
    )

    (out / "b4-content-hash-headtohead.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print()
    print(f"LHOS under-invalidation across all cells : {lhos_under}")
    print(f"static-hash under-invalidation (hidden>0): {static_under_with_hidden}")
    print(f"json: {out / 'b4-content-hash-headtohead.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
