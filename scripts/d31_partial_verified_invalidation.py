"""Partial-verification over-/under-invalidation audit (coverage gap closure).

WHY THIS EXISTS
---------------
``d31_over_under_invalidation.py`` builds every 1000-task graph with **all
tasks VERIFIED** (``TNode(tid, "verified")``) before seeding invalidation.  In
that regime the D3 propagation rule

    propagate to a dependent ONLY IF that dependent is currently VERIFIED
    (src/lhos/runtimes/invalidation/cone.py, "only invalidate VERIFIED
    dependents -> preserve UNVERIFIED/others")

is trivially satisfied for every dependent, so the reference algorithm
(unconditional reverse reachability) and the implementation **cannot disagree**.
The 0/0 result is therefore correct but obtained on inputs that cannot
discriminate the VERIFIED gate.

Long-horizon runs are the opposite case: the world changes while the graph is
only partially closed.  This audit exercises exactly that regime.

SEMANTICS UNDER TEST
--------------------
Invalidation means "a conclusion that WAS true is now false".  A task that
never concluded anything cannot be invalidated -- what changes for an
UNVERIFIED dependent is its *readiness*, not its *validity*.  The authoritative
expected-affected set is therefore the reverse-reachability closure from the
seed **restricted to propagation through VERIFIED dependents**:

    REF_gated(seed) = {seed} u {t : t reaches seed via DEPENDS_ON using only
                                   VERIFIED intermediate dependents}

We additionally compute the *ungated* closure used by the all-verified audit
and report how often the two differ.  That difference is the discriminating
power this fixture adds: if it were always zero, the new fixture would be as
vacuous as the old one.

METRICS (all must be ZERO)
--------------------------
  over-invalidation  := D3 stale but not in REF_gated
  under-invalidation := in REF_gated, currently VERIFIED, but D3 left it alone

Exit 0 iff both are zero across all trials AND the fixture is shown to be
discriminating (ungated-vs-gated differences > 0).
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

from d31_over_under_invalidation import (  # reuse the identical graph generator
    TNode,
    _cause,
    build_large_dag,
)

from lhos.runtimes.invalidation.engine import (
    EngineInputs,
    build_invalidation_result,
    run_invalidation_engine,
)

VERIFIED = "verified"
UNVERIFIED = "unverified"


def _reverse_deps(task_ids, edges):
    rev = {tid: set() for tid in task_ids}
    for e in edges:
        if e.edge_type.value == "depends_on":
            rev[e.target_node_id].add(e.source_node_id)
    return rev


def reference_gated(seed, tasks, edges):
    """Independent expected-affected set, gated on VERIFIED dependents.

    Deliberately written as a plain BFS over the raw edge list with no import
    from lhos.runtimes.invalidation -- it must stay auditable by inspection.
    """
    rev = _reverse_deps(list(tasks), edges)
    affected = set()
    q = deque([seed])
    while q:
        u = q.popleft()
        if u in affected:
            continue
        affected.add(u)
        for v in rev.get(u, ()):
            if v in affected:
                continue
            if tasks[v].validity.value == VERIFIED:
                q.append(v)
    return affected


def reference_ungated(seed, tasks, edges):
    """The all-verified audit's reference: unconditional reverse reachability."""
    rev = _reverse_deps(list(tasks), edges)
    affected = set()
    q = deque([seed])
    while q:
        u = q.popleft()
        if u in affected:
            continue
        affected.add(u)
        q.extend(rev.get(u, ()))
    return affected


def assign_partial(ids, rng, verified_fraction):
    """Mark a random subset VERIFIED, the rest UNVERIFIED (work in progress)."""
    verified = set(rng.sample(ids, int(round(len(ids) * verified_fraction))))
    return {tid: TNode(tid, VERIFIED if tid in verified else UNVERIFIED) for tid in ids}


def run_engine(tasks, edges, seed):
    inp = EngineInputs(
        graph_id="gpart",
        current_version=1,
        task_nodes=tasks,
        goal_nodes={},
        evidence_nodes={},
        edges=edges,
        explicit_causes=(_cause("gpart", 1, seed),),
    )
    res = build_invalidation_result(inp, run_invalidation_engine(inp))
    return set(res.stale_nodes)


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3"
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(0xD31FA)

    graphs = [build_large_dag(rng, 1000) for _ in range(8)]
    fractions = (0.3, 0.6, 0.9)
    trials_per_fraction = 200

    report = {
        "audit": "partial_verification_over_under_invalidation",
        "rationale": (
            "d31_over_under_invalidation.py runs with every task VERIFIED, a regime "
            "in which the VERIFIED-gated propagation rule cannot be distinguished "
            "from unconditional reverse reachability. This audit mutates the world "
            "while the graph is only partially closed."
        ),
        "graphs": len(graphs),
        "nodes_per_graph": 1000,
        "verified_fractions": list(fractions),
        "trials_per_fraction": trials_per_fraction,
        "by_fraction": {},
    }

    total_over = total_under = 0
    total_trials = 0
    total_discriminating = 0

    for frac in fractions:
        over = under = discriminating = 0
        gated_sizes: list[int] = []
        ungated_sizes: list[int] = []
        for _ in range(trials_per_fraction):
            g = rng.choice(graphs)
            tasks = assign_partial(g["ids"], rng, frac)
            seed = rng.choice(g["ids"])

            exp_gated = reference_gated(seed, tasks, g["edges"])
            exp_ungated = reference_ungated(seed, tasks, g["edges"])
            affected = run_engine(tasks, g["edges"], seed)

            over += len(affected - exp_gated)
            under += len(
                {
                    t
                    for t in (exp_gated - {seed})
                    if t not in affected and tasks[t].validity.value == VERIFIED
                }
            )
            if exp_gated != exp_ungated:
                discriminating += 1
            gated_sizes.append(len(exp_gated))
            ungated_sizes.append(len(exp_ungated))
            total_trials += 1

        total_over += over
        total_under += under
        total_discriminating += discriminating
        report["by_fraction"][str(frac)] = {
            "trials": trials_per_fraction,
            "over_invalidation": over,
            "under_invalidation": under,
            "trials_where_gated_differs_from_ungated": discriminating,
            "mean_gated_affected": round(sum(gated_sizes) / len(gated_sizes), 2),
            "mean_ungated_affected": round(sum(ungated_sizes) / len(ungated_sizes), 2),
        }
        print(
            f"verified_fraction={frac:<4} trials={trials_per_fraction:<4} "
            f"over={over:<4} under={under:<4} "
            f"discriminating={discriminating}/{trials_per_fraction} "
            f"mean_affected gated={report['by_fraction'][str(frac)]['mean_gated_affected']} "
            f"ungated={report['by_fraction'][str(frac)]['mean_ungated_affected']}"
        )

    report["total_trials"] = total_trials
    report["over_invalidation_total"] = total_over
    report["under_invalidation_total"] = total_under
    report["discriminating_trials_total"] = total_discriminating
    correct = total_over == 0 and total_under == 0
    discriminates = total_discriminating > 0
    report["correct"] = correct
    report["fixture_discriminates"] = discriminates

    (out_dir / "partial-verification-invalidation.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

    print()
    print(f"TOTAL trials={total_trials} over={total_over} under={total_under}")
    print(
        f"fixture discriminates in {total_discriminating}/{total_trials} trials "
        f"(gated != ungated) -> {'NON-VACUOUS' if discriminates else 'VACUOUS'}"
    )
    print(f"result: {'PASS' if correct and discriminates else 'FAIL'}")
    print(f"json: {out_dir / 'partial-verification-invalidation.json'}")
    return 0 if (correct and discriminates) else 2


if __name__ == "__main__":
    sys.exit(main())
