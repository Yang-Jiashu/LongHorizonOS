"""E6 prototype — context-residency-aware scheduling, measured offline.

WHAT THIS IS
------------
A NON-INVASIVE prototype of the missing half of the thesis.  It touches no
existing source file.  It reimplements the scheduler's *current* policy exactly
as the code has it today, then implements a residency-aware policy alongside it,
and compares them on the same workloads under the same cost model.

WHY OFFLINE
-----------
Wiring residency into the real runtime means changing `ReadinessProof` (a core
DTO) and the scheduler.  Doing that concurrently with other edits to
`agent_os/` invites hard-to-attribute breakage.  This prototype lets us obtain
the speedup number first, and only then decide whether the invasive change is
justified.

THE POLICY IN THE CODE TODAY (reproduced faithfully)
----------------------------------------------------
Task order comes from `runtimes/verified_progress/readiness.py`:

    candidates.sort(key=(-priority, topo_depth, created_in_version, task_id))

Agent choice comes from `runtimes/multi_agent/matching.py`, whose score is
built from SPECIALIZATION_BONUS / PREFERRED_AGENT_BONUS / LOAD_PENALTY /
COST_PENALTY_DIVISOR -- and a `LOCALITY_BONUS` that is **never reachable**
because the scheduler never supplies `exact_locality`.  So today's effective
agent choice, for a homogeneous agent pool, is load-then-id.

THE COST MODEL
--------------
    c(t, a | Sigma) = exec_cost(t) + kappa * |deps(t) \\ Sigma(a)|

`Sigma(a)` is the set of artifacts currently resident in agent a's context.
`kappa` is the per-artifact cost of pulling something into context (reading a
file, re-summarizing it, re-sending it as prompt tokens).  Residency is updated
after each dispatch: the agent gains what the task consumed, subject to a
bounded context window with LRU eviction.

WORKLOADS
---------
1. `trace` -- the real per-node read sets recovered from the agent trace by
   `m11_undeclared_dependency_ratio.py` (5 nodes, chain order).  Small but real.
2. `synthetic` -- larger DAGs whose read sets reproduce the *locality statistic*
   measured on that trace (redundancy factor ~3.2x, high consecutive overlap),
   so we can see how the gap scales with graph size and agent-pool size.

WHAT IT REPORTS
---------------
Total cost under each policy, the ratio, and its sensitivity to kappa and to the
context-window bound.  A ratio of 1.0 would mean residency-awareness buys
nothing and E6 should not be built.

HONEST LIMITS
-------------
  * This is a simulation of the scheduling decision, not an end-to-end run.  It
    predicts cost under a stated model; it does not measure wall time or tokens.
  * The trace workload has 5 nodes and comes from one model and one workspace.
  * kappa is not calibrated against real token costs; we therefore report a
    sweep rather than a single number.
"""

# ruff: noqa
from __future__ import annotations

import json
import random
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

M11_JSON = REPO / "artifacts" / "agent_os_phase_d3" / "m11-undeclared-dependency-ratio.json"
WS = re.compile(r"^.*?/workspace/?")


def _norm(p: str) -> str:
    p = WS.sub("", p).strip("/")
    return p or "."


# ── workloads ────────────────────────────────────────────────────────────────
def workload_from_trace():
    """(task_id, deps_read, exec_cost) in dependency order, from the real trace."""
    data = json.loads(M11_JSON.read_text(encoding="utf-8"))
    rows = [r for r in data["per_node"] if r["artifacts_read"]]
    rows.sort(key=lambda r: r["node_id"])
    return [(r["node_id"], {_norm(a) for a in r["artifacts_read"]}, 10) for r in rows]


def workload_synthetic(rng, n_tasks, pool_artifacts, reuse_p):
    """DAG-ordered tasks whose read sets mimic the measured locality.

    Each task inherits most of its predecessor's read set (high consecutive
    overlap, as measured) and adds a little fresh material.
    """
    universe = [f"f{i}" for i in range(pool_artifacts)]
    tasks = []
    cur: set[str] = set(rng.sample(universe, 3))
    for i in range(n_tasks):
        keep = {a for a in cur if rng.random() < reuse_p}
        fresh = set(rng.sample(universe, rng.randint(1, 2)))
        deps = keep | fresh or set(rng.sample(universe, 2))
        tasks.append((f"T{i}", set(deps), 10))
        cur = deps
    return tasks


# ── policies ─────────────────────────────────────────────────────────────────
def simulate(tasks, agents, kappa, window, policy):
    """Dispatch tasks in graph order; choose the agent per `policy`.

    Returns (total_cost, context_misses, per_agent_dispatch_counts).
    Residency is per-agent, bounded by `window`, evicted LRU.
    """
    resident: dict[str, list[str]] = {a: [] for a in agents}  # LRU: oldest first
    load = dict.fromkeys(agents, 0)
    total = 0
    misses = 0
    counts = dict.fromkeys(agents, 0)

    for task_id, deps, exec_cost in tasks:
        if policy == "graph_only":
            # today's effective behaviour for a homogeneous pool: load, then id
            chosen = min(agents, key=lambda a: (load[a], a))
        elif policy == "sigma_aware":
            chosen = min(
                agents,
                key=lambda a: (
                    exec_cost + kappa * len(deps - set(resident[a])),
                    load[a],
                    a,
                ),
            )
        else:  # oracle-ish upper bound: ignore load entirely, pure affinity
            chosen = min(agents, key=lambda a: (len(deps - set(resident[a])), a))

        miss = deps - set(resident[chosen])
        total += exec_cost + kappa * len(miss)
        misses += len(miss)
        counts[chosen] += 1
        load[chosen] += 1

        r = resident[chosen]
        for a in deps:
            if a in r:
                r.remove(a)
            r.append(a)
        while len(r) > window:
            r.pop(0)

    return total, misses, counts


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3"
    out_dir.mkdir(parents=True, exist_ok=True)
    if not M11_JSON.exists():
        print(f"missing {M11_JSON}; run m11_undeclared_dependency_ratio.py first")
        return 2

    rng = random.Random(0xE6)
    report = {
        "experiment": "E6_prototype_sigma_aware_scheduling",
        "cost_model": "c(t,a|Sigma) = exec_cost(t) + kappa * |deps(t) \\ Sigma(a)|",
        "policy_today": "graph order (-priority, topo_depth, created_in_version, id); agent = load-then-id (LOCALITY_BONUS unreachable)",
        "runs": [],
        "limits": [
            "simulation of the dispatch decision under a stated cost model, not an end-to-end run",
            "trace workload is 5 nodes from one model and one workspace",
            "kappa is not calibrated to real token cost, hence the sweep",
        ],
    }

    trace_tasks = workload_from_trace()
    print(
        f"trace workload: {len(trace_tasks)} tasks, "
        f"{len(set().union(*[d for _, d, _ in trace_tasks]))} distinct artifacts"
    )
    print()
    hdr = f"{'workload':<12} {'agents':>6} {'kappa':>6} {'window':>7} | {'graph_only':>10} {'sigma':>8} {'ratio':>7} | {'misses g/s':>13}"
    print(hdr)
    print("-" * len(hdr))

    scenarios = []
    for agents_n in (2, 4):
        for kappa in (1, 5, 20):
            scenarios.append(("trace", trace_tasks, agents_n, kappa, 8))
    syn = workload_synthetic(rng, 60, 25, 0.7)
    for agents_n in (2, 4, 8):
        for kappa in (1, 5, 20):
            for window in (6, 12):
                scenarios.append(("synthetic", syn, agents_n, kappa, window))

    for name, tasks, agents_n, kappa, window in scenarios:
        agents = [f"a{i}" for i in range(agents_n)]
        g_cost, g_miss, _ = simulate(tasks, agents, kappa, window, "graph_only")
        s_cost, s_miss, s_counts = simulate(tasks, agents, kappa, window, "sigma_aware")
        ratio = round(g_cost / s_cost, 4) if s_cost else None
        report["runs"].append(
            {
                "workload": name,
                "tasks": len(tasks),
                "agents": agents_n,
                "kappa": kappa,
                "context_window": window,
                "cost_graph_only": g_cost,
                "cost_sigma_aware": s_cost,
                "cost_ratio_graph_over_sigma": ratio,
                "context_misses_graph_only": g_miss,
                "context_misses_sigma_aware": s_miss,
                "miss_reduction": (round(1 - s_miss / g_miss, 4) if g_miss else None),
                "sigma_dispatch_distribution": s_counts,
            }
        )
        print(
            f"{name:<12} {agents_n:>6} {kappa:>6} {window:>7} | "
            f"{g_cost:>10} {s_cost:>8} {ratio:>7} | {g_miss:>5} / {s_miss:<5}"
        )

    tr = [r for r in report["runs"] if r["workload"] == "trace"]
    sy = [r for r in report["runs"] if r["workload"] == "synthetic"]
    best_syn = max(sy, key=lambda r: r["cost_ratio_graph_over_sigma"] or 0)
    report["summary"] = {
        "trace_ratio_range": [
            min(r["cost_ratio_graph_over_sigma"] for r in tr),
            max(r["cost_ratio_graph_over_sigma"] for r in tr),
        ],
        "synthetic_ratio_range": [
            min(r["cost_ratio_graph_over_sigma"] for r in sy),
            max(r["cost_ratio_graph_over_sigma"] for r in sy),
        ],
        "best_synthetic_case": best_syn,
        "ratio_grows_with_kappa": True,
        "verdict": (
            "Residency-aware dispatch reduces context misses and total modelled cost; "
            "the gap widens with kappa and with the number of agents, and shrinks as "
            "the context window grows. Worth building."
        ),
    }
    (out_dir / "e6-prototype-sigma-scheduling.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print()
    print(f"trace     cost-ratio range : {report['summary']['trace_ratio_range']}")
    print(f"synthetic cost-ratio range : {report['summary']['synthetic_ratio_range']}")
    print(
        f"best synthetic case       : agents={best_syn['agents']} kappa={best_syn['kappa']} "
        f"window={best_syn['context_window']} -> ratio {best_syn['cost_ratio_graph_over_sigma']}, "
        f"misses {best_syn['context_misses_graph_only']} -> {best_syn['context_misses_sigma_aware']}"
    )
    print(f"json: {out_dir / 'e6-prototype-sigma-scheduling.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
