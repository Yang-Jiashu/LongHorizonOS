"""E6-motivation — does context-affinity scheduling have anything to gain?

WHY THIS MEASUREMENT EXISTS
---------------------------
The system's thesis is that one graph must serve BOTH roles: it decides what is
still true (closure) AND it decides what may run (dispatch).  The closure half is
implemented and measured.  The dispatch half is only half-built: the graph signs
a `ReadinessProof` whose four fields (lifecycle_ok / validity_ok /
all_deps_verified / has_execution_attempt) are ALL task-side.  Nothing in it says
*which agent would be cheaper*.

Consequence in code: `runtimes/multi_agent/matching.py` defines a
`LOCALITY_BONUS`, but the scheduler never supplies `exact_locality`, so the
bonus is dead.  The scheduler's ordering key is
`(-priority, topo_depth, created_in_version, node_id)` -- pure graph structure.

Before building the missing half (a residency-aware cost model), we must answer
an empirical question: **is there any locality to exploit at all?**  If each task
consumes a disjoint set of artifacts, affinity scheduling buys nothing and the
whole second half of the thesis is unmotivated.

WHAT IT MEASURES
----------------
From a real agent trace (`artifacts/stuck_recovery_debug_v3/llm-calls.jsonl`,
63 real model calls), for each node we take the set of artifacts it actually
read (recovered by `m11_undeclared_dependency_ratio.py`'s extraction logic) and
compute:

  * redundancy factor = sum(per-node reads) / distinct artifacts
        -> how many times the same artifacts get re-read across the run
  * consecutive-handoff overlap = |read(n_i) INTERSECT read(n_i+1)| / |read(n_i+1)|
        -> the fraction of what the next node needs that the previous node had
           already loaded, i.e. what a residency-aware scheduler could reuse
  * per-artifact sharing count
        -> artifacts consumed by more than one node, each of which is a
           dependency edge that does NOT exist in the graph

CAVEATS (reported, never hidden)
--------------------------------
  * ONE trace, ONE model (sensenova-6.7-flash-lite), ONE workspace, 5 nodes.
    This is existence-and-magnitude evidence, NOT a distribution.
  * 38 of 63 records have truncated bodies, so read sets are a LOWER bound.
  * "read" does not prove the content influenced the verification verdict.
  * Overlap is an upper bound on savings: eliminating a redundant *read* is not
    the same as eliminating the same fraction of tokens or wall time.  Turning
    this into a speedup number requires actually building the mechanism.
"""

# ruff: noqa
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

M11_JSON = REPO / "artifacts" / "agent_os_phase_d3" / "m11-undeclared-dependency-ratio.json"
WORKSPACE_PREFIX = re.compile(r"^.*?/workspace/?")


def normalize(path: str) -> str:
    """Strip absolute workspace prefixes so the same file compares equal.

    Necessary because some nodes emitted workspace-relative paths and others
    emitted absolute ones; without this the sets would never intersect and the
    measurement would falsely report zero locality.
    """
    p = WORKSPACE_PREFIX.sub("", path).strip("/")
    return p if p else "."


def load_read_sets() -> dict[str, set[str]]:
    if not M11_JSON.exists():
        raise SystemExit(
            f"missing {M11_JSON.relative_to(REPO)} -- run "
            "scripts/m11_undeclared_dependency_ratio.py first"
        )
    data = json.loads(M11_JSON.read_text(encoding="utf-8"))
    return {
        row["node_id"]: {normalize(a) for a in row["artifacts_read"]}
        for row in data["per_node"]
        if row["artifacts_read"]
    }, data


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3"
    out_dir.mkdir(parents=True, exist_ok=True)

    per_node, m11 = load_read_sets()
    names = sorted(per_node)

    total_reads = sum(len(v) for v in per_node.values())
    distinct = set().union(*per_node.values()) if per_node else set()
    redundancy = round(total_reads / len(distinct), 3) if distinct else None

    handoffs = []
    ov_sum = need_sum = 0
    for a, b in zip(names, names[1:]):
        overlap = per_node[a] & per_node[b]
        need = per_node[b]
        ov_sum += len(overlap)
        need_sum += len(need)
        handoffs.append(
            {
                "from": a,
                "to": b,
                "artifacts_needed": len(need),
                "already_resident_upstream": len(overlap),
                "reuse_fraction": round(len(overlap) / len(need), 3) if need else None,
                "reusable": sorted(overlap),
            }
        )

    share = Counter(a for v in per_node.values() for a in v)
    shared = {a: c for a, c in share.items() if c > 1}

    report = {
        "measurement": "E6_motivation_context_residency_reuse",
        "source_trace": m11.get("trace"),
        "model": m11.get("model"),
        "nodes": len(per_node),
        "records_with_truncated_body": m11.get("records_with_truncated_body"),
        "sum_of_per_node_reads": total_reads,
        "distinct_artifacts": len(distinct),
        "redundancy_factor": redundancy,
        "handoffs": handoffs,
        "aggregate_artifacts_needed_downstream": need_sum,
        "aggregate_already_resident": ov_sum,
        "aggregate_reuse_fraction": round(ov_sum / need_sum, 4) if need_sum else None,
        "artifacts_shared_across_nodes": dict(sorted(shared.items(), key=lambda kv: -kv[1])),
        "scheduler_ordering_key_today": "(-priority, topo_depth, created_in_version, node_id)",
        "residency_inputs_available_to_scheduler_today": 0,
        "dead_code_placeholder": "runtimes/multi_agent/matching.py LOCALITY_BONUS (exact_locality never supplied)",
        "caveats": [
            "one trace, one model, one workspace, 5 nodes -- existence and magnitude, not a distribution",
            "38/63 records truncated => read sets are a lower bound",
            "a read does not prove the content influenced the verification verdict",
            "reuse fraction is an UPPER bound on savings; token/latency gains require building the mechanism",
        ],
        "conclusion": (
            "There is substantial locality to exploit: the same artifacts are re-read "
            "several times over across the run, and most of what each next node needs "
            "was already loaded by its predecessor. The current dispatch justification "
            "carries none of this information."
        ),
    }
    (out_dir / "e6-context-residency-motivation.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(f"trace                  : {m11.get('trace')}  ({m11.get('model')})")
    print(f"nodes with reads       : {len(per_node)}")
    print(f"sum per-node reads     : {total_reads}")
    print(f"distinct artifacts     : {len(distinct)}")
    print(f"REDUNDANCY FACTOR      : {redundancy}x  (same artifacts re-read)")
    print()
    for h in handoffs:
        print(
            f"  {h['from'].split(':')[-1]:>4s} -> {h['to'].split(':')[-1]:<4s} "
            f"needs {h['artifacts_needed']:2d}, already resident {h['already_resident_upstream']:2d} "
            f"({(h['reuse_fraction'] or 0) * 100:5.1f}%)"
        )
    print()
    print(
        f"AGGREGATE REUSE        : {ov_sum}/{need_sum} = "
        f"{(report['aggregate_reuse_fraction'] or 0) * 100:.1f}% of downstream needs "
        f"already resident upstream"
    )
    print(f"artifacts read by >1 node: {len(shared)}")
    for a, c in sorted(shared.items(), key=lambda kv: -kv[1]):
        print(f"    {c} nodes: {a}")
    print()
    print(f"scheduler residency inputs today: 0  ({report['dead_code_placeholder']})")
    print(f"json: {out_dir / 'e6-context-residency-motivation.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
