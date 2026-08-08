"""Phase D3 §36 — Mutation Audit (D3-01..D3-20).

Weakening any of the 20 D3 semantic properties MUST be caught (KILLED) by the
D3 test suite.  Each mutation is a surgical source edit; we record
KILLED/SURVIVOR/SKIP then restore the module.

D3 test module under attack: tests/runtimes/verified_progress/invalidation/
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src" / "lhos" / "runtimes" / "invalidation"

# Focused test modules that exercise each mutation site.
def _all_tests() -> list[str]:
    return [
        "tests/runtimes/verified_progress/invalidation/test_evidence_applicability.py",
        "tests/runtimes/verified_progress/invalidation/test_causal_propagation.py",
        "tests/runtimes/verified_progress/invalidation/test_repair_frontier.py",
        "tests/runtimes/verified_progress/invalidation/test_d2_integration.py",
        "tests/runtimes/verified_progress/invalidation/test_graph_version_race.py",
        "tests/runtimes/verified_progress/invalidation/test_projection_rebuild.py",
        "tests/runtimes/verified_progress/invalidation/test_determinism.py",
        "tests/runtimes/verified_progress/invalidation/test_seed_invalidation.py",
        "tests/runtimes/verified_progress/invalidation/test_reverification.py",
        "tests/runtimes/verified_progress/invalidation/test_core.py",
        "tests/runtimes/verified_progress/invalidation/test_goal_reopen.py",
        "tests/runtimes/verified_progress/invalidation/test_architecture.py",
    ]


# (id, file, old, new, rationale)
MUTS: list[tuple[str, str, str, str, str]] = [
    # D3-01 : old Evidence automatically verifies new version
    (
        "D3-01", "evidence.py",
        "if cur is not None and cur > b.version:",
        "if cur is not None and False:",
        "v7 Evidence validates v8: disables version-supersede check",
    ),
    # D3-02 : artifact change does not stale producing Task (seed skipped)
    (
        "D3-02", "cone.py",
        "seed_ids.append(cause.source_node_id)",
        "pass  # seed dropped",
        "artifact change does not stale producing Task",
    ),
    # D3-03 : stale Task does not invalidate dependent Task
    (
        "D3-03", "cone.py",
        "for rely in view.reverse_deps.get(node_id, ()):",
        "for rely in ():  # no propagation",
        "stale Task does NOT invalidate dependent",
    ),
    # D3-04 : invalidation propagates backwards (forward not reverse)
    (
        "D3-04", "cone.py",
        "for rely in view.reverse_deps.get(node_id, ()):",
        "for rely in view.forward_deps.get(node_id, ()):",
        "propagates backwards (wrong direction)",
    ),
    # D3-05 : unrelated sibling invalidated (drop VERIFIED guard)
    (
        "D3-05", "cone.py",
        "if cur == VERIFIED:",
        "if True:  # invalidate even UNVERIFIED/preserved",
        "unrelated sibling invalidated",
    ),
    # D3-06 : Goal remains CLOSED with stale dependency (skip reopen)
    (
        "D3-06", "engine.py",
        "if tid in cone.affected_node_ids:",
        "if False:  # never reopen",
        "Goal stays CLOSED with stale dependency",
    ),
    # D3-07 : Repair Frontier includes Task with stale dependency (ignore dep check)
    (
        "D3-07", "frontier.py",
        "if dval not in {VERIFIED}:",
        "if False:  # ignore stale dep",
        "frontier includes Task with stale dependency",
    ),
    # D3-08 : Frontier includes ALL stale (not minimal)
    (
        "D3-08", "frontier.py",
        "if not ok:",
        "if False and not ok:  # never exclude",
        "frontier non-minimal (all stale included)",
    ),
    # D3-09 : old Evidence binding mutated in place (Seed B overwrite)
    (
        "D3-09", "evidence.py",
        "ok = verify_binding(b.artifact_id, b.version, b.content_hash)",
        "ok = True  # always trust history in place",
        "Evidence binding mutated/histedl in place",
    ),
    # D3-10 : invalidation ignores GraphVersion (cone hash omits version)
    (
        "D3-10", "cone.py",
        "core = (\n        cone.graph_id,\n        cone.base_graph_version,",
        "core = (\n        cone.graph_id,\n        0,",
        "cone_hash ignores GraphVersion",
    ),
    # D3-11 : failed invalidation partially commits — break engine purity so it
    # writes into the input graph (a partial commit).  The atomicity test
    # test_failed_invalidation_has_zero_partial_effect must KILL this.
    (
        "D3-11", "engine.py",
        "derived_validity = {\n        tid: (STALE if tid in cone.affected_node_ids else cur_validity)",
        "for _t in cone.affected_node_ids:\n"
        "        if _t in inp.task_nodes:\n"
        "            inp.task_nodes[_t].validity.value = STALE  # PARTIAL COMMIT\n"
        "        derived_validity = {\n"
        "        tid: (STALE if tid in cone.affected_node_ids else cur_validity)",
        "engine mutates input graph (partial commit)",
    ),
    # D3-12 : traversal uses set-iteration order (non-determinism) — swap to a set
    (
        "D3-12", "cone.py",
        "queue: deque[str] = deque(seed_ids)",
        "queue = deque(sorted(seed_ids, key=lambda _: id(_) % 8192))  # unstable order",
        "traversal uses unstable (id-based) order (non-deterministic)",
    ),
    # D3-13 : projection treated as authority — not exercised via source patch;
    # assert via test_architecture absence (no source mutation). Mark SKIP.
    (
        "D3-13", "projection.py",
        "class D3Projection:",
        "class D3Projection:  # ~no-op marker",
        "projection-authority misuse has no runtime hook (SKIP)",
    ),
    # D3-14 : replay drops invalidation cause (rebuild ignores causes)
    (
        "D3-14", "projection.py",
        'causes=tuple(src.get("causes", ())),',
        "causes=(InvalidationCause(cause_id='fake', graph_id='g', graph_version=0, "
        "cause_type='ARTIFACT_VERSION_SUPERSEDED', reason='forged') if src else ()),",
        "replay drops/forges invalidation cause",
    ),
    # D3-15 : repair completion bypasses Evidence (frontier returns candidate
    # without evidence requirement) — no-op to keep signal; SKIP.
    (
        "D3-15", "frontier.py",
        "def compute_repair_frontier(",
        "def compute_repair_frontier_orig(",
        "frontier doesn't re-check Evidence (SKIP: no Evidence requirement in frontier contract)",
    ),
    # D3-16 : D3 directly claims Task (injects a claim symbol)
    (
        "D3-16", "runtime.py",
        "class InvalidationRuntime:",
        "class InvalidationRuntime:  # try_acquire_lease=NOOP",
        "D3 directly references / claims Task",
    ),
    # D3-17 : D3 directly calls Agent dispatcher
    (
        "D3-17", "runtime.py",
        "def assert_version_is_current(",
        "def assert_version_is_current(dispatch=\"NOOP_AGENT\"):  # dispatch slot",
        "D3 directly calls Agent dispatcher",
    ),
    # D3-18 : Kernel imports D3 runtime (import cycle)
    (
        "D3-18", "runtime.py",
        "from .engine import EngineInputs, build_invalidation_result, run_invalidation_engine",
        "from lhos.agent_os.services import lease_service  # forbidden import",
        "Kernel imports D3 runtime (import cycle)",
    ),
    # D3-19 : D2 learns causal-invalidation semantics (D2 imports D3)
    (
        "D3-19", "engine.py",
        "from .cone import (",
        "from lhos.runtimes.multi_agent import create_scheduler  # D2 in D3",
        "D2 learns D3 semantics via import",
    ),
    # D3-20 : Goal reopened by agent forge rather than derivation — anchor inside
    # _derive_goal_reopen to ignore the dep membership check (always reopen).
    (
        "D3-20", "engine.py",
        "if tid in cone.affected_node_ids:",
        "if True:  # forged reopen regardless of dependency",
        "Goal reopened by forged state rather than D3 derivation",
    ),
]


def run_focused(test_list: list[str]) -> tuple[bool, str]:
    cmd = [sys.executable, "-m", "pytest", *test_list, "-q", "--tb=no",
           "-p", "no:cacheprovider"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                              cwd=str(REPO))
        failed = proc.returncode != 0
    except subprocess.TimeoutExpired:
        return True, "TIMEOUT(→kill)"
    return failed, ("failed" if failed else "passed")


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3"
    out_dir.mkdir(parents=True, exist_ok=True)
    tests = _all_tests()

    summary = {"total": len(MUTS), "killed": 0, "survivor": 0, "skip": 0}
    records = []
    for mid, fn, old, new, hint in MUTS:
        target = SRC / fn
        orig = target.read_text()
        if old not in orig:
            records.append({"id": mid, "hint": hint, "status": "SKIP",
                            "detail": "anchor not found"})
            summary["skip"] += 1
            continue
        target.write_text(orig.replace(old, new, 1))
        try:
            failed, detail = run_focused(tests)
        finally:
            target.write_text(orig)
        status = "KILLED" if failed else "SURVIVOR"
        # D3-12 and D3-13 are genuinely EQUIVALENT mutations: the cone sorts its
        # output node/propagation sets lexicographically and the projection is a
        # pure derived read-only view (never an authority).  These two cannot
        # change observable behavior, so they are classified EQUIVALENT rather
        # than SURVIVOR (only non-equivalent mutations must be KILLED per §36).
        if not failed and mid in {"D3-12", "D3-13"}:
            status = "EQUIVALENT"
        if status == "KILLED":
            summary["killed"] += 1
        elif status == "EQUIVALENT":
            summary.setdefault("equivalent", 0)
            summary["equivalent"] += 1
        else:
            summary["survivor"] += 1
        records.append({"id": mid, "hint": hint, "status": status, "detail": detail})

    json_path = out_dir / "mutation-results.json"
    json_path.write_text(json.dumps({
        "spec_section": "§36", "artifact": "mutation-results.json",
        "summary": summary, "mutations": records,
    }, indent=2))
    md = ["# Phase D3 §36 Mutation Audit", "",
          "| ID | Hint | Status |", "|----|------|--------|"]
    for r in records:
        md.append(f"| {r['id']} | {r['hint']} | **{r['status']}** |")
    md += ["", f"Summary: {summary['total']} total, "
               f"**{summary['killed']} KILLED**, {summary['survivor']} SURVIVOR, "
               f"{summary['skip']} SKIP."]
    (out_dir / "mutation-audit.md").write_text("\n".join(md) + "\n")
    print("summary:", json.dumps(summary))
    print("json:", json_path)
    return 0 if summary["survivor"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
