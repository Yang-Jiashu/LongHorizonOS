"""Decisive test: is semantic state preserved ACROSS a crash-recovery rebuild?

BACKGROUND
----------
`rebuild_projection` is the crash-recovery path: drop the materialized
projection, replay the committed patch history, re-run admission, re-derive
VERIFIED/CLOSED.  A probe showed two differences between the pre-rebuild and
post-rebuild projections:

  (a) `created_at` / `produced_at` are regenerated.  This is a DESIGN CHOICE, not
      a defect: patches record *operations*, and the original commit timestamp is
      not part of an operation, so replay cannot recover it.  It is operational
      metadata, not semantic state.

  (b) `metadata["__verified_artifact_versions"]` was absent from the rebuild's
      return value.  That IS semantic state -- it records which exact artifact
      versions a task was verified against, and `_recompute_derived_state`
      compares it against the currently pinned versions to decide staleness.

Reading the code cannot settle (b), because `rebuild_projection` is passed
`facts_artifact` / `facts_kernel` and re-runs verification derivation, so the
field may simply be re-derived later rather than lost.

THIS TEST SETTLES IT BEHAVIOURALLY
----------------------------------
Two arms on identical goals:

  ARM A (control)  : close the goal, bump an artifact version, check invalidation
  ARM B (recovered): close the goal, REBUILD THE PROJECTION, then bump the same
                     artifact version, check invalidation

If ARM B detects the same staleness as ARM A, the field is re-derived and there
is no defect.  If ARM B fails to invalidate, the rebuild loses information that
crash recovery needs, and the defect is real and correctness-affecting.

We also report whether the field is present in the stored projection after a
rebuild + a subsequent derivation pass.
"""

# ruff: noqa
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from lhos.sdk import Agent, AgentOS, Goal
from lhos.sdk.verification import VerificationOutcome

N = 6
ARTIFACT = "shared.txt"


def _versioned_executor(versions: dict[str, int], artifact_id: str):
    def _run() -> VerificationOutcome:
        v = versions[artifact_id]
        return VerificationOutcome(
            passed=True,
            artifact_id=artifact_id,
            version=v,
            content=f"{artifact_id}-v{v}",
            evidence_note="i6-decisive",
        )

    return _run


def build_and_close():
    """Chain of N tasks; the FIRST task pins the artifact we will later bump."""
    rt = AgentOS(":memory:")
    rt.add_agent(Agent("w", specializations=("python",), max_concurrency=4))
    g = Goal("I6")
    versions = {ARTIFACT: 1}
    prev = None
    for i in range(N):
        art = ARTIFACT if i == 0 else f"a{i}.txt"
        versions.setdefault(art, 1)
        prev = g.task(
            f"t{i}",
            agent="w",
            depends_on=(prev,) if prev else (),
            verify=_versioned_executor(versions, art),
        )
    res = rt.run(g, max_dispatches=N * 6 + 40, max_steps=N * 6 + 40)
    return rt, g, versions, res


def graph_id_of(rt):
    return rt.vpg.store.conn.execute("SELECT graph_id FROM graphs").fetchone()[0]


def verified_versions_present(rt, gid):
    """How many task nodes carry __verified_artifact_versions in the STORE."""
    nodes, _ = rt.vpg.snapshot_projection(gid)
    total = present = 0
    for n in nodes.values():
        if getattr(n, "node_type", None) is None:
            continue
        if str(getattr(n.node_type, "value", n.node_type)) != "task":
            continue
        total += 1
        if (n.metadata or {}).get("__verified_artifact_versions"):
            present += 1
    return present, total


def arm(recover: bool):
    rt, g, versions, initial = build_and_close()
    gid = graph_id_of(rt)
    before_present, before_total = verified_versions_present(rt, gid)

    rebuilt_present = rebuilt_total = None
    if recover:
        rt.vpg.rebuild_projection(gid)
        rebuilt_present, rebuilt_total = verified_versions_present(rt, gid)

    status_pre = rt.status(g)
    versions[ARTIFACT] = 2
    repair = rt.repair(g, artifact_id=ARTIFACT, new_artifact_version=2)
    status_post = rt.status(g)

    final = rt.run(g, max_dispatches=N * 6 + 40, max_steps=N * 6 + 40)

    return {
        "recovered": recover,
        "initial_goal_closed": initial.goal_state == "closed",
        "verified_versions_present_before": [before_present, before_total],
        "verified_versions_present_after_rebuild": (
            [rebuilt_present, rebuilt_total] if recover else None
        ),
        "verified_before_bump": len(status_pre.verified),
        "affected": sorted(repair.affected),
        "preserved": sorted(repair.preserved),
        "frontier": sorted(repair.frontier),
        "verified_after_bump": len(status_post.verified),
        "false_verified_after_bump": len(set(repair.affected) & set(status_post.verified)),
        "final_goal_closed": final.goal_state == "closed",
    }


def main() -> int:
    out = REPO / "artifacts" / "agent_os_phase_d3"
    out.mkdir(parents=True, exist_ok=True)

    a = arm(recover=False)
    b = arm(recover=True)

    same_affected = a["affected"] == b["affected"]
    same_frontier = a["frontier"] == b["frontier"]
    b_detects = len(b["affected"]) > 0
    verdict = (
        "NO DEFECT - staleness re-derived after rebuild"
        if (same_affected and same_frontier and b_detects)
        else "DEFECT - rebuild loses information needed for invalidation"
    )

    report = {
        "test": "I6_semantic_state_survives_crash_recovery_rebuild",
        "control_arm": a,
        "recovered_arm": b,
        "affected_sets_identical": same_affected,
        "frontier_sets_identical": same_frontier,
        "recovered_arm_detected_staleness": b_detects,
        "verdict": verdict,
    }
    (out / "i6-rebuild-semantic-preservation.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

    for label, arm_res in (("CONTROL   ", a), ("RECOVERED ", b)):
        print(
            f"{label} closed={arm_res['initial_goal_closed']} "
            f"verified_before={arm_res['verified_before_bump']} "
            f"affected={arm_res['affected']} "
            f"frontier={arm_res['frontier']} "
            f"false_verified={arm_res['false_verified_after_bump']} "
            f"reclosed={arm_res['final_goal_closed']}"
        )
    print()
    print(
        f"__verified_artifact_versions in store, control : {a['verified_versions_present_before']}"
    )
    print(
        f"__verified_artifact_versions in store, after rebuild: "
        f"{b['verified_versions_present_after_rebuild']}"
    )
    print()
    print(f"affected identical : {same_affected}")
    print(f"frontier identical : {same_frontier}")
    print(f"VERDICT            : {verdict}")
    print(f"json: {out / 'i6-rebuild-semantic-preservation.json'}")
    return 0 if same_affected and same_frontier and b_detects else 2


if __name__ == "__main__":
    sys.exit(main())
