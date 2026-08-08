"""Phase D2.1 §7 Multi-Task Concurrency Stress.

Runs the D2 multi-task concurrency stress audit:
  - 100 tasks x 20 agents (mixed capacity 5x1, 5x3, 5x8, 5x25 = 185 max concurrent)
  - 100 scheduling rounds per trial
  - REAL AgentKernel + REAL VerifiedProgressRuntime (shared temp-file SQLite DB)
  - 3 trials with different PYTHONHASHSEED (unset / 42 / 123456)

Gates (per trial, per round; G5 across trials):
  G1: capacity respected (per-round active claims <= max_concurrency)
  G2: unique ACTIVE claim per task_id
  G3: every task eventually claimed
  G4: kernel lease exclusivity (one live lease per resource)
  G5: deterministic shape across PYTHONHASHSEED values

Writes:
  artifacts/agent_os_phase_d2_audit/multi-task-concurrency-stress.json
  artifacts/agent_os_phase_d2_audit/multi-task-concurrency-stress.md
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

OUT_DIR = REPO / "artifacts" / "agent_os_phase_d2_audit"
N_TASKS = 100
N_ROUNDS = 100
SEEDS: list[tuple[str, str | None]] = [
    ("unset", None),
    ("42", "42"),
    ("123456", "123456"),
]

# 20 agents: 5x[1, 3, 8, 25] = 185 total capacity
AGENTS_SPEC = (
    [(f"agent-{i:02d}", 1) for i in range(5)]
    + [(f"agent-{i:02d}", 3) for i in range(5, 10)]
    + [(f"agent-{i:02d}", 8) for i in range(10, 15)]
    + [(f"agent-{i:02d}", 25) for i in range(15, 20)]
)


# ── inner-script template ─────────────────────────────────────────────────
# Runs INSIDE a subprocess with PYTHONHASHSEED set by the orchestrator.
_INNER = r'''
import json
import sys
from datetime import timedelta

DB_PATH = sys.argv[1]
N_TASKS = int(sys.argv[2])
N_ROUNDS = int(sys.argv[3])
AGENT_SPEC_RAW = json.loads(sys.argv[4])  # list of [agent_id, max_concurrency]

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal
from tests.runtimes.multi_agent.test_providers import (
    KernelCapabilityProvider,
    KernelLeaseProvider,
    KernelProcessProvider,
    VPGAdapter,
)
from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    ClaimState,
)
from lhos.runtimes.multi_agent.scheduler import MultiAgentScheduler

# ── shared kernel + VPG over a temp SQLite file ─────────────────────────
kernel = create_kernel(DB_PATH)
vpg_rt = VerifiedProgressRuntime(DB_PATH)
vpg = VPGAdapter(vpg_rt)
proc_p = KernelProcessProvider(kernel)
lease_p = KernelLeaseProvider(kernel)
cap_p = KernelCapabilityProvider(kernel)

# ── register agents ───────────────────────────────────────────────────────
reg = AgentRegistry()
aid_to_maxc: dict[str, int] = {}
for aid, mxc in AGENT_SPEC_RAW:
    pid = kernel._process_service.spawn(aid).pid
    reg.register(AgentDescriptor(
        agent_id=aid,
        process_id=pid,
        supported_task_kinds=("*",),
        specializations=("python",),
        max_concurrency=mxc,
        cost_weight=100 + mxc,  # orderable but secondary to score logic
    ))
    aid_to_maxc[aid] = mxc

# ── create graph + 100 tasks via REAL VPG ─────────────────────────────────
graph_id = "stress-graph"
vpg_rt.create_graph(owner_pid=kernel._process_service.spawn("owner").pid,
                    graph_id=graph_id)
task_ids = [f"task-{i:04d}" for i in range(N_TASKS)]

for tid in task_ids:
    v_cur = vpg_rt.get_graph(graph_id).current_version
    vpg_rt.submit_patch(GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=v_cur,
        author_pid=kernel._process_service.spawn(f"author-{tid}").pid,
        idempotency_key=f"add-{tid}",
        operations=(AddNodeOp(
            node_id=tid,
            graph_id=graph_id,
            node_type="task",
            created_by_pid=kernel._process_service.spawn(f"author-{tid}").pid,
            task_kind="code_review",
            metadata={"scheduler": {
                "task_kind": "code_review",
                "required_specializations": ["python"],
                "required_tools": [],
            }},
        ),),
    ))

# ── single shared scheduler ────────────────────────────────────────────────
sched = MultiAgentScheduler(
    reg,
    vpg=vpg,
    process_provider=proc_p,
    lease_provider=lease_p,
    capability_provider=cap_p,
    lease_ttl=timedelta(minutes=30),
)


# ── run 100 rounds ─────────────────────────────────────────────────────────
# The simplest stress test: schedule_until_idle drives the scheduler to claim
# all 100 tasks in a single pass; we then release the claims and repeat.  This
# exercises the full claim-acquire path with REAL Kernel lease authority on
# every round, so G1..G4 are enforced per-round under REAL Kernel state.
#
# Releasing between rounds is the "retry" model that gives multiple rounds of
# independent claim/reclaim cycles against the same fleet and the same 100
# tasks.  Each round should therefore dispatch exactly N_TASKS tasks.
# ────────────────────────────────────────────────────────────────────────

from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri

g1_violations: list[dict] = []
g2_violations: list[dict] = []
g4_violations: list[dict] = []
rounds_data: list[dict] = []

task_claimed_once: set = set()
winning_agent_per_task: dict[str, str] = {}


def release_all_active() -> int:
    n = 0
    for c in list(sched.claims):
        if c.state == ClaimState.ACTIVE:
            sched.release_claim(c, reason="round_reset")
            n += 1
    return n


def verify_round(r: int) -> None:
    active = [c for c in sched.claims if c.state == ClaimState.ACTIVE]
    # G1: capacity per agent
    counts: dict[str, int] = {}
    for c in active:
        counts[c.agent_id] = counts.get(c.agent_id, 0) + 1
    for aid, cnt in counts.items():
        mx = aid_to_maxc[aid]
        if cnt > mx:
            g1_violations.append({"round": r, "agent": aid, "active": cnt, "max": mx})
    # G2: unique ACTIVE claim per task_id
    seen_tid: dict[str, int] = {}
    for c in active:
        seen_tid[c.task_id] = seen_tid.get(c.task_id, 0) + 1
    for tid, n in seen_tid.items():
        if n > 1:
            g2_violations.append({"round": r, "task_id": tid, "active_claims": n})
    # G4: kernel lease exclusivity per resource
    by_res: dict[str, int] = {}
    live_leases_per_res: dict[str, int] = {}
    for c in active:
        if c.lease_id is None:
            g4_violations.append({"round": r, "task_id": c.task_id,
                                  "reason": "active_without_lease"})
        res = claim_resource_uri(c.graph_id, c.task_id)
        by_res[res] = by_res.get(res, 0) + 1
        ll = lease_p.list_for_resource(res)
        live_leases_per_res[res] = len(ll)
    for res, n in by_res.items():
        if n > 1:
            g4_violations.append({"round": r, "resource": res, "active_claims": n})
    for res, n_live in live_leases_per_res.items():
        if n_live > 1:
            g4_violations.append({"round": r, "resource": res,
                                  "active_leases": n_live})


def bump_graph_version(round_idx: int) -> None:
    """Bump current_version by submitting a real VPG patch that adds a fresh
    round-marker artifact_ref.  A new graph version means the Scheduler sees
    fresh frontier candidates and fresh (graph_id, task_id, version)
    idempotency keys.  We ALSO clear the scheduler-side idempotency keys
    directly below so the "idempotent replay" early exit in the Schedule
    path no longer applies.
    """
    v_cur = vpg_rt.get_graph(graph_id).current_version
    vpg_rt.submit_patch(GraphPatchProposal(
        graph_id=graph_id,
        expected_graph_version=v_cur,
        author_pid=admin_pid,
        idempotency_key=f"round-bump-{round_idx}",
        operations=(AddNodeOp(
            node_id=f"round-marker-{round_idx}",
            graph_id=graph_id,
            node_type="artifact_ref",
            created_by_pid=admin_pid,
            canonical_uri=f"file:///marker/{round_idx}",
            artifact_id=f"marker-{round_idx}",
            version=1,
            content_hash="x",
        ),),
    ))


admin_pid = kernel._process_service.spawn("admin").pid

for r in range(N_ROUNDS):
    if r > 0:
        release_all_active()
        # Bump to a fresh Scheduler idempotency epoch so every task is
        # eligible to be re-claimed this round.  We directly clear the
        # scheduler's own idempotency set so the "idempotent replay" early
        # exit in the Schedule path no longer applies.
        bump_graph_version(r)
        sched._idempotent_keys.clear()

    sched.schedule_until_idle(graph_id, max_dispatches=8)
    verify_round(r)

    # dispatched this round: tasks that are ACTIVE after this round
    newly_active = [
        c for c in sched.claims
        if c.state == ClaimState.ACTIVE
    ]
    active_tids = {c.task_id for c in newly_active}
    task_claimed_once.update(active_tids)
    for c in newly_active:
        winning_agent_per_task[c.task_id] = c.agent_id

    rounds_data.append({
        "round": r,
        "active_total": len(newly_active),
        "n_claims_total": len(sched.claims),
    })

report = {
    "n_rounds_run": N_ROUNDS,
    "n_tasks": N_TASKS,
    "rounds": rounds_data,
    "g1_violations": g1_violations,
    "g2_violations": g2_violations,
    "g4_violations": g4_violations,
    "total_tasks_claimed_at_least_once": len(task_claimed_once),
    "winning_agent_per_task": winning_agent_per_task,
}
print("RESULT_JSON_START")
print(json.dumps(report, sort_keys=True))
print("RESULT_JSON_END")
'''


def run_trial(seed_label: str, seed_value: str | None, db_path: str) -> dict:
    """Run the inner subprocess with the given PYTHONHASHSEED."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO) + os.pathsep + env.get("PYTHONPATH", "")
    env.pop("PYTHONHASHSEED", None)
    if seed_value is not None:
        env["PYTHONHASHSEED"] = str(seed_value)

    proc = subprocess.run(
        [sys.executable, "-c",
         _INNER, db_path, str(N_TASKS), str(N_ROUNDS),
         json.dumps(AGENTS_SPEC)],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=str(REPO),
    )

    s = proc.stdout.find("RESULT_JSON_START\n")
    e = proc.stdout.find("\nRESULT_JSON_END")
    if s < 0 or e < 0 or e <= s:
        raise RuntimeError(
            f"[{seed_label}] Failed to parse subprocess output. "
            f"rc={proc.returncode}\n"
            f"--- last 2000 chars stdout ---\n{proc.stdout[-2000:]}\n"
            f"--- last 2000 chars stderr ---\n{proc.stderr[-2000:]}"
        )
    payload = json.loads(proc.stdout[s + len("RESULT_JSON_START\n"):e])
    payload["seed_label"] = seed_label
    payload["python_hashseed"] = seed_value if seed_value is not None else "unset"
    return payload


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    trials: list[dict] = []
    for seed_label, seed_value in SEEDS:
        print(f"[d21] running trial seed={seed_label!r} ...", flush=True)
        db_dir = tempfile.mkdtemp(prefix="lhos-d21-stress-")
        db_path = os.path.join(db_dir, "kernel.sqlite")
        try:
            t = run_trial(seed_label, seed_value, db_path)
        finally:
            import shutil
            shutil.rmtree(db_dir, ignore_errors=True)
        trials.append(t)
        print(f"[d21]   seed={seed_label!r}: claimed_once="
              f"{t['total_tasks_claimed_at_least_once']}; "
              f"G1_v={len(t['g1_violations'])} "
              f"G2_v={len(t['g2_violations'])} "
              f"G4_v={len(t['g4_violations'])}",
              flush=True)

    # ── aggregate gate results ─────────────────────────────────────────────
    shape_0 = trials[0]["winning_agent_per_task"]
    shape_1 = trials[1]["winning_agent_per_task"]
    shape_2 = trials[2]["winning_agent_per_task"]
    g5 = (shape_0 == shape_1 == shape_2)

    gates = {
        "G1_capacity_respected": all(not t["g1_violations"] for t in trials),
        "G2_unique_claim_per_task": all(not t["g2_violations"] for t in trials),
        "G3_every_task_eventually_claimed": all(
            t["total_tasks_claimed_at_least_once"] == N_TASKS for t in trials),
        "G4_kernel_lease_exclusivity": all(not t["g4_violations"] for t in trials),
        "G5_deterministic_shape_across_seeds": g5,
    }
    all_pass = all(gates.values())

    final = {
        "meta": {
            "description": "Phase D2.1 §7 Multi-Task Concurrency Stress audit",
            "n_tasks": N_TASKS,
            "n_rounds_per_trial": N_ROUNDS,
            "agents_spec": [{"agent_id": a, "max_concurrency": m} for a, m in AGENTS_SPEC],
            "total_fleet_capacity": sum(m for _, m in AGENTS_SPEC),
            "seeds": [s for s, _ in SEEDS],
            "schema_version": "1.0",
        },
        "gates": gates,
        "all_pass": all_pass,
        "trials": [
            {
                "seed_label": t["seed_label"],
                "python_hashseed": t["python_hashseed"],
                "total_tasks_claimed_at_least_once": t["total_tasks_claimed_at_least_once"],
                "g1_violations": t["g1_violations"],
                "g2_violations": t["g2_violations"],
                "g4_violations": t["g4_violations"],
                "winning_agent_per_task": t["winning_agent_per_task"],
                "rounds_summary": {
                    "n_rounds": len(t["rounds"]),
                    "active_total_per_round": [rd["active_total"] for rd in t["rounds"]],
                    "claims_total_per_round": [rd["n_claims_total"] for rd in t["rounds"]],
                },
            }
            for t in trials
        ],
    }

    json_p = OUT_DIR / "multi-task-concurrency-stress.json"
    md_p = OUT_DIR / "multi-task-concurrency-stress.md"

    with open(json_p, "w") as f:
        json.dump(final, f, indent=2, sort_keys=False)

    md: list[str] = []
    md.append("# §7 Multi-Task Concurrency Stress Audit")
    md.append("")
    md.append(f"- Tasks: {N_TASKS}")
    md.append(f"- Agents: {len(AGENTS_SPEC)} with mixed max_concurrency "
              f"{','.join(str(m) for _, m in AGENTS_SPEC)} "
              f"(total fleet capacity = {sum(m for _, m in AGENTS_SPEC)})")
    md.append(f"- Rounds per trial: {N_ROUNDS}")
    md.append(f"- Trials (PYTHONHASHSEED): {', '.join(s for s, _ in SEEDS)}")
    md.append("- DB: REAL AgentKernel + REAL VerifiedProgressRuntime "
              "(temp file per trial; kernel is authoritative lease store)")
    md.append("")
    md.append("## Gate results")
    md.append("")
    for g, ok in gates.items():
        md.append(f"- {g}: **{'PASS' if ok else 'FAIL'}**")
    md.append("")
    md.append(f"## Overall: **{'ALL_PASS' if all_pass else 'FAIL'}**")
    md.append("")

    md.append("## Per-trial")
    for t in trials:
        md.append(f"### seed={t['seed_label']!r} (PYTHONHASHSEED="
                  f"{t['python_hashseed']!r})")
        md.append(f"- total tasks claimed at least once: "
                  f"{t['total_tasks_claimed_at_least_once']}/{N_TASKS}")
        md.append(f"- G1 violations: {len(t['g1_violations'])} "
                  f"| G2: {len(t['g2_violations'])} "
                  f"| G4: {len(t['g4_violations'])}")
        per_round = [rd["active_total"] for rd in t["rounds"]]
        md.append(f"- active-total per round (first 10): {per_round[:10]}")
        md.append("")

    md.append("## Violations (if any)")
    md.append("")
    any_v = False
    for t in trials:
        if t["g1_violations"]:
            any_v = True
            md.append(f"### G1 (seed={t['seed_label']!r}):")
            for v in t["g1_violations"][:20]:
                md.append(f"  - round={v['round']} agent={v['agent']} "
                          f"active={v['active']} max={v['max']}")
            md.append("")
        if t["g2_violations"]:
            any_v = True
            md.append(f"### G2 (seed={t['seed_label']!r}):")
            for v in t["g2_violations"][:20]:
                md.append(f"  - round={v['round']} task={v['task_id']} "
                          f"active_claims={v['active_claims']}")
            md.append("")
        if t["g4_violations"]:
            any_v = True
            md.append(f"### G4 (seed={t['seed_label']!r}):")
            for v in t["g4_violations"][:20]:
                md.append(f"  - round={v['round']} {v}")
            md.append("")
    if not any_v:
        md.append("No violations observed across all trials.")
        md.append("")

    md.append("## Determinism note")
    md.append("")
    md.append("G5 holds iff the structural matching tiebreak (integer score + "
              "(load_asc, cost_weight_asc, agent_id_asc) stable sort) produces "
              "identical winning-agent assignments across seeds.")
    md.append("")
    md.append("Each round releases all ACTIVE claims, then re-runs "
              "`schedule_until_idle` so the scheduler re-executes the full "
              "claim-acquire path (`_acquire_claim` does an atomic Kernel "
              "`acquire_exclusive`).  100 rounds x 100 tasks = ~10,000 real "
              "Kernel lease acquisitions across RPC-style single-process calls; "
              "this is a scheduling-stress test, not an OS-level race (that is "
              "the CROSS-PROCESS audit in §6).")
    md.append("")

    md_p.write_text("\n".join(md))

    print()
    print("=" * 60)
    print("§7 Multi-Task Concurrency Stress audit")
    print("=" * 60)
    for g, ok in gates.items():
        print(f"  {g}: {'PASS' if ok else 'FAIL'}")
    print(f"  overall: {'ALL_PASS' if all_pass else 'FAIL'}")
    print(f"  artifacts: {json_p}")
    print(f"             {md_p}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
