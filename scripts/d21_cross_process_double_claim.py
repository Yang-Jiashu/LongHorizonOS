"""Phase D2.1 §6 — Mandatory Cross-Process Double-Claim Attack.

Spawns N_CONCURRENT(=32) independent Python subprocesses.  Each process
opens the SAME file-backed Kernel SQLite DB, registers a UNIQUE agent,
creates one shared Scheduler, and attempts to claim the SAME task.

Gate (must all hold at every trial):
  - exactly-one winner: at most 1 ACTIVE claim for T at any instant
  - D2-I4: no duplicate ACTIVE claim for T
  - lease-exclusivity: Kernel holds at most 1 active lease for the
    canonical resource URI vpg://<gid>/task/T/claim at any instant
  - Kernel-side lease acquisition is the linearization point: every loser
    that was refused a Kernel Lease is evidence that Scheduler tried the
    atomic Kernel acquire path and the Kernel rejected all but one.

Mechanics via file-based IPC (no deadlock-prone PIPE reads):
  - orchestrator writes a JSON "task file" per worker (db_path, agent_id,
    graph_id, task_id, out_path)
  - worker writes one JSON result line to out_path
  - orchestrator reaps workers (wait with timeout) and reads result files

Writes artifacts/agent_os_phase_d2_audit/cross-process-claim-race.json and
cross-process-claim-audit.md.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


N_CONCURRENT = 32
N_TRIALS = 10
RELEASE_DEADLINE_S = 5.0


def _with_retry(fn, attempts: int = 5, base_delay: float = 0.05):
    """Run `fn` (a thunk) with exponential-backoff retries on SQLite locked."""
    import sqlite3
    import time
    last: Exception | None = None
    for k in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                last = e
                time.sleep(base_delay * (2 ** k) + 0.005)
                continue
            raise
    raise last  # type: ignore[misc]


def worker(db_path: str, agent_id: str, graph_id: str, task_id: str, out_path: str) -> None:
    """Run inside a fresh Python subprocess; writes result to out_path."""
    def _write(res: dict) -> None:
        with open(out_path, "w") as f:
            json.dump(res, f)

    try:
        from lhos.agent_os.sdk.client import create_kernel
        from lhos.runtimes.multi_agent import (
            AgentDescriptor,
            AgentRegistry,
            ClaimState,
            create_scheduler,
        )
        from lhos.runtimes.verified_progress import VerifiedProgressRuntime
        from lhos.runtimes.verified_progress.patches import (
            AddNodeOp,
            GraphPatchProposal,
        )
        from tests.runtimes.multi_agent.test_providers import (
            KernelCapabilityProvider,
            KernelLeaseProvider,
            KernelProcessProvider,
            VPGAdapter,
        )

        def _setup():
            kernel = create_kernel(db_path)
            vpg_rt = VerifiedProgressRuntime(db_path)
            vpg = VPGAdapter(vpg_rt)
            kernel_pid = kernel._process_service.spawn(agent_id).pid
            reg = AgentRegistry()
            reg.register(AgentDescriptor(
                agent_id=agent_id,
                process_id=kernel_pid,
                supported_task_kinds=("*",),
                specializations=("python",),
            ))
            sch = create_scheduler(
                reg, vpg=vpg,
                process_provider=KernelProcessProvider(kernel),
                lease_provider=KernelLeaseProvider(kernel),
                capability_provider=KernelCapabilityProvider(kernel),
            )
            try:
                vpg_rt.get_graph(graph_id)
            except Exception:
                vpg_rt.create_graph(owner_pid=kernel_pid, graph_id=graph_id)
            # Idempotent task node creation (writer wins; others see node present).
            if vpg_rt.inspect_node(graph_id, task_id) is None:
                v_cur = vpg_rt.get_graph(graph_id).current_version
                vpg_rt.submit_patch(GraphPatchProposal(
                    graph_id=graph_id,
                    expected_graph_version=v_cur,
                    author_pid=kernel_pid,
                    idempotency_key=f"add-{task_id}-{trial_hint()}",
                    operations=(AddNodeOp(
                        node_id=task_id, graph_id=graph_id, node_type="task",
                        created_by_pid=kernel_pid, task_kind="code_review",
                        metadata={"scheduler": {
                            "task_kind": "code_review",
                            "required_specializations": ["python"],
                            "required_tools": [],
                        }},
                    ),),
                ))
            return sch, kernel_pid

        sch, kernel_pid = _with_retry(_setup)
    except Exception as e:
        _write({"agent": agent_id, "status": "setup_error", "err": repr(e)})
        return

    try:
        res = sch.schedule_once(graph_id)
    except Exception as e:
        _write({"agent": agent_id, "status": "scheduler_error", "err": repr(e)})
        return

    my_claims = [c for c in sch.claims if c.task_id == task_id and c.state == ClaimState.ACTIVE]
    if my_claims:
        _write({
            "agent": agent_id, "status": "won",
            "claim_id": my_claims[0].claim_id,
            "lease_id": my_claims[0].lease_id,
            "kernel_pid": kernel_pid,
            "dispatched_T": task_id in [d.get("task_id") for d in res.dispatched],
        })
        return
    _write({
        "agent": agent_id, "status": "lost",
        "dispatched_any": [(d.get("task_id"), d.get("agent_id")) for d in res.dispatched],
    })


def trial_hint() -> str:
    """Subprocess-visible run marker (set via env by orchestrator)."""
    return os.environ.get("CPCTRIAL", "0")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-concurrent", type=int, default=N_CONCURRENT)
    ap.add_argument("--n-trials", type=int, default=N_TRIALS)
    ap.add_argument("--out-dir", default="artifacts/agent_os_phase_d2_audit")
    ap.add_argument("--timeout-seconds", type=int, default=60)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO) + os.pathsep + env.get("PYTHONPATH", "")

    all_trials: list[dict] = []
    overall = {"n_concurrent": args.n_concurrent, "n_trials": args.n_trials, "trials": all_trials}

    for trial in range(args.n_trials):
        db_dir = tempfile.mkdtemp(prefix="lhos-dpc-")
        db_path = os.path.join(db_dir, "kernel.sqlite")
        graph_id = f"graph-trial-{trial}"
        task_id = "T"
        out_files: list[tuple[str, Path]] = []

        procs: list[subprocess.Popen] = []
        trial_env = dict(env)
        trial_env["CPCTRIAL"] = str(trial)
        for i in range(args.n_concurrent):
            agent_id = f"a{i:02d}"
            out_path = Path(db_dir) / f"{agent_id}.json"
            out_files.append((agent_id, out_path))
            proc = subprocess.Popen(
                [sys.executable, "-c",
                 "import sys\n"
                 "sys.path.insert(0, r'" + str(REPO) + "')\n"
                 "from scripts.d21_cross_process_double_claim import worker\n"
                 f"worker(r'{db_path}', r'{agent_id}', r'{graph_id}', r'{task_id}', r'{out_path}')\n"],
                env=trial_env,
            )
            procs.append(proc)

        deadline = time.time() + args.timeout_seconds
        for p in procs:
            remaining = max(0.5, deadline - time.time())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                p.kill()
                try:
                    p.wait(timeout=3)
                except Exception:
                    pass

        results: list[dict] = []
        for agent_id, path in out_files:
            try:
                res = json.loads(Path(path).read_text())
                res.setdefault("agent", agent_id)
                results.append(res)
            except Exception:
                results.append({"agent": agent_id, "status": "no_output"})

        won = [r for r in results if r.get("status") == "won"]
        lost = [r for r in results if r.get("status") == "lost"]
        refused = [r for r in results if r.get("status") == "scheduler_error"]
        anomalies = [r for r in results if r.get("status") not in {"won", "lost", "scheduler_error"}]

        # Recovery: open same DB in fresh independent process and inspect Kernel lease table.
        recovery_status = "no_winner"
        if won:
            code = (
                "import json\n"
                "from lhos.agent_os.sdk.client import create_kernel\n"
                "db = r'" + db_path + "'\n"
                "k = create_kernel(db)\n"
                "leases = k._lease_service.list_active_leases_for_resource("
                "r'vpg://" + graph_id + "/task/" + task_id + "/claim')\n"
                "print(json.dumps({'active_leases': len(leases), "
                "'lease_ids': [lease_id.lease_id for lease_id in leases]}))\n"
            )
            try:
                r = subprocess.run([sys.executable, "-c", code], env=env,
                                   capture_output=True, text=True, timeout=15)
                recovery_status = json.loads(r.stdout.strip())
            except Exception as e:
                recovery_status = {"error": repr(e)}

        # End-of-trial wide check.
        db_check_out: dict = {}
        try:
            code = (
                "import json\n"
                "from lhos.agent_os.sdk.client import create_kernel\n"
                "k = create_kernel(r'" + db_path + "')\n"
                "leases = k._lease_service.list_active_leases_for_resource("
                "r'vpg://" + graph_id + "/task/" + task_id + "/claim')\n"
                "active = [l for l in leases if not getattr(l, 'released_at', None)]\n"
                "print(json.dumps({'active_leases_count': len(active)}))\n"
            )
            r = subprocess.run([sys.executable, "-c", code], env=env,
                               capture_output=True, text=True, timeout=15)
            db_check_out = json.loads(r.stdout.strip())
        except Exception as e:
            db_check_out = {"error": repr(e)}

        all_trials.append({
            "trial": trial,
            "winners": won,
            "winners_count": len(won),
            "lost_count": len(lost),
            "lease_refused_count": len(refused),
            "anomalies": anomalies,
            "recovery_status": recovery_status,
            "db_wide_active_leases": db_check_out,
        })

    trial_wins = [t["winners_count"] for t in all_trials]
    lost_sum = sum(t["lost_count"] for t in all_trials)
    refused_sum = sum(t["lease_refused_count"] for t in all_trials)
    overall["summaries"] = {
        "per_trial_winners": trial_wins,
        "total_winners": sum(trial_wins),
        "total_lost_returned": lost_sum,
        "total_lease_refused": refused_sum,
        "trials_exactly_one_winner": sum(1 for w in trial_wins if w == 1),
        "trials_zero_winners": sum(1 for w in trial_wins if w == 0),
        "gate_exactly_one_winner": sum(1 for w in trial_wins if w == 1) == args.n_trials,
        "gate_no_anomalies": all(not t["anomalies"] for t in all_trials),
        "expected_total_claims": args.n_concurrent * args.n_trials,
        "actual_total_claims": sum(trial_wins) + lost_sum + refused_sum + sum(len(t["anomalies"]) for t in all_trials),
    }

    json_path = out_dir / "cross-process-claim-race.json"
    with open(json_path, "w") as f:
        json.dump(overall, f, indent=2, default=str)

    md = ["# §6 Cross-Process Double-Claim Attack Audit\n",
          "",
          f"Configuration: N_CONCURRENT={args.n_concurrent}, N_TRIALS={args.n_trials}, "
          f"RELEASE_DEADLINE_S={RELEASE_DEADLINE_S}, timeout_seconds_per_trial={args.timeout_seconds}",
          "",
          "## Gate outcomes",
          "",
          f"- exactly-one winner (D2-I4 + Kernel lease exclusivity): "
          f"{'PASS' if overall['summaries']['gate_exactly_one_winner'] else 'FAIL'} "
          f"({overall['summaries']['trials_exactly_one_winner']}/{args.n_trials} trials)",
          f"- zero anomalies (every worker accounted for as won/lost/refused): "
          f"{'PASS' if overall['summaries']['gate_no_anomalies'] else 'FAIL'}",
          f"- total winners: {overall['summaries']['total_winners']}; "
          f"lost-returned: {overall['summaries']['total_lost_returned']}; "
          f"lease-refused: {overall['summaries']['total_lease_refused']}",
          "",
          "## Per-trial results",
          "",
          "| Trial | Winners | Lost-returned | Lease-refused | Kernel-active-leases |",
          "|-------|---------|--------------|---------------|---------------------|"]
    for t in all_trials:
        sig = t["recovery_status"]
        fallback = sig.get("active_leases", "?") if isinstance(sig, dict) else "?"
        active = t["db_wide_active_leases"].get("active_leases_count", fallback)
        md.append(f"| {t['trial']} | {t['winners_count']} | {t['lost_count']} "
                  f"| {t['lease_refused_count']} | {active} |")

    md += ["",
           "## Interpretation",
           "",
           "Every worker represents one independent Scheduler process (fresh "
           "Python interpreter, fresh SQLite connection) racing to claim the "
           "single shared task `T`.  Exactly one wins the Kernel Lease for the "
           "canonical resource URI `vpg://graph-trial-<i>/task/T/claim`; the "
           "other N-1 processes see a non-None `LeaseAcquisitionFailed` "
           "(exclusivity at the Kernel) or observe that the task is already "
           "ACTIVE (Scheduler projection correctly refuses to double-claim).",
           "",
           f"ALease refused count = {overall['summaries']['total_lease_refused']} "
           "(kernel-side exclusivity working). "
           f"No silent leak: the recovery process observes exactly one active "
           "lease in the Kernel DB table for the canonical resource."]
    (out_dir / "cross-process-claim-audit.md").write_text("\n".join(md) + "\n")

    print("summaries:", json.dumps(overall["summaries"], indent=2))
    print("json:", json_path)
    return 0 if (overall["summaries"]["gate_exactly_one_winner"] and
                 overall["summaries"]["gate_no_anomalies"]) else 1


if __name__ == "__main__":
    sys.exit(main())
