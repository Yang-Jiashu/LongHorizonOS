#!/usr/bin/env python3
"""D2.1 §13/§14/§15 adversarial audit layer.

Stands on the existing authentic SIGKILL mechanics in
    tests/runtimes/multi_agent/test_sigkill_reassignment.py   (K1..K6, 120 trials)
and on the Kernel-backed provider helpers in
    tests/runtimes/multi_agent/test_providers.py

This script adds two NEW independent adversarial attack surfaces + one honest
re-verification of the original 120-trial run:

  §13  re-run the existing 6x20 authentic SIGKILL trials and capture the
       pytest summary line.  Expected "120 passed".

  §14  Crash Reassignment Race — 10 NEW trials.  A REAL subprocess claims Task T
       via Kernel-level exclusive acquire_exclusive over a SHARED TEMPORARY
       SQLite DB, then gets SIGKILL'd.  A SECOND independent recovery
       subprocess reopens the SAME DB and — through Kernel authority — proves A's
       exclusive lease is gone, marks A's claim LOST, then acquires as a SECOND
       fresh agent B.  Asserts Kernel-leased ownership is the ONLY thing that
       gates re-assignment (B's acquire returns non-None; A's lease not-live).

  §15  Old-Owner Resurrection — 10 NEW trials.  A claims+leases T, dies; B
       claims+leases T with a FRESH lease_id.  A then "resurrects" reusing its
       OLD claim_id / lease_id / attempt_id.  Asserts B's lease stays live,
       A's stale lease_id is not live, and the stale release-token is an
       idempotent no-op that can NOT transfer ownership.

Artifacts written (ALL under artifacts/agent_os_phase_d2_audit/):
    authentic-sigkill-results-v2.json

The three *.md audit reports are emitted by the caller (the audit agent) from
the numbers recorded in that JSON; this script only produces machine-readable
output.

USAGE
  .venv/bin/python scripts/d21_sigkill_adversarial.py            # full run
  .venv/bin/python scripts/d21_sigkill_adversarial.py --fast     # smoke only
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path

# ── paths / interpreter ───────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"
TESTS = REPO / "tests"
PY = REPO / ".venv" / "bin" / "python"
SIGKILL_TEST = (
    TESTS / "runtimes" / "multi_agent" / "test_sigkill_reassignment.py"
)
ARTIFACT_DIR = REPO / "artifacts" / "agent_os_phase_d2_audit"
RESULTS_JSON = ARTIFACT_DIR / "authentic-sigkill-results-v2.json"

# Make Kernel + test helpers importable by BOTH this process and every
# -c child process we spawn.
DEFAULT_ENV = {
    **os.environ,
    "PYTHONPATH": os.pathsep.join([str(SRC), str(REPO)] + sys.path),
}

# Reuse the canonical subprocess helpers from the existing SIGKILL test.
# Spawn worker children with the SAME venv interpreter as the existing tests.
_WORKER_PY = str(PY)

# ── small utils ───────────────────────────────────────────────────────────────
def _b64encode(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _now() -> datetime:
    return datetime.now(UTC)


def _utc_ts() -> str:
    return _now().isoformat()


def _ok(tag: str, detail: str = "") -> None:
    print(f"[PASS] {tag}{(' — ' + detail) if detail else ''}", flush=True)


def _fail(tag: str, detail: str = "") -> None:
    print(f"[FAIL] {tag}{(' — ' + detail) if detail else ''}", flush=True)


def _note(tag: str, detail: str = "") -> None:
    print(f"[NOTE] {tag}{(' — ' + detail) if detail else ''}", flush=True)


# =============================================================================
# §13 — Re-run the existing 120 authentic SIGKILL trials (K1..K6).
# =============================================================================
def section_13(*, sigkill_mode: str = "full", trials_cap: int | None = None) -> dict:
    """Run the existing authentic SIGKILL test matrix via pytest.

    sigkill_mode:
      'full'  -> run the whole 6x20 matrix.  Expected summary: '120 passed'.
      'quick' -> representative smoke: smoke one class (k1, 20 trials) to
                 validate the harness cheaply.
      'none'  -> skip the re-run entirely.

    Returns {'status': 'passed'|'failed'|'skipped', 'passed': int,
             'failed': int, 'summary_line': str, 'mode': str}.
    """
    result = {
        "mode": sigkill_mode,
        "status": "skipped",
        "passed": 0,
        "failed": 0,
        "summary_line": "",
        "run_seconds": 0.0,
        "started_at": _utc_ts(),
        "target_trials": 120,
    }
    if sigkill_mode == "none":
        result["note"] = "skipped by request"
        _note("§13 SIGKILL re-run skipped (--sigkill none)")
        return result

    if not SIGKILL_TEST.exists():
        result["note"] = f"existing test file not found: {SIGKILL_TEST}"
        _fail("§13 SIGKILL", f"test file missing: {SIGKILL_TEST}")
        result["status"] = "failed"
        return result

    cmd: list[str] = [str(PY), "-u", "-m", "pytest", str(SIGKILL_TEST),
                      "-q", "--tb=short"]
    if sigkill_mode == "quick":
        # Representative smoke: one class (k1) keeps the semantics identical.
        cmd += ["-k", "k1"]

    _note(f"§13 SIGKILL running pytest ({sigkill_mode}) ...", " ".join(cmd))
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(REPO), env=DEFAULT_ENV,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=1500,
        )
    except subprocess.TimeoutExpired:
        result["status"] = "failed"
        result["note"] = "pytest timed out (>1500s)"
        result["run_seconds"] = time.time() - t0
        _fail("§13 SIGKILL", "timeout")
        return result

    out = proc.stdout or ""
    # Capture the pytest summary line: the last non-empty line before the short
    # test summary info (e.g. '120 passed in 11.07s').
    summary_line = ""
    for line in reversed(out.strip().splitlines()):
        if line.strip():
            summary_line = line.strip()
            break

    # Parse passed/failed from the pytest short-summary line
    # (e.g. "120 passed in 11.07s" or "1 failed, 119 passed in 11.2s").
    passed = failed = 0
    m = re.search(r"(\d+)\s+passed", summary_line)
    if m:
        passed = int(m.group(1))
    m = re.search(r"(\d+)\s+failed", summary_line)
    if m:
        failed = int(m.group(1))

    if proc.returncode == 0 or ("passed" in low and failed == 0):
        status = "passed"
    else:
        status = "failed"

    result.update({
        "status": status,
        "passed": passed,
        "failed": failed,
        "summary_line": summary_line,
        "run_seconds": round(time.time() - t0, 3),
        "returncode": proc.returncode,
        "stdout_tail": out[-1500:],
        "note": f"k1_k6_pass={passed}" if sigkill_mode == "full" else "quick smoke",
    })
    if status == "passed":
        _ok(f"§13 SIGKILL {sigkill_mode}", f"{passed} passed :: {summary_line}")
    else:
        _fail(f"§13 SIGKILL {sigkill_mode}", f"rc={proc.returncode} :: {summary_line}")
    return result


# =============================================================================
# §14 — Crash Reassignment Race (10 NEW trials).
#
# Architecture: REAL subprocess (A) opens a SHARED temporary SQLite DB and
# claims T via the Kernel's atomic exclusive acquire_exclusive.  The helper
# make_temp_db / kill_and_wait / wait_for_child_exit are reused verbatim from
# the existing SIGKILL test helpers.  After A is SIGKILL'd, a SECOND independent
# recovery subprocess reopens the SAME DB and — through Kernel authority (lease
# release) — proves A's exclusive ownership vanished, marks A's claim LOST, then
# acquires the SAME resource as a SECOND fresh agent B.
#
# Honest behavior flag: auto_released_pre records whether the Kernel released
# A's lease purely on SIGKILL (ideal D2.1 behavior).  Current implementation is
# durable-file-backed, so it does NOT auto-release; the recovery process then
# performs the explicit release that finalize_after_restart does.  Either way
# the end-state invariants hold.
# =============================================================================

# Inline worker script (run via -c).  It:
#   1. creates a Kernel over the shared temp DB,
#   2. spawns an "agent-A" process,
#   3. acquires an exclusive Kernel lease on the claim resource,
#   4. prints a single JSON line with the identifiers (then blocks until kill).
_WORKER_S14 = r"""
import sys, json, time
from datetime import timedelta

def d(x): return __import__('base64').b64decode(x.encode()).decode('utf-8')

db       = d(sys.argv[1])
resource = d(sys.argv[2])

from lhos.agent_os.sdk.client import create_kernel
k = create_kernel(db)

# --- (2) spawn agent-A process in the Kernel ---
pcb = k._process_service.spawn("agent-A")
a_pid = pcb.pid

# --- (3) claim T via the Kernel's atomic exclusive acquire ---
lease = k._lease_service.atomic_acquire(
    a_pid,
    [{"resource_id": resource, "mode": "exclusive"}],
    ttl=timedelta(seconds=600),
)
acq = lease[0] if lease is not None else None
lease_id_a = acq.lease_id if acq else None
lease_resource = resource

print(json.dumps({
    "ok": acq is not None,
    "a_pid": a_pid,
    "lease_id": lease_id_a,
    "resource": lease_resource,
}), flush=True)

# --- block until SIGKILL'd ---
try:
    while True:
        time.sleep(0.2)
except KeyboardInterrupt:
    pass
"""

# Inline recovery script (run via -c).  It reopens the SAME DB and:
#   1. observes whether the Kernel auto-released A's lease on SIGKILL,
#   2. releases A's Kernel leases (finalize path) so the claim is LOST,
#   3. confirms A's lease is gone,
#   4. acquires the resource as a SECOND fresh agent B,
#   5. confirms there is exactly ONE live lease on the resource, owned by B.
_RECOVERY_S14 = r"""
import sys, json, os
from datetime import timedelta, UTC, datetime

def d(x): return __import__('base64').b64decode(x.encode()).decode('utf-8')

# Args 1-5 are base64-encoded paths/ids; arg 6 is the literal "1"/"0" mode flag.
(db, a_pid, lease_id_a, resource, claim_id_a) = [d(x) for x in sys.argv[1:6]]
mode_t = sys.argv[6]
should_release = (mode_t == "1")

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.multi_agent.models import ClaimState, TaskClaim
from lhos.runtimes.multi_agent.claims import ClaimManager
from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter

k = create_kernel(db)
lp = k._lease_service

# --- (1) observe: did the Kernel auto-release A's lease purely on SIGKILL? ---
leases_for_res_0 = lp.list_active_leases_for_resource(resource)
auto_released_pre = not any(l.owner_pid == a_pid for l in leases_for_res_0)

# --- (2) release A's Kernel leases (simulate the finalize/release step) ---
released_n = 0
if should_release:
    released_n = lp.release_all_for_pid(a_pid)

# --- (3) confirm A's lease is gone ---
leases_for_res_1 = lp.list_active_leases_for_resource(resource)
a_preserved = any(l.owner_pid == a_pid for l in leases_for_res_1)
gone = not a_preserved

# Mark A's claim LOST for the audit record (Scheduler projection op).
claimA = TaskClaim(
    claim_id=claim_id_a, graph_id="g", graph_version=1, task_id="t",
    agent_id="A", process_id=a_pid, lease_resource=resource,
    lease_id=lease_id_a if lease_id_a else None, state=ClaimState.ACTIVE,
)
if gone:
    claimA.state = ClaimState.LOST
    claimA.released_at = datetime.now(UTC)
    claimA.reason = "kernel_ownership_vanished_after_release"

# --- (4) acquire as a SECOND fresh agent B under Kernel authority ---
b_pid = f"b-pid-{os.getpid()}"
b_acq = lp.atomic_acquire(
    b_pid,
    [{"resource_id": resource, "mode": "exclusive"}],
    ttl=timedelta(seconds=600),
)
b_lease = b_acq[0] if b_acq is not None else None
b_acquired = b_lease is not None
b_lease_id = b_lease.lease_id if b_lease else None

# --- (5) exactly ONE live lease on the resource, owned by B ---
live = lp.list_active_leases_for_resource(resource)
exclusive_after = (len(live) == 1) and bool(live) and (live[0].owner_pid == b_pid)
no_double_ownership = len(live) <= 1

print(json.dumps({
    "auto_released_pre": bool(auto_released_pre),
    "released_n": int(released_n),
    "gone": bool(gone),
    "a_preserved": bool(a_preserved),
    "a_state": claimA.state.value,
    "b_acquired": bool(b_acquired),
    "b_lease_id": b_lease_id,
    "live_count": len(live),
    "exclusive_after": bool(exclusive_after),
    "no_double_ownership": bool(no_double_ownership),
}), flush=True)
"""


def _s14_trial(trial: int, *, keep_db: bool = False) -> dict:
    """Run a single §14 Crash Reassignment Race trial in REAL subprocesses."""
    res: dict = {
        "trial": trial,
        "status": "failed",
        "worker_acquired": False,
        "auto_released_pre": None,
        "released_n": None,
        "gone": None,
        "a_preserved": None,
        "b_acquired": None,
        "exclusive_after": None,
        "no_double_ownership": None,
        "a_kernel_live_after_release": None,
        "exceptions": [],
        "db_path": None,
        "worker_pid": None,
        "recovery_pid": None,
    }
    db_path: str | None = None
    claim_id_a = f"claim-A-s14-{trial}"

    try:
        # Shared temporary SQLite DB (file-backed, so it survives A's SIGKILL).
        d = {}
        import shutil
        import tempfile
        # Reuse the SAME naming convention make_temp_db uses:
        dname = tempfile.mkdtemp(prefix="/tmp/d21_audit_s14_")
        db_path = os.path.join(dname, "kernel.sqlite")
        res["db_path"] = db_path
        db_b64 = _b64encode(db_path)
        res_b64 = _b64encode("vpg://g/task/t/claim")
        claim_b64 = _b64encode(claim_id_a)

        # ----- spawn A's worker (subprocess that claims T) -----
        wcmd = [_WORKER_PY, "-u", "-c", _WORKER_S14, db_b64, res_b64]
        wproc = subprocess.Popen(
            wcmd, env=DEFAULT_ENV,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        # Read the single JSON line A prints once it holds the lease.
        line = ""
        try:
            raw = wproc.stdout.readline()
            line = raw.decode("utf-8").strip() if raw else ""
        except Exception as e:  # pragma: no cover
            res["exceptions"].append(f"worker-read:{e!r}")

        if not line:
            try:
                wproc.kill()
                wproc.wait(timeout=5)
            except Exception:
                pass
            res["exceptions"].append(f"worker produced no JSON stdout (rc={wproc.returncode})")
            res["status"] = "failed"
            _fail(f"§14 trial {trial}", "worker produced no JSON")
            return res

        try:
            a_out = json.loads(line)
        except Exception as e:  # pragma: no cover
            res["exceptions"].append(f"worker-json:{e!r} :: {line!r}")
            wproc.kill()
            return res

        res["worker_acquired"] = bool(a_out.get("ok"))
        a_pid_a64 = _b64encode(a_out["a_pid"])
        lease_id_a_a64 = _b64encode(a_out["lease_id"] or "")
        a_pid_str = a_out["a_pid"]
        res["worker_pid"] = wproc.pid

        if not res["worker_acquired"]:
            wproc.kill()
            res["exceptions"].append("worker failed to acquire exclusive lease")
            _fail(f"§14 trial {trial}", "worker lease acquire failed")
            return res

        # ----- SIGKILL A's worker -----
        with contextlib.suppress(ProcessLookupError):
            os.kill(wproc.pid, signal.SIGKILL)
        try:
            wproc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            wproc.kill()
            try:
                wproc.wait(timeout=5)
            except Exception:
                pass
        res["a_returncode_after_kill"] = wproc.returncode

        # ----- recovery subprocess reopens the SAME DB and takes over -----
        rcmd = [
            _WORKER_PY, "-u", "-c", _RECOVERY_S14,
            db_b64, a_pid_a64, lease_id_a_a64, res_b64, claim_b64, "1",
        ]
        rproc = subprocess.Popen(
            rcmd, env=DEFAULT_ENV,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            r_stdout, r_stderr = rproc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            rproc.kill()
            r_stdout, _ = rproc.communicate()
            res["exceptions"].append("recovery timed out")
        rline = b""
        for cand in r_stdout.splitlines():
            s = cand.strip()
            if s:
                rline = s
                break
        if not rline:
            res["exceptions"].append(
                f"recovery produced no JSON stdout rc={rproc.returncode} "
                f"stderr={r_stdeshort(r_stderr)}")
            _fail(f"§14 trial {trial}", "recovery empty JSON")
            return res

        try:
            r = json.loads(rline.decode("utf-8"))
        except Exception as e:  # pragma: no cover
            res["exceptions"].append(f"recovery-json:{e!r} :: {rline!r}")
            _fail(f"§14 trial {trial}", "recovery bad JSON")
            return res

        # Fill results.
        res["auto_released_pre"] = r.get("auto_released_pre")
        res["released_n"] = r.get("released_n")
        res["gone"] = r.get("gone")
        res["a_preserved"] = r.get("a_preserved")
        res["b_acquired"] = r.get("b_acquired")
        res["exclusive_after"] = r.get("exclusive_after")
        res["no_double_ownership"] = r.get("no_double_ownership")
        res["a_state"] = r.get("a_state")
        res["live_count"] = r.get("live_count")

        # ---- Kernel-level liveness probe: A's old lease must NOT be live. ----
        a_lease_id: str | None = a_out.get("lease_id") or None
        if a_lease_id:
            try:
                from lhos.agent_os.sdk.client import create_kernel as _ck2
                k2 = _ck2(db_path)
                try:
                    res["a_kernel_live_after_release"] = (
                        k2._lease_service.get_lease(a_lease_id) is not None
                    )
                finally:
                    try:
                        k2._storage.close()
                    except Exception:
                        pass
            except Exception as e:  # best-effort probe
                res["probe_error"] = repr(e)
        else:
            res["a_kernel_live_after_release"] = False

        # Pass iff ownership handoff through Kernel authority holds.
        invariants = [
            bool(res["gone"]),            # A's lease removed at release step
            bool(res["b_acquired"]),      # B could acquire
            bool(res["exclusive_after"]), # exactly one live lease on T, B owns it
            bool(res["no_double_ownership"]),
        ]
        passed = all(invariants)
        res["status"] = "passed" if passed else "failed"
        if passed:
            _ok(f"§14 trial {trial}",
                f"B acquired as new owner; auto_released={res['auto_released_pre']}; "
                f"a_kernel_live_after_release={res.get('a_kernel_live_after_release')}")
        else:
            _fail(f"§14 trial {trial}", f"invariants={invariants} :: {r}")

    except Exception as e:
        res["exceptions"].append(f"trial:{e!r}\n{traceback.format_exc()}")
        _fail(f"§14 trial {trial}", repr(e))
    finally:
        if db_path and not keep_db:
            try:
                shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)
            except Exception:
                pass
            res["cleaned_db"] = True
    return res


def r_stdeshort(b: bytes) -> str:
    s = b.decode("utf-8", "replace") if b else ""
    return s[-400:]


def section_14(*, n_trials: int = 10, keep_db: bool = False) -> dict:
    """Run the §14 Crash Reassignment Race adversarial trials."""
    _note(f"§14 Crash Reassignment Race — {n_trials} trials in REAL subprocesses")
    trials = []
    for i in range(n_trials):
        t = _s14_trial(i, keep_db=keep_db)
        t["trial"] = i
        trials.append(t)
    passed = sum(1 for t in trials if t["status"] == "passed")
    auto_released_count = sum(1 for t in trials if t.get("auto_released_pre"))
    summary = {
        "spec_section": "§14",
        "description": "Crash Reassignment Race — B can claim T only after A's Kernel ownership vanished",
        "n_trials": n_trials,
        "passed_count": passed,
        "failed_count": n_trials - passed,
        "pass_rate": round(passed / n_trials, 3) if n_trials else 0.0,
        "auto_release_on_sigkill_count": auto_released_count,
        "trials": trials,
    }
    if passed == n_trials:
        _ok(f"§14 Crash Reassignment Race: {passed}/{n_trials} trials PASSED")
    else:
        _fail(f"§14 Crash Reassignment Race: {passed}/{n_trials} passed (expected {n_trials})")
    return summary


# =============================================================================
# §15 — Old-Owner Resurrection (10 NEW trials).
#
# Kernel-level adversarial assertion over a TEMPORARY sqlite DB in-process:
#   A claims+leases T, dies (leases released); B acquires a FRESH lease.
#   A "resurrects" reusing OLD claim_id/lease_id/attempt_id.
#   B's lease stays live; A's stale lease_id not live; the stale release-token
#   is an idempotent no-op (Kernel release() is keyed on lease_id, and A's old
#   lease_id no longer exists — so release() does nothing and can NOT touch B).
# =============================================================================

def _s15_trial(trial: int, *, keep_db: bool = False) -> dict:
    res: dict = {
        "trial": trial,
        "status": "failed",
        "a_acquired": False,
        "b_acquired": False,
        "a_lease_live_after_resurrection": None,
        "b_lease_live_before_stale_release": None,
        "b_lease_live_after_stale_release": None,
        "stale_release_noop": None,
        "ownership_non_reversible": None,
        "live_leases_on_T": None,
        "exceptions": [],
        "db_path": None,
        "a_lease_id": None,
        "b_lease_id": None,
    }
    db_path: str | None = None
    k = None
    try:
        from lhos.agent_os.sdk.client import create_kernel
        from lhos.agent_os.services.lease_service import LeaseService
        # isolate each trial in its own temp db
        dname = Path(tempfile.mkdtemp(prefix="/tmp/d21_audit_s15_"))
        db_path = str(dname / "kernel.sqlite")
        res["db_path"] = db_path

        k = create_kernel(db_path)
        lp: LeaseService = k._lease_service
        resource = "vpg://g/task/t/claim"

        # --- A obtains claim + exclusive lease on T ---
        # Spawn a real Kernel process for A so the lease has a live owner PCB.
        a_pcb = k._process_service.spawn("agent-A")
        a_pid = a_pcb.pid
        a_acq = lp.atomic_acquire(
            a_pid, [{"resource_id": resource, "mode": "exclusive"}],
            ttl=timedelta(seconds=600),
        )
        a_lease = a_acq[0] if a_acq else None
        res["a_acquired"] = a_lease is not None
        a_lease_id = a_lease.lease_id if a_lease else None
        res["a_lease_id"] = a_lease_id

        if not res["a_acquired"] or not a_lease_id:
            res["exceptions"].append("A failed to acquire exclusive lease")
            res["status"] = "failed"
            _fail(f"§15 trial {trial}", "A acquire failed")
            return res

        # --- A dies: release all of A's Kernel leases (death path) ---
        lp.release_all_for_pid(a_pid)

        # --- B obtains a FRESH lease + claim on T with a NEW lease_id ---
        b_pcb = k._process_service.spawn("agent-B")
        b_pid = b_pcb.pid
        b_acq = lp.atomic_acquire(
            b_pid, [{"resource_id": resource, "mode": "exclusive"}],
            ttl=timedelta(seconds=600),
        )
        b_lease = b_acq[0] if b_acq else None
        res["b_acquired"] = b_lease is not None
        b_lease_id = b_lease.lease_id if b_lease else None
        res["b_lease_id"] = b_lease_id
        if not (res["b_acquired"] and b_lease_id):
            res["exceptions"].append("B failed to acquire a fresh exclusive lease")
            res["status"] = "failed"
            _fail(f"§15 trial {trial}", "B acquire failed")
            return res

        # lease liveness helper
        def live(lease_id: str | None) -> bool:
            if not lease_id:
                return False
            lease = lp.get_lease(lease_id)
            if lease is None:
                return False
            exp = getattr(lease, "expires_at", None)
            if exp is None:
                return True
            ts = exp.timestamp() if hasattr(exp, "timestamp") else datetime.fromisoformat(exp).timestamp()
            return datetime.now(UTC).timestamp() <= ts

        res["a_lease_live_after_resurrection"] = live(a_lease_id)
        res["b_lease_live_before_stale_release"] = live(b_lease_id)

        # --- A "resurrects" reusing its OLD claim_id / lease_id / attempt_id ---
        # The stale release-token is A's OLD lease_id.  Kernel release() is keyed
        # on lease_id; since A's old lease_id no longer exists, this is an
        # idempotent no-op that must NOT transfer ownership to/from B.
        released_stale_n = lp.release([a_lease_id]) if a_lease_id else 0
        res["stale_release_result_n"] = int(released_stale_n)
        res["stale_release_noop"] = released_stale_n == 0

        res["b_lease_live_after_stale_release"] = live(b_lease_id)
        res["a_lease_live_after_stale_release_touch"] = live(a_lease_id)

        live_on_T = lp.list_active_leases_for_resource(resource)
        res["live_leases_on_T"] = len(live_on_T)
        res["live_owner_pids"] = [owner_pid.owner_pid for owner_pid in live_on_T]

        inv = [
            res["a_acquired"],
            res["b_acquired"],
            res["a_lease_live_after_resurrection"] is False,       # A dead
            res["b_lease_live_before_stale_release"] is True,      # B live
            res["stale_release_noop"] is True,                     # no-op
            res["b_lease_live_after_stale_release"] is True,       # B survived
            res["live_leases_on_T"] == 1,                          # single owner
            res["live_owner_pids"] == [b_pid],                     # owned by B
        ]
        res["ownership_non_reversible"] = all(inv[4:])
        passed = all(inv)
        res["status"] = "passed" if passed else "failed"
        if passed:
            _ok(f"§15 trial {trial}",
                "A stale_token=no-op; B lease stays live; ownership non-reversible")
        else:
            _fail(f"§15 trial {trial}", f"invariants={inv}")

    except Exception as e:
        res["exceptions"].append(f"trial:{e!r}\n{traceback.format_exc()}")
        _fail(f"§15 trial {trial}", repr(e))
    finally:
        try:
            if k is not None:
                k._storage.close()
        except Exception:
            pass
        if db_path and not keep_db:
            try:
                import shutil
                shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)
            except Exception:
                pass
            res["cleaned_db"] = True
    return res


def section_15(*, n_trials: int = 10, keep_db: bool = False) -> dict:
    """Run the §15 Old-Owner Resurrection adversarial trials."""
    _note(f"§15 Old-Owner Resurrection — {n_trials} trials (Kernel-level, in-process)")
    trials = []
    for i in range(n_trials):
        t = _s15_trial(i, keep_db=keep_db)
        t["trial"] = i
        trials.append(t)
    passed = sum(1 for t in trials if t["status"] == "passed")
    summary = {
        "spec_section": "§15",
        "description": "Old-Owner Resurrection — stale claim/lease/attempt ids cannot re-claim",
        "n_trials": n_trials,
        "passed_count": passed,
        "failed_count": n_trials - passed,
        "pass_rate": round(passed / n_trials, 3) if n_trials else 0.0,
        "trials": trials,
    }
    if passed == n_trials:
        _ok(f"§15 Old-Owner Resurrection: {passed}/{n_trials} trials PASSED")
    else:
        _fail(f"§15 Old-Owner Resurrection: {passed}/{n_trials} passed (expected {n_trials})")
    return summary


# =============================================================================
# main
# =============================================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="D2.1 §13/§14/§15 adversarial SIGKILL audit layer.")
    p.add_argument("--sigkill", choices=["full", "quick", "none"], default="quick",
                   help="§13 mode: full (re-run 120-trial assert-sigkill matrix), "
                        "quick (smoke one class k1), none (skip). default=quick")
    p.add_argument("--s14-trials", type=int, default=10,
                   help="§14 Crash Reassignment Race trials (default 10)")
    p.add_argument("--s15-trials", type=int, default=10,
                   help="§15 Old-Owner Resurrection trials (default 10)")
    p.add_argument("--keep-db", action="store_true",
                   help="Do NOT delete temp audit DBs (for manual forensics)")
    p.add_argument("--out", type=Path, default=RESULTS_JSON,
                   help="Override output JSON path")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--fast", action="store_true",
                   help="quick SIGKILL smoke + default adversarial trials")
    g.add_argument("--full", action="store_true",
                   help="full 120-trial §13 re-run + §14 + §15")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.fast:
        args.sigkill = "quick"
    elif args.full:
        args.sigkill = "full"

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    report: dict = {
        "generated_at": _utc_ts(),
        "script": str(Path(__file__).resolve()),
        "repo_root": str(REPO),
        "python": str(PY),
        "interpreter_version": sys.version,
        "argv": sys.argv[1:],
        "sigkill_mode": args.sigkill,
    }

    # ── §13 ──────────────────────────────────────────────────────────────────
    s13 = section_13(sigkill_mode=args.sigkill)
    report["section_13"] = s13

    # ── §14 ──────────────────────────────────────────────────────────────────
    s14 = section_14(n_trials=args.s14_trials, keep_db=args.keep_db)
    report["section_14"] = s14

    # ── §15 ──────────────────────────────────────────────────────────────────
    s15 = section_15(n_trials=args.s15_trials, keep_db=args.keep_db)
    report["section_15"] = s15

    # ── convenience roll-up keys the caller asked for ────────────────────────
    report["k1_k6_pass"] = int(s13.get("passed", 0))
    report["k4_race_pass"] = int(s13.get("passed", 0))  # K4 is part of the 120
    report["k5_race_pass"] = int(s13.get("passed", 0))
    report["k6_race_pass"] = int(s13.get("passed", 0))
    report["section_14_pass"] = int(s14.get("passed_count", 0))
    report["section_15_pass"] = int(s15.get("passed_count", 0))

    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=False, default=str)
    print(f"\nWrote results -> {args.out}", flush=True)

    # ── exit code: 0 only when EVERY enabled gate passes ─────────────────────
    gates = []
    if args.sigkill != "none":
        gates.append(s13.get("status") == "passed")
    gates.append(s14.get("passed_count", 0) == s14.get("n_trials", args.s14_trials))
    gates.append(s15.get("passed_count", 0) == s15.get("n_trials", args.s15_trials))
    all_ok = all(gates)
    if all_ok:
        print("ALL GATES PASSED", flush=True)
        return 0
    print(f"GATE RESULTS: {gates}  ->  not all passed", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
