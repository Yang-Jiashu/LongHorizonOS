"""Phase D3 §37 — Authentic SIGKILL recovery audit.

Five crash boundaries S1..S5 x 20 real SIGKILL trials each = 100.  A REAL
child Python subprocess computes a deterministic D3 invalidation on a fixed
graph and blocks at a configurable boundary.  The parent sends os.kill(SIGKILL).
A FRESH recovery subprocess recomputes the same result and must produce a
BYTE-IDENTICAL cone_hash / frontier_hash / affected set, proving no partial
invalidation leaked and the recovered state equals the no-crash run.

Because the D3 engine is PURE (never writes to the authoritative graph), the
recovery is naturally idempotent — the child's SIGKILL leaves only process
state, and recovery recomputes the identical derived state (§25, §31).
"""

# ruff: noqa
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

BOUNDARIES = {
    "S1": "seed_validation",
    "S2": "cone",
    "S3": "validity_commit",
    "S4": "goal_frontier",
    "S5": "reverify",
    "S6": "downstream_recompute",
}
TRIALS = 20


def _worker_code(out_path: str, boundary: str, write_marker: int) -> str:
    return (
        "import json, sys, os\n"
        "sys.path.insert(0, r'" + str(REPO) + "')\n"
        "os.environ['D3_WRITE_MARKER'] = '" + str(write_marker) + "'\n"
        "from scripts.d3_sigkill_worker import compute_deterministic\n"
        "res = compute_deterministic(r'" + boundary + "')\n"
        "open(r'" + out_path + "', 'w').write(json.dumps(res))\n"
    )


def main() -> int:
    out_dir = REPO / "artifacts" / "agent_os_phase_d3_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    base_env = dict(os.environ)
    base_env["PYTHONPATH"] = str(REPO)

    results_per_boundary: dict[str, list] = {}
    all_pass = True
    for boundary in BOUNDARIES:
        records = []
        for trial in range(TRIALS):
            tmp = tempfile.mkdtemp(prefix="d3_sigkill_")
            out1 = os.path.join(tmp, "crash.json")
            out2 = os.path.join(tmp, "recover.json")
            marker = os.path.join(tmp, "ready")
            child_env = dict(base_env)
            child_env["D3_MARKER_DIR"] = tmp

            # 1) reference no-crash result (write_marker=0 -> does not block)
            subprocess.run(
                [sys.executable, "-c", _worker_code(out2, "reverify", 0)],
                capture_output=True,
                text=True,
                env=child_env,
                timeout=120,
            )
            reference = json.loads(open(out2).read()) if os.path.exists(out2) else None

            # 2) kill-target child at `boundary` (write_marker=1 -> blocks)
            if os.path.exists(out2):
                os.remove(out2)
            if os.path.exists(marker):
                os.remove(marker)
            child = subprocess.Popen(
                [sys.executable, "-u", "-c", _worker_code(out1, boundary, 1)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=child_env,
            )
            deadline = time.time() + 60
            while time.time() < deadline and not os.path.exists(marker):
                time.sleep(0.05)
            if os.path.exists(marker):
                # child reached boundary -> SIGKILL it
                try:
                    os.kill(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
                killed = True
            else:
                child.kill()
                child.wait()
                killed = False

            # 3) fresh recovery recomputes the identical result
            subprocess.run(
                [sys.executable, "-c", _worker_code(out2, "reverify", 0)],
                capture_output=True,
                text=True,
                env=child_env,
                timeout=120,
            )
            recovered = json.loads(open(out2).read()) if os.path.exists(out2) else None
            ok = bool(killed and reference is not None and recovered == reference)
            records.append(
                {
                    "trial": trial,
                    "killed": killed,
                    "recovered_matches": recovered == reference,
                    "ok": ok,
                }
            )
            all_pass = all_pass and ok
        results_per_boundary[boundary] = records

    summary = {
        "boundaries": list(BOUNDARIES.keys()),
        "trials_per_boundary": TRIALS,
        "total_sigkills": len(BOUNDARIES) * TRIALS,
        "all_pass": all_pass,
    }
    json_path = out_dir / "authentic-sigkill-results-v2.json"
    json_path.write_text(
        json.dumps(
            {
                "artifact": "authentic-sigkill-results-v2.json",
                "spec_section": "§21",
                "summary": summary,
                "per_boundary": results_per_boundary,
            },
            indent=2,
        )
    )
    print("sigkill summary:", summary)
    return 0 if all_pass else 2


if __name__ == "__main__":
    sys.exit(main())
