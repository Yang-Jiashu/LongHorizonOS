"""Idempotency Under Concurrency — Phase D1.1 Step 13.

Prove: 32 threads simultaneously submit the SAME idempotency key on the SAME
graph. Exactly ONE wins, others are either idempotent_replay OR rejected as a
duplicate at commit time. Post-race, graph_version == 1 (only one commit).
Restart the runtime from the SAME SQLite file; recovered graph_version == 1
and the stored idempotency keys contain exactly one entry.
"""

from __future__ import annotations

import threading
from pathlib import Path
import tempfile
import json

import pytest

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    GraphPatchProposal,
)

NUM_THREADS = 32
SHARED_KEY = "shared-idempotency-key"
NODE_ID = "shared_node"

AUDIT_RESULTS: dict[str, dict] = {}


@pytest.fixture(autouse=True, scope="session")
def _dump_audit_results_after_session():
    yield
    scenarios = [AUDIT_RESULTS[k] for k in sorted(AUDIT_RESULTS)]
    surviving = [s for s in scenarios if s.get("verdict") == "RISK"]
    out = {
        "step": 13,
        "step_name": "IdempotencyUnderConcurrency",
        "scenarios": scenarios,
        "overall_verdict": "RISK" if surviving else "PASS",
        "survining_risks": [s["id"] for s in surviving],
    }
    path = (
        Path(__file__).resolve().parents[3]
        / "artifacts/agent_os_phase_d1_audit/step-13-idempotency.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)


class TestS13_SameKey32Threads:
    def test_S13_same_key_32_threads_restart_once(self):
        tmp = tempfile.mkdtemp()
        db_path = str(Path(tmp) / "vpg_s13.db")

        # Bootstrap
        bootstrap_rt = VerifiedProgressRuntime(db_path)
        rec = bootstrap_rt.create_graph(owner_pid="p1")
        gid = rec.graph_id

        barrier = threading.Barrier(NUM_THREADS)
        results: list = [None] * NUM_THREADS
        lock = threading.Lock()

        def worker(idx: int):
            rt = VerifiedProgressRuntime(db_path)
            expected_version = rt.get_graph(gid).current_version
            barrier.wait()
            try:
                pr = rt.submit_patch(
                    GraphPatchProposal(
                        graph_id=gid,
                        expected_graph_version=expected_version,
                        author_pid="p1",
                        idempotency_key=SHARED_KEY,
                        operations=(
                            AddNodeOp(
                                node_id=NODE_ID,
                                graph_id=gid,
                                node_type="task",
                                created_by_pid="p1",
                                title=NODE_ID,
                            ),
                        ),
                    )
                )
                with lock:
                    results[idx] = ("ok", pr)
            except Exception as e:
                with lock:
                    results[idx] = ("err", e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        post_race_rt = VerifiedProgressRuntime(db_path)

        # 1) graph_version must be exactly 1
        version_after = post_race_rt.get_graph(gid).current_version
        version_ok = version_after == 1

        # 2) Exactly one commit applied; others are idempotent_replay OR rejected.
        applied = [r for r in results if isinstance(r, tuple) and r[0] == "ok"]
        idem = [r for r in applied if r[1].idempotent_replay]
        fresh = [r for r in applied if r[1].patch_applied]
        errs = [r for r in results if isinstance(r, tuple) and r[0] == "err"]

        assert len(fresh) == 1, (
            f"Expected exactly 1 fresh commit, got {len(fresh)} "
            f"(idem_replay={len(idem)}, errors={len(errs)})"
        )

        # 3) Restart: close all, reopen.  Recovered graph_version == 1.
        restarted_rt = VerifiedProgressRuntime(db_path)
        version_restart = restarted_rt.get_graph(gid).current_version
        version_restart_ok = version_restart == 1

        # 4) Exactly one idempotency row for (p1, gid, SHARED_KEY).
        idem_rows = restarted_rt.store.conn.execute(
            "SELECT patch_id, committed_version FROM graph_idempotency "
            "WHERE author_pid = 'p1' AND graph_id = ? AND idempotency_key = ?",
            (gid, SHARED_KEY),
        ).fetchall()
        idem_count_ok = len(idem_rows) == 1

        # Sample error types for the audit record
        err_types = sorted(set(type(e).__name__ for _, e in errs))

        all_ok = version_ok and version_restart_ok and idem_count_ok
        AUDIT_RESULTS["S13"] = {
            "id": "S13",
            "step": 13,
            "name": "same_key_32_threads_restart_once",
            "expected": "PASS",
            "verdict": "PASS" if all_ok else "RISK",
            "evidence": (
                f"fresh_commits={len(fresh)}, idem_replay={len(idem)}, "
                f"errors={len(errs)} (types={err_types}); "
                f"post_race_version={version_after}, "
                f"restart_version={version_restart}; "
                f"idempotency_rows={len(idem_rows)}"
            ),
        }
        assert all_ok, (
            f"S13 RISK: version_ok={version_ok} (got {version_after}), "
            f"restart_ok={version_restart_ok} (got {version_restart}), "
            f"idem_count_ok={idem_count_ok} (rows={len(idem_rows)})"
        )
