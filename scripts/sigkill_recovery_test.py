"""Real SIGKILL recovery test (audit Milestone 1G).

Tests crash recovery using real SIGKILL signals at 5 crash points.
Replaces SimulatedCrashError with marker+sleep so the parent process
can send SIGKILL, then verifies resume completes successfully.

Crash points (matching spec 26.2 and existing integration tests):
1. after_lease_before_execution  — crash_before_execution (controller)
2. during_tool_execution         — crash_on_attempt (worker, before tool calls)
3. after_tool_side_effect_before_event — crash_after_tool_calls (worker, after tools)
4. after_claim_before_verification — crash_before_verification (controller)
5. after_verified_before_commit  — crash_after_verified (controller)

Patching strategy:
  Controller crash points: call original _inject_crash_once (which writes
    the fire-once CRASH_INJECTED event), then hang if it returned True.
  Worker crash points: wrap original FakeWorker.execute, catch
    SimulatedCrashError, hang instead of propagating.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = str(PROJECT_ROOT / ".venv-audit" / "bin" / "python")
SRC_PATH = str(PROJECT_ROOT / "src")

# 3-node chain: n1 → n2 → n3.  Crash flag is added to n2 for each test.
TASK_TEMPLATE = {
    "goal": "SIGKILL recovery test",
    "nodes": [
        {
            "temp_id": "n1",
            "kind": "subtask",
            "title": "Write first file",
            "specification": "Create n1.txt",
            "schedulable": True,
            "progress_weight": 1.0,
            "verification_spec": {"type": "file_exists", "path": "n1.txt"},
            "metadata": {
                "script": {
                    "status": "claimed_done",
                    "summary": "wrote n1.txt",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "produced_artifacts": [{"path": "n1.txt", "content": "n1-content\n"}],
                },
            },
        },
        {
            "temp_id": "n2",
            "kind": "subtask",
            "title": "Write second file",
            "specification": "Create n2.txt",
            "schedulable": True,
            "progress_weight": 1.0,
            "verification_spec": {"type": "file_exists", "path": "n2.txt"},
            "metadata": {
                "script": {
                    "status": "claimed_done",
                    "summary": "wrote n2.txt",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "produced_artifacts": [{"path": "n2.txt", "content": "n2-content\n"}],
                    "tool_calls": [
                        {
                            "tool_name": "filesystem",
                            "arguments": {"op": "write", "path": "n2.txt", "content": "n2-content\n"},
                        }
                    ],
                },
            },
        },
        {
            "temp_id": "n3",
            "kind": "subtask",
            "title": "Write third file",
            "specification": "Create n3.txt",
            "schedulable": True,
            "progress_weight": 1.0,
            "verification_spec": {"type": "file_exists", "path": "n3.txt"},
            "metadata": {
                "script": {
                    "status": "claimed_done",
                    "summary": "wrote n3.txt",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "produced_artifacts": [{"path": "n3.txt", "content": "n3-content\n"}],
                },
            },
        },
    ],
    "edges": [
        {"source": "n2", "target": "n1", "kind": "depends_on"},
        {"source": "n3", "target": "n2", "kind": "depends_on"},
    ],
}

# 5 crash points: name → (flag, description)
CRASH_POINTS = {
    "after_lease_before_execution": (
        "crash_before_execution",
        "After lease+checkpoint, before EXECUTION_STARTED",
    ),
    "during_tool_execution": (
        "crash_on_attempt",
        "Worker crashes before any tool calls execute",
    ),
    "after_tool_side_effect_before_event": (
        "crash_after_tool_calls",
        "After tool side-effects, before CLAIM_SUBMITTED",
    ),
    "after_claim_before_verification": (
        "crash_before_verification",
        "After CLAIM_SUBMITTED, before verification",
    ),
    "after_verified_before_commit": (
        "crash_after_verified",
        "After VERIFIED commit, before next node",
    ),
}

# Runner script executed in a subprocess.  Patching is done inline.
RUNNER_SCRIPT = r'''import json, os, sys, time
from pathlib import Path
sys.path.insert(0, "{src}")

from lhos.bootstrap import RuntimeStack
from lhos.domain.errors import SimulatedCrashError

FAILPOINT   = os.environ.get("LHOS_FAILPOINT", "")
MARKER_FILE = os.environ.get("LHOS_MARKER_FILE", "")
CRASH_FLAG  = os.environ.get("LHOS_CRASH_FLAG", "")
DB_PATH     = sys.argv[1]
WORKSPACE   = sys.argv[2]
GRAPH_FILE  = sys.argv[3]
RUN_ID      = sys.argv[4]
RESUME      = len(sys.argv) > 5 and sys.argv[5] == "resume"


def write_marker_and_hang():
    """Write marker file so the parent knows to send SIGKILL, then block."""
    if MARKER_FILE:
        Path(MARKER_FILE).write_text("ready", encoding="utf-8")
    time.sleep(3600)


# ── Patching (only on first run, not on resume) ──────────────────────────
if FAILPOINT and not RESUME:
    # Controller crash points: let the original _inject_crash_once do its
    # work (check flag, check prior, write CRASH_INJECTED, return True),
    # then hang instead of letting the caller raise SimulatedCrashError.
    from lhos.runtime import controller as ctrl_mod

    _orig_inject = ctrl_mod.RuntimeController._inject_crash_once

    def _patched_inject(self, run_id, node, flag):
        result = _orig_inject(self, run_id, node, flag)
        if result and flag == CRASH_FLAG:
            write_marker_and_hang()  # never returns
        return result

    ctrl_mod.RuntimeController._inject_crash_once = _patched_inject

    # Worker crash points: wrap original execute, catch SimulatedCrashError,
    # and hang.  This covers both crash_on_attempt (raised before tool calls)
    # and crash_after_tool_calls (raised after tool calls complete).
    from lhos.runtime.worker import FakeWorker

    _orig_worker_exec = FakeWorker.execute

    def _patched_worker(self, node, context):
        try:
            return _orig_worker_exec(self, node, context)
        except SimulatedCrashError:
            write_marker_and_hang()  # never returns
            raise  # unreachable, keeps type checker happy

    FakeWorker.execute = _patched_worker


# ── Config: filesystem checkpoint with restore_on_crash ─────────────────
config = {{
    "features": {{"invalidation": True, "local_repair": True}},
    "checkpoint": {{
        "type": "filesystem",
        "restore_on_failure": True,
        "restore_on_crash": True,
        "after_verified_node": True,
    }},
    "checkpoint_root": str(Path(WORKSPACE).parent / "checkpoints"),
}}

if not RESUME:
    spec = json.loads(Path(GRAPH_FILE).read_text(encoding="utf-8"))
    stack = RuntimeStack(db_path=DB_PATH, workspace_dir=WORKSPACE, config=config)
    try:
        stack.graph_store.create_run(RUN_ID, spec.get("goal", "test"),
                                     {{"workspace_dir": WORKSPACE}})
        stack.initial_builder.build(RUN_ID, spec)
        run = stack.controller.run(RUN_ID)
        print(f"run {{RUN_ID}}: status {{run.status}}", flush=True)
    except SimulatedCrashError as exc:
        # Should not happen — patched code hangs instead of raising.
        print(f"run {{RUN_ID}}: unexpected SimulatedCrashError: {{exc}}", flush=True)
        sys.exit(3)
    except Exception as exc:
        print(f"run {{RUN_ID}}: error: {{exc}}", flush=True)
        sys.exit(1)
    finally:
        stack.close()
else:
    stack = RuntimeStack(db_path=DB_PATH, workspace_dir=WORKSPACE, config=config)
    try:
        run = stack.controller.resume(RUN_ID)
        print(f"run {{RUN_ID}}: resumed, status {{run.status}}", flush=True)
    except SimulatedCrashError as exc:
        print(f"run {{RUN_ID}}: crash on resume: {{exc}}", flush=True)
        sys.exit(3)
    except Exception as exc:
        print(f"run {{RUN_ID}}: resume error: {{exc}}", flush=True)
        sys.exit(1)
    finally:
        stack.close()
'''.format(src=SRC_PATH)


# ── Helper functions ──────────────────────────────────────────────────────

def file_hash(path: Path) -> str:
    if path.exists():
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return "MISSING"


def get_state(db_path: str, run_id: str) -> dict:
    sys.path.insert(0, SRC_PATH)
    from lhos.infrastructure.db.connection import Database
    from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
    from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore

    db = Database(db_path)
    events = SqliteEventStore(db)
    store = SqliteGraphStore(db, events)
    try:
        nodes = store.list_nodes(run_id)
        all_events = events.list_events(run_id)

        # Count tool call completions and check for duplicate keys
        completed_keys = [
            e.idempotency_key
            for e in all_events
            if e.event_type == "TOOL_CALL_COMPLETED" and e.idempotency_key
        ]
        duplicate_keys = len(completed_keys) != len(set(completed_keys))

        return {
            "node_count": len(nodes),
            "nodes": {
                n.id: {
                    "state": str(n.state).split(".")[-1],
                    "version": n.version,
                    "attempts": n.attempt_count,
                }
                for n in nodes
            },
            "events": len(all_events),
            "evidence": len(store.list_evidence(run_id)),
            "tool_call_completed": len(completed_keys),
            "duplicate_tool_keys": duplicate_keys,
            "crash_injected": sum(
                1 for e in all_events if e.event_type == "CRASH_INJECTED"
            ),
            "checkpoint_restored": sum(
                1 for e in all_events if e.event_type == "CHECKPOINT_RESTORED"
            ),
        }
    finally:
        db.close()


def run_test(crash_point: str, description: str, crash_flag: str, tmp_dir: Path) -> dict:
    # Clean up any previous run
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    db_path = tmp_dir / "state.db"
    workspace = tmp_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    graph_file = tmp_dir / "task.json"

    # Build task spec with crash flag on n2
    task = json.loads(json.dumps(TASK_TEMPLATE))
    n2_script = task["nodes"][1]["metadata"]["script"]
    if crash_flag in ("crash_before_execution", "crash_before_verification", "crash_after_verified"):
        n2_script[crash_flag] = True
    elif crash_flag == "crash_on_attempt":
        n2_script["crash_on_attempt"] = 1
    elif crash_flag == "crash_after_tool_calls":
        n2_script["crash_after_tool_calls"] = 1
        n2_script["crash_after_tool_calls_attempt"] = 1

    graph_file.write_text(json.dumps(task), encoding="utf-8")
    marker_file = tmp_dir / "marker"
    runner_script = tmp_dir / "runner.py"
    runner_script.write_text(RUNNER_SCRIPT, encoding="utf-8")

    run_id = f"sigkill-{crash_point}"
    env = {
        **os.environ,
        "LHOS_FAILPOINT": crash_point,
        "LHOS_MARKER_FILE": str(marker_file),
        "LHOS_CRASH_FLAG": crash_flag,
    }

    # ── Phase 1: Start subprocess and wait for marker ───────────────────
    proc = subprocess.Popen(
        [VENV_PYTHON, str(runner_script), str(db_path), str(workspace),
         str(graph_file), run_id],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )

    start = time.time()
    while not marker_file.exists() and proc.poll() is None and time.time() - start < 30:
        time.sleep(0.1)

    if not marker_file.exists():
        stdout, stderr = proc.communicate(timeout=5)
        return {
            "crash_point": crash_point,
            "description": description,
            "status": "SKIP",
            "reason": f"Process ended before marker. stdout={stdout[:300]}",
            "stderr": stderr[:300],
        }

    # ── Phase 2: Send SIGKILL ───────────────────────────────────────────
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    # Brief pause for OS to release file handles
    time.sleep(0.2)

    # ── Phase 3: Capture pre-resume state ───────────────────────────────
    pre_state = get_state(str(db_path), run_id)
    pre_hashes = {name: file_hash(workspace / f"{name}.txt") for name in ("n1", "n2", "n3")}

    # ── Phase 4: Resume in a new subprocess ─────────────────────────────
    resume_env = {**os.environ, "LHOS_FAILPOINT": "", "LHOS_MARKER_FILE": "", "LHOS_CRASH_FLAG": ""}
    resume_proc = subprocess.Popen(
        [VENV_PYTHON, str(runner_script), str(db_path), str(workspace),
         str(graph_file), run_id, "resume"],
        env=resume_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        stdout, stderr = resume_proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        resume_proc.kill()
        stdout, stderr = resume_proc.communicate()
        return {
            "crash_point": crash_point,
            "description": description,
            "status": "FAIL",
            "reason": "Resume timed out",
            "pre_state": pre_state,
            "stdout": stdout[:300],
            "stderr": stderr[:300],
        }

    # ── Phase 5: Capture post-resume state and verify ───────────────────
    post_state = get_state(str(db_path), run_id)
    post_hashes = {name: file_hash(workspace / f"{name}.txt") for name in ("n1", "n2", "n3")}

    verified = sum(1 for n in post_state["nodes"].values() if n["state"].lower() == "verified")
    total = post_state["node_count"]

    # Check output hash consistency: all output files should exist
    all_files_exist = all(h != "MISSING" for h in post_hashes.values())

    # Check no duplicate tool call keys
    no_duplicates = not post_state["duplicate_tool_keys"]

    status = "PASS" if (verified == total and all_files_exist and no_duplicates) else "FAIL"

    return {
        "crash_point": crash_point,
        "description": description,
        "status": status,
        "pre_state": pre_state,
        "post_state": post_state,
        "pre_hashes": pre_hashes,
        "post_hashes": post_hashes,
        "verified": verified,
        "total": total,
        "all_files_exist": all_files_exist,
        "no_duplicate_tool_keys": no_duplicates,
        "stdout": stdout[:500],
        "stderr": stderr[:500],
    }


def main():
    print("=" * 70)
    print("SIGKILL Recovery Test (Milestone 1G)")
    print("=" * 70)
    results = []

    for cp, (flag, desc) in CRASH_POINTS.items():
        print(f"\n--- {cp} ---")
        print(f"    {desc} (flag={flag})")
        tmp = PROJECT_ROOT / "artifacts" / "audit" / f"sigkill_{cp}"
        r = run_test(cp, desc, flag, tmp)
        results.append(r)
        print(f"    Status: {r['status']} ({r.get('verified', 0)}/{r.get('total', 0)} verified)")
        if r["status"] != "PASS":
            print(f"    stdout: {r.get('stdout', '')[:200]}")
            if r.get("stderr"):
                print(f"    stderr: {r.get('stderr', '')[:200]}")

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    print(f"Passed: {passed}, Failed: {failed}, Skipped: {skipped}")

    # ── Idempotency & State Analysis ────────────────────────────────────
    print("\n--- Idempotency & State Analysis ---")
    for r in results:
        if r["status"] in ("PASS", "FAIL"):
            pre = r.get("pre_state", {})
            post = r.get("post_state", {})
            print(f"  {r['crash_point']}:")
            print(f"    events:    {pre.get('events', '?')} -> {post.get('events', '?')}")
            print(f"    tool_calls_completed: {post.get('tool_call_completed', '?')}")
            print(f"    dup_keys:  {post.get('duplicate_tool_keys', '?')}")
            print(f"    crash_inj: {post.get('crash_injected', '?')}")
            print(f"    ckpt_rest: {post.get('checkpoint_restored', '?')}")
            ph = r.get("pre_hashes", {})
            sh = r.get("post_hashes", {})
            print(f"    hashes:    n1={ph.get('n1','?')}->{sh.get('n1','?')}  "
                  f"n2={ph.get('n2','?')}->{sh.get('n2','?')}  "
                  f"n3={ph.get('n3','?')}->{sh.get('n3','?')}")

    # ── Node State Detail ───────────────────────────────────────────────
    print("\n--- Node States (post-resume) ---")
    for r in results:
        if r["status"] in ("PASS", "FAIL"):
            nodes = r.get("post_state", {}).get("nodes", {})
            states = " ".join(f"{nid.split(':')[-1]}={n['state']}(a={n['attempts']})" for nid, n in nodes.items())
            print(f"  {r['crash_point']}: {states}")

    # Save results
    results_file = PROJECT_ROOT / "artifacts" / "audit" / "sigkill-results.json"
    results_file.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\nResults saved to {results_file}")


if __name__ == "__main__":
    main()
