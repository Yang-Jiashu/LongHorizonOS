import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "/Users/jiashuyang/Documents/kimi/Workspaces/longhorizonOS/longhorizonos/src")

from lhos.bootstrap import RuntimeStack
from lhos.domain.errors import SimulatedCrashError

FAILPOINT = os.environ.get("LHOS_FAILPOINT", "")
MARKER_FILE = os.environ.get("LHOS_MARKER_FILE", "")
CRASH_FLAG = os.environ.get("LHOS_CRASH_FLAG", "")
DB_PATH = sys.argv[1]
WORKSPACE = sys.argv[2]
GRAPH_FILE = sys.argv[3]
RUN_ID = sys.argv[4]
RESUME = len(sys.argv) > 5 and sys.argv[5] == "resume"


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
config = {
    "features": {"invalidation": True, "local_repair": True},
    "checkpoint": {
        "type": "filesystem",
        "restore_on_failure": True,
        "restore_on_crash": True,
        "after_verified_node": True,
    },
    "checkpoint_root": str(Path(WORKSPACE).parent / "checkpoints"),
}

if not RESUME:
    spec = json.loads(Path(GRAPH_FILE).read_text(encoding="utf-8"))
    stack = RuntimeStack(db_path=DB_PATH, workspace_dir=WORKSPACE, config=config)
    try:
        stack.graph_store.create_run(RUN_ID, spec.get("goal", "test"), {"workspace_dir": WORKSPACE})
        stack.initial_builder.build(RUN_ID, spec)
        run = stack.controller.run(RUN_ID)
        print(f"run {RUN_ID}: status {run.status}", flush=True)
    except SimulatedCrashError as exc:
        # Should not happen — patched code hangs instead of raising.
        print(f"run {RUN_ID}: unexpected SimulatedCrashError: {exc}", flush=True)
        sys.exit(3)
    except Exception as exc:
        print(f"run {RUN_ID}: error: {exc}", flush=True)
        sys.exit(1)
    finally:
        stack.close()
else:
    stack = RuntimeStack(db_path=DB_PATH, workspace_dir=WORKSPACE, config=config)
    try:
        run = stack.controller.resume(RUN_ID)
        print(f"run {RUN_ID}: resumed, status {run.status}", flush=True)
    except SimulatedCrashError as exc:
        print(f"run {RUN_ID}: crash on resume: {exc}", flush=True)
        sys.exit(3)
    except Exception as exc:
        print(f"run {RUN_ID}: resume error: {exc}", flush=True)
        sys.exit(1)
    finally:
        stack.close()
