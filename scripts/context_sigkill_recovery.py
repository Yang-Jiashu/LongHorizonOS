"""Authentic OS-level SIGKILL recovery test for Context VM (Phase C2 audit S20).

Unlike the prior in-process SIGKILL ``simulations`` (which rebuild service state
in the same process), this test:

1. Uses **persistent** state:
   - file-backed SQLite for ArtifactProjections + Journal (so artifact metadata
     survives process death)
   - file-backed LocalArtifactStorageDriver (so committed ArtifactVersion bytes
     survive process death)
2. Spawns a *child* Python process per trial that:
   - writes a set of test artifacts,
   - builds a ContextManifest,
   - calls ContextSDK.load() to materialize a WorkingSet,
   - writes a small recovery file with (pid, artifact binding fields,
     observed materialized_hash, observed page content digests),
   - sends itself SIGKILL (signal 9).
3. The parent process observes the SIGKILL death, then spawns a *fresh* child
   process that:
   - opens the SAME persistent SQLite + CAS dirs (so the committed
     ArtifactVersions are reachable in the new process),
   - rebuilds the manifest from the persisted binding fields,
   - calls ContextSDK.load() to re-materialize,
   - asserts the new output is byte-identical to the pre-kill observation
     (this proves WorkingSet materialization is deterministic AND that
     committed ArtifactVersions are durable across process death).

The snapshot itself is in-memory only — so SIGKILL also verifies the
**re-materialization path** from committed ArtifactVersion bytes, which is the
stronger durability claim.

Run:
    .venv/bin/python scripts/context_sigkill_recovery.py
    .venv/bin/python scripts/context_sigkill_recovery.py --trials 25   # quick smoke
"""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = str(PROJECT_ROOT / ".venv" / "bin" / "python")
SRC_PATH = str(PROJECT_ROOT / "src")


# ── Child process entrypoints ────────────────────────────────────────────────


LOAD_SCRIPT = r"""
import json, os, signal, sys, hashlib
from pathlib import Path
sys.path.insert(0, "{SRC_PATH}")

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage

STATE_DIR   = Path(sys.argv[1])
RELOAD_FILE = STATE_DIR / "reload.json"


class _AllowsAllCaps:
    def can_context_operation(self, **kw): return True
    def can_artifact_read(self, **kw): return True


def build_env(state_dir: Path):
    storage = SQLiteStorage(str(state_dir / "state.db"))
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(state_dir / "cas")
    cap_svc = CapabilityService(storage, journal)
    ns_svc = NamespaceService(projections, journal)
    artifact_svc = ArtifactFSService(projections, driver, journal,
                                     capability_service=cap_svc)
    artifact_svc._ns_resolver = ns_svc  # type: ignore[attr-defined]
    ns_svc.create_namespace("p1")
    cap_svc.grant("p1", Capability(
        resource_pattern="artifact://ns-p1/**", operations={"read", "write"}
    ))
    artifact_sdk = ArtifactSDK(artifact_svc, ns_svc)

    class _Sup:
        def __init__(self, svc, pid):
            self._svc, self._pid = svc, pid
        def read_version(self, *, artifact_id, version, canonical_uri):
            return self._svc.read(pid=self._pid, uri=canonical_uri,
                                  version=version)

    ctx_svc = ContextService(
        content_supplier=_Sup(artifact_svc, "p1"),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    return artifact_sdk, ctx_svc


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _refs_for(artifact_sdk, pid, ws_uri):
    vrow = next(iter(artifact_sdk.list_versions(pid, ws_uri)))
    rows = artifact_sdk.list(pid)
    arow = next(r for r in rows if r["artifact_id"] == vrow["artifact_id"])
    return dict(
        canonical_uri=arow["canonical_uri"],
        artifact_id=vrow["artifact_id"],
        version=vrow["version"],
        content_hash=vrow["content_hash"],
    )


# Phase 1: write artifacts + load manifest + snapshot + persist recovery info.
ARTIFACTS = [
    ("workspace:///doc_alpha.md", b"alpha content " * 17),
    ("workspace:///doc_beta.md",  b"beta content " * 19),
    ("workspace:///doc_gamma.md", b"gamma content " * 23),
]
PID = "p1"

artifact_sdk, ctx_svc = build_env(STATE_DIR)
for uri, content in ARTIFACTS:
    artifact_sdk.write(PID, uri, content, "write-" + uri)

refs_list = [_refs_for(artifact_sdk, PID, uri) for (uri, _) in ARTIFACTS]
manifest = ContextManifest(
    manifest_id="ctx-sigkill",
    owner_pid=PID,
    refs=tuple(
        ContentRef(
            ref_id="r%d" % i,
            canonical_uri=r["canonical_uri"],
            artifact_id=r["artifact_id"],
            version=r["version"],
            content_hash=r["content_hash"],
            media_type="text/plain",
        )
        for i, r in enumerate(refs_list)
    ),
    token_budget=1_000_000,
    page_size_bytes=64,
)
sdk = ContextSDK(ctx_svc)
h, loaded = sdk.load(pid=PID, manifest=manifest,
                     idempotency_key="ctx-sigkill")

page_digests = [
    {"page_id": p.page_id, "sha": _sha(p.content), "size": p.size_bytes}
    for p in loaded.ordered_pages
]
snapshot = sdk.snapshot(pid=PID, context_id=loaded.context_id,
                        idempotency_key="ctx-sigkill")

reload_data = {
    "pid": PID,
    "manifest_id": "ctx-sigkill",
    "token_budget": 1_000_000,
    "page_size_bytes": 64,
    "idempotency_key": "ctx-sigkill",
    "manifest_hash": loaded.manifest_hash,
    "materialized_hash": loaded.materialized_hash,
    "snapshot_id": snapshot.snapshot_id,
    "tokens_used": loaded.tokens_used,
    "bytes_used": loaded.bytes_used,
    "bindings": refs_list,
    "page_digests": page_digests,
    "ordered_contents": [p.content.hex() for p in loaded.ordered_pages],
}
RELOAD_FILE.write_text(json.dumps(reload_data), encoding="utf-8")

# Handshake: stdout "READY" + short sleep so parent can SIGKILL us.
print("READY", flush=True)
import time as _time
_time.sleep(0.2)

# Send ourselves a real SIGKILL — kernel terminates us immediately.
os.kill(os.getpid(), signal.SIGKILL)
# Unreachable.
""".replace("{SRC_PATH}", SRC_PATH)


RECOVER_SCRIPT = r"""
import json, sys, hashlib
from pathlib import Path
sys.path.insert(0, "{SRC_PATH}")

from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage

STATE_DIR   = Path(sys.argv[1])
RELOAD_FILE = STATE_DIR / "reload.json"
OK_FILE     = STATE_DIR / "reload_ok.json"


class _AllowsAllCaps:
    def can_context_operation(self, **kw): return True
    def can_artifact_read(self, **kw): return True


def build_env(state_dir: Path):
    storage = SQLiteStorage(str(state_dir / "state.db"))
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(state_dir / "cas")
    # No CapabilityService needed — we sidestep capability checking entirely
    artifact_svc = ArtifactFSService(projections, driver, journal)

    class _Sup:
        def __init__(self, svc, pid):
            self._svc, self._pid = svc, pid
        def read_version(self, *, artifact_id, version, canonical_uri):
            return self._svc.read(pid=self._pid, uri=canonical_uri,
                                  version=version)

    ctx_svc = ContextService(
        content_supplier=_Sup(artifact_svc, "p1"),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    return ctx_svc


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


rel = json.loads(RELOAD_FILE.read_text(encoding="utf-8"))

# Fresh service on the SAME persistent SQLite + CAS files. Any in-memory
# snapshot from the prior (now killed) process is gone.
ctx_svc = build_env(STATE_DIR)
sdk = ContextSDK(ctx_svc)

manifest = ContextManifest(
    manifest_id=rel["manifest_id"],
    owner_pid=rel["pid"],
    refs=tuple(
        ContentRef(
            ref_id="r%d" % i,
            canonical_uri=b["canonical_uri"],
            artifact_id=b["artifact_id"],
            version=b["version"],
            content_hash=b["content_hash"],
            media_type="text/plain",
        )
        for i, b in enumerate(rel["bindings"])
    ),
    token_budget=rel["token_budget"],
    page_size_bytes=rel["page_size_bytes"],
)

# Same idempotency key as pre-kill load. Even if the snapshot had been
# durable this would hit the idem cache — but since it's gone, this is a
# genuine re-materialization from committed ArtifactVersion bytes.
h, loaded = sdk.load(pid=rel["pid"], manifest=manifest,
                     idempotency_key=rel["idempotency_key"])

# Verify the fresh materialization matches the pre-kill observation byte-for-byte.
match_materialized = loaded.materialized_hash == rel["materialized_hash"]
fresh_contents = [p.content.hex() for p in loaded.ordered_pages]
match_contents = fresh_contents == rel["ordered_contents"]
fresh_digests = [
    {"page_id": p.page_id, "sha": _sha(p.content), "size": p.size_bytes}
    for p in loaded.ordered_pages
]
match_digests = fresh_digests == rel["page_digests"]

all_ok = match_materialized and match_contents and match_digests
OK_FILE.write_text(json.dumps({
    "ok": all_ok,
    "match_materialized": match_materialized,
    "match_contents": match_contents,
    "match_digests": match_digests,
    "pre_kill_materialized_hash": rel["materialized_hash"],
    "post_kill_materialized_hash": loaded.materialized_hash,
    "pre_kill_page_count": len(rel["page_digests"]),
    "post_kill_page_count": len(fresh_digests),
}, indent=2), encoding="utf-8")
print("OK" if all_ok else "MISMATCH", flush=True)
""".replace("{SRC_PATH}", SRC_PATH)


# ── Parent orchestrator ──────────────────────────────────────────────────────


def run_one_trial(trial_idx: int, state_dir: Path) -> dict:
    """Spawn a load child (dies via SIGKILL), then a recover child (verifies)."""
    if state_dir.exists():
        shutil.rmtree(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    load_script = state_dir / "load.py"
    recover_script = state_dir / "recover.py"
    load_script.write_text(LOAD_SCRIPT, encoding="utf-8")
    recover_script.write_text(RECOVER_SCRIPT, encoding="utf-8")

    # ── Phase 1: load child — must print READY then die by SIGKILL. ────────
    load_proc = subprocess.run(
        [VENV_PYTHON, str(load_script), str(state_dir)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # After SIGKILL, subprocess.run returncode is -9 on POSIX.
    died_by_sigkill = load_proc.returncode == -signal.SIGKILL
    ready_seen = "READY" in load_proc.stdout

    if not died_by_sigkill:
        return {
            "trial": trial_idx,
            "status": "FAIL",
            "reason": (
                f"load child did not die by SIGKILL "
                f"(returncode={load_proc.returncode}, ready={ready_seen})"
            ),
            "load_stdout": load_proc.stdout[:500],
            "load_stderr": load_proc.stderr[:500],
        }

    reload_file = state_dir / "reload.json"
    if not reload_file.exists():
        return {
            "trial": trial_idx,
            "status": "FAIL",
            "reason": "no reload.json after load child death",
            "load_stdout": load_proc.stdout[:500],
        }

    # ── Phase 2: recover child — must succeed and persist reload_ok.json ───
    rec_proc = subprocess.run(
        [VENV_PYTHON, str(recover_script), str(state_dir)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if rec_proc.returncode != 0:
        return {
            "trial": trial_idx,
            "status": "FAIL",
            "reason": f"recover child exit {rec_proc.returncode}",
            "rec_stdout": rec_proc.stdout[:500],
            "rec_stderr": rec_proc.stderr[:500],
        }

    ok_file = state_dir / "reload_ok.json"
    if not ok_file.exists():
        return {
            "trial": trial_idx,
            "status": "FAIL",
            "reason": "no reload_ok.json after recover child",
            "rec_stdout": rec_proc.stdout[:500],
        }

    ok = json.loads(ok_file.read_text(encoding="utf-8"))
    return {
        "trial": trial_idx,
        "status": "PASS" if ok["ok"] else "FAIL",
        "ok": ok,
        "load_returncode": load_proc.returncode,
        "recover_returncode": rec_proc.returncode,
        "died_by_sigkill": True,
        "ready_seen": ready_seen,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trials", type=int, default=125, help="Number of SIGKILL trials (default 125)"
    )
    parser.add_argument(
        "--base",
        type=str,
        default=str(PROJECT_ROOT / "artifacts" / "agent_os_phase_c2_audit" / "sigkill_runs"),
        help="Base directory for per-trial state dirs",
    )
    parser.add_argument(
        "--keep-sample",
        type=int,
        default=0,
        help="Keep the state dir of the N-th trial for inspection",
    )
    args = parser.parse_args()

    base = Path(args.base)
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)

    print("Context VM authentic SIGKILL recovery test")
    print(f"Trials:   {args.trials}")
    print(f"State at: {base}")
    print(f"Python:   {VENV_PYTHON}")
    print()

    results: list[dict] = []
    t0 = time.time()
    for i in range(1, args.trials + 1):
        state_dir = base / f"trial_{i:04d}"
        r = run_one_trial(i, state_dir)
        results.append(r)
        tag = r["status"]
        if i % 10 == 0 or tag != "PASS":
            print(f"  [{i:3d}/{args.trials}] {tag}")
        keep_this = args.keep_sample == i
        if not keep_this:
            shutil.rmtree(state_dir, ignore_errors=True)
        elif keep_this:
            print(f"  (kept sample trial {i} at {state_dir})")

    dt = time.time() - t0
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = [r for r in results if r["status"] != "PASS"]

    print()
    print("=" * 60)
    print(f"PASSED: {passed}/{args.trials} in {dt:.1f}s")
    print(f"FAILED: {len(failed)}")
    if failed:
        print()
        print("FAILURES (first 10):")
        for r in failed[:10]:
            print(f"  trial {r['trial']}: {r.get('reason', '?')}")
            for k in ("load_stdout", "load_stderr", "rec_stdout", "rec_stderr"):
                if r.get(k):
                    print(f"    {k}: {r[k]}")

    # Persist the full results.
    summary = {
        "trials": args.trials,
        "passed": passed,
        "failed": len(failed),
        "duration_s": dt,
        "failures": failed[:50],
    }
    out = base / "summary.json"
    out.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print()
    print(f"Summary saved to {out}")

    # Overall result: only pass if ALL trials pass.
    sys.exit(0 if len(failed) == 0 else 1)
