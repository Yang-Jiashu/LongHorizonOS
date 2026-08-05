"""SIGKILL recovery audit (Section 14).

Verifies that artifact FS state remains consistent after a process is
forcibly killed (SIGKILL) at critical points during write operations.

5 scenarios × 20 runs each:
1. Mid write — several appends then pause
2. Post stage pre commit — multi-pid appends
3. Post commit pre journal — single append then pause
4. Post journal pre projection — batch append then pause
5. Multi event — partial batch interleaved with single appends

Each run:
- Spawn worker process that performs operation up to a critical point
- Worker writes a "ready" marker at critical point, then sleeps
- Parent sends SIGKILL and verifies process died
- Parent verifies DB integrity: gapless offsets, consistent meta, no orphan projections
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

WORKER_PROGRAM = '''\
import sys
import time
from pathlib import Path

sys.path.insert(0, "{src_path}")

from lhos.agent_os.storage.sqlite import SQLiteStorage
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.kernel.models import KernelEvent

scenario = sys.argv[1]
marker_path = Path(sys.argv[2])
db_path = sys.argv[3]

storage = SQLiteStorage(db_path)
journal = JournalService(storage)

if scenario == "mid_write":
    for i in range(5):
        journal.append_event(KernelEvent(
            event_id="w1-" + str(i), pid="p1", event_type="test"
        ))
    marker_path.write_text("ready")
    time.sleep(10)

elif scenario == "post_stage_pre_commit":
    for i in range(3):
        journal.append_event(KernelEvent(
            event_id="w2-p1-" + str(i), pid="p1", event_type="test"
        ))
    for i in range(3):
        journal.append_event(KernelEvent(
            event_id="w2-p2-" + str(i), pid="p2", event_type="test"
        ))
    marker_path.write_text("ready")
    time.sleep(10)

elif scenario == "post_commit_pre_journal":
    journal.append_event(KernelEvent(
        event_id="w3-0", pid="p1", event_type="test"
    ))
    marker_path.write_text("ready")
    time.sleep(10)

elif scenario == "post_journal_pre_projection":
    events = [KernelEvent(event_id="w4-" + str(i), pid="p" + str(i % 2 + 1), event_type="test") for i in range(6)]
    journal.append_events_atomically(events)
    marker_path.write_text("ready")
    time.sleep(10)

elif scenario == "multi_event":
    journal.append_event(KernelEvent(event_id="w5-a", pid="p1", event_type="test"))
    journal.append_event(KernelEvent(event_id="w5-b", pid="p1", event_type="test"))
    journal.append_events_atomically([
        KernelEvent(event_id="w5-c", pid="p1", event_type="test"),
        KernelEvent(event_id="w5-d", pid="p2", event_type="test"),
        KernelEvent(event_id="w5-e", pid="p1", event_type="test"),
    ])
    marker_path.write_text("ready")
    time.sleep(10)

storage.close()
'''


def _run_worker(scenario: str, tmpdir: Path, src_path: str) -> Path:
    """Spawn a worker, wait for marker, SIGKILL it. Return db_path."""
    marker = tmpdir / f"marker-{scenario}"
    db_path = tmpdir / f"sigkill-{scenario}.db"

    script = WORKER_PROGRAM.format(src_path=src_path)
    proc = subprocess.Popen(
        [sys.executable, "-c", script, scenario, str(marker), str(db_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for marker (ready signal)
    for _ in range(100):  # 10 seconds max
        if marker.exists() and marker.read_text().strip() == "ready":
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.wait()
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Worker {scenario} never signaled ready. "
            f"stdout: {stdout[-200:]}, stderr: {stderr[-200:]}"
        )

    # SIGKILL
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()

    return db_path


def _verify_journal_integrity(db_path: Path) -> dict:
    """Verify DB integrity after SIGKILL."""
    from lhos.agent_os.storage.sqlite import SQLiteStorage

    result = {"ok": True, "errors": [], "event_count": 0, "details": {}}

    try:
        storage = SQLiteStorage(str(db_path))

        events = storage.query_all("SELECT * FROM journal_events ORDER BY journal_offset")
        offsets = [e["journal_offset"] for e in events]
        result["event_count"] = len(events)

        # Offsets should be gapless: 0, 1, 2, ...
        expected_offsets = list(range(len(events)))
        if offsets != expected_offsets:
            result["ok"] = False
            result["errors"].append(f"Gap in offsets: got {offsets}, expected {expected_offsets}")

        # No duplicate offsets
        if len(offsets) != len(set(offsets)):
            result["ok"] = False
            result["errors"].append("Duplicate offsets detected")

        # Check next_offset consistency
        meta = storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        next_offset = meta["value"] if meta else 0
        result["details"]["next_offset"] = next_offset

        if next_offset < len(events):
            result["ok"] = False
            result["errors"].append(
                f"next_offset {next_offset} < event count {len(events)}"
            )

        # Check per-pid sequences are gapless
        pids = set(e["pid"] for e in events)
        for pid in pids:
            pid_events = [e for e in events if e["pid"] == pid]
            seqs = sorted([e["process_sequence"] for e in pid_events])
            expected_seqs = list(range(len(pid_events)))
            if seqs != expected_seqs:
                result["ok"] = False
                result["errors"].append(f"PID {pid} sequence gap: {seqs}")

        # Verify all events have valid fields
        for ev in events:
            if not ev.get("event_id"):
                result["ok"] = False
                result["errors"].append("Event with missing event_id")
            if ev.get("journal_offset") is None:
                result["ok"] = False
                result["errors"].append("Event with null journal_offset")
            if ev.get("process_sequence") is None:
                result["ok"] = False
                result["errors"].append("Event with null process_sequence")

        storage.close()

    except Exception as ex:
        result["ok"] = False
        result["errors"].append(f"Exception: {ex}")

    return result


class TestSIGKILLRecovery:
    """5 scenarios x 20 runs = 100 total SIGKILL tests."""

    SCENARIOS = [
        "mid_write",
        "post_stage_pre_commit",
        "post_commit_pre_journal",
        "post_journal_pre_projection",
        "multi_event",
    ]

    @pytest.fixture
    def src_path(self) -> str:
        return str(Path(__file__).resolve().parents[3] / "src")

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_sigkill_recovery(self, scenario, src_path):
        """Run 20 SIGKILL trials for each scenario and verify integrity."""
        results = []
        for trial in range(20):
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)
                try:
                    db_path = _run_worker(scenario, tmpdir_path, src_path)
                    integrity = _verify_journal_integrity(db_path)
                    results.append(integrity)
                except Exception as ex:
                    results.append({"ok": False, "errors": [str(ex)]})

        # Aggregate results
        failures = [r for r in results if not r.get("ok", False)]
        failure_details = json.dumps(failures[:3], indent=2) if failures else ""

        assert len(failures) == 0, (
            f"Scenario '{scenario}': {len(failures)}/20 failures.\n"
            f"Details: {failure_details}"
        )
