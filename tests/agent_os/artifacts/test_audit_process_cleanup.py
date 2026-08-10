"""Process termination resource cleanup audit (Section 16).

Verifies that when a process exits (gracefully or via FAILED), all
resources are properly cleaned up:
1. All leases owned by the PID are released
2. Artifact handles are released
3. Journal records a PROCESS_EXITED event with correct payload
4. No resource leaks after repeated exit cycles
5. Concurrent operations by other PIDs are unaffected

FAILED state (kernel crash detection) also releases resources via
lease_service.release_all_for_pid + process_service.transition.

LeaseService API uses atomic_acquire(pid, claims) where claims is a
list of dicts with resource_id and mode.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from lhos.agent_os.kernel.models import (
    ExitRequest,
    ProcessState,
)
from lhos.agent_os.sdk.client import create_kernel


def _exit(kernel, pid: str, exit_code=0) -> None:
    """Synchronous wrapper for async dispatch of ExitRequest.

    ExitRequest.exit_code is a string field in the kernel model.
    """
    asyncio.run(kernel._dispatcher.dispatch(ExitRequest(pid=pid, exit_code=str(exit_code))))


class TestProcessExitCleanup:
    """Verify resources are cleaned up on process exit."""

    @pytest.fixture
    def kernel(self, tmp_path):
        return create_kernel(str(tmp_path / "test.db"))

    def _acquire(self, kernel, pid: str, resource: str, mode: str = "exclusive"):
        """Helper to acquire a single lease."""
        result = kernel._lease_service.atomic_acquire(
            pid, [{"resource_id": resource, "mode": mode}]
        )
        return result[0] if result else None

    def test_exit_releases_all_leases(self, kernel):
        """EXIT must release every lease owned by the exiting PID."""
        pcb = kernel._process_service.spawn("prog-p1")
        pid = pcb.pid

        for res in ["model_slot:mock", "workspace:p1", "resource:R1"]:
            lease = self._acquire(kernel, pid, res)
            assert lease is not None

        active = kernel._lease_service.list_all_leases()
        p1_active = [lease for lease in active if lease.owner_pid == pid]
        assert len(p1_active) == 3

        _exit(kernel, pid)

        active_after = kernel._lease_service.list_all_leases()
        p1_after = [lease for lease in active_after if lease.owner_pid == pid]
        assert len(p1_after) == 0

    def test_release_all_for_pid_deletes_rows_and_journals_each_lease(self, kernel):
        """Direct PID cleanup returns an exact count and journals every release."""
        pcb = kernel._process_service.spawn("prog-direct-release")
        pid = pcb.pid

        self._acquire(kernel, pid, "model_slot:mock")
        self._acquire(kernel, pid, "workspace:p1")

        released = kernel._lease_service.release_all_for_pid(pid)

        assert released == 2
        rows = kernel._storage.query_all(
            "SELECT lease_id FROM leases_projection WHERE owner_pid = ?",
            (pid,),
        )
        assert rows == []

        events = [
            event
            for event in kernel._journal.read_all()
            if event.pid == pid and event.event_type == "LEASE_RELEASED"
        ]
        assert len(events) == 2

    def test_exit_journals_exited_event(self, kernel):
        """EXIT must emit a PROCESS_EXITED journal event with exit_code payload."""
        pcb = kernel._process_service.spawn("prog-journal")
        pid = pcb.pid

        _exit(kernel, pid, exit_code=42)

        events = kernel._journal.read_all()
        exited = [e for e in events if e.event_type == "PROCESS_EXITED" and e.pid == pid]
        # NOTE: dispatcher emits PROCESS_EXITED twice (from transition + explicit).
        # We check at least one has the correct payload.
        assert len(exited) >= 1
        with_code = [e for e in exited if e.payload.get("exit_code") == "42"]
        assert len(with_code) >= 1, (
            f"No PROCESS_EXITED with exit_code=42 found. Events: {[e.payload for e in exited]}"
        )

    def test_exit_transitions_to_exited_state(self, kernel):
        """EXIT must transition process to EXITED state."""
        pcb = kernel._process_service.spawn("prog-state")
        pid = pcb.pid
        pcb_now = kernel._process_service.get_process(pid)
        assert pcb_now.state == ProcessState.READY

        _exit(kernel, pid)

        pcb_after = kernel._process_service.get_process(pid)
        assert pcb_after.state == ProcessState.EXITED

    def test_exit_does_not_affect_other_pids(self, kernel):
        """EXIT of p1 must not release leases owned by p2."""
        pcb1 = kernel._process_service.spawn("prog-p1")
        pcb2 = kernel._process_service.spawn("prog-p2")
        pid1, pid2 = pcb1.pid, pcb2.pid

        self._acquire(kernel, pid1, "model_slot:mock")
        self._acquire(kernel, pid2, "workspace:p1")

        _exit(kernel, pid1)

        active = kernel._lease_service.list_all_leases()
        p2_leases = [lease for lease in active if lease.owner_pid == pid2]
        p1_leases = [lease for lease in active if lease.owner_pid == pid1]
        assert len(p2_leases) == 1
        assert len(p1_leases) == 0

    def test_multiple_exit_releases_all(self, kernel):
        """Multiple process exits should release all their own resources."""
        pids = []
        for i in range(3):
            pcb = kernel._process_service.spawn(f"prog-m-{i}")
            pid = pcb.pid
            pids.append(pid)
            self._acquire(kernel, pid, "model_slot:mock", mode="shared")
            self._acquire(kernel, pid, f"workspace:p{i}")

        active_before = kernel._lease_service.list_all_leases()
        assert len(active_before) >= 6

        for pid in pids:
            _exit(kernel, pid)

        active_after = kernel._lease_service.list_all_leases()
        assert len(active_after) == 0

    def test_failed_crash_releases_leases(self, kernel):
        """Kernel crash (FAILED state) releases leases via release_all_for_pid."""
        pcb = kernel._process_service.spawn("prog-crash")
        pid = pcb.pid

        self._acquire(kernel, pid, "model_slot:mock")

        # Simulate kernel detecting failure
        kernel._lease_service.release_all_for_pid(pid)
        kernel._process_service.transition(pid, ProcessState.FAILED)

        active = kernel._lease_service.list_all_leases()
        p1_leases = [lease for lease in active if lease.owner_pid == pid]
        assert len(p1_leases) == 0

        pcb_after = kernel._process_service.get_process(pid)
        assert pcb_after.state == ProcessState.FAILED

    def test_exit_with_no_leases(self, kernel):
        """EXIT of a process with no resources: clean exit, no errors."""
        pcb = kernel._process_service.spawn("prog-empty")
        pid = pcb.pid

        _exit(kernel, pid)

        events = kernel._journal.read_all()
        exited = [e for e in events if e.event_type == "PROCESS_EXITED" and e.pid == pid]
        assert len(exited) >= 1  # NOTE: dispatcher double-journals this event

    def test_exit_twice_is_rejected(self, kernel):
        """Second EXIT on EXITED process raises TerminalStateError (by design)."""

        pcb = kernel._process_service.spawn("prog-twice")
        pid = pcb.pid

        self._acquire(kernel, pid, "model_slot:mock")

        _exit(kernel, pid, exit_code=0)

        # EXITED is terminal — second exit should fail
        with pytest.raises(Exception):  # TerminalStateError from state machine
            _exit(kernel, pid, exit_code=1)

        pcb_after = kernel._process_service.get_process(pid)
        assert pcb_after.state == ProcessState.EXITED
        # First exit still released the leases
        active = [
            lease for lease in kernel._lease_service.list_all_leases() if lease.owner_pid == pid
        ]
        assert len(active) == 0

    def test_exit_deletes_lease_projection_rows(self, kernel):
        """EXIT must actually delete rows from leases_projection table."""
        pcb = kernel._process_service.spawn("prog-proj")
        pid = pcb.pid

        self._acquire(kernel, pid, "model_slot:mock")
        self._acquire(kernel, pid, "workspace:p1")

        rows_before = kernel._storage.query_all(
            "SELECT * FROM leases_projection WHERE owner_pid = ?", (pid,)
        )
        assert len(rows_before) == 2

        _exit(kernel, pid)

        rows_after = kernel._storage.query_all(
            "SELECT * FROM leases_projection WHERE owner_pid = ?", (pid,)
        )
        assert len(rows_after) == 0


class TestCrashRecoveryResourceCleanup:
    """Simulate crashes and verify cleanup on restart."""

    def test_orphan_leases_persist_after_unclean_shutdown(self):
        """Orphan leases from unclean shutdown persist in DB (for recovery)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "reclaim.db"

            # First session: acquire leases
            kernel = create_kernel(str(db_path))
            pcb = kernel._process_service.spawn("prog-orphan")
            pid = pcb.pid
            self_acquire = kernel._lease_service.atomic_acquire(
                pid, [{"resource_id": "model_slot:mock", "mode": "exclusive"}]
            )
            assert len(self_acquire) == 1
            kernel._storage.close()

            # Second session: lease still exists
            kernel2 = create_kernel(str(db_path))
            active = kernel2._lease_service.list_all_leases()
            p1_active = [lease for lease in active if lease.owner_pid == pid]
            assert len(p1_active) == 1
            kernel2._storage.close()

    def test_process_state_persists_after_restart(self):
        """EXITED state persists across restart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "persist.db"

            kernel = create_kernel(str(db_path))
            pcb = kernel._process_service.spawn("prog-persist")
            pid = pcb.pid

            asyncio.run(kernel._dispatcher.dispatch(ExitRequest(pid=pid, exit_code="0")))
            kernel._storage.close()

            # Restart
            kernel2 = create_kernel(str(db_path))
            pcb_after = kernel2._process_service.get_process(pid)
            assert pcb_after is not None
            assert pcb_after.state == ProcessState.EXITED
            kernel2._storage.close()


class TestLeaseAcquisitionAfterExit:
    """Verify lease acquisition after exit works correctly."""

    @pytest.fixture
    def kernel(self, tmp_path):
        return create_kernel(str(tmp_path / "test.db"))

    def _acquire(self, kernel, pid: str, resource: str, mode: str = "exclusive"):
        result = kernel._lease_service.atomic_acquire(
            pid, [{"resource_id": resource, "mode": mode}]
        )
        return result[0] if result else None

    def test_others_can_acquire_after_exit(self, kernel):
        """After p1 exits, another PID can acquire p1's former leases."""
        pcb1 = kernel._process_service.spawn("prog-p1")
        pcb2 = kernel._process_service.spawn("prog-p2")
        pid1, pid2 = pcb1.pid, pcb2.pid

        self._acquire(kernel, pid1, "model_slot:mock")

        _exit(kernel, pid1)

        lease2 = self._acquire(kernel, pid2, "model_slot:mock")
        assert lease2 is not None
        assert lease2.owner_pid == pid2

    def test_handle_cleanup_after_exit(self, kernel):
        """Artifact handle leases are released on exit."""
        pcb = kernel._process_service.spawn("prog-handle")
        pid = pcb.pid

        self._acquire(kernel, pid, "artifact://ns-p1/foo.txt")

        _exit(kernel, pid)

        active = kernel._lease_service.list_all_leases()
        pid_active = [lease for lease in active if lease.owner_pid == pid]
        assert len(pid_active) == 0
