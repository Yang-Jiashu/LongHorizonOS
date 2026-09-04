"""Regression tests for process-terminal/lease-admission ordering.

The process lifecycle transition and lease cleanup are intentionally separate
transactions in the current single-host kernel.  The important safety
invariant is therefore ordering: mark a process terminal *before* releasing
its leases, and reject any subsequent acquire for that PID.  This prevents a
terminal PID from opening a fresh ownership epoch in the cleanup window.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import ExitRequest, ProcessState
from lhos.agent_os.sdk.client import create_kernel


def test_terminal_process_cannot_acquire_new_lease(tmp_path) -> None:
    """Both terminal process states fence new LeaseService acquisitions."""
    kernel = create_kernel(str(tmp_path / "terminal-gate.db"))
    try:
        exited = kernel._process_service.spawn("exited")
        failed = kernel._process_service.spawn("failed")
        kernel._process_service.transition(exited.pid, ProcessState.EXITED)
        kernel._process_service.transition(failed.pid, ProcessState.FAILED)

        for pid in (exited.pid, failed.pid):
            with pytest.raises(LeaseAcquisitionFailed):
                kernel._lease_service.atomic_acquire(
                    pid,
                    [{"resource_id": f"resource:{pid}", "mode": "exclusive"}],
                )
            assert kernel._lease_service.list_leases_for_pid(pid) == []
    finally:
        kernel.close()


def test_exit_marks_terminal_before_cleanup_release(tmp_path) -> None:
    """An acquire during the cleanup window is rejected.

    ``release_all_for_pid`` is gated after its deletion transaction commits.
    With the required terminal-first ordering, the process is already EXITED
    while this gate is held.  The previous release-first ordering allowed the
    same PID to acquire a different resource at this exact point.
    """
    kernel = create_kernel(str(tmp_path / "exit-order.db"))
    release_entered = threading.Event()
    allow_release_return = threading.Event()
    exit_error: list[BaseException] = []

    try:
        pcb = kernel._process_service.spawn("exit-order")
        pid = pcb.pid
        kernel._lease_service.atomic_acquire(
            pid,
            [{"resource_id": "resource:R1", "mode": "exclusive"}],
        )

        original_release_all = kernel._lease_service.release_all_for_pid

        def gated_release(owner_pid: str) -> int:
            released = original_release_all(owner_pid)
            release_entered.set()
            if not allow_release_return.wait(5):
                raise RuntimeError("test did not release cleanup gate")
            return released

        # Instance-level replacement is deliberate: it gates only this
        # lifecycle call and leaves the service implementation under test
        # otherwise unchanged.
        kernel._lease_service.release_all_for_pid = gated_release  # type: ignore[method-assign]

        def exit_process() -> None:
            try:
                asyncio.run(
                    kernel._dispatcher.dispatch(
                        ExitRequest(pid=pid, exit_code="0"),
                    )
                )
            except BaseException as exc:
                exit_error.append(exc)

        worker = threading.Thread(target=exit_process)
        worker.start()
        assert release_entered.wait(5), "exit did not reach the cleanup gate"

        # The lease cleanup transaction has completed, but the lifecycle call
        # has not returned.  The process must already be terminal here.
        current = kernel._process_service.get_process(pid)
        assert current is not None
        assert current.state == ProcessState.EXITED

        with pytest.raises(LeaseAcquisitionFailed):
            kernel._lease_service.atomic_acquire(
                pid,
                [{"resource_id": "resource:R2", "mode": "exclusive"}],
            )

        allow_release_return.set()
        worker.join(5)
        assert not worker.is_alive()
        assert exit_error == []
        assert kernel._lease_service.list_leases_for_pid(pid) == []
    finally:
        allow_release_return.set()
        kernel.close()
