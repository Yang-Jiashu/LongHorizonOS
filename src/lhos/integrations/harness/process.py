"""Process-tree lifecycle used by external Harness adapters."""

from __future__ import annotations

import asyncio
import inspect
import os
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _create_windows_kill_job(pid: int) -> int | None:
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    ULONG_PTR = wintypes.WPARAM
    SIZE_T = ctypes.c_size_t

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", SIZE_T),
            ("MaximumWorkingSetSize", SIZE_T),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ULONG_PTR),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", SIZE_T),
            ("JobMemoryLimit", SIZE_T),
            ("PeakProcessMemoryUsed", SIZE_T),
            ("PeakJobMemoryUsed", SIZE_T),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(
        job,
        9,  # JobObjectExtendedLimitInformation
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        kernel32.CloseHandle(job)
        return None
    process_handle = kernel32.OpenProcess(
        0x0001 | 0x0100 | 0x1000,  # TERMINATE | SET_QUOTA | QUERY_LIMITED_INFORMATION
        False,
        pid,
    )
    if not process_handle:
        kernel32.CloseHandle(job)
        return None
    try:
        if not kernel32.AssignProcessToJobObject(job, process_handle):
            kernel32.CloseHandle(job)
            return None
    finally:
        kernel32.CloseHandle(process_handle)
    return int(job)


def _terminate_windows_job(job_handle: int) -> bool:
    if os.name != "nt" or not job_handle:
        return False
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    return bool(kernel32.TerminateJobObject(wintypes.HANDLE(job_handle), 1))


def _close_windows_job(job_handle: int | None) -> None:
    if os.name != "nt" or not job_handle:
        return
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.CloseHandle(wintypes.HANDLE(job_handle))


@dataclass(frozen=True, slots=True)
class ManagedProcessResult:
    command: tuple[str, ...]
    pid: int
    exit_code: int | None
    elapsed_ms: int
    stdout_tail: str
    stderr_tail: str
    timed_out: bool
    terminated_by: str | None
    hard_killed: bool


async def _read_bounded_tail(
    stream: asyncio.StreamReader | None,
    *,
    limit: int,
) -> bytes:
    """Drain a child pipe while retaining only its bounded byte tail."""

    if stream is None:
        return b""
    tail = bytearray()
    while chunk := await stream.read(64 * 1024):
        if len(chunk) >= limit:
            tail = bytearray(chunk[-limit:])
            continue
        overflow = len(tail) + len(chunk) - limit
        if overflow > 0:
            del tail[:overflow]
        tail.extend(chunk)
    return bytes(tail)


def _decode_tail(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


async def _wait_for_exit(process: asyncio.subprocess.Process, timeout: float) -> bool:
    try:
        await asyncio.wait_for(process.wait(), timeout=timeout)
        return True
    except TimeoutError:
        return False


async def _wait_for_process_group_exit(process_group_id: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            pass
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.02)


async def _terminate_process_tree(
    process: asyncio.subprocess.Process,
    *,
    grace_period_seconds: float,
    windows_job_handle: int | None = None,
) -> bool:
    """Terminate a process group/tree, then hard-kill after the grace period."""

    if os.name == "nt":
        # Without a Job Object, a graceful CTRL_BREAK has a race: the parent
        # can exit before an uncooperative descendant, after which taskkill
        # can no longer discover the tree by the original PID.  Headless DSH
        # therefore uses the forceful native tree operation immediately.
        terminated = _terminate_windows_job(windows_job_handle)
        if not terminated and process.returncode is None:
            await asyncio.to_thread(
                subprocess.run,
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
            with suppress(ProcessLookupError, PermissionError, OSError):
                process.kill()
            if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
                raise RuntimeError(
                    f"failed to reap Windows process tree rooted at PID {process.pid}"
                )
        return True

    with suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(process.pid, signal.SIGTERM)
    if await _wait_for_process_group_exit(process.pid, grace_period_seconds):
        if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
            with suppress(ProcessLookupError, PermissionError, OSError):
                process.kill()
            if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
                raise RuntimeError(f"failed to reap process PID {process.pid}")
        return False

    with suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(process.pid, signal.SIGKILL)
    await _wait_for_process_group_exit(process.pid, max(1.0, grace_period_seconds))
    if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
        with suppress(ProcessLookupError, PermissionError, OSError):
            process.kill()
        if not await _wait_for_exit(process, max(1.0, grace_period_seconds)):
            raise RuntimeError(f"failed to reap process tree rooted at PID {process.pid}")
    return True


async def run_managed_process(
    command: Sequence[str],
    *,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float | None = None,
    cancellation_token: Any | None = None,
    grace_period_seconds: float = 2.0,
    poll_seconds: float = 0.05,
    max_capture_bytes: int = 128_000,
    on_started: Any | None = None,
) -> ManagedProcessResult:
    """Run one argv directly and retain control of its descendant process tree."""

    argv = tuple(str(item) for item in command)
    if not argv or not argv[0].strip():
        raise ValueError("managed process command must be a non-empty argv")
    if isinstance(command, (str, bytes)):
        raise ValueError("managed process command must be argv, not a shell string")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if grace_period_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("grace_period_seconds and poll_seconds must be positive")
    if max_capture_bytes < 1:
        raise ValueError("max_capture_bytes must be positive")

    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        start_new_session = True
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        *argv,
        cwd=None if cwd is None else str(Path(cwd)),
        env=None if env is None else dict(env),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        creationflags=creationflags,
        start_new_session=start_new_session,
    )
    windows_job_handle = _create_windows_kill_job(process.pid)
    wait_task = asyncio.create_task(process.wait())
    stdout_task = asyncio.create_task(_read_bounded_tail(process.stdout, limit=max_capture_bytes))
    stderr_task = asyncio.create_task(_read_bounded_tail(process.stderr, limit=max_capture_bytes))
    timed_out = False
    terminated_by: str | None = None
    hard_killed = False
    deadline = None if timeout_seconds is None else started + float(timeout_seconds)
    drain_deadline: float | None = None

    try:
        if on_started is not None:
            started_result = on_started(process.pid)
            if inspect.isawaitable(started_result):
                await started_result
        while not (wait_task.done() and stdout_task.done() and stderr_task.done()):
            if cancellation_token is not None and bool(
                getattr(cancellation_token, "request_pending", False)
            ):
                terminated_by = "semantic_interrupt"
                hard_killed = await _terminate_process_tree(
                    process,
                    grace_period_seconds=grace_period_seconds,
                    windows_job_handle=windows_job_handle,
                )
                break
            if deadline is not None and time.monotonic() >= deadline:
                timed_out = True
                terminated_by = "wall_clock_timeout"
                hard_killed = await _terminate_process_tree(
                    process,
                    grace_period_seconds=grace_period_seconds,
                    windows_job_handle=windows_job_handle,
                )
                break
            if wait_task.done() and drain_deadline is None:
                drain_deadline = time.monotonic() + max(5.0, grace_period_seconds)
            if drain_deadline is not None and time.monotonic() >= drain_deadline:
                terminated_by = "pipe_drain_timeout"
                hard_killed = await _terminate_process_tree(
                    process,
                    grace_period_seconds=grace_period_seconds,
                    windows_job_handle=windows_job_handle,
                )
                break
            await asyncio.sleep(poll_seconds)
        if not wait_task.done() and not await _wait_for_exit(
            process,
            max(1.0, grace_period_seconds),
        ):
            raise RuntimeError(f"managed process PID {process.pid} did not exit")
        await wait_task
        stdout, stderr = await asyncio.wait_for(
            asyncio.gather(stdout_task, stderr_task),
            timeout=max(5.0, grace_period_seconds),
        )
    except BaseException:
        if process.returncode is None:
            hard_killed = (
                await _terminate_process_tree(
                    process,
                    grace_period_seconds=grace_period_seconds,
                    windows_job_handle=windows_job_handle,
                )
                or hard_killed
            )
        with suppress(BaseException):
            await wait_task
        for task in (stdout_task, stderr_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        raise
    finally:
        _close_windows_job(windows_job_handle)

    return ManagedProcessResult(
        command=argv,
        pid=process.pid,
        exit_code=process.returncode,
        elapsed_ms=max(0, round((time.monotonic() - started) * 1000)),
        stdout_tail=_decode_tail(stdout),
        stderr_tail=_decode_tail(stderr),
        timed_out=timed_out,
        terminated_by=terminated_by,
        hard_killed=hard_killed,
    )


__all__ = ["ManagedProcessResult", "run_managed_process"]
