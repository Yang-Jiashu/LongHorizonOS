"""A real, killable Harness execution boundary backed by a child process.

:class:`CallableHarnessAdapter` runs Agent work *in-process* and, as its own
docstring admits, "cannot kill an uncooperative Python callable".  This module
adds :class:`SubprocessHarnessAdapter`, which implements the identical
:class:`~lhos.sdk.harness.HarnessSessionAdapter` protocol but runs the work in a
child process created with ``subprocess.Popen`` (never ``shell=True``).  Because
the work lives in a separate OS process, ``PREEMPT`` can actually terminate it:
a graceful signal first, then a hard kill after a bounded grace period, then a
reap so no orphan is left behind.  A wall-clock timeout is enforced the same
way by a background watchdog.

Conformance is guaranteed by composition rather than reimplementation: this
adapter drives an internal :class:`CallableHarnessAdapter` through its stateful
hooks, so the exact identity/revision/checkpoint fences, capability rules,
idempotent replay, and state transitions are the frozen protocol's, not a
parallel copy.  The hooks only manage the child process.

Wiring: this adapter is a drop-in ``HarnessSessionAdapter``.  The existing
control-plane interrupt path (:func:`lhos.sdk.computation_control.make_harness_dispatcher`
turning a ``PREEMPT`` :class:`ComputationAction` into ``adapter.control(PREEMPT)``)
therefore kills a subprocess-backed attempt for real, with no change to the
dispatcher, the scheduler, or ``AgentOS``.  Claim/Lease release stays on the
Scheduler/worker-pool path; this adapter never touches ownership.

Limitations, stated plainly:

* The frozen v1 capability vocabulary only offers ``preemption_mode`` values
  ``"none"`` and ``"cooperative"``; there is no ``"forceful"`` token.  This
  adapter must therefore *declare* ``"cooperative"`` even though its ``PREEMPT``
  is a real OS-level kill.  The declared mode is a schema artefact; the
  behaviour is the kill.
* ``CHECKPOINT`` records a durable usage/progress *marker*; it does not snapshot
  a black-box child's memory and does not pause it.
* Termination targets the child pid (plus its process group/session isolation).
  A child that forks its own grandchildren can still leak those; a true process
  tree kill needs an OS job object / cgroup, which this adapter does not create.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import Any
from uuid import uuid4

from .errors import ConfigurationError, ExecutionError


def _windows_job_tools():
    """Lazy access to the Windows kill-job helpers.

    Imported lazily because lhos.integrations.harness transitively imports
    lhos.sdk; a module-level import here would close an import cycle.
    Returns None off Windows or if the helpers are unavailable -- callers
    degrade to direct-child termination, the previous behaviour.
    """

    if os.name != "nt":
        return None
    try:
        from lhos.integrations.harness import process as harness_process
    except Exception:
        return None
    return harness_process
from .harness import (
    CallableHarnessAdapter,
    HarnessCapabilities,
    HarnessControlRequest,
    HarnessControlResult,
    HarnessHookOutcome,
    HarnessSessionIdentity,
    HarnessSessionSnapshot,
)
from .harness_child import READY_SENTINEL, USAGE_SENTINEL, parse_usage_line
from .read_recorder import READS_SENTINEL, parse_reads_line

# ``resource.getrusage`` is POSIX-only.  ``psutil`` is deliberately *not*
# imported: it is not a declared dependency (see ``pyproject.toml``) and this
# module must not add one.  On platforms without ``resource`` the child CPU/RSS
# fields are reported unavailable rather than fabricated.  The ``sys.platform``
# guard (not ``os.name``) is what lets a static type-checker prune the
# unavailable branch cleanly.
if sys.platform == "win32":  # pragma: no cover - exercised only on Windows hosts
    _resource: Any = None
    _RESOURCE_UNAVAILABLE_REASON = (
        "resource.getrusage is POSIX-only and psutil is not a declared dependency"
    )
else:
    import resource as _resource

    _RESOURCE_UNAVAILABLE_REASON = "resource.getrusage(RUSAGE_CHILDREN) is unavailable on this host"


def _read_rusage_children() -> tuple[float, int] | None:
    """Return ``(cpu_seconds, peak_rss_bytes)`` for reaped children, or ``None``.

    POSIX only, via ``resource.getrusage(RUSAGE_CHILDREN)``.  This measure is
    inherently *process-wide and cumulative*: it aggregates every child this
    process has already reaped, not one child in isolation.  A single child's CPU
    is recovered by differencing a start snapshot against an end snapshot; peak
    RSS cannot be differenced (it is a maximum, not a sum) and is therefore the
    aggregate peak, not a per-child figure.  Returns ``None`` when the platform
    has no ``resource`` module or the call fails.
    """

    if _resource is None:
        return None
    try:
        usage = _resource.getrusage(_resource.RUSAGE_CHILDREN)
    except Exception:
        return None
    cpu_seconds = float(usage.ru_utime) + float(usage.ru_stime)
    maxrss = int(usage.ru_maxrss)
    # ru_maxrss is kilobytes on Linux but bytes on macOS.
    rss_bytes = maxrss if sys.platform == "darwin" else maxrss * 1024
    return (cpu_seconds, rss_bytes)


class SubprocessHarnessError(ExecutionError):
    """A subprocess-backed Harness attempt failed operationally."""


class _ChildProcess:
    """Thread-safe lifecycle manager for one killable child process.

    All mutation of the process handle and its terminal accounting is guarded
    by ``_lock`` so a background timeout watchdog and a ``PREEMPT`` request
    cannot corrupt each other.  Reader threads never take ``_lock``; they only
    append to bounded, individually locked capture buffers.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        cwd: str | None,
        env: Mapping[str, str] | None,
        grace_period: float,
        hard_kill_timeout: float,
        wall_clock_timeout: float | None,
        max_capture_bytes: int,
    ) -> None:
        self._command: tuple[str, ...] = tuple(command)
        self._cwd = cwd
        self._env = dict(env) if env is not None else None
        self._grace_period = grace_period
        self._hard_kill_timeout = hard_kill_timeout
        self._wall_clock_timeout = wall_clock_timeout
        self._max_capture_bytes = max_capture_bytes

        self._lock = threading.Lock()
        self._capture_lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._started_monotonic: float | None = None
        self._ended_monotonic: float | None = None
        self._timed_out = False
        self._hard_killed = False
        self._terminated_by: str = ""
        self._last_usage_line: str | None = None
        self._last_reads_line: str | None = None
        self._rusage_children_start: tuple[float, int] | None = None
        self._stdout_tail: deque[str] = deque(maxlen=256)
        self._stderr_tail: deque[str] = deque(maxlen=256)
        self._captured_bytes = 0
        self._ready = threading.Event()
        self._threads: list[threading.Thread] = []
        # Kill-on-close job object assigned at start(): if the OWNING process
        # (the benchmark runner) dies unexpectedly, the OS closes the handle
        # and terminates the child -- closing the top link of the supervision
        # chain.  Previously a runner death orphaned workers (and their
        # benchmark containers) indefinitely.
        self._windows_job_handle: int | None = None

    @property
    def command(self) -> list[str]:
        return list(self._command)

    @property
    def pid(self) -> int | None:
        proc = self._proc
        return proc.pid if proc is not None else None

    @property
    def timed_out(self) -> bool:
        with self._lock:
            return self._timed_out

    @property
    def started(self) -> bool:
        with self._lock:
            return self._started_monotonic is not None

    def start(self) -> None:
        with self._lock:
            if self._proc is not None:
                raise SubprocessHarnessError("child process has already been started")
            creationflags = 0
            start_new_session = False
            if os.name == "nt":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                # Isolate the child in its own session so a stray Ctrl-C to the
                # parent test/OS process does not race our explicit signals.
                start_new_session = True
            self._proc = subprocess.Popen(
                list(self._command),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self._cwd,
                env=self._env,
                shell=False,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creationflags,
                start_new_session=start_new_session,
            )
            self._started_monotonic = time.monotonic()
            # Snapshot cumulative reaped-children CPU now so usage() can isolate
            # this child's slice by differencing against the value after it exits.
            self._rusage_children_start = _read_rusage_children()
            job_tools = _windows_job_tools()
            if job_tools is not None:
                with suppress(Exception):
                    self._windows_job_handle = job_tools._create_windows_kill_job(
                        self._proc.pid
                    )
            proc = self._proc

        self._spawn_thread(self._pump, (proc.stdout, True), "lhos-child-stdout")
        self._spawn_thread(self._pump, (proc.stderr, False), "lhos-child-stderr")
        if self._wall_clock_timeout is not None:
            self._spawn_thread(self._run_watchdog, (proc,), "lhos-child-watchdog")

    def _spawn_thread(self, target: Any, args: tuple[Any, ...], name: str) -> None:
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        self._threads.append(thread)
        thread.start()

    def _pump(self, stream: Any, is_stdout: bool) -> None:
        if stream is None:
            return
        try:
            for line in stream:
                text = line.rstrip("\n")
                if is_stdout and text.startswith(USAGE_SENTINEL):
                    with self._capture_lock:
                        self._last_usage_line = text
                    continue
                if is_stdout and text.startswith(READS_SENTINEL):
                    with self._capture_lock:
                        self._last_reads_line = text
                    continue
                if is_stdout and text.strip() == READY_SENTINEL:
                    self._ready.set()
                    continue
                self._append_capture(self._stdout_tail if is_stdout else self._stderr_tail, line)
        except Exception:
            # A decode/read error on the child's stream must never crash the OS.
            pass
        finally:
            with suppress(Exception):
                stream.close()

    def _append_capture(self, sink: deque[str], line: str) -> None:
        with self._capture_lock:
            if self._captured_bytes >= self._max_capture_bytes:
                return
            self._captured_bytes += len(line.encode("utf-8", "replace"))
            sink.append(line)

    def _run_watchdog(self, proc: subprocess.Popen[str]) -> None:
        timeout = self._wall_clock_timeout
        if timeout is None:
            return
        try:
            proc.wait(timeout=timeout)
            return
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            return
        with self._lock:
            self._timed_out = True
        self.terminate_and_reap(graceful=True, reason="wall_clock_timeout")

    def poll(self) -> int | None:
        proc = self._proc
        if proc is None:
            return None
        code = proc.poll()
        if code is not None:
            with self._lock:
                if self._ended_monotonic is None:
                    self._ended_monotonic = time.monotonic()
        return code

    def wait_ready(self, timeout: float | None) -> bool:
        """Block until the child announces readiness, exits, or ``timeout``."""

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._ready.wait(0.05):
                return True
            if self.poll() is not None:
                return self._ready.is_set()
            if deadline is not None and time.monotonic() >= deadline:
                return self._ready.is_set()

    def terminate_and_reap(self, *, graceful: bool = True, reason: str = "") -> None:
        """Stop the child and reap it: graceful signal, then hard kill.

        Idempotent and safe to call from the watchdog and a ``PREEMPT`` at once;
        the second caller simply observes an already-reaped process.
        """

        with self._lock:
            proc = self._proc
            if proc is None:
                return
            if not self._terminated_by:
                self._terminated_by = reason or "preempt"
            if proc.poll() is not None:
                self._finalize_locked()
                return
            if graceful:
                with suppress(Exception):
                    proc.terminate()  # SIGTERM on POSIX, TerminateProcess on Windows
                with suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=self._grace_period)
            if proc.poll() is None:
                # Prefer the job object: it terminates the whole tree, while
                # proc.kill() only reaches the direct child and lets
                # grandchildren (and their containers) escape.
                terminated_via_job = False
                job_tools = _windows_job_tools()
                if job_tools is not None and self._windows_job_handle:
                    with suppress(Exception):
                        terminated_via_job = bool(
                            job_tools._terminate_windows_job(
                                self._windows_job_handle
                            )
                        )
                if not terminated_via_job:
                    with suppress(Exception):
                        proc.kill()  # SIGKILL on POSIX; TerminateProcess again on Windows
                self._hard_killed = True
                with suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=self._hard_kill_timeout)
            self._finalize_locked()

    def _finalize_locked(self) -> None:
        if self._ended_monotonic is None:
            self._ended_monotonic = time.monotonic()

    def usage(self) -> dict[str, Any]:
        """Return measured usage: parent wall-clock plus child self-report.

        Beyond the self-reported tokens/cost this also carries two *measured*
        signals that need no model or API:

        * ``observed_reads`` -- absolute paths the child opened for reading, or
          ``None`` when the child emitted no read report (fail-closed: unobserved
          is never conflated with "read nothing").  Compare against a task's
          declared inputs via :func:`lhos.sdk.undeclared_reads.compare_reads`.
        * ``cpu_time_ms`` / ``peak_rss_bytes`` -- child CPU and peak RSS on
          POSIX, or ``None`` where unmeasurable (see ``resource_metrics_*``).
        """

        with self._lock:
            started = self._started_monotonic
            ended = self._ended_monotonic
            timed_out = self._timed_out
            hard_killed = self._hard_killed
            terminated_by = self._terminated_by
            rusage_start = self._rusage_children_start
        proc = self._proc
        exit_code = proc.poll() if proc is not None else None
        end_ts = ended if ended is not None else time.monotonic()
        wall_ms = 0 if started is None else max(0, round((end_ts - started) * 1000))
        with self._capture_lock:
            usage_line = self._last_usage_line
            reads_line = self._last_reads_line
        child_usage = parse_usage_line(usage_line) if usage_line else None
        observed_reads = parse_reads_line(reads_line) if reads_line else None

        rusage_end = _read_rusage_children()
        if rusage_end is None:
            cpu_time_ms: int | None = None
            peak_rss_bytes: int | None = None
            resource_available = False
            resource_reason: str | None = _RESOURCE_UNAVAILABLE_REASON
        else:
            start_cpu = rusage_start[0] if rusage_start is not None else 0.0
            cpu_time_ms = max(0, round((rusage_end[0] - start_cpu) * 1000))
            peak_rss_bytes = rusage_end[1]
            resource_available = True
            resource_reason = None

        return {
            "wall_time_ms": wall_ms,
            "tokens_in": child_usage.tokens_in if child_usage is not None else 0,
            "tokens_out": child_usage.tokens_out if child_usage is not None else 0,
            "cost_microusd": child_usage.cost_microusd if child_usage is not None else 0,
            "measured_usage": child_usage is not None,
            "exit_code": exit_code,
            "pid": proc.pid if proc is not None else None,
            "timed_out": timed_out,
            "hard_killed": hard_killed,
            "terminated_by": terminated_by or None,
            "observed_reads": observed_reads,
            "reads_observed": observed_reads is not None,
            "cpu_time_ms": cpu_time_ms,
            "peak_rss_bytes": peak_rss_bytes,
            "resource_metrics_available": resource_available,
            "resource_metrics_unavailable_reason": resource_reason,
        }

    def close(self) -> None:
        self.terminate_and_reap(graceful=False, reason="close")
        for thread in self._threads:
            with suppress(Exception):
                thread.join(timeout=2.0)
        job_tools = _windows_job_tools()
        if job_tools is not None and self._windows_job_handle:
            with suppress(Exception):
                job_tools._close_windows_job(self._windows_job_handle)
        self._windows_job_handle = None


def _validate_positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be a number")
    number = float(value)
    if number <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return number


class SubprocessHarnessAdapter:
    """Killable :class:`~lhos.sdk.harness.HarnessSessionAdapter` over a child process.

    Parameters
    ----------
    identity:
        The exact session identity (same fences as any Harness session).
    command:
        The child ``argv`` list.  Passed straight to ``subprocess.Popen`` with
        ``shell=False``; a bare string is rejected so a shell can never be
        reintroduced.
    grace_period:
        Seconds to wait after the graceful signal before hard-killing.
    timeout:
        Optional wall-clock cap.  When set, a background watchdog hard-kills and
        reaps the child once exceeded, independent of any ``CONTINUE`` poll.
    """

    def __init__(
        self,
        identity: HarnessSessionIdentity,
        command: Sequence[str],
        *,
        grace_period: float = 5.0,
        timeout: float | None = None,
        hard_kill_timeout: float = 5.0,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        max_capture_bytes: int = 1_000_000,
        max_cached_requests: int = 1024,
    ) -> None:
        if not isinstance(identity, HarnessSessionIdentity):
            raise ConfigurationError("identity must be a HarnessSessionIdentity")
        if isinstance(command, (str, bytes)):
            raise ConfigurationError(
                "command must be an argv sequence, not a string (shell=True is forbidden)"
            )
        if not isinstance(command, Sequence):
            raise ConfigurationError("command must be a sequence of arguments")
        argv = [str(part) for part in command]
        if not argv or not argv[0].strip():
            raise ConfigurationError("command must be a non-empty argv with an executable")
        grace = _validate_positive(grace_period, "grace_period")
        hard = _validate_positive(hard_kill_timeout, "hard_kill_timeout")
        wall_timeout = None if timeout is None else _validate_positive(timeout, "timeout")
        if isinstance(max_capture_bytes, bool) or not isinstance(max_capture_bytes, int):
            raise ConfigurationError("max_capture_bytes must be an integer")
        if max_capture_bytes < 1:
            raise ConfigurationError("max_capture_bytes must be >= 1")

        self._child = _ChildProcess(
            argv,
            cwd=None if cwd is None else os.fspath(cwd),
            env=env,
            grace_period=grace,
            hard_kill_timeout=hard,
            wall_clock_timeout=wall_timeout,
            max_capture_bytes=max_capture_bytes,
        )
        # Compose the frozen in-process adapter so all identity/revision/
        # checkpoint fencing, capability derivation, idempotent replay, and
        # state transitions come from the protocol implementation itself.
        self._inner = CallableHarnessAdapter(
            identity,
            start=self._on_start,
            continue_handler=self._on_continue,
            checkpoint=self._on_checkpoint,
            preempt=self._on_preempt,
            max_cached_requests=max_cached_requests,
        )

    # ── HarnessSessionAdapter protocol surface ──────────────────────────────
    @property
    def capabilities(self) -> HarnessCapabilities:
        return self._inner.capabilities

    @property
    def snapshot(self) -> HarnessSessionSnapshot:
        return self._inner.snapshot

    async def control(self, request: HarnessControlRequest) -> HarnessControlResult:
        return await self._inner.control(request)

    def control_sync(self, request: HarnessControlRequest) -> HarnessControlResult:
        return self._inner.control_sync(request)

    def make_request(self, *args: Any, **kwargs: Any) -> HarnessControlRequest:
        return self._inner.make_request(*args, **kwargs)

    def restore_durable_snapshot(self, snapshot: HarnessSessionSnapshot) -> None:
        # Restoring logical session metadata never relaunches a child.
        self._inner.restore_durable_snapshot(snapshot)

    # ── child introspection / cleanup ───────────────────────────────────────
    @property
    def pid(self) -> int | None:
        return self._child.pid

    @property
    def child(self) -> _ChildProcess:
        return self._child

    def wait_ready(self, timeout: float | None = None) -> bool:
        """Block until the child reaches its work phase (or exits/timeout)."""

        return self._child.wait_ready(timeout)

    def usage(self) -> dict[str, Any]:
        return self._child.usage()

    def close(self) -> None:
        """Terminate and reap the child; safe to call more than once."""

        self._child.close()

    def __del__(self) -> None:  # pragma: no cover - GC backstop against orphans
        with suppress(Exception):
            self._child.close()

    # ── lifecycle hooks driving the child process ───────────────────────────
    async def _on_start(
        self,
        _request: HarnessControlRequest,
        _snapshot: HarnessSessionSnapshot,
    ) -> HarnessHookOutcome:
        await asyncio.to_thread(self._child.start)
        return HarnessHookOutcome(
            progress=0.0,
            details={
                "status": "running",
                "pid": self._child.pid,
                "command": list(self._child.command),
            },
        )

    async def _on_continue(
        self,
        _request: HarnessControlRequest,
        snapshot: HarnessSessionSnapshot,
    ) -> HarnessHookOutcome:
        exit_code = await asyncio.to_thread(self._child.poll)
        usage = self._child.usage()
        if exit_code is None:
            return HarnessHookOutcome(
                progress=snapshot.progress,
                details={"status": "running", **usage},
            )
        if usage["timed_out"]:
            raise SubprocessHarnessError(
                f"child exceeded wall-clock timeout after {usage['wall_time_ms']} ms"
            )
        if exit_code != 0:
            raise SubprocessHarnessError(f"child exited with non-zero status {exit_code}")
        return HarnessHookOutcome(
            completed=True,
            progress=1.0,
            details={"status": "completed", **usage},
        )

    async def _on_checkpoint(
        self,
        _request: HarnessControlRequest,
        snapshot: HarnessSessionSnapshot,
    ) -> HarnessHookOutcome:
        # A durable usage/progress marker only; the black-box child is not
        # paused and its memory is not snapshotted.
        usage = self._child.usage()
        checkpoint_id = f"subproc-cp-r{snapshot.revision}-{uuid4().hex[:12]}"
        return HarnessHookOutcome(
            checkpoint_id=checkpoint_id,
            progress=snapshot.progress,
            details={"status": "checkpoint_marker", **usage},
        )

    async def _on_preempt(
        self,
        _request: HarnessControlRequest,
        snapshot: HarnessSessionSnapshot,
    ) -> HarnessHookOutcome:
        await asyncio.to_thread(self._child.terminate_and_reap, graceful=True, reason="preempt")
        usage = self._child.usage()
        return HarnessHookOutcome(
            progress=snapshot.progress,
            details={"status": "preempted", **usage},
        )


__all__ = [
    "SubprocessHarnessAdapter",
    "SubprocessHarnessError",
]
