"""Command-backed Agents: a killable execution boundary on the normal path.

`SubprocessHarnessAdapter` gives this SDK a real, killable child process, but it
sits on the Harness control plane and a caller must register it explicitly.  The
ordinary execution path still runs a plain in-process Python callable, and the
worker pool cannot terminate a callable that ignores its cancellation token --
so a blocking model call ran to completion and its cost was paid even after the
work became stale.

This module closes that gap without changing any default: an Agent whose work is
a *command* gets an executor supplied by the SDK itself.  Because the SDK owns
that executor, it always honours the cancellation token, and because the work
lives in a child process there is something concrete to kill.  So `PREEMPT` on
the normal execution path stops real computation for these Agents rather than
merely discarding its result.

The executor is deliberately `context_v1`: that is the only executor API the
runtime binds a cancellation token into (see `ExecutionContext`), and without the
token this would be one more non-preemptible callable.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import Any, Final

from .errors import ConfigurationError, ExecutionError
from .subprocess_harness import _ChildProcess

DEFAULT_GRACE_PERIOD_SECONDS: Final[float] = 2.0
DEFAULT_HARD_KILL_TIMEOUT_SECONDS: Final[float] = 2.0
DEFAULT_POLL_SECONDS: Final[float] = 0.02
DEFAULT_MAX_CAPTURE_BYTES: Final[int] = 64_000

CommandSpec = Sequence[str] | Callable[[str], Sequence[str]]


def _resolve_command(command: CommandSpec, task_id: str) -> list[str]:
    argv = list(command(task_id)) if callable(command) else [*command, task_id]
    argv = [str(part) for part in argv]
    if not argv or not argv[0].strip():
        raise ConfigurationError("subprocess agent command must be a non-empty argv")
    return argv


def subprocess_task_executor(
    command: CommandSpec,
    *,
    timeout_seconds: float | None = None,
    grace_period_seconds: float = DEFAULT_GRACE_PERIOD_SECONDS,
    hard_kill_timeout_seconds: float = DEFAULT_HARD_KILL_TIMEOUT_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    max_capture_bytes: int = DEFAULT_MAX_CAPTURE_BYTES,
    on_usage: Callable[[str, dict[str, Any]], None] | None = None,
) -> Callable[..., Any]:
    """Build a preemptible `context_v1` executor that runs a task in a child.

    ``command`` is either an argv prefix, in which case the task id is appended,
    or a callable mapping a task id to a full argv.  A non-zero exit, a timeout,
    and a preemption are all distinct failures: a caller must be able to tell
    "the work was wrong" from "the work was stopped".

    ``on_usage`` receives ``(task_id, usage)`` for every attempt, whether it
    completed, timed out, or was preempted.  ``usage`` carries the parent's
    measured wall-clock, the child's self-reported tokens/cost when it emitted
    any, and ``terminated_by`` -- so measured cost is attributable and a
    preemption is distinguishable from an ordinary teardown.  It additionally
    carries measured (model-free) signals: ``observed_reads`` (absolute paths the
    child opened for reading, or ``None`` when unobserved -- fail-closed) and
    ``cpu_time_ms`` / ``peak_rss_bytes`` (child CPU/RSS on POSIX, ``None`` where
    unmeasurable).  These fields are additive; existing keys are unchanged.
    """

    if timeout_seconds is not None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise ConfigurationError("timeout_seconds must be a number or None")
        if timeout_seconds <= 0:
            raise ConfigurationError("timeout_seconds must be > 0")
    for name, value in (
        ("grace_period_seconds", grace_period_seconds),
        ("hard_kill_timeout_seconds", hard_kill_timeout_seconds),
        ("poll_seconds", poll_seconds),
    ):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ConfigurationError(f"{name} must be a positive number")

    async def execute(ctx: Any, task_id: str) -> None:
        argv = _resolve_command(command, str(task_id))
        child = _ChildProcess(
            argv,
            cwd=cwd,
            env=env,
            grace_period=float(grace_period_seconds),
            hard_kill_timeout=float(hard_kill_timeout_seconds),
            wall_clock_timeout=None if timeout_seconds is None else float(timeout_seconds),
            max_capture_bytes=int(max_capture_bytes),
        )
        token = getattr(ctx, "cancellation_token", None)
        try:
            await asyncio.to_thread(child.start)
            while True:
                exit_code = child.poll()
                if exit_code is not None:
                    break
                if token is not None and token.request_pending:
                    # Kill first, then acknowledge: acknowledging a preemption
                    # while the child is still burning tokens would report a
                    # stop that did not happen.
                    await asyncio.to_thread(
                        child.terminate_and_reap,
                        graceful=True,
                        reason="semantic_interrupt",
                    )
                    token.raise_if_cancelled()
                await asyncio.sleep(float(poll_seconds))

            if child.timed_out:
                raise ExecutionError(
                    f"task {task_id!r} exceeded its {timeout_seconds}s wall-clock timeout "
                    "and its child process was killed"
                )
            if exit_code != 0:
                raise ExecutionError(f"task {task_id!r} child process exited with code {exit_code}")
        finally:
            # Snapshot usage before close(), which reaps with reason="close" and
            # would otherwise overwrite the attribution of a real preemption.
            if on_usage is not None:
                with suppress(Exception):
                    on_usage(str(task_id), child.usage())
            child.close()

    return execute


__all__ = [
    "DEFAULT_GRACE_PERIOD_SECONDS",
    "DEFAULT_HARD_KILL_TIMEOUT_SECONDS",
    "DEFAULT_MAX_CAPTURE_BYTES",
    "DEFAULT_POLL_SECONDS",
    "CommandSpec",
    "subprocess_task_executor",
]
