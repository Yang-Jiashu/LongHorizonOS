"""External Harness process lifecycle tests."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from lhos.integrations.harness import run_managed_process


class _CancellationToken:
    request_pending = False


async def test_managed_process_streams_bounded_stdout_and_stderr() -> None:
    result = await run_managed_process(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "sys.stdout.write('x' * 200000 + 'STDOUT_END'); "
                "sys.stderr.write('y' * 200000 + 'STDERR_END')"
            ),
        ],
        max_capture_bytes=1024,
    )

    assert result.exit_code == 0
    assert len(result.stdout_tail.encode("utf-8")) <= 1024
    assert len(result.stderr_tail.encode("utf-8")) <= 1024
    assert result.stdout_tail.endswith("STDOUT_END")
    assert result.stderr_tail.endswith("STDERR_END")


async def test_managed_process_cancellation_is_distinct_from_timeout() -> None:
    token = _CancellationToken()

    async def cancel() -> None:
        await asyncio.sleep(0.15)
        token.request_pending = True

    cancel_task = asyncio.create_task(cancel())
    result = await run_managed_process(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        timeout_seconds=10,
        cancellation_token=token,
        grace_period_seconds=0.1,
        poll_seconds=0.01,
    )
    await cancel_task

    assert result.terminated_by == "semantic_interrupt"
    assert result.timed_out is False
    assert result.exit_code is not None


async def test_timeout_kills_descendant_process_tree(tmp_path: Path) -> None:
    marker = tmp_path / "orphan-marker.txt"
    child_code = (
        "import pathlib, signal, time; "
        "sigbreak=getattr(signal, 'SIGBREAK', None); "
        "sigterm=getattr(signal, 'SIGTERM', None); "
        "sigbreak and signal.signal(sigbreak, signal.SIG_IGN); "
        "sigterm and signal.signal(sigterm, signal.SIG_IGN); "
        "time.sleep(0.8); "
        f"pathlib.Path({str(marker)!r}).write_text('orphan', encoding='utf-8')"
    )
    parent_code = (
        "import signal, subprocess, sys, time; "
        "sigbreak=getattr(signal, 'SIGBREAK', None); "
        "sigterm=getattr(signal, 'SIGTERM', None); "
        "sigbreak and signal.signal(sigbreak, signal.SIG_IGN); "
        "sigterm and signal.signal(sigterm, signal.SIG_IGN); "
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}]); "
        "time.sleep(120)"
    )

    result = await run_managed_process(
        [sys.executable, "-c", parent_code],
        timeout_seconds=0.2,
        grace_period_seconds=0.1,
        poll_seconds=0.01,
    )
    await asyncio.sleep(1.0)

    assert result.timed_out is True
    assert result.terminated_by == "wall_clock_timeout"
    assert result.exit_code is not None
    assert marker.exists() is False


async def test_timeout_still_applies_after_parent_exits_with_live_descendant(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "detached-descendant.txt"
    child_code = (
        "import pathlib, signal, time; "
        "sigbreak=getattr(signal, 'SIGBREAK', None); "
        "sigterm=getattr(signal, 'SIGTERM', None); "
        "sigbreak and signal.signal(sigbreak, signal.SIG_IGN); "
        "sigterm and signal.signal(sigterm, signal.SIG_IGN); "
        "time.sleep(1.0); "
        f"pathlib.Path({str(marker)!r}).write_text('alive', encoding='utf-8')"
    )
    parent_code = (
        f"import subprocess, sys; subprocess.Popen([sys.executable, '-c', {child_code!r}])"
    )

    result = await run_managed_process(
        [sys.executable, "-c", parent_code],
        timeout_seconds=0.2,
        grace_period_seconds=0.1,
        poll_seconds=0.01,
    )
    await asyncio.sleep(1.1)

    assert result.timed_out is True
    assert result.elapsed_ms < 1000
    assert marker.exists() is False
