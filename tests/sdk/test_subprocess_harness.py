"""Tests for the killable subprocess-backed Harness adapter.

These spawn real child processes (like ``tests/agent_os/test_audit_sigkill.py``)
so preemption, wall-clock timeout, measured-usage capture, and orphan reaping
are exercised end to end rather than mocked.  The child is
``python -m lhos.sdk.harness_child`` whose behaviour is fully flag-driven.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import pytest

from lhos.sdk.computation_control import (
    ComputationAction,
    ControlActionKind,
    DispatchStatus,
    make_harness_dispatcher,
)
from lhos.sdk.errors import ConfigurationError
from lhos.sdk.harness import (
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionAdapter,
    HarnessSessionIdentity,
    HarnessSessionState,
)
from lhos.sdk.harness_child import (
    USAGE_SENTINEL,
    ChildUsage,
    format_usage_line,
    parse_usage_line,
)
from lhos.sdk.subprocess_harness import SubprocessHarnessAdapter

# The child imports ``lhos.sdk`` before doing work; be generous so readiness and
# termination assertions never race a slow cold import on CI.
_READY_TIMEOUT = 30.0
_EXIT_TIMEOUT = 30.0


def _identity() -> HarnessSessionIdentity:
    return HarnessSessionIdentity(
        session_id="subproc-session",
        graph_id="graph-sp",
        graph_version=3,
        semantic_epoch=1,
        task_id="task-sp",
        agent_id="agent-sp",
        claim_id="claim-sp",
        attempt_id="attempt-sp",
    )


def _child_cmd(*args: str) -> list[str]:
    return [sys.executable, "-m", "lhos.sdk.harness_child", *args]


def _wait_for_exit(adapter: SubprocessHarnessAdapter, timeout: float = _EXIT_TIMEOUT) -> int | None:
    deadline = time.monotonic() + timeout
    while adapter.child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    return adapter.child.poll()


async def _continue_until_terminal(
    adapter: SubprocessHarnessAdapter,
    timeout: float = _EXIT_TIMEOUT,
):
    deadline = time.monotonic() + timeout
    result = None
    while time.monotonic() < deadline:
        result = await adapter.control(adapter.make_request(HarnessOperation.CONTINUE))
        if result.status is not HarnessResultStatus.APPLIED:
            return result
        if result.after.state is HarnessSessionState.COMPLETED:
            return result
        await asyncio.sleep(0.1)
    return result


def _preempt_action(identity: HarnessSessionIdentity) -> ComputationAction:
    return ComputationAction(
        action_id="a" * 64,
        request_id="b" * 64,
        epoch_id=0,
        target_kind="attempt",
        target_id=identity.attempt_id,
        graph_id=identity.graph_id,
        graph_version=identity.graph_version,
        task_id=identity.task_id,
        agent_id=identity.agent_id,
        claim_id=identity.claim_id,
        attempt_id=identity.attempt_id,
        semantic_epoch=identity.semantic_epoch,
        action=ControlActionKind.PREEMPT,
        harness_operation=HarnessOperation.PREEMPT,
        reason="semantic interrupt requested",
        source_decision_hash="c" * 64,
    )


def test_adapter_satisfies_protocol_and_declares_expected_capabilities() -> None:
    adapter = SubprocessHarnessAdapter(_identity(), _child_cmd("--sleep", "0"))
    try:
        assert isinstance(adapter, HarnessSessionAdapter)
        caps = adapter.capabilities
        assert set(caps.operations) == {
            HarnessOperation.START,
            HarnessOperation.CONTINUE,
            HarnessOperation.CHECKPOINT,
            HarnessOperation.PREEMPT,
        }
        assert caps.checkpoint_scope == "session"
        # The v1 vocabulary has no "forceful" token; the adapter must declare
        # "cooperative" even though its PREEMPT is a real OS-level kill.
        assert caps.preemption_mode == "cooperative"
        assert caps.rebase_mode == "none"
        assert adapter.snapshot.state is HarnessSessionState.CREATED
    finally:
        adapter.close()


def test_command_must_be_argv_not_shell_string() -> None:
    with pytest.raises(ConfigurationError, match="shell=True is forbidden"):
        SubprocessHarnessAdapter(_identity(), "python -m lhos.sdk.harness_child --sleep 1")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="non-empty argv"):
        SubprocessHarnessAdapter(_identity(), [])
    with pytest.raises(ConfigurationError, match="positive"):
        SubprocessHarnessAdapter(_identity(), _child_cmd("--sleep", "0"), grace_period=0)


async def test_preempt_kills_child_that_ignores_cooperative_cancellation() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120", "--ignore-termination"),
        grace_period=0.5,
    )
    try:
        started = await adapter.control(adapter.make_request(HarnessOperation.START))
        assert started.status is HarnessResultStatus.APPLIED
        assert started.after.state is HarnessSessionState.RUNNING
        assert adapter.wait_ready(_READY_TIMEOUT) is True
        pid = adapter.pid
        assert pid is not None
        assert adapter.child.poll() is None  # genuinely running

        result = await adapter.control(
            adapter.make_request(HarnessOperation.PREEMPT, reason="preempt now")
        )
        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state is HarnessSessionState.PREEMPTED
        # The uncooperative child is dead and reaped: no orphan, no zombie.
        assert adapter.child.poll() is not None
        assert result.details["terminated_by"] == "preempt"
        # On POSIX the ignored SIGTERM forces the hard-kill fallback; Windows
        # TerminateProcess stops it on the first (graceful) call.
        if os.name != "nt":
            assert result.details["hard_killed"] is True
    finally:
        adapter.close()


async def test_preempt_via_existing_dispatcher_interrupt_path_kills_subprocess() -> None:
    """A PREEMPT action routed through the unmodified control-plane dispatcher."""

    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120", "--ignore-termination"),
        grace_period=0.5,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        assert adapter.wait_ready(_READY_TIMEOUT) is True
        dispatcher = make_harness_dispatcher({adapter.snapshot.identity.attempt_id: adapter})

        result = await dispatcher(_preempt_action(adapter.snapshot.identity))

        assert result.status is DispatchStatus.APPLIED
        assert adapter.snapshot.state is HarnessSessionState.PREEMPTED
        assert adapter.child.poll() is not None
    finally:
        adapter.close()


async def test_wall_clock_timeout_terminates_and_reaps_child() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120"),
        grace_period=0.5,
        timeout=1.0,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        exit_code = _wait_for_exit(adapter)
        assert exit_code is not None  # watchdog reaped it without any CONTINUE
        assert adapter.child.timed_out is True

        result = await adapter.control(adapter.make_request(HarnessOperation.CONTINUE))
        assert result.status is HarnessResultStatus.FAILED
        assert "timeout" in result.message
        # Fail-closed: a timed-out attempt is not reported as a completion.
        assert result.after.state is HarnessSessionState.RUNNING
    finally:
        adapter.close()


async def test_measured_usage_is_captured_on_clean_completion() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd(
            "--sleep",
            "0.2",
            "--tokens-in",
            "13",
            "--tokens-out",
            "29",
            "--cost-microusd",
            "4200",
        ),
        grace_period=0.5,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        assert adapter.wait_ready(_READY_TIMEOUT) is True
        result = await _continue_until_terminal(adapter)
        assert result is not None
        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state is HarnessSessionState.COMPLETED
        assert result.after.progress == 1.0
        details = result.details
        assert details["measured_usage"] is True
        assert details["tokens_in"] == 13
        assert details["tokens_out"] == 29
        assert details["cost_microusd"] == 4200
        assert details["exit_code"] == 0
        # Wall-clock is measured by the parent, not self-reported by the child.
        assert isinstance(details["wall_time_ms"], int)
        assert details["wall_time_ms"] >= 0
    finally:
        adapter.close()


async def test_garbage_child_stdout_does_not_crash_and_usage_defaults_zero() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "0.1", "--corrupt-usage"),
        grace_period=0.5,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        assert adapter.wait_ready(_READY_TIMEOUT) is True
        result = await _continue_until_terminal(adapter)
        assert result is not None
        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state is HarnessSessionState.COMPLETED
        # A malformed sentinel line is ignored; usage falls back to measured
        # wall-clock only, and the OS never sees an exception.
        assert result.details["measured_usage"] is False
        assert result.details["tokens_in"] == 0
        assert result.details["cost_microusd"] == 0
    finally:
        adapter.close()


async def test_checkpoint_records_marker_without_killing_child() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120", "--ignore-termination"),
        grace_period=0.5,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        assert adapter.wait_ready(_READY_TIMEOUT) is True
        result = await adapter.control(adapter.make_request(HarnessOperation.CHECKPOINT))
        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state is HarnessSessionState.CHECKPOINTED
        assert result.after.checkpoint_id
        # The marker does not pause or kill the black-box child.
        assert adapter.child.poll() is None
    finally:
        adapter.close()


async def test_preempt_replay_is_idempotent_and_terminal() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120", "--ignore-termination"),
        grace_period=0.5,
    )
    try:
        await adapter.control(adapter.make_request(HarnessOperation.START))
        assert adapter.wait_ready(_READY_TIMEOUT) is True

        request = adapter.make_request(HarnessOperation.PREEMPT, request_id="preempt-once")
        first = await adapter.control(request)
        replay = await adapter.control(request)
        assert first == replay
        assert first.status is HarnessResultStatus.APPLIED
        assert adapter.child.poll() is not None

        # Reusing the id with a different request fails closed rather than
        # re-controlling the terminated session.
        conflict = await adapter.control(request.model_copy(update={"reason": "changed"}))
        assert conflict.status is HarnessResultStatus.REJECTED
        assert "different control request" in conflict.message

        # PREEMPT is terminal: a fresh request against the preempted session is
        # rejected by the state fence.
        again = await adapter.control(adapter.make_request(HarnessOperation.PREEMPT))
        assert again.status is HarnessResultStatus.REJECTED
        assert "preempted" in again.message
    finally:
        adapter.close()


async def test_stale_revision_and_wrong_identity_fail_closed() -> None:
    adapter = SubprocessHarnessAdapter(_identity(), _child_cmd("--sleep", "0.1"))
    try:
        started = await adapter.control(adapter.make_request(HarnessOperation.START))
        assert started.after.state is HarnessSessionState.RUNNING

        stale = adapter.make_request(HarnessOperation.CONTINUE).model_copy(
            update={"request_id": "stale-rev", "expected_revision": 99}
        )
        stale_result = await adapter.control(stale)
        assert stale_result.status is HarnessResultStatus.REJECTED
        assert "revision" in stale_result.message

        wrong = adapter.make_request(HarnessOperation.CONTINUE).model_copy(
            update={
                "request_id": "wrong-owner",
                "session": adapter.snapshot.identity.model_copy(update={"claim_id": "other-claim"}),
            }
        )
        wrong_result = await adapter.control(wrong)
        assert wrong_result.status is HarnessResultStatus.REJECTED
        assert "identity" in wrong_result.message
    finally:
        adapter.close()


async def test_close_reaps_child_leaving_no_orphan() -> None:
    adapter = SubprocessHarnessAdapter(
        _identity(),
        _child_cmd("--sleep", "120", "--ignore-termination"),
        grace_period=0.5,
    )
    await adapter.control(adapter.make_request(HarnessOperation.START))
    assert adapter.wait_ready(_READY_TIMEOUT) is True
    assert adapter.child.poll() is None
    adapter.close()
    assert adapter.child.poll() is not None
    # Idempotent: a second close is a harmless no-op.
    adapter.close()


def test_module_entrypoint_runs_and_reports_usage() -> None:
    """The packaged ``python -m lhos.sdk.harness_child`` entry point works."""

    import subprocess

    proc = subprocess.run(
        _child_cmd("--sleep", "0", "--tokens-in", "3", "--tokens-out", "4", "--cost-microusd", "5"),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    assert proc.returncode == 0
    usage_lines = [line for line in proc.stdout.splitlines() if line.startswith(USAGE_SENTINEL)]
    assert usage_lines, proc.stdout
    parsed = parse_usage_line(usage_lines[-1])
    assert parsed == ChildUsage(tokens_in=3, tokens_out=4, cost_microusd=5)


def test_usage_line_protocol_roundtrip_and_defensive_parse() -> None:
    line = format_usage_line(tokens_in=7, tokens_out=8, cost_microusd=9)
    assert line.startswith(USAGE_SENTINEL)
    assert parse_usage_line(line) == ChildUsage(tokens_in=7, tokens_out=8, cost_microusd=9)

    # Defensive: none of these may raise; all resolve to None.
    assert parse_usage_line("just some log output") is None
    assert parse_usage_line(f"{USAGE_SENTINEL} {{not json") is None
    assert parse_usage_line(f"{USAGE_SENTINEL} [1,2,3]") is None
    assert parse_usage_line(f'{USAGE_SENTINEL} {{"tokens_in": -1}}') is None
    assert parse_usage_line(f'{USAGE_SENTINEL} {{"unknown": 1}}') is None
    assert parse_usage_line(f"{USAGE_SENTINEL}") is None


@pytest.mark.skipif(os.name != "nt", reason="kill-on-close job is Windows-only")
def test_child_process_assigned_kill_on_close_job() -> None:
    """A started child is assigned a kill-on-close job handle so an owner
    death reaps it; close() releases the handle."""
    from lhos.sdk.subprocess_harness import _ChildProcess

    child = _ChildProcess(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=None,
        env=None,
        grace_period=0.5,
        hard_kill_timeout=2.0,
        wall_clock_timeout=None,
        max_capture_bytes=1024,
    )
    try:
        child.start()
        assert child._windows_job_handle
    finally:
        child.close()
    assert child._windows_job_handle is None
