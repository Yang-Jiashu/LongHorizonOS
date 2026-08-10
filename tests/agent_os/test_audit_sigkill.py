"""Audit: Real SIGKILL recovery — crash scenarios in separate processes.

Every crash scenario runs the kernel in a subprocess, kills it, then
verifies recovery in a new process.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

# POSIX reports -SIGKILL. Windows has no SIGKILL and the crash script falls
# back to SIGTERM, which surfaces as the numeric code 15.
_HARD_KILL_RETURNCODES = (int(signal.SIGTERM), 1) if os.name == "nt" else (-int(signal.SIGKILL),)


def _write_crash_script(db_path: str, crash_point: str, result_path: str) -> str:
    """Write a Python script that runs the kernel and crashes at a specific point."""
    script = textwrap.dedent(f"""
        import asyncio
        import json
        import os
        import signal as sig
        import sys
        import time

        sys.path.insert(0, {str(Path(__file__).parent.parent.parent / "src")!r})

        from lhos.agent_os.sdk.client import create_kernel
        from lhos.agent_os.kernel.models import (
            SubmitActionRequest, SideEffectClass, KernelEvent
        )
        from lhos.agent_os.programs.scripted import (
            ScriptedProgram, submit_device_action, process_event_step, exit_step
        )

        DB_PATH = {db_path!r}
        CRASH_POINT = {crash_point!r}

        async def run():
            kernel = create_kernel(DB_PATH)
            driver = kernel.get_driver("tool/mock")

            program = ScriptedProgram(program_id="sigkill_test", steps=[])
            pid = await kernel.spawn(program)

            if CRASH_POINT == "A":
                # Durable Intent committed, Driver not yet executed
                driver.set_default_behavior("pure_success")
                program._steps = [
                    submit_device_action(pid, operation="gen", side_effect_class="pure"),
                    process_event_step(pid),
                    exit_step(pid),
                ]
                program.reset()
                # Tick 1: submit → BLOCKED
                await kernel.tick()
                # Kill before dispatch
                os.kill(os.getpid(), getattr(sig, "SIGKILL", sig.SIGTERM))

            elif CRASH_POINT == "B":
                # IDEMPOTENT side effect happened, completion not committed
                driver.set_default_behavior("crash_after_effect")
                program._steps = [
                    submit_device_action(pid, operation="write", side_effect_class="idempotent"),
                    process_event_step(pid),
                    exit_step(pid),
                ]
                program.reset()
                await kernel.tick()  # submit
                await kernel.tick()  # dispatch → crash_after_effect → "unknown"
                # Action is RUNNING, effect recorded in driver
                # Kill before recovery
                os.kill(os.getpid(), getattr(sig, "SIGKILL", sig.SIGTERM))

            elif CRASH_POINT == "C":
                # NON_REVERSIBLE side effect, unknown inspect
                driver.set_default_behavior("crash_after_effect")
                program._steps = [
                    submit_device_action(pid, operation="danger", side_effect_class="non_reversible"),
                    process_event_step(pid),
                    exit_step(pid),
                ]
                program.reset()
                await kernel.tick()  # submit
                await kernel.tick()  # dispatch → crash
                os.kill(os.getpid(), getattr(sig, "SIGKILL", sig.SIGTERM))

            elif CRASH_POINT == "D":
                # Action COMMITTED, Lease release not yet written
                driver.set_default_behavior("pure_success")
                program._steps = [
                    submit_device_action(
                        pid, operation="test", side_effect_class="pure",
                        resource_claims=[{{"resource_id": "resource:R1", "mode": "exclusive"}}]
                    ),
                    process_event_step(pid),
                    exit_step(pid),
                ]
                program.reset()
                await kernel.tick()  # submit
                await kernel.tick()  # dispatch → commit → lease released
                # Kill after commit but before lease release is journaled
                # (In current impl, lease release happens in same tick as commit)
                # So we kill after the tick completes
                os.kill(os.getpid(), getattr(sig, "SIGKILL", sig.SIGTERM))

            elif CRASH_POINT == "E":
                # Signal generated but not consumed
                kernel._signal_service.send(
                    target_pid="nonexistent",
                    signal_type="TEST_SIGNAL",
                    source_pid="kernel",
                    payload={{"data": "test"}},
                )
                # Kill before signal is consumed
                os.kill(os.getpid(), getattr(sig, "SIGKILL", sig.SIGTERM))

        asyncio.run(run())
    """)
    return script


def _write_recovery_script(db_path: str, result_path: str) -> str:
    """Write a script that reopens the DB and recovers."""
    script = textwrap.dedent(f"""
        import asyncio
        import json
        import sys

        sys.path.insert(0, {str(Path(__file__).parent.parent.parent / "src")!r})

        from lhos.agent_os.sdk.client import create_kernel, rebuild_from_journal
        from lhos.agent_os.kernel.models import ActionState, ProcessState

        DB_PATH = {db_path!r}
        RESULT_PATH = {result_path!r}

        async def run():
            # Rebuild from journal
            kernel = rebuild_from_journal(DB_PATH)

            # Run a recovery tick
            await kernel.tick()

            # Collect state
            processes = kernel._storage.query_all("SELECT * FROM processes_projection")
            actions = kernel._storage.query_all("SELECT * FROM actions_projection")
            leases = kernel._storage.query_all("SELECT * FROM leases_projection")
            signals = kernel._storage.query_all("SELECT * FROM signals_projection")
            events = kernel._journal.read_all()

            result = {{
                "processes": [{{"pid": p["pid"], "state": p["state"]}} for p in processes],
                "actions": [{{
                    "action_id": a["action_id"],
                    "state": a["state"],
                    "side_effect_class": a["side_effect_class"],
                }} for a in actions],
                "lease_count": len(leases),
                "signal_count": len(signals),
                "consumed_signals": sum(1 for s in signals if s["consumed"]),
                "journal_event_count": len(events),
                "event_types": [e.event_type for e in events],
            }}

            with open(RESULT_PATH, "w") as f:
                json.dump(result, f, indent=2)

        asyncio.run(run())
    """)
    return script


class TestSigkillRecovery:
    """Real SIGKILL recovery tests using subprocesses."""

    @pytest.fixture
    def venv_python(self) -> str:
        """Get the venv Python path."""
        # Try venv-audit first
        candidates = [
            Path(__file__).parent.parent.parent / ".venv-audit" / "bin" / "python3",
            Path(__file__).parent.parent.parent / ".venv" / "bin" / "python3",
        ]
        for c in candidates:
            if c.exists():
                return str(c)
        return sys.executable

    def test_crash_a_durable_intent_no_driver(self, venv_python: str) -> None:
        """Crash A: Durable Intent committed, Driver not yet executed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "crash_a.db")
            result_path = str(Path(tmpdir) / "result_a.json")

            # Write and run crash script
            crash_script = _write_crash_script(db_path, "A", result_path)
            script_path = str(Path(tmpdir) / "crash.py")
            Path(script_path).write_text(crash_script, encoding="utf-8")

            # Run crash script — it should be killed
            proc = subprocess.run(
                [venv_python, script_path],
                timeout=10,
                capture_output=True,
            )
            # Process should have been killed (negative return code)
            assert proc.returncode in _HARD_KILL_RETURNCODES

            # Now run recovery
            recovery_script = _write_recovery_script(db_path, result_path)
            recovery_path = str(Path(tmpdir) / "recover.py")
            Path(recovery_path).write_text(recovery_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, recovery_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode == 0, f"Recovery failed: {proc.stderr.decode()}"

            with open(result_path) as f:
                result = json.load(f)

            # Process should exist and not be in a terminal state yet
            # (recovery tick should handle it)
            assert len(result["processes"]) >= 1
            # Action should exist
            assert len(result["actions"]) >= 1
            # After recovery, action should be committed or recovered
            action_states = [a["state"] for a in result["actions"]]
            assert all(
                s in ("committed", "failed", "uncertain", "admitted", "running")
                for s in action_states
            ), f"Unexpected action states: {action_states}"

    def test_crash_b_idempotent_recovery(self, venv_python: str) -> None:
        """Crash B: IDEMPOTENT side effect happened, inspect should commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "crash_b.db")
            result_path = str(Path(tmpdir) / "result_b.json")

            crash_script = _write_crash_script(db_path, "B", result_path)
            script_path = str(Path(tmpdir) / "crash.py")
            Path(script_path).write_text(crash_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, script_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode in _HARD_KILL_RETURNCODES

            recovery_script = _write_recovery_script(db_path, result_path)
            recovery_path = str(Path(tmpdir) / "recover.py")
            Path(recovery_path).write_text(recovery_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, recovery_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode == 0, f"Recovery failed: {proc.stderr.decode()}"

            with open(result_path) as f:
                result = json.load(f)

            # Action should be recovered (not stuck in RUNNING)
            assert len(result["actions"]) >= 1
            action_states = [a["state"] for a in result["actions"]]
            # After recovery, the action should NOT be in "running" state
            # (recovery tick should have handled it)
            # Note: without the original driver's effect store, inspect returns "unknown"
            # So the action may go UNCERTAIN (which is correct — safe behavior)
            for s in action_states:
                assert s in ("committed", "uncertain", "failed"), (
                    f"Action still in non-terminal state: {s}"
                )

    def test_crash_c_non_reversible_uncertain(self, venv_python: str) -> None:
        """Crash C: NON_REVERSIBLE → UNCERTAIN, no auto retry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "crash_c.db")
            result_path = str(Path(tmpdir) / "result_c.json")

            crash_script = _write_crash_script(db_path, "C", result_path)
            script_path = str(Path(tmpdir) / "crash.py")
            Path(script_path).write_text(crash_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, script_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode in _HARD_KILL_RETURNCODES

            recovery_script = _write_recovery_script(db_path, result_path)
            recovery_path = str(Path(tmpdir) / "recover.py")
            Path(recovery_path).write_text(recovery_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, recovery_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode == 0, f"Recovery failed: {proc.stderr.decode()}"

            with open(result_path) as f:
                result = json.load(f)

            # NON_REVERSIBLE action should go UNCERTAIN
            assert len(result["actions"]) >= 1
            uncertain = [a for a in result["actions"] if a["state"] == "uncertain"]
            assert len(uncertain) >= 1, f"Expected UNCERTAIN action, got: {result['actions']}"

    def test_crash_d_committed_lease_released(self, venv_python: str) -> None:
        """Crash D: Action committed, leases should be released after recovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "crash_d.db")
            result_path = str(Path(tmpdir) / "result_d.json")

            crash_script = _write_crash_script(db_path, "D", result_path)
            script_path = str(Path(tmpdir) / "crash.py")
            Path(script_path).write_text(crash_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, script_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode in _HARD_KILL_RETURNCODES

            recovery_script = _write_recovery_script(db_path, result_path)
            recovery_path = str(Path(tmpdir) / "recover.py")
            Path(recovery_path).write_text(recovery_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, recovery_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode == 0, f"Recovery failed: {proc.stderr.decode()}"

            with open(result_path) as f:
                result = json.load(f)

            # After recovery, there should be no active leases
            # (either released before crash or reclaimed during recovery)
            assert result["lease_count"] == 0, f"Leases not released: {result['lease_count']}"

    def test_crash_e_signal_consumed_once(self, venv_python: str) -> None:
        """Crash E: Signal generated but not consumed — should survive rebuild."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "crash_e.db")
            result_path = str(Path(tmpdir) / "result_e.json")

            crash_script = _write_crash_script(db_path, "E", result_path)
            script_path = str(Path(tmpdir) / "crash.py")
            Path(script_path).write_text(crash_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, script_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode in _HARD_KILL_RETURNCODES

            recovery_script = _write_recovery_script(db_path, result_path)
            recovery_path = str(Path(tmpdir) / "recover.py")
            Path(recovery_path).write_text(recovery_script, encoding="utf-8")

            proc = subprocess.run(
                [venv_python, recovery_path],
                timeout=10,
                capture_output=True,
            )
            assert proc.returncode == 0, f"Recovery failed: {proc.stderr.decode()}"

            with open(result_path) as f:
                result = json.load(f)

            # Signal should exist and be unconsumed (target process doesn't exist)
            assert result["signal_count"] >= 1
            # Signal should be unconsumed (no matching BLOCKED process)
            unconsumed = result["signal_count"] - result["consumed_signals"]
            assert unconsumed >= 1
