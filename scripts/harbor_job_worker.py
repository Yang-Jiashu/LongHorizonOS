"""Run a Harbor job as a normalized subprocess worker.

Harbor returns a non-zero process code for an unresolved benchmark trial. The
LHOS verifier must inspect the result artifact rather than treat that expected
benchmark outcome as an executor crash, so this wrapper always records status
and exits zero after Harbor itself has terminated.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from lhos.integrations.harness.process import (
    _close_windows_job,
    _create_windows_kill_job,
    _terminate_windows_job,
)

STATUS_SCHEMA_VERSION = "lhos-harbor-job-worker.v2"
WINDOWS_CONTROL_C_EXIT = 0xC000013A


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        indent=2,
        sort_keys=True,
    ) + "\n"
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _decode_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _windows_control_interrupted(exit_code: int | None) -> bool:
    if exit_code is None:
        return False
    return int(exit_code) in {
        WINDOWS_CONTROL_C_EXIT,
        WINDOWS_CONTROL_C_EXIT - (1 << 32),
    }


def _process_isolation() -> dict[str, Any]:
    if os.name == "nt":
        creationflags = int(
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
        return {
            "creationflags": creationflags,
            "creationflag_names": [
                "CREATE_NEW_PROCESS_GROUP",
                "CREATE_NO_WINDOW",
            ],
            "start_new_session": False,
            "process_group_isolated": True,
            "process_group_mode": "windows_new_process_group",
            "tree_supervision": "windows_job_object_or_taskkill_fallback",
        }
    return {
        "creationflags": 0,
        "creationflag_names": [],
        "start_new_session": True,
        "process_group_isolated": True,
        "process_group_mode": "posix_new_session",
        "tree_supervision": "posix_process_group",
    }


def _terminate_process_tree(
    process: subprocess.Popen[str],
    *,
    windows_job_handle: int | None,
) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        terminated = _terminate_windows_job(windows_job_handle or 0)
        if not terminated:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
    else:
        with suppress(ProcessLookupError, PermissionError, OSError):
            os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name != "nt":
            with suppress(ProcessLookupError, PermissionError, OSError):
                os.killpg(process.pid, signal.SIGKILL)
        with suppress(OSError):
            process.kill()
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)


def _status_payload(
    *,
    state: str,
    command: list[str],
    repo_root: Path,
    timeout_seconds: float,
    isolation: dict[str, Any],
    started_at: str,
    harbor_pid: int | None = None,
    windows_job_assigned: bool | None = None,
    exit_code: int | None = None,
    elapsed_ms: float | None = None,
    stdout: str = "",
    stderr: str = "",
    failure: str = "",
) -> dict[str, Any]:
    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "status": state,
        "command": command,
        "repo_root": str(repo_root),
        "timeout_seconds": timeout_seconds,
        "worker_pid": os.getpid(),
        "harbor_pid": harbor_pid,
        "process_group_id": harbor_pid if isolation["process_group_isolated"] else None,
        "process_group_isolated": isolation["process_group_isolated"],
        "process_group_mode": isolation["process_group_mode"],
        "tree_supervision": isolation["tree_supervision"],
        "creationflags": isolation["creationflags"],
        "creationflag_names": isolation["creationflag_names"],
        "start_new_session": isolation["start_new_session"],
        "windows_job_assigned": windows_job_assigned,
        "started_at": started_at,
        "updated_at": _now(),
        "exit_code": exit_code,
        "elapsed_ms": elapsed_ms,
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-4000:],
        "failure": failure,
        "infrastructure_interrupted": (
            state == "infrastructure_interrupted"
            or _windows_control_interrupted(exit_code)
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harbor", type=Path)
    parser.add_argument("--harbor-project", type=Path)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--jobs-dir", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, required=True)
    parser.add_argument("--status", type=Path, required=True)
    args = parser.parse_args()
    if (args.harbor is None) == (args.harbor_project is None):
        raise SystemExit("provide exactly one of --harbor or --harbor-project")

    launcher = (
        [str(args.harbor.resolve())]
        if args.harbor is not None
        else [
            os.environ.get("UV_EXECUTABLE", "uv"),
            "run",
            "--project",
            str(args.harbor_project.resolve()),
            "harbor",
        ]
    )
    command = [
        *launcher,
        "run",
        "-c",
        str(args.config.resolve()),
        "-o",
        str(args.jobs_dir.resolve()),
        "-n",
        "1",
        "-y",
    ]
    # LHTB configs resolve `./tasks` relative to the benchmark repository root,
    # not relative to `configs/examples/`.
    repo_root = (
        args.harbor_project.resolve().parent
        if args.harbor_project is not None
        else args.config.resolve().parents[2]
    )
    timeout_seconds = float(args.timeout_seconds)
    isolation = _process_isolation()
    started_at = _now()
    started = time.monotonic()
    failure = ""
    exit_code: int | None = None
    stdout = ""
    stderr = ""
    process: subprocess.Popen[str] | None = None
    windows_job_handle: int | None = None
    _atomic_write_json(
        args.status,
        _status_payload(
            state="running",
            command=command,
            repo_root=repo_root,
            timeout_seconds=timeout_seconds,
            isolation=isolation,
            started_at=started_at,
        ),
    )
    try:
        process = subprocess.Popen(
            command,
            cwd=repo_root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=int(isolation["creationflags"]),
            start_new_session=bool(isolation["start_new_session"]),
        )
        if os.name == "nt":
            windows_job_handle = _create_windows_kill_job(process.pid)
        _atomic_write_json(
            args.status,
            _status_payload(
                state="running",
                command=command,
                repo_root=repo_root,
                timeout_seconds=timeout_seconds,
                isolation=isolation,
                started_at=started_at,
                harbor_pid=process.pid,
                windows_job_assigned=bool(windows_job_handle),
            ),
        )
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        exit_code = int(process.returncode)
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = _decode_output(exc.stdout)
        stderr = _decode_output(exc.stderr)
        failure = f"Harbor timed out after {timeout_seconds}s"
        if process is not None:
            _terminate_process_tree(
                process,
                windows_job_handle=windows_job_handle,
            )
            tail_stdout, tail_stderr = process.communicate()
            stdout += _decode_output(tail_stdout)
            stderr += _decode_output(tail_stderr)
    except KeyboardInterrupt:
        exit_code = WINDOWS_CONTROL_C_EXIT
        failure = "KeyboardInterrupt: Harbor worker received a console interrupt"
        if process is not None:
            _terminate_process_tree(
                process,
                windows_job_handle=windows_job_handle,
            )
            tail_stdout, tail_stderr = process.communicate()
            stdout += _decode_output(tail_stdout)
            stderr += _decode_output(tail_stderr)
    except BaseException as exc:
        exit_code = 125
        failure = f"{type(exc).__name__}: {exc}"
        if process is not None:
            _terminate_process_tree(
                process,
                windows_job_handle=windows_job_handle,
            )
            tail_stdout, tail_stderr = process.communicate()
            stdout += _decode_output(tail_stdout)
            stderr += _decode_output(tail_stderr)
    finally:
        _close_windows_job(windows_job_handle)

    state = (
        "infrastructure_interrupted"
        if _windows_control_interrupted(exit_code)
        else "timed_out"
        if exit_code == 124
        else "finished"
    )
    _atomic_write_json(
        args.status,
        _status_payload(
            state=state,
            command=command,
            repo_root=repo_root,
            timeout_seconds=timeout_seconds,
            isolation=isolation,
            started_at=started_at,
            harbor_pid=None if process is None else process.pid,
            windows_job_assigned=(
                None if os.name != "nt" else bool(windows_job_handle)
            ),
            exit_code=exit_code,
            elapsed_ms=round((time.monotonic() - started) * 1000, 3),
            stdout=stdout,
            stderr=stderr,
            failure=failure,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
