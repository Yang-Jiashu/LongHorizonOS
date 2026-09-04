"""Run prepared LHTB experiment families serially.

The queue deliberately permits only one Docker experiment family at a time.
That keeps CPU, memory, I/O, and provider contention from invalidating the
controlled wall-time comparison while still allowing each family to use its
prepared resource-aware concurrency.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

SCHEMA_VERSION = "lhos-lhtb-controlled-queue.v1"
STATE_SCHEMA_VERSION = "lhos-lhtb-controlled-queue-state.v1"


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _pid_exists(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    if os.name == "nt":
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 0 }} else {{ exit 1 }}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return completed.returncode == 0
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _run_complete(output: Path) -> bool:
    admission = output / "pair-admission.json"
    result_exists = (output / "result.json").is_file() or (output / "arm-result.json").is_file()
    if not admission.is_file() or not result_exists:
        return False
    try:
        return _load_json(admission).get("status") == "completed"
    except (OSError, ValueError, TypeError):
        return False


def _preflight_output(output: Path) -> None:
    manifest = output / "manifest.json"
    prebuild = output / "prebuild.json"
    if not manifest.is_file():
        raise RuntimeError(f"prepared manifest is missing: {manifest}")
    if not prebuild.is_file() or _load_json(prebuild).get("complete") is not True:
        raise RuntimeError(f"complete prebuild is missing: {prebuild}")


def _creationflags() -> int:
    if os.name != "nt":
        return 0
    return int(
        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        | getattr(subprocess, "CREATE_NO_WINDOW", 0)
    )


def _step_environment(repo: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        (
            str(repo),
            str(repo / "src"),
            env.get("PYTHONPATH", ""),
        )
    ).rstrip(os.pathsep)
    return env


def _run_step(
    *,
    repo: Path,
    python: str,
    credential_env: str,
    step: dict[str, Any],
) -> int:
    output = Path(str(step["output"])).resolve()
    jobs_dir = Path(str(step["jobs_dir"])).resolve()
    _preflight_output(output)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    command = [
        python,
        str(repo / "scripts" / "run_lhtb_software5_pair.py"),
        "run",
        "--output",
        str(output),
        "--jobs-dir",
        str(jobs_dir),
        "--credential-env",
        credential_env,
        "--arm",
        str(step.get("arm", "both")),
    ]
    if step.get("resource_aware_pairs", True):
        command.append("--resource-aware-pairs")
    for option, key in (
        ("--max-concurrency", "max_concurrency"),
        ("--pair-capacity-cpus", "pair_capacity_cpus"),
        ("--pair-capacity-memory-mb", "pair_capacity_memory_mb"),
    ):
        if step.get(key) is not None:
            command.extend((option, str(step[key])))

    stdout_path = output / "queue-run.stdout.log"
    stderr_path = output / "queue-run.stderr.log"
    env = _step_environment(repo)
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        completed = subprocess.run(
            command,
            cwd=repo,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=False,
            creationflags=_creationflags(),
            start_new_session=os.name != "nt",
        )
    return int(completed.returncode)


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=15)


def _wait_for_checkpoint_watcher(
    *,
    process: subprocess.Popen[bytes],
    index_path: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(f"checkpoint watcher exited before startup handshake: {return_code}")
        try:
            index = _load_json(index_path)
        except (OSError, ValueError, TypeError) as exc:
            last_error = exc
        else:
            if index.get("status") == "watching":
                return index
        time.sleep(0.2)
    detail = "" if last_error is None else f"; last index error: {last_error}"
    raise RuntimeError(f"checkpoint watcher startup timed out{detail}")


def _run_checkpointed_step(
    *,
    repo: Path,
    python: str,
    credential_env: str,
    step: dict[str, Any],
) -> int:
    output = Path(str(step["output"])).resolve()
    jobs_dir = Path(str(step["jobs_dir"])).resolve()
    checkpoint_root = Path(
        str(step.get("checkpoint_root") or output / "external-checkpoints")
    ).resolve()
    _preflight_output(output)
    jobs_dir.mkdir(parents=True, exist_ok=True)

    command = [
        python,
        str(repo / "scripts" / "lhtb_external_checkpoint.py"),
        "watch",
        "--output",
        str(output),
        "--jobs-dir",
        str(jobs_dir),
        "--checkpoint-root",
        str(checkpoint_root),
        "--poll-seconds",
        str(float(step.get("checkpoint_poll_seconds", 0.25))),
        "--capture-concurrency",
        str(int(step.get("checkpoint_capture_concurrency", 2))),
        "--max-capture-attempts",
        str(int(step.get("checkpoint_max_capture_attempts", 3))),
    ]
    for cutoff in step.get("checkpoint_cutoffs", ()):
        command.extend(("--cutoff", str(int(cutoff))))

    stdout_path = output / "checkpoint-watch.stdout.log"
    stderr_path = output / "checkpoint-watch.stderr.log"
    process_record_path = output / "checkpoint-watch-process.json"
    process_record: dict[str, Any] = {
        "schema_version": "lhos-lhtb-checkpoint-watch-process.v1",
        "checkpoint_root": str(checkpoint_root),
        "credential_env": credential_env,
        "credential_value_persisted": False,
        "cutoffs_active_seconds": [int(value) for value in step.get("checkpoint_cutoffs", ())],
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "status": "starting",
        "started_at": _now(),
    }
    _atomic_write_json(process_record_path, process_record)

    env = _step_environment(repo)
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        watcher = subprocess.Popen(
            command,
            cwd=repo,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            creationflags=_creationflags(),
            start_new_session=os.name != "nt",
        )
        process_record["pid"] = watcher.pid
        _atomic_write_json(process_record_path, process_record)
        try:
            _wait_for_checkpoint_watcher(
                process=watcher,
                index_path=checkpoint_root / "index.json",
                timeout_seconds=max(
                    1.0,
                    float(step.get("checkpoint_startup_timeout_seconds", 60.0)),
                ),
            )
            process_record.update({"status": "watching", "ready_at": _now()})
            _atomic_write_json(process_record_path, process_record)

            runner_exit = _run_step(
                repo=repo,
                python=python,
                credential_env=credential_env,
                step=step,
            )
            process_record["runner_exit_code"] = runner_exit
            process_record["runner_finished_at"] = _now()
            _atomic_write_json(process_record_path, process_record)
            try:
                watcher_exit = watcher.wait(
                    timeout=max(
                        1.0,
                        float(step.get("checkpoint_completion_timeout_seconds", 600.0)),
                    )
                )
            except subprocess.TimeoutExpired:
                process_record["status"] = "watcher_completion_timeout"
                process_record["updated_at"] = _now()
                _atomic_write_json(process_record_path, process_record)
                _stop_process(watcher)
                return runner_exit if runner_exit != 0 else 124
        except Exception as exc:
            process_record.update(
                {
                    "status": "failed",
                    "error": {"type": type(exc).__name__, "message": str(exc)[:1200]},
                    "updated_at": _now(),
                }
            )
            _atomic_write_json(process_record_path, process_record)
            _stop_process(watcher)
            raise

    process_record.update(
        {
            "status": "completed" if watcher_exit == 0 else "watcher_failed",
            "watcher_exit_code": watcher_exit,
            "finished_at": _now(),
            "updated_at": _now(),
        }
    )
    _atomic_write_json(process_record_path, process_record)
    return runner_exit if runner_exit != 0 else int(watcher_exit)


def _merge_step(
    *,
    repo: Path,
    python: str,
    step: dict[str, Any],
) -> int:
    output = Path(str(step["output"])).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        python,
        str(repo / "scripts" / "run_lhtb_software5_pair.py"),
        "merge-arms",
        "--fresh-output",
        str(Path(str(step["fresh_output"])).resolve()),
        "--lhos-output",
        str(Path(str(step["lhos_output"])).resolve()),
        "--output",
        str(output),
    ]
    stdout_path = output.parent / f"{output.name}.queue-merge.stdout.log"
    stderr_path = output.parent / f"{output.name}.queue-merge.stderr.log"
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        completed = subprocess.run(
            command,
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=False,
            creationflags=_creationflags(),
            start_new_session=os.name != "nt",
        )
    return int(completed.returncode)


def _summarize(repo: Path, python: str, output: Path) -> int:
    with (
        (output / "queue-summarize.stdout.log").open("ab") as stdout,
        (output / "queue-summarize.stderr.log").open("ab") as stderr,
    ):
        completed = subprocess.run(
            [
                python,
                str(repo / "scripts" / "run_lhtb_software5_pair.py"),
                "summarize",
                "--output",
                str(output),
            ],
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=False,
            creationflags=_creationflags(),
            start_new_session=os.name != "nt",
        )
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    parser.add_argument("--credential-env", default="STEPFUN_API_KEY")
    args = parser.parse_args()

    if not os.environ.get(args.credential_env, "").strip():
        raise SystemExit(f"missing credential environment variable: {args.credential_env}")

    plan = _load_json(args.plan.resolve())
    if plan.get("schema_version") != SCHEMA_VERSION:
        raise SystemExit(f"unsupported queue plan schema: {plan.get('schema_version')!r}")
    repo = Path(str(plan["repo"])).resolve()
    python = str(plan.get("python") or sys.executable)
    wait_for = dict(plan["wait_for"])
    primary_output = Path(str(wait_for["output"])).resolve()
    primary_pid = int(wait_for["pid"]) if wait_for.get("pid") is not None else None

    state: dict[str, Any] = {
        "schema_version": STATE_SCHEMA_VERSION,
        "plan": str(args.plan.resolve()),
        "started_at": _now(),
        "credential_env": args.credential_env,
        "credential_value_persisted": False,
        "status": "waiting_for_primary",
        "steps": [],
        "updated_at": _now(),
    }
    _atomic_write_json(args.state.resolve(), state)

    while not _run_complete(primary_output):
        if not _pid_exists(primary_pid):
            state.update(
                {
                    "status": "blocked",
                    "blocking_reason": "primary process exited before a complete result",
                    "updated_at": _now(),
                }
            )
            _atomic_write_json(args.state.resolve(), state)
            return 2
        time.sleep(max(1.0, float(args.poll_seconds)))

    state.update({"status": "running_steps", "updated_at": _now()})
    _atomic_write_json(args.state.resolve(), state)
    primary_summary_exit = _summarize(repo, python, primary_output)
    if primary_summary_exit != 0:
        state.update(
            {
                "status": "blocked",
                "blocking_reason": "primary summarize failed",
                "primary_summarize_exit_code": primary_summary_exit,
                "updated_at": _now(),
            }
        )
        _atomic_write_json(args.state.resolve(), state)
        return primary_summary_exit

    for index, step in enumerate(plan.get("steps", ()), start=1):
        name = str(step["name"])
        kind = str(step.get("kind", "run"))
        output = Path(str(step["output"])).resolve()
        record: dict[str, Any] = {
            "index": index,
            "name": name,
            "kind": kind,
            "output": str(output),
            "started_at": _now(),
            "status": "running",
        }
        if kind == "run":
            record["jobs_dir"] = str(Path(str(step["jobs_dir"])).resolve())
            record["arm"] = str(step.get("arm", "both"))
            record["external_checkpoint_watch"] = bool(step.get("external_checkpoint_watch", False))
        elif kind == "merge_arms":
            record["fresh_output"] = str(Path(str(step["fresh_output"])).resolve())
            record["lhos_output"] = str(Path(str(step["lhos_output"])).resolve())
        else:
            raise RuntimeError(f"unsupported queue step kind: {kind!r}")
        state["steps"].append(record)
        state["current_step"] = name
        state["updated_at"] = _now()
        _atomic_write_json(args.state.resolve(), state)

        already_complete = (kind == "run" and _run_complete(output)) or (
            kind == "merge_arms" and (output / "result.json").is_file()
        )
        if already_complete:
            exit_code = 0
            record["status"] = "already_completed"
        elif kind == "merge_arms":
            exit_code = _merge_step(
                repo=repo,
                python=python,
                step=step,
            )
            record["status"] = "completed" if exit_code == 0 else "failed"
        else:
            if step.get("external_checkpoint_watch", False):
                exit_code = _run_checkpointed_step(
                    repo=repo,
                    python=python,
                    credential_env=args.credential_env,
                    step=step,
                )
            else:
                exit_code = _run_step(
                    repo=repo,
                    python=python,
                    credential_env=args.credential_env,
                    step=step,
                )
            record["status"] = "completed" if exit_code == 0 else "failed"
        record["exit_code"] = exit_code
        record["finished_at"] = _now()
        state["updated_at"] = _now()
        _atomic_write_json(args.state.resolve(), state)
        if exit_code != 0:
            state.update(
                {
                    "status": "blocked",
                    "blocking_reason": f"step {name!r} failed",
                    "updated_at": _now(),
                }
            )
            _atomic_write_json(args.state.resolve(), state)
            return exit_code

        summary_exit = _summarize(repo, python, output)
        record["summarize_exit_code"] = summary_exit
        state["updated_at"] = _now()
        _atomic_write_json(args.state.resolve(), state)
        if summary_exit != 0:
            state.update(
                {
                    "status": "blocked",
                    "blocking_reason": f"step {name!r} summarize failed",
                    "updated_at": _now(),
                }
            )
            _atomic_write_json(args.state.resolve(), state)
            return summary_exit

        cooldown_seconds = max(0.0, float(step.get("cooldown_seconds", 0.0)))
        if cooldown_seconds:
            record["cooldown_seconds"] = cooldown_seconds
            state["status"] = "cooldown"
            state["updated_at"] = _now()
            _atomic_write_json(args.state.resolve(), state)
            time.sleep(cooldown_seconds)
            state["status"] = "running_steps"
            state["updated_at"] = _now()
            _atomic_write_json(args.state.resolve(), state)

    state.pop("current_step", None)
    state.update({"status": "completed", "completed_at": _now(), "updated_at": _now()})
    _atomic_write_json(args.state.resolve(), state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
