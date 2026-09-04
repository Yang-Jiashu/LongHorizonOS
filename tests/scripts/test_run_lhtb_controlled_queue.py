from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts import run_lhtb_controlled_queue as queue


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class _FakeProcess:
    def __init__(self, *, return_code: int | None = None) -> None:
        self.pid = 4321
        self.return_code = return_code
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.return_code

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.return_code is None:
            self.return_code = 0
        return self.return_code

    def terminate(self) -> None:
        self.terminated = True
        self.return_code = 1

    def kill(self) -> None:
        self.killed = True
        self.return_code = 1


def test_wait_for_checkpoint_watcher_accepts_ready_index(tmp_path: Path) -> None:
    index_path = tmp_path / "index.json"
    _write_json(index_path, {"status": "watching"})

    result = queue._wait_for_checkpoint_watcher(
        process=_FakeProcess(),  # type: ignore[arg-type]
        index_path=index_path,
        timeout_seconds=1.0,
    )

    assert result == {"status": "watching"}


def test_wait_for_checkpoint_watcher_rejects_early_exit(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="exited before startup handshake: 7"):
        queue._wait_for_checkpoint_watcher(
            process=_FakeProcess(return_code=7),  # type: ignore[arg-type]
            index_path=tmp_path / "missing.json",
            timeout_seconds=1.0,
        )


def test_checkpointed_step_starts_watcher_before_runner_and_does_not_persist_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    output = tmp_path / "output"
    jobs_dir = tmp_path / "jobs"
    repo.mkdir()
    _write_json(output / "manifest.json", {})
    _write_json(output / "prebuild.json", {"complete": True})
    events: list[str] = []
    process = _FakeProcess()
    popen_call: dict[str, Any] = {}

    def fake_popen(command: list[str], **kwargs: Any) -> _FakeProcess:
        events.append("watcher_started")
        popen_call.update({"command": command, **kwargs})
        return process

    def fake_wait(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["process"] is process
        events.append("watcher_ready")
        return {"status": "watching"}

    def fake_run_step(**kwargs: Any) -> int:
        assert kwargs["step"]["output"] == str(output)
        events.append("runner_started")
        return 0

    secret = "provider-secret-must-not-be-persisted"
    monkeypatch.setenv("STEPFUN_API_KEY", secret)
    monkeypatch.setattr(queue.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(queue, "_wait_for_checkpoint_watcher", fake_wait)
    monkeypatch.setattr(queue, "_run_step", fake_run_step)

    result = queue._run_checkpointed_step(
        repo=repo,
        python="python",
        credential_env="STEPFUN_API_KEY",
        step={
            "output": str(output),
            "jobs_dir": str(jobs_dir),
            "checkpoint_cutoffs": [3600, 5400],
        },
    )

    assert result == 0
    assert events == ["watcher_started", "watcher_ready", "runner_started"]
    command = popen_call["command"]
    assert command.count("--cutoff") == 2
    assert "3600" in command
    assert "5400" in command
    assert popen_call["env"]["STEPFUN_API_KEY"] == secret
    process_record = (output / "checkpoint-watch-process.json").read_text(encoding="utf-8")
    assert secret not in process_record
    assert json.loads(process_record)["credential_value_persisted"] is False
