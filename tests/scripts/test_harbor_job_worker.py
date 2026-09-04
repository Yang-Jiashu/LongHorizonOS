from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from scripts import harbor_job_worker as worker


def test_atomic_status_write_preserves_previous_file_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "status.json"
    worker._atomic_write_json(path, {"status": "running"})

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("simulated interruption")

    monkeypatch.setattr(worker.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated interruption"):
        worker._atomic_write_json(path, {"status": "finished"})

    assert json.loads(path.read_text(encoding="utf-8")) == {
        "status": "running"
    }
    assert list(tmp_path.glob(".*.tmp")) == []


def test_process_isolation_declares_platform_process_group() -> None:
    isolation = worker._process_isolation()

    assert isolation["process_group_isolated"] is True
    if os.name == "nt":
        assert "CREATE_NEW_PROCESS_GROUP" in isolation["creationflag_names"]
        assert "CREATE_NO_WINDOW" in isolation["creationflag_names"]
        assert isolation["start_new_session"] is False
        assert isolation["tree_supervision"] == (
            "windows_job_object_or_taskkill_fallback"
        )
    else:
        assert isolation["start_new_session"] is True
        assert isolation["tree_supervision"] == "posix_process_group"


@pytest.mark.parametrize(
    ("harbor_exit", "expected_status", "interrupted"),
    [
        (0, "finished", False),
        (worker.WINDOWS_CONTROL_C_EXIT, "infrastructure_interrupted", True),
    ],
)
def test_main_writes_running_then_atomic_terminal_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    harbor_exit: int,
    expected_status: str,
    interrupted: bool,
) -> None:
    status = tmp_path / "worker-status.json"
    config = tmp_path / "configs" / "job.yaml"
    config.parent.mkdir()
    config.write_text("job_name: fixture\n", encoding="utf-8")
    harbor_project = tmp_path / "harbor"
    harbor_project.mkdir()
    observed: list[dict[str, object]] = []

    class FakeProcess:
        pid = 4242
        returncode = harbor_exit

        def communicate(self, timeout=None):
            current = json.loads(status.read_text(encoding="utf-8"))
            observed.append(current)
            return "stdout", "stderr"

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            return None

    def popen(*_args, **_kwargs):
        initial = json.loads(status.read_text(encoding="utf-8"))
        assert initial["status"] == "running"
        assert initial["harbor_pid"] is None
        return FakeProcess()

    monkeypatch.setattr(worker.subprocess, "Popen", popen)
    monkeypatch.setattr(worker, "_create_windows_kill_job", lambda _pid: 99)
    monkeypatch.setattr(worker, "_close_windows_job", lambda _handle: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "harbor_job_worker.py",
            "--harbor-project",
            str(harbor_project),
            "--config",
            str(config),
            "--jobs-dir",
            str(tmp_path / "jobs"),
            "--timeout-seconds",
            "10",
            "--status",
            str(status),
        ],
    )

    assert worker.main() == 0

    assert observed[0]["status"] == "running"
    assert observed[0]["harbor_pid"] == 4242
    final = json.loads(status.read_text(encoding="utf-8"))
    assert final["schema_version"] == worker.STATUS_SCHEMA_VERSION
    assert final["status"] == expected_status
    assert final["harbor_pid"] == 4242
    assert final["process_group_id"] == 4242
    assert final["exit_code"] == harbor_exit
    assert final["infrastructure_interrupted"] is interrupted
    assert final["stdout_tail"] == "stdout"
    assert final["stderr_tail"] == "stderr"
