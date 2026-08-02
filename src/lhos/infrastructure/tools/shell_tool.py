"""Shell command tool. Conservative: declared local_write, not retry-safe."""

from __future__ import annotations

import subprocess
from datetime import datetime

from lhos.domain.errors import ToolExecutionError
from lhos.ports.tools import ToolMetadata, ToolRequest, ToolResult


def _now() -> datetime:
    return datetime.now().astimezone()


class ShellTool:
    name = "shell"

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult:
        command = request.arguments.get("command")
        if not command:
            raise ToolExecutionError("shell tool requires arguments.command")
        started = _now()
        try:
            proc = subprocess.run(
                command,
                shell=True,
                cwd=workspace_dir,
                capture_output=True,
                text=True,
                timeout=request.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise ToolExecutionError(
                f"shell command timed out after {request.timeout_seconds}s: {command}"
            ) from exc
        finished = _now()
        return ToolResult(
            success=proc.returncode == 0,
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            started_at=started,
            finished_at=finished,
        )


SHELL_METADATA = ToolMetadata(
    name="shell",
    side_effect_level="local_write",
    retry_safe=False,
    default_timeout_seconds=60,
    supports_idempotency=True,
)
