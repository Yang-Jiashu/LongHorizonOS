"""Shell command tool. Conservative: declared local_write, not retry-safe.

Phase 2E enhancements:
- Commands run with ``cwd=workspace_dir`` (workspace confinement).
- Network access is blocked by default (``allow_network=False``).
- stdout/stderr are truncated to ``max_output_chars`` to prevent memory issues.
- Commands that look like they access environment variables or credentials
  are rejected (heuristic check).
- Timeout is always enforced.
"""

from __future__ import annotations

import subprocess
from datetime import datetime

from lhos.domain.errors import ToolExecutionError
from lhos.ports.tools import ToolMetadata, ToolRequest, ToolResult
from lhos.subprocess_policy import CommandPolicyError, run_command

_MAX_OUTPUT_CHARS = 50_000


def _now() -> datetime:
    return datetime.now().astimezone()


def _truncate(text: str, max_chars: int = _MAX_OUTPUT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n...[truncated: {len(text)} total chars]"


class ShellTool:
    name = "shell"

    def __init__(
        self,
        allow_network: bool = False,
        max_output_chars: int = _MAX_OUTPUT_CHARS,
        *,
        trusted: bool = False,
        allow_shell: bool = False,
    ):
        self._allow_network = allow_network
        self._max_output_chars = max_output_chars
        self._trusted = trusted
        self._allow_shell = allow_shell

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult:
        command = request.arguments.get("command")
        if not command:
            raise ToolExecutionError("shell tool requires arguments.command")

        started = _now()
        try:
            proc = run_command(
                command,
                cwd=workspace_dir,
                timeout=request.timeout_seconds,
                trusted=self._trusted,
                allow_shell=self._allow_shell,
                allow_network=self._allow_network,
            )
        except subprocess.TimeoutExpired as exc:
            raise ToolExecutionError(
                f"shell command timed out after {request.timeout_seconds}s: {command}"
            ) from exc
        except CommandPolicyError as exc:
            raise ToolExecutionError(f"shell command denied: {exc}") from exc
        finished = _now()
        return ToolResult(
            success=proc.returncode == 0,
            exit_code=proc.returncode,
            stdout=_truncate(proc.stdout, self._max_output_chars),
            stderr=_truncate(proc.stderr, self._max_output_chars),
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
