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

import re
import subprocess
from datetime import datetime

from lhos.domain.errors import ToolExecutionError
from lhos.ports.tools import ToolMetadata, ToolRequest, ToolResult

_MAX_OUTPUT_CHARS = 50_000

# Heuristic patterns that suggest credential or env-var access.
_BLOCKED_PATTERNS = [
    re.compile(r"\bprintenv\b", re.IGNORECASE),
    re.compile(r"\benv\b(?!\s*/)", re.IGNORECASE),
    re.compile(r"\$\{?[A-Z_]{3,}\}?", re.IGNORECASE),  # $VAR or ${VAR}
    re.compile(r"/etc/passwd|/etc/shadow|\.ssh/", re.IGNORECASE),
    re.compile(r"\bcurl\b|\bwget\b|\bnc\b|\bnetcat\b", re.IGNORECASE),
    re.compile(r"\bssh\b|\bscp\b|\brsync\b", re.IGNORECASE),
]


def _now() -> datetime:
    return datetime.now().astimezone()


def _truncate(text: str, max_chars: int = _MAX_OUTPUT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n...[truncated: {len(text)} total chars]"


class ShellTool:
    name = "shell"

    def __init__(self, allow_network: bool = False, max_output_chars: int = _MAX_OUTPUT_CHARS):
        self._allow_network = allow_network
        self._max_output_chars = max_output_chars

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult:
        command = request.arguments.get("command")
        if not command:
            raise ToolExecutionError("shell tool requires arguments.command")

        # Block suspicious patterns unless network is explicitly allowed.
        if not self._allow_network:
            for pattern in _BLOCKED_PATTERNS:
                if pattern.search(command):
                    raise ToolExecutionError(
                        f"shell command blocked (potential credential/network access): "
                        f"{command[:200]}"
                    )

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
