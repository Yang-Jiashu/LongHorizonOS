"""LongHorizonOS E2 — ShellTool (local, capability-governed).

Runs a command in a controlled working directory with a timeout and output
bound, governed by a Capability.  Capability-denied yields a developer-facing
error (never a silent fallback to a raw subprocess).  A ShellTool result is an
*execution fact*, not a semantic conclusion: only a CommandVerifier → Evidence →
VPG path can make a task VERIFIED.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from .base import ToolResult


@dataclass
class ShellTool:
    """Capability-governed local shell.

    ``capability`` is checked before each run by the binder (the AgentOS facade
    calls ``check_capability`` before invoking).  If capability is denied, callers
    surface a CapabilityError rather than falling back to a raw subprocess.
    """

    capability: str = "shell"
    timeout_s: float = 15.0
    max_output_chars: int = 32_000

    def run(
        self,
        command: str,
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        check_capability=None,
    ) -> ToolResult:
        """If ``check_capability(capability)`` returns falsy, the run is denied."""
        if check_capability is not None and not check_capability(self.capability):
            return ToolResult(
                ok=False, error=f"capability {self.capability!r} denied", kind="shell"
            )
        try:
            proc = subprocess.run(
                command,
                shell=True,
                cwd=str(cwd) if cwd else None,
                env=dict(env) if env else None,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )
        except subprocess.TimeoutExpired:
            return ToolResult(
                ok=False, error=f"shell timeout after {self.timeout_s}s", kind="shell"
            )
        except Exception as e:
            return ToolResult(ok=False, error=f"shell failed: {e}", kind="shell")
        out = (proc.stdout or "")[: self.max_output_chars]
        err = (proc.stderr or "")[: self.max_output_chars]
        return ToolResult(
            ok=proc.returncode == 0,
            value={"exit_code": proc.returncode, "stdout": out, "stderr": err},
            kind="shell",
        )
