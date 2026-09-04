"""LongHorizonOS E2 — minimal Git tool (read-only by default).

Provides git status / diff / current revision — the valuable read surface.  Writes
(commit/etc.) are excluded from E2 scope (must be very conservative).  Capability-
governed through the ShellTool path so it is attributable/auditable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .base import ToolResult
from .shell import ShellTool


@dataclass
class GitTool:
    workspace_root: str | Path
    capability: str = "git"
    shell: ShellTool | None = None

    @property
    def name(self) -> str:
        return f"git({self.workspace_root})"

    def _sh(self, command: str, *, check_capability=None) -> ToolResult:
        sh = self.shell or ShellTool(capability=self.capability, trusted=True)
        cwd = Path(self.workspace_root)

        # re-bind capability to 'git' via a wrapper check
        def _check(cap: str) -> bool:
            if check_capability is None:
                return True
            return bool(check_capability(self.capability))

        return sh.run(command, cwd=cwd, check_capability=_check)

    def status(self, *, check_capability=None) -> ToolResult:
        r = self._sh("git status --short", check_capability=check_capability)
        r.kind = "git"
        return r

    def diff(self, *, check_capability=None) -> ToolResult:
        r = self._sh("git diff HEAD", check_capability=check_capability)
        r.kind = "git"
        return r

    def rev(self, *, check_capability=None) -> ToolResult:
        r = self._sh("git rev-parse HEAD", check_capability=check_capability)
        r.kind = "git"
        return r
