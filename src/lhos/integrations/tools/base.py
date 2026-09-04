"""LongHorizonOS E2 — tool driver base + ToolResult (execution facts only)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ToolResult:
    """Execution fact from a tool; NEVER a semantic conclusion.

    A ToolResult becomes semantic only if a Verifier interprets it and VPG
    derives VERIFIED from resulting Evidence.
    """

    ok: bool
    value: Any = ""
    kind: str = ""  # "shell" | "workspace" | "git" | ...
    action_id: str = ""  # Kernel Action attribute, when available
    error: str = ""

    @property
    def success(self) -> bool:
        return self.ok

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "value": self.value,
            "kind": self.kind,
            "action_id": self.action_id,
            "error": self.error,
        }
