"""Scripted fake tool for tests: deterministic, no side effects by default."""

from __future__ import annotations

from datetime import datetime

from lhos.domain.errors import SimulatedCrashError, ToolExecutionError
from lhos.ports.tools import ToolMetadata, ToolRequest, ToolResult


def _now() -> datetime:
    return datetime.now().astimezone()


class FakeTool:
    """Returns scripted results in order; raises when the script is exhausted
    or when the scripted entry has ``"fail": true``."""

    name = "fake"

    def __init__(self, script: list[dict] | None = None):
        self._script = list(script or [])
        self.calls: list[ToolRequest] = []

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult:
        self.calls.append(request)
        started = _now()
        if request.arguments.get("simulate_crash"):
            # Process death DURING tool execution (spec 26.2): the runtime
            # must not write a terminal event for this call.
            raise SimulatedCrashError("simulated mid-tool crash")
        if not self._script:
            raise ToolExecutionError("fake tool script exhausted")
        entry = self._script.pop(0)
        if entry.get("fail"):
            raise ToolExecutionError(entry.get("stderr", "scripted fake tool failure"))
        return ToolResult(
            success=entry.get("success", True),
            exit_code=entry.get("exit_code", 0),
            stdout=entry.get("stdout", ""),
            stderr=entry.get("stderr", ""),
            environment_delta=entry.get("environment_delta", {}),
            started_at=started,
            finished_at=_now(),
        )


FAKE_METADATA = ToolMetadata(
    name="fake",
    side_effect_level="read_only",
    retry_safe=True,
    default_timeout_seconds=10,
    supports_idempotency=True,
)
