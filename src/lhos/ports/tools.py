"""Tool runtime port (spec sections 13.1-13.2).

Note: the spec sketches ``async def execute``; the MVP is single-worker and
sequential, so the port is synchronous. Names and semantics are unchanged.
"""

from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel, Field

from lhos.domain.models import EvidenceRef, ResourceClaim


class ToolRequest(BaseModel):
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 60
    idempotency_key: str = ""
    resource_claim: ResourceClaim | None = None


class ToolResult(BaseModel):
    success: bool
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    artifacts: list[EvidenceRef] = Field(default_factory=list)
    environment_delta: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime
    finished_at: datetime


class ToolMetadata(BaseModel):
    name: str
    side_effect_level: str  # read_only | local_write | external_write | destructive
    retry_safe: bool
    default_timeout_seconds: int
    supports_idempotency: bool


class Tool(Protocol):
    name: str

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult: ...
