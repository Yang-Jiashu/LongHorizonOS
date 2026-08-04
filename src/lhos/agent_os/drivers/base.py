"""Base Driver protocol and result types."""

from __future__ import annotations

from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field


class DriverResult(BaseModel):
    """Result of a driver execution."""

    status: Literal["completed", "running", "failed", "unknown"] = "completed"
    output: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
    side_effect_recorded: bool = False


class DriverInspect(BaseModel):
    """Result of inspecting a driver action (for crash recovery)."""

    status: Literal["completed", "running", "failed", "unknown"] = "unknown"
    output: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None


class BaseDriver(Protocol):
    """Driver protocol — dispatches actions to external systems."""

    @property
    def device_type(self) -> str: ...

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult: ...

    async def inspect(self, action_id: str) -> DriverInspect: ...

    def reset(self) -> None: ...
