"""Agent Program Protocol — event-driven coroutine model.

The kernel calls step(state, event) -> ProgramStepResult.
Each step yields at most one KernelRequest.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from lhos.agent_os.kernel.models import KernelEvent


class ProgramStepResult(BaseModel):
    """Result of a single program step."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    new_state: dict[str, Any] = Field(default_factory=dict)
    request: Any | None = None  # KernelRequest | None
    exit_code: str | None = None
    result_ref: str | None = None


@runtime_checkable
class AgentProgram(Protocol):
    """Event-driven agent program protocol."""

    @property
    def program_id(self) -> str: ...

    async def step(
        self,
        state: dict[str, Any],
        event: KernelEvent | None,
    ) -> ProgramStepResult: ...
