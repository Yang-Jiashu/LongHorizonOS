"""Benchmark adapter port (spec section 23). Implemented in a later phase."""

from typing import Any, Protocol

from pydantic import BaseModel, Field


class BenchmarkScore(BaseModel):
    success: bool
    verified_progress: float
    total_progress: float
    task_specific_metrics: dict[str, float] = Field(default_factory=dict)


class BenchmarkAdapter(Protocol):
    async def reset(self, task_id: str, seed: int) -> None: ...

    async def get_goal(self) -> str: ...

    async def get_environment_snapshot(self) -> dict[str, Any]: ...

    async def score(self) -> BenchmarkScore: ...

    async def cleanup(self) -> None: ...
