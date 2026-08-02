"""Scheduler protocol and shared types (spec section 11.1)."""

from typing import Protocol

from pydantic import BaseModel, Field

from lhos.domain.budgets import BudgetState
from lhos.domain.models import GraphNode
from lhos.graph.queries import ProgressGraph


class ResourceState(BaseModel):
    """Single-worker MVP: no real resource pools; tracks context-switch info."""

    available: list[str] = Field(default_factory=list)
    last_tool_type: str | None = None


class Scheduler(Protocol):
    def select(
        self,
        ready_nodes: list[GraphNode],
        graph: ProgressGraph,
        budget: BudgetState,
        resources: ResourceState,
    ) -> GraphNode | None: ...
