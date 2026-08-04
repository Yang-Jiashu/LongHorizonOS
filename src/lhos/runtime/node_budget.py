"""Per-node execution budget (Milestone 2.2 Step 7).

Tracks resource usage per node independently of the global budget.
When a single node exceeds its budget, a NODE_LOCAL_BUDGET_EXHAUSTED
failure code is produced, triggering local failure analysis.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel


class NodeBudgetConfig(BaseModel):
    """Configuration for per-node execution budget."""

    # Defaults are set above _MAX_TOOL_ROUNDS (20) so the round limit fires
    # first in normal cases; the per-node budget acts as a safety net for
    # runaway token usage or excessive wall time.
    max_model_calls: int = 25
    max_tool_calls: int = 30
    max_total_tokens: int = 200_000
    max_wall_time_seconds: float = 600.0  # 10 minutes per node


class NodeBudgetState(BaseModel):
    """Accumulated usage for a single node."""

    node_id: str
    model_calls: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    wall_time_seconds: float = 0.0
    external_progress_delta: int = 0
    started_at: float | None = None

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def is_exhausted(self) -> bool:
        """Check if this node has exceeded its budget."""
        return (
            self.model_calls >= _DEFAULT_CONFIG.max_model_calls
            or self.tool_calls >= _DEFAULT_CONFIG.max_tool_calls
            or self.total_tokens >= _DEFAULT_CONFIG.max_total_tokens
            or self.wall_time_seconds >= _DEFAULT_CONFIG.max_wall_time_seconds
        )


_DEFAULT_CONFIG = NodeBudgetConfig()


class NodeExecutionBudget:
    """Tracks per-node execution budget.

    Each node gets its own budget tracker. When the budget is exhausted,
    the controller should stop the worker loop and emit a specific failure code.
    """

    def __init__(self, config: NodeBudgetConfig | None = None) -> None:
        self._config = config or _DEFAULT_CONFIG
        self._states: dict[str, NodeBudgetState] = {}

    def start_node(self, node_id: str) -> NodeBudgetState:
        """Start tracking a node execution."""
        state = NodeBudgetState(node_id=node_id, started_at=time.monotonic())
        self._states[node_id] = state
        return state

    def get_state(self, node_id: str) -> NodeBudgetState | None:
        return self._states.get(node_id)

    def record_model_call(
        self, node_id: str, input_tokens: int, output_tokens: int
    ) -> NodeBudgetState | None:
        state = self._states.get(node_id)
        if state is None:
            return None
        state.model_calls += 1
        state.input_tokens += input_tokens
        state.output_tokens += output_tokens
        self._update_wall_time(state)
        return state

    def record_tool_call(self, node_id: str) -> NodeBudgetState | None:
        state = self._states.get(node_id)
        if state is None:
            return None
        state.tool_calls += 1
        self._update_wall_time(state)
        return state

    def record_progress_delta(self, node_id: str, delta: int) -> None:
        state = self._states.get(node_id)
        if state is not None:
            state.external_progress_delta += delta

    def is_exhausted(self, node_id: str) -> bool:
        """Check if a node has exceeded its per-node budget."""
        state = self._states.get(node_id)
        if state is None:
            return False
        self._update_wall_time(state)
        return (
            state.model_calls >= self._config.max_model_calls
            or state.tool_calls >= self._config.max_tool_calls
            or state.total_tokens >= self._config.max_total_tokens
            or state.wall_time_seconds >= self._config.max_wall_time_seconds
        )

    def get_exhaustion_reason(self, node_id: str) -> str | None:
        """Return the specific reason for budget exhaustion, if any."""
        state = self._states.get(node_id)
        if state is None:
            return None
        self._update_wall_time(state)
        if state.model_calls >= self._config.max_model_calls:
            return f"model_calls ({state.model_calls}) >= {self._config.max_model_calls}"
        if state.tool_calls >= self._config.max_tool_calls:
            return f"tool_calls ({state.tool_calls}) >= {self._config.max_tool_calls}"
        if state.total_tokens >= self._config.max_total_tokens:
            return f"tokens ({state.total_tokens}) >= {self._config.max_total_tokens}"
        if state.wall_time_seconds >= self._config.max_wall_time_seconds:
            return f"wall_time ({state.wall_time_seconds:.1f}s) >= {self._config.max_wall_time_seconds}s"
        return None

    def get_report(self, node_id: str) -> dict[str, Any]:
        """Return a per-node budget report for diagnostics."""
        state = self._states.get(node_id)
        if state is None:
            return {"node_id": node_id, "tracked": False}
        self._update_wall_time(state)
        return {
            "node_id": node_id,
            "model_calls": state.model_calls,
            "tool_calls": state.tool_calls,
            "input_tokens": state.input_tokens,
            "output_tokens": state.output_tokens,
            "total_tokens": state.total_tokens,
            "wall_time_seconds": round(state.wall_time_seconds, 2),
            "external_progress_delta": state.external_progress_delta,
            "is_exhausted": self.is_exhausted(node_id),
            "exhaustion_reason": self.get_exhaustion_reason(node_id),
        }

    def clear(self, node_id: str) -> None:
        """Clear budget tracking for a node (after completion)."""
        self._states.pop(node_id, None)

    @staticmethod
    def _update_wall_time(state: NodeBudgetState) -> None:
        if state.started_at is not None:
            state.wall_time_seconds = time.monotonic() - state.started_at
