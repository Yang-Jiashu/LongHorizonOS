"""Budget limits and state (spec section 17)."""

from pydantic import BaseModel


class BudgetLimits(BaseModel):
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    max_total_tokens: int | None = None
    max_wall_time_seconds: int | None = None
    max_tool_calls: int | None = None
    max_model_calls: int | None = None
    max_cost_usd: float | None = None


class BudgetState(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    model_calls: int = 0
    elapsed_seconds: float = 0.0
    cost_usd: float = 0.0


def budget_exhausted(limits: BudgetLimits, state: BudgetState) -> bool:
    """True when any configured limit has been reached."""
    if limits.max_input_tokens is not None and state.input_tokens >= limits.max_input_tokens:
        return True
    if limits.max_output_tokens is not None and state.output_tokens >= limits.max_output_tokens:
        return True
    if limits.max_total_tokens is not None and (
        state.input_tokens + state.output_tokens >= limits.max_total_tokens
    ):
        return True
    if limits.max_wall_time_seconds is not None and (
        state.elapsed_seconds >= limits.max_wall_time_seconds
    ):
        return True
    if limits.max_tool_calls is not None and state.tool_calls >= limits.max_tool_calls:
        return True
    if limits.max_model_calls is not None and state.model_calls >= limits.max_model_calls:
        return True
    return limits.max_cost_usd is not None and state.cost_usd >= limits.max_cost_usd
