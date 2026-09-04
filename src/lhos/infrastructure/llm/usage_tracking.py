"""Usage tracking for model calls (spec 24.2: every model call is recorded)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from lhos.ports.llm import LegacyLLMResponse


class ModelUsage(BaseModel):
    model: str
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0


class UsageTracker(BaseModel):
    per_model: dict[str, ModelUsage] = Field(default_factory=dict)

    def record(self, model: str, response: LegacyLLMResponse) -> None:
        usage = self.per_model.setdefault(model, ModelUsage(model=model))
        usage.calls += 1
        usage.input_tokens += response.input_tokens
        usage.output_tokens += response.output_tokens

    @property
    def total_input_tokens(self) -> int:
        return sum(u.input_tokens for u in self.per_model.values())

    @property
    def total_output_tokens(self) -> int:
        return sum(u.output_tokens for u in self.per_model.values())

    @property
    def total_calls(self) -> int:
        return sum(u.calls for u in self.per_model.values())
