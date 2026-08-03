"""Mock LLM adapter: deterministic, scriptable, usage-tracked.

Real API calls are out of scope for this phase; this adapter implements the
LLM port so planner/executor shells and tests can run end to end.
"""

from __future__ import annotations

from typing import Any

from lhos.infrastructure.llm.usage_tracking import UsageTracker
from lhos.ports.llm import LLMResponse


class MockLLM:
    """Returns scripted responses in order (or a constant default)."""

    def __init__(
        self,
        responses: list[str] | None = None,
        default_response: str = "{}",
        tracker: UsageTracker | None = None,
        tokens_per_call: tuple[int, int] = (100, 50),
    ):
        self._responses = list(responses or [])
        self._default = default_response
        self._tracker = tracker or UsageTracker()
        self._tokens = tokens_per_call
        self.prompts: list[str] = []

    @property
    def tracker(self) -> UsageTracker:
        return self._tracker

    def complete(
        self,
        prompt: str,
        *,
        model: str,
        temperature: float = 0.0,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        self.prompts.append(prompt)
        text = self._responses.pop(0) if self._responses else self._default
        response = LLMResponse(
            text=text,
            model=model,
            input_tokens=self._tokens[0],
            output_tokens=self._tokens[1],
        )
        self._tracker.record(model, response)
        return response
