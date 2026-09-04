"""Mock LLM adapter: deterministic, scriptable, usage-tracked.

Implements the ``LLMClient`` protocol so all runtime code can run end to end
without a real model. Also provides a ``LegacyMockLLM`` for backward
compatibility with Phase 1 agent shells.
"""

from __future__ import annotations

from typing import Any

from lhos.infrastructure.llm.usage_tracking import UsageTracker
from lhos.ports.llm import (
    LegacyLLMResponse,
    LLMRequest,
    LLMResponse,
    LLMUsage,
)


class MockLLMClient:
    """Implements ``LLMClient`` with scripted responses (spec 2B-B1).

    Returns scripted responses in order (or a constant default). Tracks all
    calls for audit and cost accounting.
    """

    def __init__(
        self,
        responses: list[str] | None = None,
        default_response: str = "{}",
        tracker: UsageTracker | None = None,
        tokens_per_call: tuple[int, int] = (100, 50),
        model_id: str = "mock-model",
        provider: str = "mock",
        latency_ms: int = 0,
    ):
        self._responses = list(responses or [])
        self._default = default_response
        self._tracker = tracker or UsageTracker()
        self._tokens = tokens_per_call
        self._model_id = model_id
        self._provider = provider
        self._latency_ms = latency_ms
        self.requests: list[LLMRequest] = []

    @property
    def tracker(self) -> UsageTracker:
        return self._tracker

    def generate(self, request: LLMRequest) -> LLMResponse:
        self.requests.append(request)
        text = self._responses.pop(0) if self._responses else self._default
        input_tokens = self._tokens[0]
        output_tokens = self._tokens[1]
        usage = LLMUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
            cost_usd=0.0,
        )
        response = LLMResponse(
            text=text,
            provider=self._provider,
            model_id=self._model_id,
            usage=usage,
            latency_ms=self._latency_ms,
            request_hash=request.request_hash,
        )
        # Record in the legacy tracker too (for backward compat).
        self._tracker.record(
            self._model_id,
            LegacyLLMResponse(
                text=text,
                model=self._model_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            ),
        )
        return response


# ---------------------------------------------------------------------------
# Legacy MockLLM (Phase 1 compatibility — implements the old ``LLM`` protocol).
# ---------------------------------------------------------------------------


class MockLLM:
    """Returns scripted responses in order (or a constant default).

    Implements the Phase 1 ``LLM`` protocol with ``complete()``.
    """

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
    ) -> LegacyLLMResponse:
        self.prompts.append(prompt)
        text = self._responses.pop(0) if self._responses else self._default
        response = LegacyLLMResponse(
            text=text,
            model=model,
            input_tokens=self._tokens[0],
            output_tokens=self._tokens[1],
        )
        self._tracker.record(model, response)
        return response
