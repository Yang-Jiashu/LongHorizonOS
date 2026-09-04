"""Provider-neutral LLM port (spec Phase 2B).

The spec sketches ``async def generate``; the MVP is single-worker and
sequential, so the port is synchronous — the same simplification used by
the tool port (spec 13.1-13.2). Names and semantics are unchanged.

Runtime code depends only on ``LLMClient``; provider logic lives in
``infrastructure/llm/``. API keys come exclusively from environment variables
and are never written to traces.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol

from pydantic import BaseModel, Field


class LLMRequest(BaseModel):
    """A single model generation request (spec 2B-B1)."""

    role: str  # planner | worker | reconciler | verifier
    messages: list[dict[str, str]]
    response_schema: dict[str, Any] | None = None
    temperature: float = 0.0
    max_output_tokens: int = 4096
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def request_hash(self) -> str:
        """Deterministic hash of the request content for dedup/audit."""
        material = json.dumps(
            {
                "role": self.role,
                "messages": self.messages,
                "response_schema": self.response_schema,
                "temperature": self.temperature,
                "max_output_tokens": self.max_output_tokens,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()


class LLMUsage(BaseModel):
    """Token and cost accounting for a single model call (spec 2B-B1)."""

    input_tokens: int
    output_tokens: int
    cached_input_tokens: int = 0
    reasoning_tokens: int | None = None
    total_tokens: int
    cost_usd: float | None = None


class LLMResponse(BaseModel):
    """A single model generation response (spec 2B-B1)."""

    text: str
    parsed_output: dict[str, Any] | None = None

    provider: str
    model_id: str
    request_id: str | None = None

    usage: LLMUsage
    latency_ms: int

    retry_count: int = 0
    parse_failure_count: int = 0

    request_hash: str
    response_hash: str = ""

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        if not self.response_hash:
            self.response_hash = hashlib.sha256(self.text.encode("utf-8")).hexdigest()


class LLMClient(Protocol):
    """Provider-neutral LLM interface (spec 2B-B1).

    Implementations:
    - ``MockLLMClient`` (infrastructure/llm/adapter.py) — scripted, for tests.
    - ``SenseNovaClient`` (infrastructure/llm/sensenova.py) — real API calls.
    """

    def generate(self, request: LLMRequest) -> LLMResponse: ...


# ---------------------------------------------------------------------------
# Legacy compatibility (used by Phase 1 agent shells; will be migrated).
# ---------------------------------------------------------------------------


class LegacyLLMResponse(BaseModel):
    """Simplified response for the Phase 1 ``LLM`` protocol."""

    text: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0


class LLM(Protocol):
    """Phase 1 LLM protocol (kept for backward compatibility)."""

    def complete(
        self,
        prompt: str,
        *,
        model: str,
        temperature: float = 0.0,
        response_schema: dict[str, Any] | None = None,
    ) -> LegacyLLMResponse: ...
