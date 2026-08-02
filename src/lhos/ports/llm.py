"""LLM port. Real API calls are out of scope for the MVP; MockLLM implements this."""

from typing import Any

from pydantic import BaseModel
from typing import Protocol


class LLMResponse(BaseModel):
    text: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0


class LLM(Protocol):
    def complete(
        self,
        prompt: str,
        *,
        model: str,
        temperature: float = 0.0,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMResponse: ...
