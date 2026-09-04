"""LongHorizonOS E2 — minimal model adapter protocol.

Provider-neutral.  A ModelAdapter is a *replaceable reasoning adapter*; its
output is a `ModelResponse` (candidate), never semantic truth.  READY/VERIFIED/
STALE/Goal CLOSED remain VPG-derived.  No provider SDK types leak here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class Message:
    role: str  # "system" | "user" | "assistant"
    content: str


@dataclass
class ModelResponse:
    text: str = ""
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    provider: str = ""
    model: str = ""
    usage: dict[str, int] = field(default_factory=dict)
    ok: bool = True
    error: str = ""


class ModelAdapter(Protocol):
    model: str

    def complete(self, messages: Sequence[Message], *, max_tokens: int = 512) -> ModelResponse: ...

    def complete_structured(
        self, messages: Sequence[Message], *, schema: dict[str, Any], max_tokens: int = 512
    ) -> ModelResponse: ...
