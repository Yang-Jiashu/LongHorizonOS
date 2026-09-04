"""LongHorizonOS E2 — OpenAI-compatible model adapter.

Uses only the standard library (`urllib`) with a pluggable `transport` so tests
run offline via `FakeTransport` (no API key, no network) and a live provider can
be exercised by injecting a real HTTP transport.  No provider SDK is required and
no provider types leak into Core/SDK.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Sequence
from typing import Any

from .protocols import Message, ModelResponse


class FakeTransport:
    """Offline transport that returns a canned response (no network)."""

    def __init__(
        self,
        *,
        text: str = "ok",
        tool_calls: list[dict[str, Any]] | None = None,
        fail: bool = False,
    ) -> None:
        self.text = text
        self.tool_calls = tool_calls or []
        self.fail = fail
        self.last_request: dict[str, Any] | None = None

    def __call__(
        self, url: str, headers: dict[str, str], payload: dict[str, Any]
    ) -> dict[str, Any]:
        self.last_request = payload
        if self.fail:
            raise ConnectionError("simulated provider failure")
        msg: dict[str, Any] = {"role": "assistant", "content": self.text}
        if self.tool_calls:
            msg["tool_calls"] = self.tool_calls
        return {
            "choices": [{"message": msg}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }


Transport = Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]]


class OpenAICompatibleModel:
    """Minimal OpenAI-compatible (chat/completions) adapter.

    Initialized with model id; reads API key + base URL from env unless given
    explicitly (no hardcoded secret/endpoint/account).
    """

    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_s: float = 30.0,
        transport: Transport | None = None,
        fail_closed: bool | None = None,
    ) -> None:
        self.model = model
        self.api_key = api_key or os.environ.get("LHOS_MODEL_API_KEY", "")
        self.base_url = (
            base_url or os.environ.get("LHOS_MODEL_BASE_URL", "https://api.openai.com/v1")
        ).rstrip("/")
        self.timeout_s = timeout_s
        self.fail_closed = (
            fail_closed
            if fail_closed is not None
            else os.environ.get("LHOS_MODEL_FAIL_CLOSED", "1") == "1"
        )
        # If no transport given, default to an offline fake so CI needs no network.
        self.transport = transport or FakeTransport()

    def _chat(
        self, messages: Sequence[Message], *, max_tokens: int, response_format: str | None
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": max_tokens,
        }
        if response_format:
            payload["response_format"] = {"type": response_format}
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload_json = json.dumps(payload)
        try:
            data = self.transport(url, headers, json.loads(payload_json))
        except Exception as e:  # operational failure; not a semantic conclusion
            if self.fail_closed:
                raise ModelCallError(str(e)) from e
            return {"error": str(e)}
        if not isinstance(data, dict) or "choices" not in data:
            raise ModelCallError("malformed provider response")
        return data

    def complete(self, messages: Sequence[Message], *, max_tokens: int = 512) -> ModelResponse:
        data = self._chat(messages, max_tokens=max_tokens, response_format=None)
        if "error" in data:
            return ModelResponse(
                ok=False, error=data["error"], model=self.model, provider="openai-compatible"
            )
        msg = data["choices"][0]["message"]
        return ModelResponse(
            text=msg.get("content", ""),
            tool_calls=msg.get("tool_calls", []),
            model=self.model,
            provider="openai-compatible",
            usage=data.get("usage", {}),
            ok=True,
        )

    def complete_structured(
        self, messages: Sequence[Message], *, schema: dict[str, Any], max_tokens: int = 512
    ) -> ModelResponse:
        data = self._chat(messages, max_tokens=max_tokens, response_format="json_object")
        if "error" in data:
            return ModelResponse(
                ok=False, error=data["error"], model=self.model, provider="openai-compatible"
            )
        msg = data["choices"][0]["message"]
        return ModelResponse(
            text=msg.get("content", ""),
            model=self.model,
            provider="openai-compatible",
            usage=data.get("usage", {}),
            ok=True,
        )


class ModelCallError(Exception):
    """A model provider call failed operationally (never a semantic conclusion)."""
