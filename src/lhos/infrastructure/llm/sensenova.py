"""SenseNova LLM adapter (spec Phase 2B-B2).

Implements ``LLMClient`` using the SenseNova API (OpenAI-compatible).

- Base URL: https://token.sensenova.cn/v1
- Auth: Bearer token from ``SENSENOVA_API_KEY`` environment variable.
- Supports JSON structured output via ``response_format``.
- Retry: exponential backoff for network/429/5xx errors (max 3).
- Structured output repair: one retry with a repair prompt.
- All token costs (including retries and repairs) are tracked.

API keys are NEVER logged, traced, or stored — they come exclusively from
the environment variable.
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

from lhos.domain.errors import LhosError
from lhos.ports.llm import LLMRequest, LLMResponse, LLMUsage

SENSENOVA_BASE_URL = "https://token.sensenova.cn/v1"
SENSENOVA_CHAT_ENDPOINT = "/chat/completions"

# Retryable HTTP status codes (spec 2B-B2).
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_INITIAL_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 30.0
_REQUEST_TIMEOUT_SECONDS = 120

# Supported models and their pricing (USD per 1M tokens).
# Updated from https://platform.sensenova.cn/docs — pricing may change.
_MODEL_PRICING: dict[str, dict[str, float]] = {
    "sensenova-6.7-flash-lite": {"input": 0.0, "output": 0.0},  # free tier
    "deepseek-v4-flash": {"input": 0.14, "output": 0.28},
}

# Default model for the vertical slice.
DEFAULT_MODEL = "sensenova-6.7-flash-lite"


class SenseNovaError(LhosError):
    """SenseNova API error."""


class SenseNovaClient:
    """Real LLM client backed by the SenseNova API.

    Implements ``LLMClient``. API key comes from the ``SENSENOVA_API_KEY``
    environment variable. If the variable is not set, raises ``SenseNovaError``
    on construction — this prevents accidental silent fallback to a mock.

    Parameters
    ----------
    model_id : str
        Exact model identifier (e.g. ``sensenova-6.7-flash-lite``).
        The model is never silently changed (spec 2B-B1).
    api_key : str | None
        Override the environment variable. If ``None``, reads from
        ``SENSENOVA_API_KEY``.
    base_url : str
        Override the base URL (for testing).
    """

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL,
        api_key: str | None = None,
        base_url: str = SENSENOVA_BASE_URL,
        timeout: int = _REQUEST_TIMEOUT_SECONDS,
        reasoning_effort: str = "none",
    ):
        self._model_id = model_id
        self._api_key = api_key or os.environ.get("SENSENOVA_API_KEY", "")
        if not self._api_key:
            raise SenseNovaError(
                "SENSENOVA_API_KEY environment variable is not set. "
                "Set it to your SenseNova API key (sk-...) to use real LLM."
            )
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._reasoning_effort = reasoning_effort
        self._call_count = 0

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def provider(self) -> str:
        return "sensenova"

    def _compute_cost(self, input_tokens: int, output_tokens: int) -> float:
        pricing = _MODEL_PRICING.get(self._model_id, {"input": 0.0, "output": 0.0})
        return (
            input_tokens * pricing["input"] / 1_000_000
            + output_tokens * pricing["output"] / 1_000_000
        )

    def _build_payload(self, request: LLMRequest) -> dict[str, Any]:
        """Build the OpenAI-compatible chat completions payload."""
        payload: dict[str, Any] = {
            "model": self._model_id,
            "messages": request.messages,
            "temperature": request.temperature,
            "max_tokens": request.max_output_tokens,
            "stream": False,
            "reasoning_effort": self._reasoning_effort,
        }
        # Structured output: use JSON mode when a schema is provided.
        if request.response_schema is not None:
            payload["response_format"] = {"type": "json_object"}
        return payload

    def _make_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a single HTTP request with retry on transient errors."""
        url = f"{self._base_url}{SENSENOVA_CHAT_ENDPOINT}"
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        last_error: Exception | None = None
        backoff = _INITIAL_BACKOFF_SECONDS

        for attempt in range(_MAX_RETRIES + 1):
            try:
                req = urllib.request.Request(url, data=body, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    result: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
                    return result
            except urllib.error.HTTPError as exc:
                status = exc.code
                body_text = ""
                with contextlib.suppress(Exception):
                    body_text = exc.read().decode("utf-8")
                last_error = SenseNovaError(f"SenseNova API error {status}: {body_text[:500]}")
                if status in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                    time.sleep(min(backoff, _MAX_BACKOFF_SECONDS))
                    backoff *= 2
                    continue
                raise last_error from exc
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = SenseNovaError(f"SenseNova network error: {exc}")
                if attempt < _MAX_RETRIES:
                    time.sleep(min(backoff, _MAX_BACKOFF_SECONDS))
                    backoff *= 2
                    continue
                raise last_error from exc
            except Exception as exc:
                raise SenseNovaError(f"SenseNova unexpected error: {exc}") from exc

        # Should not reach here, but just in case.
        raise last_error or SenseNovaError("SenseNova request failed after retries")

    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response from the SenseNova API.

        Includes:
        - Exponential backoff retry for network/429/5xx errors (max 3).
        - One structured output repair retry on JSON parse failure.
        - Full usage tracking (input, output, cached, reasoning tokens).
        - Latency tracking.
        - Cost computation.

        All retry and repair token costs are accumulated in the response.

        Response parsing priority (Step 7):
        1. If ``parsed_output`` from structured response is available, use it.
        2. If ``content`` has final JSON, parse and use it.
        3. ``reasoning`` is ONLY diagnostic — never used as the action text.
        4. If ``content`` is empty but ``reasoning`` is non-empty, treat as
           a parse failure (do NOT fake success with reasoning text).
        5. If ``content`` is empty and ``reasoning`` is empty, raise error.
        """
        self._call_count += 1
        payload = self._build_payload(request)

        retry_count = 0
        total_input = 0
        total_output = 0
        total_cached = 0
        total_reasoning = 0
        total_latency = 0
        final_text = ""
        reasoning_text = ""
        request_id: str | None = None
        parse_failure_count = 0

        # --- Main call with retry ---
        started = time.perf_counter()
        data = self._make_request(payload)
        total_latency += int((time.perf_counter() - started) * 1000)

        # Parse response.
        request_id = data.get("id")
        choices = data.get("choices", [])
        if not choices:
            raise SenseNovaError(f"SenseNova returned no choices: {json.dumps(data)[:500]}")
        message = choices[0].get("message", {})

        # Step 7: content is the ONLY source for the action text.
        # reasoning is diagnostic only — never used as a fallback.
        final_text = message.get("content") or ""
        reasoning_text = message.get("reasoning") or ""

        # If content is empty but reasoning is non-empty, we do NOT use
        # reasoning as the action text. Instead, we treat this as a parse
        # failure and attempt a repair.
        if not final_text and reasoning_text:
            parse_failure_count = 1
            final_text = ""  # Keep empty — repair will be attempted.
        elif not final_text and not reasoning_text:
            raise SenseNovaError(
                "SenseNova returned empty content and empty reasoning. "
                f"Full response: {json.dumps(data)[:500]}"
            )

        # Parse usage.
        usage_data = data.get("usage", {})
        input_tokens = int(usage_data.get("prompt_tokens", 0))
        output_tokens = int(usage_data.get("completion_tokens", 0))
        cached_tokens = 0
        reasoning_tokens = 0
        prompt_details = usage_data.get("prompt_tokens_details", {})
        if isinstance(prompt_details, dict):
            cached_tokens = int(prompt_details.get("cached_tokens", 0))
        completion_details = usage_data.get("completion_tokens_details", {})
        if isinstance(completion_details, dict):
            reasoning_tokens = int(completion_details.get("reasoning_tokens", 0))
        total_tokens = int(usage_data.get("total_tokens", input_tokens + output_tokens))

        total_input += input_tokens
        total_output += output_tokens
        total_cached += cached_tokens
        total_reasoning += reasoning_tokens

        # --- Structured output repair (one retry) ---
        parsed_output: dict[str, Any] | None = None
        if request.response_schema is not None:
            try:
                parsed_output = json.loads(final_text)
            except (json.JSONDecodeError, ValueError):
                parse_failure_count = 1
                # Repair: retry with an explicit instruction.
                repair_messages = list(request.messages)
                # If we have reasoning text, include it as context for the repair.
                if reasoning_text:
                    repair_messages.append(
                        {
                            "role": "assistant",
                            "content": reasoning_text,
                        }
                    )
                repair_messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Your previous output was not valid JSON or was empty. "
                            "Return ONLY a valid JSON object matching the schema. "
                            "Do not include markdown fences, prose, or explanations. "
                            "Put the final answer in the 'content' field, not 'reasoning'."
                        ),
                    }
                )
                repair_payload = self._build_payload(
                    LLMRequest(
                        role=request.role,
                        messages=repair_messages,
                        response_schema=request.response_schema,
                        temperature=request.temperature,
                        max_output_tokens=request.max_output_tokens,
                        metadata={**request.metadata, "repair_attempt": True},
                    )
                )
                started = time.perf_counter()
                repair_data = self._make_request(repair_payload)
                total_latency += int((time.perf_counter() - started) * 1000)

                repair_choices = repair_data.get("choices", [])
                if repair_choices:
                    repair_message = repair_choices[0].get("message", {})
                    # Step 7: only use content from repair, not reasoning.
                    final_text = repair_message.get("content") or ""
                    # If repair also returns empty content, keep the original.
                    if not final_text:
                        final_text = ""

                repair_usage = repair_data.get("usage", {})
                r_input = int(repair_usage.get("prompt_tokens", 0))
                r_output = int(repair_usage.get("completion_tokens", 0))
                r_total = int(repair_usage.get("total_tokens", r_input + r_output))
                r_cached = 0
                r_reasoning = 0
                r_prompt_details = repair_usage.get("prompt_tokens_details", {})
                if isinstance(r_prompt_details, dict):
                    r_cached = int(r_prompt_details.get("cached_tokens", 0))
                r_completion_details = repair_usage.get("completion_tokens_details", {})
                if isinstance(r_completion_details, dict):
                    r_reasoning = int(r_completion_details.get("reasoning_tokens", 0))

                total_input += r_input
                total_output += r_output
                total_cached += r_cached
                total_reasoning += r_reasoning
                total_tokens += r_total

                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    parsed_output = json.loads(final_text)

        cost = self._compute_cost(total_input, total_output)

        return LLMResponse(
            text=final_text,
            parsed_output=parsed_output,
            provider=self.provider,
            model_id=self._model_id,
            request_id=request_id,
            usage=LLMUsage(
                input_tokens=total_input,
                output_tokens=total_output,
                cached_input_tokens=total_cached,
                reasoning_tokens=total_reasoning or None,
                total_tokens=total_input + total_output,
                cost_usd=cost,
            ),
            latency_ms=total_latency,
            retry_count=retry_count,
            parse_failure_count=parse_failure_count,
            request_hash=request.request_hash,
        )
