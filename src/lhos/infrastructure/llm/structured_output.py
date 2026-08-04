"""Structured output parsing for LLM responses (spec 12.2, Phase 2).

Parses strict JSON (optionally fenced) into a pydantic model. On schema
failure the caller may retry once (spec Phase 2 acceptance) — ``parse_with_retry``
implements exactly one retry.

Two entry points:
- ``parse_structured(text, model_cls)`` — pure parsing, no LLM dependency.
- ``parse_with_retry(llm, prompt, model_cls, ...)`` — uses the legacy ``LLM``
  protocol (Phase 1 compatibility).
- ``parse_with_retry_client(client, request, model_cls)`` — uses the new
  ``LLMClient`` protocol (Phase 2).

Step 8 (Milestone 2.2): Improved markdown fence extraction, JSON truncation
detection, and parse failure type classification.
"""

from __future__ import annotations

import json
import re
from enum import StrEnum
from typing import TypeVar

from pydantic import BaseModel

from lhos.domain.errors import StructuredOutputError
from lhos.ports.llm import LLMClient, LLMRequest, LLMResponse, LLMUsage

T = TypeVar("T", bound=BaseModel)


class ParseFailureType(StrEnum):
    """Classification of structured output parse failures."""

    EMPTY_CONTENT = "empty_content"
    REASONING_ONLY = "reasoning_only"
    MARKDOWN_CODE_FENCE = "markdown_code_fence"
    JSON_TRUNCATION = "json_truncation"
    MISSING_SCHEMA_FIELD = "missing_schema_field"
    UNKNOWN_TOOL_NAME = "unknown_tool_name"
    INVALID_ENUM = "invalid_enum"
    EXTRA_NATURAL_LANGUAGE = "extra_natural_language"
    UNKNOWN = "unknown"


class ParseFailureStats:
    """Accumulates parse failure statistics by type."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._repair_attempts: int = 0
        self._repair_successes: int = 0
        self._repair_token_cost: int = 0
        self._total_parse_latency_ms: float = 0.0

    def record_failure(self, failure_type: ParseFailureType) -> None:
        key = failure_type.value
        self._counts[key] = self._counts.get(key, 0) + 1

    def record_repair_attempt(self, tokens: int = 0, success: bool = False) -> None:
        self._repair_attempts += 1
        self._repair_token_cost += tokens
        if success:
            self._repair_successes += 1

    def record_latency(self, ms: float) -> None:
        self._total_parse_latency_ms += ms

    @property
    def total_failures(self) -> int:
        return sum(self._counts.values())

    @property
    def failure_rate(self) -> float:
        total = self.total_failures + self._repair_successes
        if total == 0:
            return 0.0
        return self.total_failures / total

    @property
    def repair_success_rate(self) -> float:
        if self._repair_attempts == 0:
            return 0.0
        return self._repair_successes / self._repair_attempts

    def get_counts(self) -> dict[str, int]:
        return dict(self._counts)

    def get_report(self) -> dict[str, object]:
        return {
            "failure_counts": self.get_counts(),
            "total_failures": self.total_failures,
            "failure_rate": round(self.failure_rate, 4),
            "repair_attempts": self._repair_attempts,
            "repair_successes": self._repair_successes,
            "repair_success_rate": round(self.repair_success_rate, 4),
            "repair_token_cost": self._repair_token_cost,
            "total_parse_latency_ms": round(self._total_parse_latency_ms, 2),
        }


# Global stats accumulator (can be reset per run).
_global_stats = ParseFailureStats()


def reset_parse_stats() -> None:
    """Reset the global parse failure stats (call at the start of each run)."""
    global _global_stats
    _global_stats = ParseFailureStats()


def get_parse_stats() -> ParseFailureStats:
    """Get the current global parse failure stats."""
    return _global_stats


def classify_parse_failure(text: str, error: str) -> ParseFailureType:
    """Classify a parse failure based on the raw text and error message."""
    if not text or not text.strip():
        return ParseFailureType.EMPTY_CONTENT

    stripped = text.strip()

    # Check for reasoning-only output (no JSON at all).
    if not any(c in stripped for c in "{}[]"):
        if any(word in stripped.lower() for word in ["i will", "let me", "i should", "i'll"]):
            return ParseFailureType.REASONING_ONLY
        return ParseFailureType.EXTRA_NATURAL_LANGUAGE

    # Check for markdown code fences.
    if stripped.startswith("```"):
        return ParseFailureType.MARKDOWN_CODE_FENCE

    error_lower = error.lower()

    # Check for JSON truncation.
    if "unterminated string" in error_lower or "expecting" in error_lower or "eof" in error_lower:
        # Try to detect if the JSON is just truncated.
        if stripped.count("{") > stripped.count("}"):
            return ParseFailureType.JSON_TRUNCATION
        if stripped.count("[") > stripped.count("]"):
            return ParseFailureType.JSON_TRUNCATION

    # Check for missing schema fields.
    if "field required" in error_lower or "missing" in error_lower:
        return ParseFailureType.MISSING_SCHEMA_FIELD

    # Check for invalid enum values.
    if "enum" in error_lower or "value is not a valid" in error_lower:
        return ParseFailureType.INVALID_ENUM

    # Check for unknown tool name.
    if "tool" in error_lower and ("unknown" in error_lower or "not found" in error_lower):
        return ParseFailureType.UNKNOWN_TOOL_NAME

    return ParseFailureType.UNKNOWN


def _extract_json_from_markdown(text: str) -> str:
    """Extract JSON from markdown code fences.

    Handles:
    - ```json ... ```
    - ``` ... ```
    - Text with JSON embedded in prose
    """
    stripped = text.strip()

    # Pattern 1: ```json\n...\n``` or ```\n...\n```
    fence_pattern = r"```(?:json)?\s*\n?(.*?)\n?```"
    matches = re.findall(fence_pattern, stripped, re.DOTALL)
    if matches:
        # Return the last match (usually the actual output).
        return str(matches[-1].strip())

    # Pattern 2: No closing fence — just opening ```json
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        # Skip the first line (fence opener).
        content_lines = [line for line in lines[1:] if not line.strip().startswith("```")]
        return "\n".join(content_lines).strip()

    # Pattern 3: Try to find JSON object/array in the text.
    # Look for the first { or [ and try to extract balanced JSON.
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        start_idx = stripped.find(start_char)
        if start_idx == -1:
            continue
        # Find the matching end by counting brackets.
        depth = 0
        in_string = False
        escape = False
        for i in range(start_idx, len(stripped)):
            c = stripped[i]
            if escape:
                escape = False
                continue
            if c == "\\":
                escape = True
                continue
            if c == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if c == start_char:
                depth += 1
            elif c == end_char:
                depth -= 1
                if depth == 0:
                    return stripped[start_idx : i + 1]

    return stripped


def _try_repair_json(text: str) -> str:
    """Attempt to repair common JSON issues (truncation, trailing commas)."""
    candidate = text.strip()

    # Remove trailing commas before closing brackets.
    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)

    # If the JSON is truncated (unbalanced braces), try to close it.
    open_braces = candidate.count("{") - candidate.count("}")
    open_brackets = candidate.count("[") - candidate.count("]")

    if open_braces > 0 or open_brackets > 0:
        # Try to close truncated JSON.
        # First, try to find the last complete key-value pair.
        # If there's an unterminated string, close it.
        if candidate.count('"') % 2 != 0:
            candidate += '"'
        # Close any open arrays first, then objects.
        candidate += "]" * max(0, open_brackets)
        candidate += "}" * max(0, open_braces)

    return candidate


def parse_structured(text: str, model_cls: type[T]) -> T:
    """Parse LLM output into a pydantic model.

    Handles:
    - Plain JSON
    - JSON in markdown code fences (```json ... ```)
    - Partially truncated JSON (best-effort repair)
    - JSON embedded in natural language prose

    Raises StructuredOutputError on failure.
    """
    import time

    start_time = time.monotonic()

    if not text or not text.strip():
        _global_stats.record_failure(ParseFailureType.EMPTY_CONTENT)
        _global_stats.record_latency((time.monotonic() - start_time) * 1000)
        raise StructuredOutputError("LLM output is empty")

    candidate = text.strip()

    # Step 1: Try direct JSON parse.
    try:
        data = json.loads(candidate)
        result = model_cls.model_validate(data)
        _global_stats.record_latency((time.monotonic() - start_time) * 1000)
        return result
    except json.JSONDecodeError:
        pass
    except Exception as exc:
        failure_type = classify_parse_failure(candidate, str(exc))
        _global_stats.record_failure(failure_type)
        _global_stats.record_latency((time.monotonic() - start_time) * 1000)
        raise StructuredOutputError(
            f"LLM output does not match schema {model_cls.__name__}: {exc}"
        ) from exc

    # Step 2: Extract from markdown fences.
    extracted = _extract_json_from_markdown(candidate)
    if extracted != candidate:
        try:
            data = json.loads(extracted)
            result = model_cls.model_validate(data)
            _global_stats.record_latency((time.monotonic() - start_time) * 1000)
            return result
        except json.JSONDecodeError:
            pass
        except Exception as exc:
            failure_type = classify_parse_failure(extracted, str(exc))
            _global_stats.record_failure(failure_type)
            _global_stats.record_latency((time.monotonic() - start_time) * 1000)
            raise StructuredOutputError(
                f"LLM output (extracted from markdown) does not match schema "
                f"{model_cls.__name__}: {exc}"
            ) from exc

    # Step 3: Attempt JSON repair (truncation, trailing commas).
    repaired = _try_repair_json(extracted)
    if repaired != extracted:
        try:
            data = json.loads(repaired)
            result = model_cls.model_validate(data)
            _global_stats.record_latency((time.monotonic() - start_time) * 1000)
            return result
        except Exception as exc:
            failure_type = classify_parse_failure(repaired, str(exc))
            _global_stats.record_failure(failure_type)
            _global_stats.record_latency((time.monotonic() - start_time) * 1000)
            raise StructuredOutputError(
                f"LLM output could not be parsed or repaired: {exc}"
            ) from exc

    # All attempts failed.
    failure_type = classify_parse_failure(candidate, "json decode error")
    _global_stats.record_failure(failure_type)
    _global_stats.record_latency((time.monotonic() - start_time) * 1000)
    raise StructuredOutputError(f"LLM output is not valid JSON: {candidate[:200]}...")


def parse_with_retry(
    llm,
    prompt: str,
    model_cls: type[T],
    *,
    model: str,
    temperature: float = 0.0,
) -> T:
    """Try once; on schema failure retry exactly once (spec Phase 2).

    Uses the legacy ``LLM`` protocol.
    """
    response = llm.complete(prompt, model=model, temperature=temperature)
    try:
        return parse_structured(response.text, model_cls)
    except StructuredOutputError:
        retry_prompt = (
            prompt + "\n\nYour previous output failed schema validation. "
            "Return ONLY valid JSON matching the required schema."
        )
        response = llm.complete(retry_prompt, model=model, temperature=temperature)
        _global_stats.record_repair_attempt(tokens=len(retry_prompt) // 4, success=False)
        try:
            result = parse_structured(response.text, model_cls)
            _global_stats.record_repair_attempt(success=True)
            return result
        except StructuredOutputError:
            _global_stats.record_repair_attempt(success=False)
            raise


def parse_with_retry_client(
    client: LLMClient,
    request: LLMRequest,
    model_cls: type[T],
) -> tuple[T, LLMResponse]:
    """Try once; on schema failure retry exactly once with a repair prompt.

    Uses the new ``LLMClient`` protocol. Returns the parsed result and the
    *final* LLMResponse (which includes retry/parse counts and accumulated
    usage).
    """
    response = client.generate(request)
    try:
        parsed = parse_structured(response.text, model_cls)
        return parsed, response
    except StructuredOutputError:
        # Minimal repair: append a system instruction to output valid JSON.
        repair_messages = list(request.messages)
        repair_messages.append(
            {
                "role": "system",
                "content": (
                    "Your previous output failed schema validation. "
                    "Return ONLY valid JSON matching the required schema. "
                    "Do not include markdown fences or prose."
                ),
            }
        )
        repair_request = LLMRequest(
            role=request.role,
            messages=repair_messages,
            response_schema=request.response_schema,
            temperature=request.temperature,
            max_output_tokens=request.max_output_tokens,
            metadata={**request.metadata, "repair_attempt": True},
        )
        repair_response = client.generate(repair_request)
        _global_stats.record_repair_attempt(
            tokens=repair_response.usage.total_tokens, success=False
        )
        try:
            parsed = parse_structured(repair_response.text, model_cls)
            _global_stats.record_repair_attempt(success=True)
        except StructuredOutputError:
            _global_stats.record_repair_attempt(success=False)
            raise
        # Merge usage so cost accounting captures the repair.
        merged = LLMResponse(
            text=repair_response.text,
            parsed_output=parsed.model_dump(),
            provider=repair_response.provider,
            model_id=repair_response.model_id,
            request_id=repair_response.request_id,
            usage=LLMUsage(
                input_tokens=response.usage.input_tokens + repair_response.usage.input_tokens,
                output_tokens=response.usage.output_tokens + repair_response.usage.output_tokens,
                cached_input_tokens=response.usage.cached_input_tokens
                + repair_response.usage.cached_input_tokens,
                reasoning_tokens=(
                    (response.usage.reasoning_tokens or 0)
                    + (repair_response.usage.reasoning_tokens or 0)
                )
                or None,
                total_tokens=response.usage.total_tokens + repair_response.usage.total_tokens,
                cost_usd=(response.usage.cost_usd or 0.0) + (repair_response.usage.cost_usd or 0.0),
            ),
            latency_ms=response.latency_ms + repair_response.latency_ms,
            retry_count=1,
            parse_failure_count=1,
            request_hash=request.request_hash,
            response_hash=repair_response.response_hash,
        )
        return parsed, merged
