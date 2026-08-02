"""Structured output parsing for LLM responses (spec 12.2, Phase 2).

Parses strict JSON (optionally fenced) into a pydantic model. On schema
failure the caller may retry once (spec Phase 2 acceptance) — ``parse_with_retry``
implements exactly one retry.
"""

from __future__ import annotations

import json
from typing import TypeVar

from pydantic import BaseModel

from lhos.domain.errors import StructuredOutputError

T = TypeVar("T", bound=BaseModel)


def parse_structured(text: str, model_cls: type[T]) -> T:
    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        candidate = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        ).strip()
    try:
        data = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise StructuredOutputError(f"LLM output is not valid JSON: {exc}") from exc
    try:
        return model_cls.model_validate(data)
    except Exception as exc:
        raise StructuredOutputError(
            f"LLM output does not match schema {model_cls.__name__}: {exc}"
        ) from exc


def parse_with_retry(
    llm,  # noqa: ANN001 - LLM port
    prompt: str,
    model_cls: type[T],
    *,
    model: str,
    temperature: float = 0.0,
) -> T:
    """Try once; on schema failure retry exactly once (spec Phase 2)."""
    response = llm.complete(prompt, model=model, temperature=temperature)
    try:
        return parse_structured(response.text, model_cls)
    except StructuredOutputError:
        retry_prompt = (
            prompt
            + "\n\nYour previous output failed schema validation. "
            "Return ONLY valid JSON matching the required schema."
        )
        response = llm.complete(retry_prompt, model=model, temperature=temperature)
        return parse_structured(response.text, model_cls)
