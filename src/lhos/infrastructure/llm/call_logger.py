"""LLM call logging (spec Phase 2B-B3).

Writes every real model call to:
1. The SQLite database (``llm_calls`` table — migration 001).
2. A JSONL trace file (for human inspection and replay).

Sanitization:
- API keys are NEVER logged (they never enter this module).
- Sensitive environment variable values are redacted.
- Tool outputs longer than ``max_output_chars`` are replaced with an
  artifact reference (the full text is written to a separate file).

This module does not break existing deterministic run replay — the
``llm_calls`` table is additive and existing tables are untouched.

Status tracking (Step 3):
- ``success`` — the call completed and the response was parsed successfully.
- ``provider_error`` — the provider raised an exception (network, auth, etc.).
- ``parse_failed`` — the provider returned a response but structured output
  parsing failed (even after repair).

Repair tracking:
- Repair calls are logged as separate entries with ``causation_id`` linking
  them to the original call.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from lhos.ports.llm import LLMRequest, LLMResponse

# Patterns to redact from logged request/response bodies.
_SENSITIVE_PATTERNS = [
    re.compile(r"(sk-)[A-Za-z0-9]+", re.IGNORECASE),
    re.compile(r"(api[_-]?key)[=:]\s*\S+", re.IGNORECASE),
    re.compile(r"(password)[=:]\s*\S+", re.IGNORECASE),
    re.compile(r"(token)[=:]\s*\S+", re.IGNORECASE),
    re.compile(r"(secret)[=:]\s*\S+", re.IGNORECASE),
    re.compile(r"(authorization)[=:]\s*\S+", re.IGNORECASE),
]

_MAX_OUTPUT_CHARS = 10_000


def _redact(text: str) -> str:
    """Redact sensitive patterns from text."""
    for pattern in _SENSITIVE_PATTERNS:
        text = pattern.sub(r"\1[REDACTED]", text)
    return text


def _sanitize_body(body: Any, max_chars: int = _MAX_OUTPUT_CHARS) -> tuple[str, str | None]:
    """Sanitize a JSON-serializable body for logging.

    Returns (sanitized_json, artifact_path). If the body is too long,
    the full text is written to an artifact file and the JSON contains
    a reference instead.
    """
    text = json.dumps(body, ensure_ascii=False, default=str)
    sanitized = _redact(text)
    if len(sanitized) <= max_chars:
        return sanitized, None
    return sanitized[:max_chars] + "...[truncated]", None


def _safe_json_loads(text: str) -> Any:
    """Try to parse JSON, return raw string if parsing fails (e.g. truncated)."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


class LLMCallLogger:
    """Logs LLM calls to the database and a JSONL trace file.

    Parameters
    ----------
    db : Database
        The SQLite database connection (must have the ``llm_calls`` table).
    trace_path : Path | None
        If set, each call is also appended as a JSONL line.
    artifacts_dir : Path | None
        Directory for long output artifacts.
    """

    def __init__(
        self,
        db,
        trace_path: str | Path | None = None,
        artifacts_dir: str | Path | None = None,
    ):
        self._db = db
        self._trace_path = Path(trace_path) if trace_path else None
        self._artifacts_dir = Path(artifacts_dir) if artifacts_dir else None
        if self._trace_path:
            self._trace_path.parent.mkdir(parents=True, exist_ok=True)
        if self._artifacts_dir:
            self._artifacts_dir.mkdir(parents=True, exist_ok=True)

    def log_call(
        self,
        request: LLMRequest,
        response: LLMResponse,
        *,
        run_id: str,
        node_id: str | None = None,
        execution_id: str | None = None,
        prompt_name: str | None = None,
        prompt_version: str | None = None,
        prompt_file_hash: str | None = None,
        status: str = "success",
        error_type: str | None = None,
        causation_id: str | None = None,
    ) -> str:
        """Log a single LLM call. Returns the call ID.

        Parameters
        ----------
        status : str
            ``success`` | ``provider_error`` | ``parse_failed``
        error_type : str | None
            Exception class name or error category.
        causation_id : str | None
            If this call is a repair, the ID of the original call.
        """
        call_id = uuid4().hex
        now = datetime.now().astimezone()

        # Sanitize request body (no API keys — they never enter here).
        request_body = {
            "role": request.role,
            "messages": request.messages,
            "response_schema": request.response_schema,
            "temperature": request.temperature,
            "max_output_tokens": request.max_output_tokens,
            "metadata": request.metadata,
        }
        req_json, _ = _sanitize_body(request_body)

        # Sanitize response body.
        response_body = {
            "text": response.text,
            "parsed_output": response.parsed_output,
            "request_id": response.request_id,
        }
        resp_json, _ = _sanitize_body(response_body)

        # If response text is very long, write it to an artifact file.
        if len(response.text) > _MAX_OUTPUT_CHARS and self._artifacts_dir:
            artifact_name = f"llm_call_{call_id}_response.txt"
            artifact_path = self._artifacts_dir / artifact_name
            artifact_path.write_text(response.text, encoding="utf-8")
            resp_json = json.dumps(
                {
                    "text": f"[artifact:{artifact_name}] (len={len(response.text)})",
                    "parsed_output": response.parsed_output,
                    "request_id": response.request_id,
                }
            )

        row = {
            "id": call_id,
            "run_id": run_id,
            "node_id": node_id,
            "execution_id": execution_id,
            "role": request.role,
            "provider": response.provider,
            "exact_model_id": response.model_id,
            "prompt_name": prompt_name,
            "prompt_version": prompt_version,
            "prompt_file_hash": prompt_file_hash,
            "request_hash": response.request_hash,
            "response_hash": response.response_hash,
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "cached_input_tokens": response.usage.cached_input_tokens,
            "reasoning_tokens": response.usage.reasoning_tokens,
            "total_tokens": response.usage.total_tokens,
            "cost_usd": response.usage.cost_usd or 0.0,
            "latency_ms": response.latency_ms,
            "retry_count": response.retry_count,
            "parse_failure_count": response.parse_failure_count,
            "status": status,
            "error_type": error_type,
            "causation_id": causation_id,
            "request_body_json": req_json,
            "response_body_json": resp_json,
            "timestamp": now.isoformat(),
            "created_at": now.isoformat(),
        }

        # Insert into database.
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        self._db.conn.execute(
            f"INSERT INTO llm_calls ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )

        # Append to JSONL trace.
        if self._trace_path:
            trace_entry = {
                "call_id": call_id,
                "run_id": run_id,
                "node_id": node_id,
                "execution_id": execution_id,
                "role": request.role,
                "provider": response.provider,
                "exact_model_id": response.model_id,
                "prompt_name": prompt_name,
                "prompt_version": prompt_version,
                "prompt_file_hash": prompt_file_hash,
                "request_hash": response.request_hash,
                "response_hash": response.response_hash,
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
                "cached_input_tokens": response.usage.cached_input_tokens,
                "reasoning_tokens": response.usage.reasoning_tokens,
                "total_tokens": response.usage.total_tokens,
                "cost_usd": response.usage.cost_usd or 0.0,
                "latency_ms": response.latency_ms,
                "retry_count": response.retry_count,
                "parse_failure_count": response.parse_failure_count,
                "status": status,
                "error_type": error_type,
                "causation_id": causation_id,
                "request_body": _safe_json_loads(req_json),
                "response_body": _safe_json_loads(resp_json),
                "timestamp": now.isoformat(),
            }
            with self._trace_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(trace_entry, ensure_ascii=False, default=str) + "\n")

        return call_id

    def log_failure(
        self,
        request: LLMRequest,
        exc: Exception,
        *,
        run_id: str,
        node_id: str | None = None,
        execution_id: str | None = None,
        prompt_name: str | None = None,
        prompt_version: str | None = None,
        prompt_file_hash: str | None = None,
        provider: str = "unknown",
        model_id: str = "unknown",
        latency_ms: int = 0,
        causation_id: str | None = None,
    ) -> str:
        """Log a failed LLM call (provider error). Returns the call ID.

        This is called when the inner client raises an exception.
        The call is recorded with ``status='provider_error'`` and zero
        token usage (since no response was received).
        """
        call_id = uuid4().hex
        now = datetime.now().astimezone()

        request_body = {
            "role": request.role,
            "messages": request.messages,
            "response_schema": request.response_schema,
            "temperature": request.temperature,
            "max_output_tokens": request.max_output_tokens,
            "metadata": request.metadata,
        }
        req_json, _ = _sanitize_body(request_body)

        error_response_body = {
            "error": str(exc)[:2000],
            "error_type": type(exc).__name__,
        }
        resp_json, _ = _sanitize_body(error_response_body)

        row = {
            "id": call_id,
            "run_id": run_id,
            "node_id": node_id,
            "execution_id": execution_id,
            "role": request.role,
            "provider": provider,
            "exact_model_id": model_id,
            "prompt_name": prompt_name,
            "prompt_version": prompt_version,
            "prompt_file_hash": prompt_file_hash,
            "request_hash": request.request_hash,
            "response_hash": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_input_tokens": 0,
            "reasoning_tokens": None,
            "total_tokens": 0,
            "cost_usd": 0.0,
            "latency_ms": latency_ms,
            "retry_count": 0,
            "parse_failure_count": 0,
            "status": "provider_error",
            "error_type": type(exc).__name__,
            "causation_id": causation_id,
            "request_body_json": req_json,
            "response_body_json": resp_json,
            "timestamp": now.isoformat(),
            "created_at": now.isoformat(),
        }

        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        self._db.conn.execute(
            f"INSERT INTO llm_calls ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )

        if self._trace_path:
            trace_entry = {
                "call_id": call_id,
                "run_id": run_id,
                "node_id": node_id,
                "execution_id": execution_id,
                "role": request.role,
                "provider": provider,
                "exact_model_id": model_id,
                "prompt_name": prompt_name,
                "prompt_version": prompt_version,
                "prompt_file_hash": prompt_file_hash,
                "request_hash": request.request_hash,
                "response_hash": "",
                "input_tokens": 0,
                "output_tokens": 0,
                "cached_input_tokens": 0,
                "reasoning_tokens": None,
                "total_tokens": 0,
                "cost_usd": 0.0,
                "latency_ms": latency_ms,
                "retry_count": 0,
                "parse_failure_count": 0,
                "status": "provider_error",
                "error_type": type(exc).__name__,
                "causation_id": causation_id,
                "request_body": _safe_json_loads(req_json),
                "response_body": _safe_json_loads(resp_json),
                "timestamp": now.isoformat(),
            }
            with self._trace_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(trace_entry, ensure_ascii=False, default=str) + "\n")

        return call_id

    def list_calls(self, run_id: str) -> list[dict[str, Any]]:
        """List all LLM calls for a run."""
        rows = self._db.conn.execute(
            "SELECT * FROM llm_calls WHERE run_id = ? ORDER BY timestamp",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def total_cost(self, run_id: str) -> float:
        """Total LLM cost for a run."""
        row = self._db.conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0.0) as total FROM llm_calls WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return float(row["total"]) if row else 0.0

    def total_tokens(self, run_id: str) -> dict[str, int]:
        """Total token usage for a run."""
        row = self._db.conn.execute(
            "SELECT "
            "  COALESCE(SUM(input_tokens), 0) as input_tokens, "
            "  COALESCE(SUM(output_tokens), 0) as output_tokens, "
            "  COALESCE(SUM(cached_input_tokens), 0) as cached_input_tokens, "
            "  COALESCE(SUM(reasoning_tokens), 0) as reasoning_tokens, "
            "  COALESCE(SUM(total_tokens), 0) as total_tokens "
            "FROM llm_calls WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row:
            return {
                "input_tokens": int(row["input_tokens"]),
                "output_tokens": int(row["output_tokens"]),
                "cached_input_tokens": int(row["cached_input_tokens"]),
                "reasoning_tokens": int(row["reasoning_tokens"] or 0),
                "total_tokens": int(row["total_tokens"]),
            }
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_input_tokens": 0,
            "reasoning_tokens": 0,
            "total_tokens": 0,
        }
