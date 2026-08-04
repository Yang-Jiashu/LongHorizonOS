"""Logging wrapper for LLM clients (spec Phase 2B-B3).

Wraps any ``LLMClient`` to automatically log every call to the database
and JSONL trace. This is the recommended way to use a real LLM client in
the runtime — it ensures all calls are auditable and costs are tracked.

Step 2 (composition root): all real roles (Planner, Worker, Reconciler)
must receive a ``LoggedLLMClient`` instance, never a raw ``SenseNovaClient``.

Step 3 (failure logging): the ``generate`` method catches exceptions from
the inner client and logs them as ``status='provider_error'`` before
re-raising. This ensures every call attempt is recorded, even if the
provider is unreachable.
"""

from __future__ import annotations

import time

from lhos.infrastructure.llm.call_logger import LLMCallLogger
from lhos.ports.llm import LLMClient, LLMRequest, LLMResponse


class LoggedLLMClient:
    """Wraps an ``LLMClient`` to log every call.

    The underlying client (e.g. ``SenseNovaClient``) makes the actual API
    call; this wrapper records the request and response to the database
    and JSONL trace.

    Parameters
    ----------
    inner : LLMClient
        The real LLM client (SenseNovaClient, MockLLMClient, etc.).
    logger : LLMCallLogger
        The call logger that writes to the database.
    run_id : str
        The current run ID.
    prompt_info : dict[str, str] | None
        Optional prompt metadata (name, version, file_hash) to attach.
    """

    def __init__(
        self,
        inner: LLMClient,
        logger: LLMCallLogger,
        run_id: str,
        prompt_info: dict[str, str] | None = None,
    ):
        self._inner = inner
        self._logger = logger
        self._run_id = run_id
        self._prompt_info = prompt_info or {}
        # Context defaults (can be overridden via set_context).
        self._node_id: str | None = None
        self._execution_id: str | None = None

    def set_context(
        self,
        *,
        node_id: str | None = None,
        execution_id: str | None = None,
        prompt_name: str | None = None,
        prompt_version: str | None = None,
        prompt_file_hash: str | None = None,
    ) -> None:
        """Set context for subsequent calls."""
        if node_id is not None:
            self._node_id = node_id
        if execution_id is not None:
            self._execution_id = execution_id
        if prompt_name:
            self._prompt_info["prompt_name"] = prompt_name
        if prompt_version:
            self._prompt_info["prompt_version"] = prompt_version
        if prompt_file_hash:
            self._prompt_info["prompt_file_hash"] = prompt_file_hash

    @property
    def provider(self) -> str:
        """Delegate to inner client."""
        return getattr(self._inner, "provider", "unknown")

    @property
    def model_id(self) -> str:
        """Delegate to inner client."""
        return getattr(self._inner, "model_id", "unknown")

    def _extract_prompt_info_from_request(self, request: LLMRequest) -> dict[str, str]:
        """Extract prompt metadata from request or instance defaults."""
        meta = request.metadata or {}
        info: dict[str, str] = {}
        info["prompt_name"] = meta.get("prompt_name") or self._prompt_info.get("prompt_name", "")
        info["prompt_version"] = meta.get("prompt_version") or self._prompt_info.get(
            "prompt_version", ""
        )
        info["prompt_file_hash"] = meta.get("prompt_file_hash") or self._prompt_info.get(
            "prompt_file_hash", ""
        )
        return info

    def _extract_node_id_from_request(self, request: LLMRequest) -> str | None:
        """Extract node_id from request metadata or instance context."""
        meta = request.metadata or {}
        return meta.get("node_id") or self._node_id

    def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response, logging both success and failure.

        On success: logs with ``status='success'`` (or ``status='parse_failed'``
        if the response has ``parse_failure_count > 0``).

        On provider error: logs with ``status='provider_error'`` and re-raises.
        """
        prompt_info = self._extract_prompt_info_from_request(request)
        node_id = self._extract_node_id_from_request(request)
        started = time.perf_counter()

        try:
            response = self._inner.generate(request)
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            self._logger.log_failure(
                request,
                exc,
                run_id=self._run_id,
                node_id=node_id,
                execution_id=self._execution_id,
                prompt_name=prompt_info.get("prompt_name"),
                prompt_version=prompt_info.get("prompt_version"),
                prompt_file_hash=prompt_info.get("prompt_file_hash"),
                provider=self.provider,
                model_id=self.model_id,
                latency_ms=latency_ms,
            )
            raise

        # Determine status: parse_failed if repair was attempted.
        status = "parse_failed" if response.parse_failure_count > 0 else "success"

        self._logger.log_call(
            request,
            response,
            run_id=self._run_id,
            node_id=node_id,
            execution_id=self._execution_id,
            prompt_name=prompt_info.get("prompt_name"),
            prompt_version=prompt_info.get("prompt_version"),
            prompt_file_hash=prompt_info.get("prompt_file_hash"),
            status=status,
            error_type=None if status == "success" else "StructuredOutputError",
        )
        return response

    @property
    def inner(self) -> LLMClient:
        return self._inner
