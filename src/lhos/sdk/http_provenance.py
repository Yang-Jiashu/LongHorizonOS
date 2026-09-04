"""SDK boundary for mediated ``context_v1`` HTTP tool calls.

The integration-layer :class:`~lhos.integrations.tools.HTTPProvenanceGateway`
is intentionally a low-level transport primitive.  This module provides the
small SDK adapter that an executor can actually use:

* it accepts only a secure ``context_v1`` :class:`ExecutionContext`;
* URL capabilities must be declared up front and are canonicalized;
* a trusted version/ETag validator (or authority) is mandatory;
* successful reads are recorded by the gateway and exposed as a coverage
  report for the current execution context;
* undeclared URLs, transport failures, and untrusted responses are
  fail-closed and leave an explicit ``known=False`` event;
* mutating HTTP methods are not silently treated as reads.

This is an explicit tool boundary.  It does **not** install a process-wide
socket hook and cannot observe requests made by code that bypasses the
adapter.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from lhos.integrations.tools.provenance_http import (
    HTTPAccessDenied,
    HTTPGatewayError,
    HTTPProvenanceGateway,
    HTTPResponseSnapshot,
    HTTPResponseValidationError,
    HTTPTransportError,
    canonical_http_url,
)
from lhos.provenance import (
    CoverageDecision,
    CoveragePolicy,
    CoverageReport,
    ExecutionContext,
    ProvenanceOperation,
    evaluate_coverage,
)

from .errors import ConfigurationError
from .provenance import build_coverage_report


class HTTPToolBoundaryError(HTTPGatewayError):
    """The SDK HTTP tool could not safely admit an operation."""


_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def _body_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    # The low-level gateway uses canonical JSON for structured bodies.  Keep
    # the adapter's diagnostic hash equivalent without importing a private
    # helper from the integration module.
    import json

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _task_inputs(task_or_inputs: Any | None) -> tuple[str, ...]:
    if task_or_inputs is None:
        return ()
    raw = getattr(task_or_inputs, "declared_inputs", None)
    if raw is None:
        raw = getattr(task_or_inputs, "inputs", task_or_inputs)
    if isinstance(raw, Mapping):
        raw = raw.keys()
    elif isinstance(raw, str):
        raw = (raw,)
    try:
        values = tuple(raw)
    except TypeError as exc:
        raise ConfigurationError(
            "HTTP tool declared URLs must be an iterable of URL strings",
            cause=exc,
        ) from exc
    return tuple(str(value).strip() for value in values if str(value).strip())


def _canonical_http_inputs(
    task_or_inputs: Any | None,
    explicit: Iterable[str] | str | None,
) -> tuple[str, ...]:
    values: list[str] = list(_task_inputs(task_or_inputs))
    if explicit is not None:
        values.extend((explicit,) if isinstance(explicit, str) else tuple(explicit))
    canonical: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        # A Task may also declare workspace/model resources.  They are not
        # capabilities of this HTTP adapter and must not be canonicalized as
        # URLs or accidentally admitted.
        if not text.lower().startswith(("http://", "https://")):
            continue
        canonical.add(canonical_http_url(text))
    return tuple(sorted(canonical))


class HTTPToolAdapter:
    """A capability-scoped, read-oriented HTTP tool for ``context_v1``.

    ``HTTPToolAdapter`` deliberately keeps the low-level gateway visible via
    ``gateway`` for diagnostics, while exposing a minimal tool-shaped API
    (``request``/``get`` and ``coverage_report``) to an Agent executor.
    """

    def __init__(
        self,
        context: ExecutionContext,
        *,
        declared_urls: Iterable[str] | str,
        validator: Any | None = None,
        version_validator: Any | None = None,
        etag_validator: Any | None = None,
        version_authority: Any | None = None,
        transport: Any | None = None,
        timeout_s: float = 30.0,
        allowed_methods: Iterable[str] = ("GET",),
    ) -> None:
        if not isinstance(context, ExecutionContext):
            raise ConfigurationError("HTTP tool requires an ExecutionContext")
        if str(getattr(context, "executor_api", "")).strip() != "context_v1":
            raise ConfigurationError(
                "HTTP tool requires executor_api='context_v1'; "
                "legacy callbacks have no mediated tool boundary"
            )
        if not bool(getattr(context, "secure_mode", False)):
            raise ConfigurationError(
                "HTTP tool requires a secure ExecutionContext (secure_mode=True)"
            )

        urls = _canonical_http_inputs(None, declared_urls)
        if not urls:
            raise ConfigurationError("HTTP tool requires at least one declared http(s) URL")

        validators = [item is not None for item in (validator, version_validator, etag_validator)]
        if sum(validators) > 1:
            raise ConfigurationError(
                "configure one of validator, version_validator, or etag_validator"
            )
        if validator is not None:
            if version_validator is not None or etag_validator is not None:
                raise ConfigurationError(
                    "validator cannot be combined with version_validator/etag_validator"
                )
            version_validator = validator
        if version_validator is None and etag_validator is None and version_authority is None:
            raise ConfigurationError(
                "secure HTTP tool requires a trusted validator or version_authority"
            )

        normalized_methods = tuple(str(method).strip().upper() for method in allowed_methods)
        if not normalized_methods:
            normalized_methods = ("GET",)
        unsafe = tuple(method for method in normalized_methods if method not in _READ_METHODS)
        if unsafe:
            raise ConfigurationError(
                "HTTPToolAdapter is a read boundary; mutating methods are not admitted: "
                + ", ".join(sorted(set(unsafe)))
            )

        self.context = context
        self.declared_urls = urls
        self.gateway = HTTPProvenanceGateway(
            context,
            allowed_urls=urls,
            strict=True,
            transport=transport,
            timeout_s=timeout_s,
            version_validator=version_validator,
            etag_validator=etag_validator,
            version_authority=version_authority,
            allowed_methods=normalized_methods,
        )
        self._closed = False

    def __enter__(self) -> HTTPToolAdapter:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the logical adapter (the injected transport owns resources)."""

        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise HTTPToolBoundaryError("HTTP tool adapter is closed")

    def _record_unknown(
        self,
        *,
        method: str,
        url: str,
        body: Any = b"",
        reason: str,
        error: str,
    ) -> None:
        """Persist a conservative unknown input marker exactly once per call."""

        canonical = canonical_http_url(url)
        payload = _body_bytes(body)
        self.context.record(
            ProvenanceOperation.NETWORK,
            resource_uri=canonical,
            source="http-sdk-adapter",
            known=False,
            metadata={
                "gateway": "http_sdk_v1",
                "method": str(method).upper(),
                "request_body_hash": hashlib.sha256(payload).hexdigest(),
                "reason": reason,
                "error": error,
            },
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Any = b"",
        headers: Mapping[str, Any] | None = None,
        version: int | None = None,
        expected_etag: str | None = None,
    ) -> HTTPResponseSnapshot:
        """Perform one declared, validated read through the gateway."""

        self._ensure_open()
        normalized_method = str(method).strip().upper()
        canonical = canonical_http_url(url)
        if normalized_method not in _READ_METHODS:
            self._record_unknown(
                method=normalized_method,
                url=canonical,
                body=body,
                reason="mutating_operation_not_admitted",
                error=(
                    "HTTPToolAdapter is read-only; use ActionGateway for "
                    "irreversible or mutating effects"
                ),
            )
            raise HTTPToolBoundaryError(
                f"HTTP method {normalized_method!r} is not admitted by the read tool"
            )
        if canonical not in self.declared_urls:
            self._record_unknown(
                method=normalized_method,
                url=canonical,
                body=body,
                reason="url_not_declared",
                error=f"URL is outside declared HTTP capability: {canonical}",
            )
            raise HTTPAccessDenied(f"HTTP URL is not declared: {canonical!r}")
        try:
            return self.gateway.request(
                normalized_method,
                canonical,
                body=body,
                headers=headers,
                version=version,
                expected_etag=expected_etag,
            )
        except (HTTPResponseValidationError, HTTPTransportError, HTTPGatewayError):
            # The low-level gateway records fail-closed events for response
            # validation and transport failures.  Do not duplicate those
            # events; the adapter's boundary marker above covers preflight
            # failures that never reach the transport.
            raise
        except Exception as exc:
            # A user validator may raise an arbitrary exception.  Ensure the
            # SDK boundary still leaves an UNKNOWN event before surfacing it.
            self._record_unknown(
                method=normalized_method,
                url=canonical,
                body=body,
                reason="adapter_call_failed",
                error=str(exc),
            )
            raise HTTPToolBoundaryError(str(exc)) from exc

    def get(self, url: str, **kwargs: Any) -> HTTPResponseSnapshot:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, *, body: Any = b"", **kwargs: Any) -> HTTPResponseSnapshot:
        """Explicitly reject POST rather than treating a mutation as a read."""

        return self.request("POST", url, body=body, **kwargs)

    @property
    def read_set(self) -> tuple[HTTPResponseSnapshot, ...]:
        return self.gateway.read_set

    def coverage_report(self) -> CoverageReport:
        """Return coverage for all declared HTTP inputs in this context."""

        return build_coverage_report(
            self.declared_urls,
            self.context,
            graph_id=self.context.graph_id,
            task_id=self.context.task_id,
            executor_api="context_v1",
        )

    def coverage_decision(
        self,
        policy: CoveragePolicy | str = CoveragePolicy.STRICT,
    ) -> CoverageDecision:
        """Evaluate the current report without mutating semantic state."""

        return evaluate_coverage(self.coverage_report(), policy)

    def assert_complete(self) -> CoverageDecision:
        """Fail closed unless every declared HTTP input is trusted and observed."""

        decision = self.coverage_decision(CoveragePolicy.STRICT)
        if not decision.allowed:
            raise HTTPToolBoundaryError(
                "HTTP tool provenance coverage denied: "
                + ("; ".join(decision.reasons) or decision.status)
            )
        return decision


def create_http_tool(
    context: ExecutionContext,
    task_or_inputs: Any | None = None,
    *,
    declared_urls: Iterable[str] | str | None = None,
    allowed_urls: Iterable[str] | str | None = None,
    validator: Any | None = None,
    version_validator: Any | None = None,
    etag_validator: Any | None = None,
    version_authority: Any | None = None,
    transport: Any | None = None,
    timeout_s: float = 30.0,
) -> HTTPToolAdapter:
    """Create an explicit secure HTTP tool for one ``context_v1`` attempt.

    ``task_or_inputs`` may be an SDK ``Task``; only its HTTP(S) inputs are
    selected.  ``declared_urls``/``allowed_urls`` are equivalent aliases.
    """

    if declared_urls is not None and allowed_urls is not None:
        raise ConfigurationError("configure either declared_urls or allowed_urls, not both")
    explicit = declared_urls if declared_urls is not None else allowed_urls
    urls = _canonical_http_inputs(task_or_inputs, explicit)
    if not urls:
        raise ConfigurationError(
            "create_http_tool requires declared_urls or a task with HTTP(S) inputs"
        )
    return HTTPToolAdapter(
        context,
        declared_urls=urls,
        validator=validator,
        version_validator=version_validator,
        etag_validator=etag_validator,
        version_authority=version_authority,
        transport=transport,
        timeout_s=timeout_s,
    )


@contextmanager
def http_tool_context(
    context: ExecutionContext,
    task_or_inputs: Any | None = None,
    **kwargs: Any,
) -> Iterator[HTTPToolAdapter]:
    """Context-manager factory for a mediated secure HTTP tool."""

    adapter = create_http_tool(context, task_or_inputs, **kwargs)
    try:
        yield adapter
    finally:
        adapter.close()


# Naming aliases make the explicit boundary discoverable without claiming
# process-wide interception.
HTTPProvenanceTool = HTTPToolAdapter
http_provenance_context = http_tool_context


__all__ = [
    "HTTPProvenanceTool",
    "HTTPToolAdapter",
    "HTTPToolBoundaryError",
    "create_http_tool",
    "http_provenance_context",
    "http_tool_context",
]
