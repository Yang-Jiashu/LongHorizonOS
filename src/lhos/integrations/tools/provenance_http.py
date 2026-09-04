"""Mediated HTTP/API observations with bounded provenance capture.

This module intentionally implements an *explicit transport boundary*, not a
process-wide network hook.  Callers opt in by sending requests through
``HTTPProvenanceGateway``.  The gateway canonicalizes the request URL,
computes request/response body and header digests, and records a
``NETWORK`` provenance event on the supplied :class:`ExecutionContext`.

The gateway is an observation primitive, not an exactly-once side-effect
gateway.  ``POST``/``PUT``/other methods are supported for API compatibility,
but callers must use the separate ``ActionGateway`` contract when an external
effect needs ownership, idempotency, or fencing guarantees.

Strict mode is deliberately fail-closed.  A response can only become a
trusted read when an explicit ``version_validator``/``etag_validator`` (or a
compatible authority) validates the exact response identity.  A caller
supplied integer or an unvalidated ETag is not treated as semantic truth.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from lhos.provenance import ExecutionContext, ProvenanceOperation


class HTTPGatewayError(RuntimeError):
    """A mediated HTTP operation could not be completed safely."""


class HTTPAccessDenied(HTTPGatewayError, PermissionError):
    """The request URL/method is outside the gateway capability."""


class HTTPTransportError(HTTPGatewayError):
    """The injected/default HTTP transport failed operationally."""


class HTTPResponseValidationError(HTTPGatewayError):
    """A response did not provide a trusted version/validator in strict mode."""


# Descriptive aliases used by integrations that name the error after the
# version rather than the whole response.
HTTPVersionValidationError = HTTPResponseValidationError


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _body_bytes(value: Any) -> bytes:
    """Encode request/response bodies deterministically without guessing text."""

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
    # Test transports and JSON API adapters commonly return a decoded object.
    # Canonical JSON keeps the identity deterministic and avoids repr-based
    # hashes that vary across processes.
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HTTPGatewayError(f"HTTP body is not bytes/text/JSON-serializable: {exc}") from exc


def _headers_dict(headers: Any) -> dict[str, str]:
    if headers is None:
        return {}
    if isinstance(headers, Mapping) or callable(getattr(headers, "items", None)):
        items: Iterable[tuple[Any, Any]] = headers.items()
    else:
        items = headers
    normalized: dict[str, str] = {}
    for key, value in items:
        name = str(key).strip().lower()
        if not name:
            raise HTTPGatewayError("HTTP header names must be non-empty")
        # Header values are identity material.  Do not silently coerce a
        # nested mapping/list into an unstable repr.
        if isinstance(value, (Mapping, list, tuple, set)):
            raise HTTPGatewayError(f"HTTP header {name!r} must be scalar")
        normalized[name] = str(value).strip()
    return dict(sorted(normalized.items()))


def _headers_hash(headers: Mapping[str, str]) -> str:
    encoded = json.dumps(
        sorted((str(key).lower(), str(value)) for key, value in headers.items()),
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256(encoded)


def canonical_http_url(url: str) -> str:
    """Return a deterministic identity for an HTTP(S) URL.

    Fragments are never sent to an HTTP server and are rejected rather than
    silently becoming a different resource identity.  Host names and schemes
    are lower-cased, default ports are removed, dot segments are normalized,
    and query pairs are sorted while preserving duplicate keys.
    """

    raw = str(url).strip()
    if not raw:
        raise HTTPGatewayError("HTTP URL must be non-empty")
    try:
        parts = urlsplit(raw)
    except ValueError as exc:
        raise HTTPGatewayError(f"invalid HTTP URL: {url!r}") from exc
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise HTTPGatewayError("HTTP gateway only supports http:// and https:// URLs")
    if parts.username is not None or parts.password is not None:
        raise HTTPGatewayError("HTTP URL userinfo is not supported by the provenance gateway")
    if parts.fragment:
        raise HTTPGatewayError("HTTP URL fragments are not valid resource identities")
    if not parts.hostname:
        raise HTTPGatewayError("HTTP URL must include a host")
    hostname = parts.hostname.lower()
    # ``urlsplit().port`` raises for malformed/non-numeric ports.
    try:
        port = parts.port
    except ValueError as exc:
        raise HTTPGatewayError(f"invalid HTTP URL port: {url!r}") from exc
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    hostport = hostname if port is None or default_port else f"{hostname}:{port}"

    path = parts.path or "/"
    # Normalize only path dot segments; percent-encoded bytes are left intact.
    trailing_slash = path.endswith("/")
    segments: list[str] = []
    for segment in path.split("/"):
        if segment in {"", "."}:
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    normalized_path = "/" + "/".join(segments)
    if trailing_slash and normalized_path != "/" and not normalized_path.endswith("/"):
        normalized_path += "/"
    # Sorting query pairs gives aliases a single identity while retaining
    # repeated parameters.  A caller requiring order-sensitive query semantics
    # should expose that order in a path or use a distinct endpoint URI.
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    normalized_query = urlencode(sorted(pairs), doseq=True)
    return urlunsplit((scheme, hostport, normalized_path, normalized_query, ""))


def _normalize_method(method: str) -> str:
    normalized = str(method).strip().upper()
    if not normalized or not re.fullmatch(r"[A-Z][A-Z0-9!#$%&'*+.^_`|~-]*", normalized):
        raise HTTPGatewayError(f"invalid HTTP method: {method!r}")
    return normalized


def _normalize_version(version: int | None) -> int | None:
    if version is None:
        return None
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise HTTPGatewayError("HTTP version must be a positive integer or None")
    return version


def _normalize_etag(etag: Any) -> str | None:
    if etag is None:
        return None
    value = str(etag).strip()
    return value or None


@dataclass(frozen=True, slots=True)
class HTTPRequest:
    """Canonical request passed to an injected transport."""

    method: str
    canonical_url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""
    timeout_s: float = 30.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "method", _normalize_method(self.method))
        object.__setattr__(self, "canonical_url", canonical_http_url(self.canonical_url))
        object.__setattr__(self, "headers", _headers_dict(self.headers))
        object.__setattr__(self, "body", bytes(self.body))
        if self.timeout_s <= 0:
            raise HTTPGatewayError("HTTP timeout_s must be > 0")

    @property
    def body_hash(self) -> str:
        return _sha256(self.body)

    @property
    def headers_hash(self) -> str:
        return _headers_hash(self.headers)


@dataclass(frozen=True, slots=True)
class HTTPResponseSnapshot:
    """Exact response identity returned by :meth:`HTTPProvenanceGateway.request`."""

    method: str
    canonical_url: str
    request_body_hash: str
    request_headers_hash: str
    response_body_hash: str
    response_headers_hash: str
    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""
    version: int | None = None
    etag: str | None = None
    identity_hash: str = ""
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "method", _normalize_method(self.method))
        object.__setattr__(self, "canonical_url", canonical_http_url(self.canonical_url))
        object.__setattr__(self, "headers", _headers_dict(self.headers))
        object.__setattr__(self, "body", bytes(self.body))
        object.__setattr__(self, "version", _normalize_version(self.version))
        object.__setattr__(self, "etag", _normalize_etag(self.etag))
        if not 100 <= int(self.status_code) <= 599:
            raise HTTPGatewayError("HTTP status_code must be between 100 and 599")
        object.__setattr__(self, "status_code", int(self.status_code))
        if not self.identity_hash:
            material = {
                "method": self.method,
                "canonical_url": self.canonical_url,
                "request_body_hash": self.request_body_hash,
                "request_headers_hash": self.request_headers_hash,
                "response_body_hash": self.response_body_hash,
                "response_headers_hash": self.response_headers_hash,
                "status_code": self.status_code,
                "version": self.version,
                "etag": self.etag,
            }
            digest = _sha256(
                json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            object.__setattr__(self, "identity_hash", digest)

    @property
    def resource_uri(self) -> str:
        return self.canonical_url

    @property
    def content_hash(self) -> str:
        return self.response_body_hash


@runtime_checkable
class HTTPTransport(Protocol):
    """Transport protocol accepted by ``HTTPProvenanceGateway``."""

    def __call__(self, request: HTTPRequest) -> Any:
        """Return an HTTP response-like object for one canonical request."""


HTTPResponseValidator = Callable[[HTTPResponseSnapshot], bool]
HTTPVersionValidator = HTTPResponseValidator


def _normalize_response(value: Any, *, request: HTTPRequest) -> tuple[int, dict[str, str], bytes]:
    """Normalize common transport return shapes without hiding malformed data."""

    if isinstance(value, HTTPResponseSnapshot):
        return value.status_code, dict(value.headers), bytes(value.body)
    if isinstance(value, Mapping):
        raw = dict(value)
        status = raw.get("status_code", raw.get("status", raw.get("code")))
        headers = raw.get("headers", {})
        body = raw.get("body", raw.get("content", raw.get("data", b"")))
    else:
        status = getattr(value, "status_code", getattr(value, "status", None))
        headers = getattr(value, "headers", {})
        body = getattr(value, "body", getattr(value, "content", b""))
    if status is None:
        raise HTTPGatewayError(
            f"HTTP transport returned no status_code for {request.method} {request.canonical_url}"
        )
    try:
        status_code = int(status)
    except (TypeError, ValueError) as exc:
        raise HTTPGatewayError("HTTP transport status_code must be an integer") from exc
    normalized_headers = _headers_dict(headers)
    return status_code, normalized_headers, _body_bytes(body)


class HTTPProvenanceGateway:
    """Explicit, capability-scoped HTTP observation boundary.

    ``allowed_urls`` is an exact URL capability set.  In strict mode a URL
    outside that set is rejected before transport invocation.  In audit mode
    undeclared URLs are allowed but marked in provenance metadata so coverage
    reports can surface the edge.
    """

    def __init__(
        self,
        context: ExecutionContext,
        *,
        allowed_urls: Iterable[str] = (),
        readable: Iterable[str] | None = None,
        strict: bool | None = None,
        transport: HTTPTransport | Callable[..., Any] | None = None,
        timeout_s: float = 30.0,
        version_validator: HTTPResponseValidator | None = None,
        etag_validator: HTTPResponseValidator | None = None,
        version_authority: Any | None = None,
        allowed_methods: Iterable[str] = (),
    ) -> None:
        if not isinstance(context, ExecutionContext):
            raise TypeError("context must be an ExecutionContext")
        requested_strict = context.secure_mode if strict is None else bool(strict)
        if context.secure_mode and not requested_strict:
            raise HTTPGatewayError("a secure ExecutionContext cannot disable HTTP strict mode")
        if timeout_s <= 0:
            raise HTTPGatewayError("timeout_s must be > 0")
        if version_validator is not None and not callable(version_validator):
            raise HTTPGatewayError("version_validator must be callable")
        if etag_validator is not None and not callable(etag_validator):
            raise HTTPGatewayError("etag_validator must be callable")
        if version_authority is not None and not (
            callable(version_authority)
            or any(
                callable(getattr(version_authority, name, None))
                for name in ("validate_http_observation", "validate_response", "validate")
            )
            or callable(getattr(version_authority, "read_hash", None))
            or callable(getattr(version_authority, "read_etag", None))
        ):
            raise HTTPGatewayError(
                "version_authority must be callable or expose validate_http_observation, "
                "validate_response, validate, read_hash, or read_etag"
            )
        self.context = context
        self.strict = requested_strict
        self.timeout_s = float(timeout_s)
        self.transport = transport or self._urllib_transport
        self._version_validator = version_validator
        self._etag_validator = etag_validator
        self._version_authority = version_authority
        resources = tuple(allowed_urls) if readable is None else tuple(readable)
        self._allowed_urls = frozenset(canonical_http_url(item) for item in resources)
        self._allowed_methods = frozenset(_normalize_method(item) for item in allowed_methods)
        self._observations: dict[tuple[str, str, str], HTTPResponseSnapshot | None] = {}

    @property
    def allowed_urls(self) -> tuple[str, ...]:
        return tuple(sorted(self._allowed_urls))

    @staticmethod
    def _urllib_transport(request: HTTPRequest) -> HTTPResponseSnapshot:
        """Default stdlib transport; response is normalized by ``request``."""

        req = Request(
            request.canonical_url,
            data=request.body if request.body else None,
            headers=dict(request.headers),
            method=request.method,
        )
        try:
            with urlopen(req, timeout=request.timeout_s) as response:
                return HTTPResponseSnapshot(
                    method=request.method,
                    canonical_url=request.canonical_url,
                    request_body_hash=request.body_hash,
                    request_headers_hash=request.headers_hash,
                    response_body_hash="0" * 64,
                    response_headers_hash="0" * 64,
                    status_code=int(response.getcode() or 200),
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except HTTPError:
            # HTTP errors still carry a real response body/status.  Re-raise
            # only transport-level failures below; callers can inspect status.
            raise
        except (URLError, OSError) as exc:
            raise HTTPTransportError(str(exc)) from exc

    def _authorize(self, method: str, canonical_url: str) -> bool:
        if self._allowed_methods and method not in self._allowed_methods:
            raise HTTPAccessDenied(f"HTTP method {method!r} is not declared")
        declared = canonical_url in self._allowed_urls
        if self.strict and not declared:
            raise HTTPAccessDenied(f"HTTP URL is not declared: {canonical_url!r}")
        return declared

    @staticmethod
    def _call_transport(
        transport: Callable[..., Any],
        request: HTTPRequest,
    ) -> Any:
        """Support ``transport(request)`` and a small 4-argument compatibility form."""

        try:
            signature = inspect.signature(transport)
        except (TypeError, ValueError):
            return transport(request)
        try:
            signature.bind(request)
        except TypeError:
            try:
                signature.bind(
                    request.method,
                    request.canonical_url,
                    dict(request.headers),
                    request.body,
                )
            except TypeError as exc:
                raise HTTPGatewayError(
                    "HTTP transport must accept transport(request) or "
                    "transport(method, url, headers, body)"
                ) from exc
            return transport(
                request.method,
                request.canonical_url,
                dict(request.headers),
                request.body,
            )
        return transport(request)

    def _authority_validate(self, snapshot: HTTPResponseSnapshot) -> bool | None:
        authority = self._version_authority
        if authority is None:
            return None
        if callable(authority):
            return bool(authority(snapshot))
        for name in ("validate_http_observation", "validate_response", "validate"):
            validator = getattr(authority, name, None)
            if callable(validator):
                return bool(validator(snapshot))
        pid = (
            str(getattr(self.context, "claim_id", "")).strip()
            or str(getattr(self.context, "attempt_id", "")).strip()
            or "http-gateway"
        )
        if snapshot.version is not None:
            read_hash = getattr(authority, "read_hash", None)
            if callable(read_hash):
                expected = read_hash(pid, snapshot.canonical_url, snapshot.version)
                return expected is not None and str(expected).lower().removeprefix("sha256:") == (
                    snapshot.response_body_hash
                )
        if snapshot.etag is not None:
            read_etag = getattr(authority, "read_etag", None)
            if callable(read_etag):
                expected = read_etag(pid, snapshot.canonical_url, snapshot.etag)
                return bool(expected)
        return None

    def _authority_version(self, canonical_url: str) -> int | None:
        """Resolve an exact current version from an explicit authority.

        A secure read cannot be fenced at semantic commit using only a body
        hash: stale-cognition validation also needs an immutable positive
        version.  FactsProvider-like authorities expose ``latest(uri)``;
        HTTP-specific authorities may instead expose
        ``resolve_http_version(uri)``.  Returning ``None`` means that the
        authority cannot provide an exact version and the secure request must
        fail closed before transport.
        """

        authority = self._version_authority
        if authority is None:
            return None
        for name in ("resolve_http_version", "latest"):
            resolver = getattr(authority, name, None)
            if not callable(resolver):
                continue
            try:
                resolved = resolver(canonical_url)
            except Exception as exc:
                raise HTTPResponseValidationError(
                    f"HTTP version authority failed for {canonical_url!r}: {exc}"
                ) from exc
            if resolved is None:
                return None
            try:
                return _normalize_version(resolved)
            except HTTPGatewayError as exc:
                raise HTTPResponseValidationError(
                    f"HTTP version authority returned an invalid version for {canonical_url!r}"
                ) from exc
        return None

    def _validate_identity(
        self,
        snapshot: HTTPResponseSnapshot,
        *,
        expected_etag: str | None,
        declared: bool,
    ) -> str:
        if expected_etag is not None and snapshot.etag != _normalize_etag(expected_etag):
            raise HTTPResponseValidationError(
                f"HTTP ETag mismatch for {snapshot.canonical_url!r}: "
                f"expected {expected_etag!r}, found {snapshot.etag!r}"
            )
        validator_result: bool | None = None
        source = "content_hash"
        if self._version_validator is not None:
            validator_result = bool(self._version_validator(snapshot))
            source = "version_validator"
        elif snapshot.etag is not None and self._etag_validator is not None:
            validator_result = bool(self._etag_validator(snapshot))
            source = "etag_validator"
        else:
            authority_result = self._authority_validate(snapshot)
            if authority_result is not None:
                validator_result = authority_result
                source = "version_authority"

        if validator_result is False:
            raise HTTPResponseValidationError(
                f"HTTP response identity was rejected for {snapshot.canonical_url!r}"
            )
        if self.strict:
            if validator_result is not True:
                raise HTTPResponseValidationError(
                    "strict HTTP provenance requires a trusted version_validator, "
                    "etag_validator, or version_authority"
                )
            if not declared:
                raise HTTPAccessDenied(f"HTTP URL is not declared: {snapshot.canonical_url!r}")
            return source
        if validator_result is True:
            return source
        if snapshot.etag is not None:
            return "unverified_etag"
        return "content_hash"

    def _record_unknown(
        self,
        request: HTTPRequest,
        *,
        response_hash: str | None = None,
        status_code: int | None = None,
        etag: str | None = None,
        reason: str,
        error: str,
        declared: bool,
    ) -> None:
        self.context.record(
            ProvenanceOperation.NETWORK,
            resource_uri=request.canonical_url,
            content_hash=response_hash,
            source="http-gateway",
            known=False,
            metadata={
                "gateway": "http_v1",
                "method": request.method,
                "request_body_hash": request.body_hash,
                "request_headers_hash": request.headers_hash,
                "status_code": status_code,
                "etag": etag,
                "declared": declared,
                "reason": reason,
                "error": error,
            },
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | str | Any = b"",
        headers: Mapping[str, Any] | None = None,
        version: int | None = None,
        expected_etag: str | None = None,
    ) -> HTTPResponseSnapshot:
        """Execute one mediated request and record its exact response identity."""

        normalized_method = _normalize_method(method)
        canonical_url = canonical_http_url(url)
        normalized_version = _normalize_version(version)
        request = HTTPRequest(
            method=normalized_method,
            canonical_url=canonical_url,
            headers=headers or {},
            body=_body_bytes(body),
            timeout_s=self.timeout_s,
        )
        declared = self._authorize(normalized_method, canonical_url)
        version_source = "caller" if normalized_version is not None else ""
        if self.context.secure_mode and normalized_version is None:
            try:
                normalized_version = self._authority_version(canonical_url)
            except HTTPResponseValidationError as exc:
                self._record_unknown(
                    request,
                    reason="response_version_unavailable",
                    error=str(exc),
                    declared=declared,
                )
                raise
            if normalized_version is None:
                error = (
                    "secure HTTP provenance requires an exact positive version "
                    "supplied by the caller or version authority"
                )
                self._record_unknown(
                    request,
                    reason="response_version_unavailable",
                    error=error,
                    declared=declared,
                )
                raise HTTPResponseValidationError(error)
            version_source = "version_authority"
        try:
            raw = self._call_transport(self.transport, request)
            status_code, response_headers, response_body = _normalize_response(
                raw,
                request=request,
            )
        except HTTPError as exc:
            # urllib's HTTPError is a response-bearing exception.
            try:
                status_code = int(exc.code)
                response_headers = _headers_dict(exc.headers)
                response_body = exc.read()
            except Exception:
                self._record_unknown(
                    request,
                    reason="transport_failed",
                    error=str(exc),
                    declared=declared,
                )
                raise HTTPTransportError(str(exc)) from exc
        except Exception as exc:
            self._record_unknown(
                request,
                reason="transport_failed",
                error=str(exc),
                declared=declared,
            )
            if isinstance(exc, HTTPGatewayError):
                raise
            raise HTTPTransportError(str(exc)) from exc

        etag = _normalize_etag(response_headers.get("etag"))
        response_hash = _sha256(response_body)
        response_headers_hash = _headers_hash(response_headers)
        snapshot = HTTPResponseSnapshot(
            method=request.method,
            canonical_url=request.canonical_url,
            request_body_hash=request.body_hash,
            request_headers_hash=request.headers_hash,
            response_body_hash=response_hash,
            response_headers_hash=response_headers_hash,
            status_code=status_code,
            headers=response_headers,
            body=response_body,
            version=normalized_version,
            etag=etag,
        )
        try:
            identity_source = self._validate_identity(
                snapshot,
                expected_etag=expected_etag,
                declared=declared,
            )
        except Exception as exc:
            self._record_unknown(
                request,
                response_hash=response_hash,
                status_code=status_code,
                etag=etag,
                reason="response_identity_unverified",
                error=str(exc),
                declared=declared,
            )
            raise

        self.context.record(
            ProvenanceOperation.NETWORK,
            resource_uri=request.canonical_url,
            # URL-backed API facts use the canonical URL as their artifact
            # identity.  This makes the successful observation usable by the
            # AgentSnapshot/VPG freshness guard instead of becoming a known
            # but unfenceable NETWORK read.
            artifact_id=request.canonical_url,
            content_hash=response_hash,
            version=normalized_version,
            known=True,
            metadata={
                "method": request.method,
                "request_body_hash": request.body_hash,
                "request_headers_hash": request.headers_hash,
                "response_body_hash": response_hash,
                "response_headers_hash": response_headers_hash,
                "status_code": status_code,
                "etag": etag,
                "identity_hash": snapshot.identity_hash,
                "identity_source": identity_source,
                "declared": declared,
                "gateway": "http_v1",
                "hash_algorithm": "sha256",
                "response_version_source": version_source or "unversioned_compatibility",
            },
        )
        self._observations[(request.method, request.canonical_url, request.body_hash)] = snapshot
        return snapshot

    def get(self, url: str, **kwargs: Any) -> HTTPResponseSnapshot:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, *, body: Any = b"", **kwargs: Any) -> HTTPResponseSnapshot:
        return self.request("POST", url, body=body, **kwargs)

    @property
    def read_set(self) -> tuple[HTTPResponseSnapshot, ...]:
        """Return the latest trusted HTTP observations in stable key order."""

        latest: dict[tuple[str, str, str], HTTPResponseSnapshot | None] = dict(self._observations)
        # A later fail-closed event for a key must hide an earlier trusted
        # binding, mirroring WorkspaceProvenanceGateway's conservative set.
        for event in self.context.events:
            if event.source != "http-gateway" or event.op is not ProvenanceOperation.NETWORK:
                continue
            method = str(event.metadata.get("method", "")).upper()
            uri = str(event.resource_uri)
            body_hash = str(event.metadata.get("request_body_hash", ""))
            if method and uri and body_hash and not event.known:
                latest[(method, uri, body_hash)] = None
        return tuple(snapshot for key, snapshot in sorted(latest.items()) if snapshot is not None)


# Camel-case aliases keep the adapter discoverable for both Python naming
# conventions used in the repository.
HttpProvenanceGateway = HTTPProvenanceGateway
HttpRequest = HTTPRequest
HttpResponseSnapshot = HTTPResponseSnapshot
HTTPObservation = HTTPResponseSnapshot
HttpObservation = HTTPResponseSnapshot


__all__ = [
    "HTTPAccessDenied",
    "HTTPGatewayError",
    "HTTPObservation",
    "HTTPProvenanceGateway",
    "HTTPRequest",
    "HTTPResponseSnapshot",
    "HTTPResponseValidationError",
    "HTTPResponseValidator",
    "HTTPTransport",
    "HTTPTransportError",
    "HTTPVersionValidationError",
    "HTTPVersionValidator",
    "HttpObservation",
    "HttpProvenanceGateway",
    "HttpRequest",
    "HttpResponseSnapshot",
    "canonical_http_url",
]
