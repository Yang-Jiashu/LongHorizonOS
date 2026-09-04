"""Focused tests for the explicit HTTP/API provenance boundary."""

from __future__ import annotations

import hashlib

import pytest

from lhos.integrations import (
    HTTPAccessDenied,
    HTTPProvenanceGateway,
    HTTPResponseValidationError,
    HTTPTransportError,
    canonical_http_url,
)
from lhos.provenance import CoverageStatus, ProvenanceOperation
from lhos.sdk import Task, build_coverage_report, create_execution_context


def _strict_context():
    return create_execution_context(
        "graph-http",
        "task-http",
        claim_id="claim-http",
        attempt_id="attempt-http",
        semantic_epoch=9,
        executor_api="context_v1",
        secure_mode=True,
    )


def test_canonical_url_normalizes_aliases_without_fragment() -> None:
    assert (
        canonical_http_url("HTTPS://Example.TEST:443/a/../v1/data?z=2&a=1")
        == "https://example.test/v1/data?a=1&z=2"
    )

    with pytest.raises(Exception, match="fragments"):
        canonical_http_url("https://example.test/data#local")


def test_strict_get_records_exact_request_response_identity_and_coverage() -> None:
    body = b'{"value": 7}'
    calls = []

    def transport(request):
        calls.append(request)
        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/json", "ETag": '"v7"'},
            "body": body,
        }

    context = _strict_context()
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/data?b=2&a=1",),
        transport=transport,
        etag_validator=lambda snapshot: snapshot.etag == '"v7"',
        allowed_methods=("GET",),
    )

    snapshot = gateway.get(
        "HTTPS://API.EXAMPLE.TEST:443/data?a=1&b=2",
        headers={"Accept": "application/json"},
        version=7,
        expected_etag='"v7"',
    )

    assert len(calls) == 1
    assert snapshot.canonical_url == "https://api.example.test/data?a=1&b=2"
    assert snapshot.response_body_hash == hashlib.sha256(body).hexdigest()
    assert snapshot.request_body_hash == hashlib.sha256(b"").hexdigest()
    assert gateway.read_set == (snapshot,)

    event = context.events[-1]
    assert event.op is ProvenanceOperation.NETWORK
    assert event.resource_uri == snapshot.canonical_url
    assert event.artifact_id == snapshot.canonical_url
    assert event.version == 7
    assert event.content_hash == snapshot.response_body_hash
    assert event.source == "sdk"
    assert event.attempt_id == "attempt-http"
    assert event.semantic_epoch == 9
    assert event.metadata["method"] == "GET"
    assert event.metadata["etag"] == '"v7"'
    assert event.metadata["identity_source"] == "etag_validator"
    assert event.metadata["response_version_source"] == "caller"
    assert event.metadata["request_body_hash"] == snapshot.request_body_hash
    assert event.metadata["response_headers_hash"] == snapshot.response_headers_hash

    task = Task(
        "task-http",
        inputs=("https://api.example.test/data?a=1&b=2",),
        executor_api="context_v1",
        provenance_policy="strict",
    )
    report = build_coverage_report(task, context)
    assert report.status is CoverageStatus.COMPLETE


def test_strict_mode_rejects_unvalidated_etag_and_records_unknown() -> None:
    context = _strict_context()
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/data",),
        transport=lambda _request: {
            "status_code": 200,
            "headers": {"ETag": '"server-supplied-only"'},
            "body": b"payload",
        },
    )

    with pytest.raises(HTTPResponseValidationError, match="requires a trusted"):
        gateway.get("https://api.example.test/data", version=1)

    assert len(context.events) == 1
    event = context.events[0]
    assert event.op is ProvenanceOperation.NETWORK
    assert event.known is False
    assert event.metadata["reason"] == "response_identity_unverified"
    assert gateway.read_set == ()


def test_strict_mode_rejects_undeclared_url_before_transport() -> None:
    called = False

    def transport(_request):
        nonlocal called
        called = True
        return {"status_code": 200, "body": b"secret"}

    context = _strict_context()
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/declared",),
        transport=transport,
        version_validator=lambda _snapshot: True,
    )

    with pytest.raises(HTTPAccessDenied, match="not declared"):
        gateway.get("https://api.example.test/hidden")
    assert called is False
    assert context.events == ()


def test_audit_mode_exposes_undeclared_api_read_to_coverage() -> None:
    context = create_execution_context(
        "graph-http",
        "task-http",
        executor_api="context_v1",
    )
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/declared",),
        strict=False,
        transport=lambda request: {
            "status_code": 200,
            "headers": {},
            "body": {"url": request.canonical_url},
        },
    )
    gateway.get("https://api.example.test/extra")

    task = Task(
        "task-http",
        inputs=("https://api.example.test/declared",),
        executor_api="context_v1",
        provenance_policy="audit",
    )
    report = build_coverage_report(task, context)
    assert report.status is CoverageStatus.PARTIAL
    assert report.missing_inputs == ("https://api.example.test/declared",)
    assert report.undeclared_inputs == ("https://api.example.test/extra",)
    assert context.events[-1].metadata["declared"] is False
    assert context.events[-1].metadata["identity_source"] == "content_hash"


def test_version_authority_validates_exact_version_and_body_hash() -> None:
    body = b"authoritative"
    digest = hashlib.sha256(body).hexdigest()

    class Authority:
        def read_hash(self, _pid, uri, version):
            if uri == "https://api.example.test/data" and version == 4:
                return digest
            return None

    context = _strict_context()
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/data",),
        transport=lambda _request: {
            "status_code": 200,
            "headers": {},
            "body": body,
        },
        version_authority=Authority(),
    )

    snapshot = gateway.get("https://api.example.test/data", version=4)
    assert snapshot.version == 4
    assert context.events[-1].version == 4
    assert context.events[-1].metadata["identity_source"] == "version_authority"


def test_authority_hash_mismatch_fails_closed_and_hides_previous_binding() -> None:
    response_body = b"v1"
    trusted_hash = hashlib.sha256(response_body).hexdigest()
    expected_hash = trusted_hash

    class Authority:
        def read_hash(self, _pid, _uri, _version):
            return expected_hash

    context = _strict_context()
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/data",),
        transport=lambda _request: {
            "status_code": 200,
            "headers": {},
            "body": response_body,
        },
        version_authority=Authority(),
    )
    first = gateway.get("https://api.example.test/data", version=1)
    assert gateway.read_set == (first,)

    expected_hash = "0" * 64
    with pytest.raises(HTTPResponseValidationError, match="rejected"):
        gateway.get("https://api.example.test/data", version=1)
    assert context.events[-1].known is False
    assert gateway.read_set == ()


def test_transport_failure_is_unknown_not_a_missing_dependency() -> None:
    context = _strict_context()

    def transport(_request):
        raise ConnectionError("provider unavailable")

    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/data",),
        transport=transport,
        version_validator=lambda _snapshot: True,
    )

    with pytest.raises(HTTPTransportError, match="provider unavailable"):
        gateway.get("https://api.example.test/data", version=1)
    event = context.events[-1]
    assert event.known is False
    assert event.metadata["reason"] == "transport_failed"


def test_post_request_hash_is_deterministic_but_not_exactly_once_claim() -> None:
    """POST is observable, but this gateway does not pretend to fence effects."""

    context = create_execution_context(
        "graph-http",
        "task-http",
        executor_api="context_v1",
    )
    gateway = HTTPProvenanceGateway(
        context,
        allowed_urls=("https://api.example.test/query",),
        strict=False,
        allowed_methods=("POST",),
        transport=lambda request: {
            "status_code": 200,
            "headers": {},
            "body": request.body,
        },
    )

    snapshot = gateway.post(
        "https://api.example.test/query",
        body={"b": 2, "a": 1},
    )
    canonical_body = b'{"a":1,"b":2}'
    assert snapshot.request_body_hash == hashlib.sha256(canonical_body).hexdigest()
    assert snapshot.response_body_hash == snapshot.request_body_hash
    assert context.events[-1].metadata["method"] == "POST"
    assert "action_id" not in context.events[-1].metadata
