"""End-to-end SDK boundary tests for mediated HTTP provenance."""

from __future__ import annotations

import hashlib

import pytest

from lhos.integrations import (
    HTTPAccessDenied,
    HTTPResponseValidationError,
    HTTPTransportError,
)
from lhos.provenance import CoverageStatus
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    HTTPToolBoundaryError,
    Task,
    VerificationError,
    VerificationOutcome,
    create_execution_context,
    create_http_tool,
    http_tool_context,
)

URL = "https://api.example.test/v1/value"


def _context(*, secure: bool = True, executor_api: str = "context_v1"):
    return create_execution_context(
        "graph-http-sdk",
        "task-http-sdk",
        claim_id="claim-http-sdk",
        attempt_id="attempt-http-sdk",
        semantic_epoch=2,
        executor_api=executor_api,
        secure_mode=secure,
    )


def _trusted_transport(body: bytes = b'{"value":1}'):
    def transport(_request):
        return {
            "status_code": 200,
            "headers": {"etag": '"v1"'},
            "body": body,
        }

    return transport


def test_factory_derives_declared_http_url_and_records_complete_coverage() -> None:
    context = _context()
    task = Task(
        "task-http-sdk",
        inputs=(URL, "workspace://requirements.txt"),
        executor_api="context_v1",
        provenance_policy="strict",
    )
    tool = create_http_tool(
        context,
        task,
        transport=_trusted_transport(),
        validator=lambda snapshot: snapshot.etag == '"v1"',
    )

    snapshot = tool.get(URL, version=1)

    assert snapshot.response_body_hash == hashlib.sha256(b'{"value":1}').hexdigest()
    assert tool.read_set == (snapshot,)
    report = tool.coverage_report()
    assert report.status is CoverageStatus.COMPLETE
    assert report.declared_inputs == (URL,)
    assert report.observed_inputs == (URL,)
    assert tool.assert_complete().allowed is True
    assert context.events[-1].source == "sdk"
    assert context.events[-1].known is True


def test_context_manager_closes_tool_after_executor_scope() -> None:
    context = _context()
    with http_tool_context(
        context,
        declared_urls=(URL,),
        transport=_trusted_transport(),
        etag_validator=lambda snapshot: snapshot.etag == '"v1"',
    ) as tool:
        assert tool.get(URL, version=1).body == b'{"value":1}'

    with pytest.raises(HTTPToolBoundaryError, match="closed"):
        tool.get(URL, version=1)


@pytest.mark.parametrize(
    ("secure", "executor_api", "message"),
    [
        (False, "context_v1", "secure ExecutionContext"),
        (False, "legacy_task_id", "context_v1"),
    ],
)
def test_factory_rejects_insecure_or_legacy_execution_context(
    secure: bool,
    executor_api: str,
    message: str,
) -> None:
    context = _context(secure=secure, executor_api=executor_api)
    with pytest.raises(ConfigurationError, match=message):
        create_http_tool(
            context,
            declared_urls=(URL,),
            validator=lambda _snapshot: True,
            transport=_trusted_transport(),
        )


def test_factory_requires_declared_url_and_validator() -> None:
    context = _context()
    with pytest.raises(ConfigurationError, match="requires declared_urls"):
        create_http_tool(
            context,
            Task("task-http-sdk", inputs=("workspace://x",)),
            validator=lambda _snapshot: True,
        )
    with pytest.raises(ConfigurationError, match="trusted validator"):
        create_http_tool(
            context,
            declared_urls=(URL,),
            transport=_trusted_transport(),
        )


def test_undeclared_url_is_known_false_and_strict_coverage_cannot_be_complete() -> None:
    context = _context()
    tool = create_http_tool(
        context,
        declared_urls=(URL,),
        validator=lambda _snapshot: True,
        transport=_trusted_transport(),
    )
    hidden = "https://api.example.test/v1/hidden"

    with pytest.raises(HTTPAccessDenied, match="not declared"):
        tool.get(hidden)

    assert context.events[-1].known is False
    assert context.events[-1].resource_uri == hidden
    assert context.events[-1].metadata["reason"] == "url_not_declared"
    report = tool.coverage_report()
    assert report.status is CoverageStatus.UNKNOWN
    assert tool.coverage_decision().allowed is False


def test_untrusted_response_and_transport_error_are_known_false() -> None:
    rejected_context = _context()
    rejected = create_http_tool(
        rejected_context,
        declared_urls=(URL,),
        validator=lambda _snapshot: False,
        transport=_trusted_transport(),
    )
    with pytest.raises(HTTPResponseValidationError, match="rejected"):
        rejected.get(URL, version=1)
    assert rejected_context.events[-1].known is False
    assert rejected_context.events[-1].metadata["reason"] == "response_identity_unverified"
    assert rejected.coverage_report().status is CoverageStatus.UNKNOWN

    failed_context = _context()

    def fail(_request):
        raise ConnectionError("API unavailable")

    failed = create_http_tool(
        failed_context,
        declared_urls=(URL,),
        validator=lambda _snapshot: True,
        transport=fail,
    )
    with pytest.raises(HTTPTransportError, match="API unavailable"):
        failed.get(URL, version=1)
    assert failed_context.events[-1].known is False
    assert failed_context.events[-1].metadata["reason"] == "transport_failed"
    assert failed.coverage_report().status is CoverageStatus.UNKNOWN


def test_mutating_http_method_is_not_misrepresented_as_a_trusted_read() -> None:
    context = _context()
    transport_called = False

    def transport(_request):
        nonlocal transport_called
        transport_called = True
        return {"status_code": 200, "body": b"changed"}

    tool = create_http_tool(
        context,
        declared_urls=(URL,),
        validator=lambda _snapshot: True,
        transport=transport,
    )
    with pytest.raises(HTTPToolBoundaryError, match="not admitted"):
        tool.post(URL, body={"new": 2})

    assert transport_called is False
    assert context.events[-1].known is False
    assert context.events[-1].metadata["reason"] == "mutating_operation_not_admitted"
    assert tool.coverage_report().status is CoverageStatus.UNKNOWN


def test_hidden_api_mutation_never_allows_strict_complete_coverage() -> None:
    """A later hidden input invalidates an earlier complete-looking trace.

    The adapter cannot globally intercept arbitrary network libraries.  A
    harness/tool adapter that detects a bypass must mark it UNKNOWN; strict
    coverage then refuses to claim COMPLETE, even though the declared URL was
    successfully read first.
    """

    context = _context()
    tool = create_http_tool(
        context,
        declared_urls=(URL,),
        validator=lambda _snapshot: True,
        transport=_trusted_transport(b'{"value":"old"}'),
    )
    tool.get(URL, version=1)
    assert tool.coverage_report().status is CoverageStatus.COMPLETE

    # Simulate a raw HTTP client bypassing the mediated tool after the server
    # mutated.  The enclosing harness can detect the bypass but cannot prove
    # the exact dependency, so it must record known=False.
    context.observe_unknown(
        resource_hint="https://api.example.test/v1/value?hidden-mutation=1",
        reason="unmediated_http_dependency_detected",
    )

    report = tool.coverage_report()
    assert report.status is CoverageStatus.UNKNOWN
    assert report.unknown_inputs
    assert tool.coverage_decision().allowed is False
    with pytest.raises(HTTPToolBoundaryError, match="coverage denied"):
        tool.assert_complete()


def test_real_context_v1_agentos_attempt_commits_trusted_http_read() -> None:
    """The adapter participates in the actual AgentOS coverage/commit path."""

    os_ = AgentOS(":memory:", secure_mode=True)
    try:

        def execute(context, _task_id):
            tool = create_http_tool(
                context,
                declared_urls=(URL,),
                validator=lambda snapshot: snapshot.etag == '"v1"',
                transport=_trusted_transport(b'{"value":"current"}'),
            )
            tool.get(URL, version=1)

        os_.add_agent(
            Agent(
                "http-worker",
                executor_api="context_v1",
                executor=execute,
            )
        )
        os_.register_external_fact(URL, 1, b'{"value":"current"}')
        goal = Goal("http-sdk-real-attempt")
        goal.task(
            "task-http-sdk",
            agent="http-worker",
            executor_api="context_v1",
            inputs=(URL,),
            provenance_policy="strict",
            verify=lambda _context: VerificationOutcome(
                passed=True,
                artifact_id="api-result",
                version=1,
                content="trusted",
            ),
        )

        result = os_.run(goal, max_dispatches=1)

        assert result.goal_state == "closed"
        assert result.verified == ["task-http-sdk"]
        attempt = os_.scheduler.attempts[0]
        assert attempt.agent_snapshot is not None
        assert [binding.resource_uri for binding in attempt.agent_snapshot.read_set] == [URL]
    finally:
        os_.close()


def test_real_context_v1_hidden_api_dependency_blocks_verified_commit() -> None:
    """A detected HTTP bypass cannot hide behind one successful declared read."""

    os_ = AgentOS(":memory:", secure_mode=True)
    try:

        def execute(context, _task_id):
            tool = create_http_tool(
                context,
                declared_urls=(URL,),
                validator=lambda _snapshot: True,
                transport=_trusted_transport(),
            )
            tool.get(URL, version=1)
            # A surrounding harness detected an unmediated API access.  The
            # SDK does not claim it intercepted the raw request; it records the
            # uncertainty so the normal secure commit path fails closed.
            context.observe_unknown(
                resource_hint="raw-http://hidden-api-mutation",
                reason="unmediated_http_dependency_detected",
            )

        os_.add_agent(
            Agent(
                "http-worker",
                executor_api="context_v1",
                executor=execute,
            )
        )
        os_.register_external_fact(URL, 1, b'{"value":1}')
        goal = Goal("http-sdk-hidden-dependency")
        goal.task(
            "task-http-sdk",
            agent="http-worker",
            executor_api="context_v1",
            inputs=(URL,),
            provenance_policy="strict",
            verify=lambda _context: VerificationOutcome(
                passed=True,
                artifact_id="api-result",
                version=1,
                content="must-not-commit",
            ),
        )

        with pytest.raises(VerificationError, match="failed to attach Evidence"):
            os_.run(goal, max_dispatches=1)

        attempt = os_.scheduler.attempts[0]
        assert attempt.state.value == "failed"
        assert attempt.error is not None
        assert "evidence_attachment_failed" in attempt.error
    finally:
        os_.close()
