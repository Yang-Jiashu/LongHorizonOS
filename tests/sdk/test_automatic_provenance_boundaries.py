"""Automatic-rebase coverage for mediated workspace and HTTP observations.

These tests intentionally stay at the explicit boundary: the gateways record
version/hash identities, ``AgentSnapshot`` projects them into a read-set, and
the automatic planner refreshes a matching ``ContextManifest`` after Facts
advances.  They do not claim interception of arbitrary Python or network I/O.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.integrations import WorkspaceTool
from lhos.integrations.tools.provenance_http import HTTPProvenanceGateway
from lhos.integrations.tools.provenance_workspace import WorkspaceProvenanceGateway
from lhos.runtimes.multi_agent import AgentSnapshot, AttemptState
from lhos.runtimes.multi_agent.models import ScheduledExecutionAttempt
from lhos.sdk import AgentOS
from lhos.sdk.automatic_rebase import plan_automatic_rebase
from lhos.sdk.provenance import create_execution_context
from lhos.sdk.providers import FactsProvider


def _attempt() -> ScheduledExecutionAttempt:
    started = datetime.now(UTC)
    return ScheduledExecutionAttempt(
        attempt_id="attempt-1",
        graph_id="graph-1",
        graph_version=1,
        semantic_epoch=2,
        task_id="task-1",
        claim_id="claim-1",
        agent_id="agent-1",
        process_id="process-1",
        state=AttemptState.STALE_COGNITION,
        started_at=started,
    )


def test_workspace_gateway_read_becomes_automatic_rebase_guard(tmp_path) -> None:
    os_ = AgentOS(":memory:")
    workspace = WorkspaceTool(tmp_path)
    try:
        assert workspace.write("requirements.md", "v1").ok
        os_.register_workspace_artifact(workspace, "requirements.md", version=1)
        context = create_execution_context(
            "graph-1",
            "task-1",
            claim_id="claim-1",
            attempt_id="attempt-1",
            semantic_epoch=2,
            executor_api="context_v1",
            secure_mode=True,
        )
        gateway = WorkspaceProvenanceGateway(
            workspace,
            context,
            readable=("workspace://requirements.md",),
            version_authority=os_._facts,
        )
        observed = gateway.snapshot("requirements.md", version=1)
        snapshot = AgentSnapshot.from_attempt(
            _attempt(),
            execution_context=context,
            captured_at=datetime.now(UTC),
        )
        manifest = ContextManifest(
            manifest_id="requirements-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements",
                    canonical_uri=observed.resource_uri,
                    artifact_id=observed.artifact_id,
                    version=1,
                    content_hash=observed.content_hash,
                    media_type="text/plain",
                ),
            ),
            token_budget=128,
        )

        assert workspace.write("requirements.md", "v2").ok
        os_.register_workspace_artifact(workspace, "requirements.md", version=2)
        decision, replacement = plan_automatic_rebase(
            task_id="task-1",
            graph_id="graph-1",
            graph_version=1,
            agent_snapshot=snapshot,
            context_manifest=manifest,
            facts=os_._facts,
        )

        assert snapshot.read_set[0].source == "workspace-gateway"
        assert decision.redispatchable
        assert decision.changed_refs[0].artifact_id == "requirements.md"
        assert replacement is not None
        assert replacement.refs[0].version == 2
    finally:
        os_.close()


def test_http_gateway_read_becomes_automatic_rebase_guard() -> None:
    os_ = AgentOS(":memory:")
    url = "https://api.example.test/requirements"
    body_v1 = b'{"version":1}'
    body_v2 = b'{"version":2}'
    try:
        os_.register_external_fact(url, 1, body_v1)
        context = create_execution_context(
            "graph-1",
            "task-1",
            claim_id="claim-1",
            attempt_id="attempt-1",
            semantic_epoch=2,
            executor_api="context_v1",
            secure_mode=True,
        )
        gateway = HTTPProvenanceGateway(
            context,
            allowed_urls=(url,),
            transport=lambda _request: {
                "status_code": 200,
                "headers": {"ETag": '"v1"'},
                "body": body_v1,
            },
            version_authority=os_._facts,
            etag_validator=lambda snapshot: snapshot.etag == '"v1"',
        )
        observed = gateway.get(url, version=1)
        snapshot = AgentSnapshot.from_attempt(
            _attempt(),
            execution_context=context,
            captured_at=datetime.now(UTC),
        )
        manifest = ContextManifest(
            manifest_id="http-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements-api",
                    canonical_uri=observed.canonical_url,
                    artifact_id=observed.canonical_url,
                    version=1,
                    content_hash=observed.response_body_hash,
                    media_type="application/json",
                ),
            ),
            token_budget=128,
        )

        os_.register_external_fact(url, 2, body_v2)
        decision, replacement = plan_automatic_rebase(
            task_id="task-1",
            graph_id="graph-1",
            graph_version=1,
            agent_snapshot=snapshot,
            context_manifest=manifest,
            facts=os_._facts,
        )

        # The HTTP adapter intentionally preserves the SDK recorder source;
        # the event's operation/metadata identify the mediated HTTP boundary.
        assert snapshot.read_set[0].source == "sdk"
        assert snapshot.read_set[0].resource_uri == url
        assert decision.redispatchable
        assert decision.changed_refs[0].artifact_id == url
        assert replacement is not None
        assert replacement.refs[0].version == 2
    finally:
        os_.close()


def test_uri_only_vpg_provenance_is_the_only_derived_identity() -> None:
    """External schemes remain fail-closed unless artifact_id is explicit."""

    facts = FactsProvider(":memory:")
    try:
        facts.add_version("requirements", 1, "v1")
        facts.add_version("requirements", 2, "v2")
        context = create_execution_context(
            "graph-1",
            "task-1",
            semantic_epoch=2,
            executor_api="context_v1",
        )
        # This lightweight event models a mediated adapter that omitted the
        # duplicate artifact_id but retained the canonical authority URI.
        context.read(
            "vpg://requirements", version=1, content_hash=facts.read_hash("test", "requirements", 1)
        )
        snapshot = AgentSnapshot.from_attempt(
            _attempt(),
            execution_context=context,
            captured_at=datetime.now(UTC),
        )
        manifest = ContextManifest(
            manifest_id="vpg-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements",
                    canonical_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=facts.read_hash("test", "requirements", 1),
                    media_type="text/plain",
                ),
            ),
            token_budget=64,
        )
        decision, replacement = plan_automatic_rebase(
            task_id="task-1",
            graph_id="graph-1",
            graph_version=1,
            agent_snapshot=snapshot,
            context_manifest=manifest,
            facts=facts,
        )
        assert decision.redispatchable
        assert replacement is not None
    finally:
        facts.close()
