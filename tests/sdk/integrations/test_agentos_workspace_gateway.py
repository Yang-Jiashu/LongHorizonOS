"""AgentOS composition-root bridge for authority-backed workspace provenance."""

from __future__ import annotations

import hashlib

import pytest

from lhos.integrations import (
    WorkspaceAccessDenied,
    WorkspaceTool,
    WorkspaceVersionValidationError,
)
from lhos.runtimes.multi_agent import AttemptState
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    Task,
    VerificationOutcome,
    create_execution_context,
)


def _context(*, secure: bool = True):
    return create_execution_context(
        "graph-agentos-workspace",
        "task-agentos-workspace",
        claim_id="claim-agentos-workspace",
        attempt_id="attempt-agentos-workspace",
        semantic_epoch=1,
        executor_api="context_v1",
        secure_mode=secure,
    )


def test_agentos_workspace_gateway_injects_facts_authority(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", b"authoritative bytes").ok
    expected = hashlib.sha256(b"authoritative bytes").hexdigest()

    os_ = AgentOS(":memory:")
    try:
        os_.register_workspace_artifact(workspace, "input.txt", 4)
        gateway = os_.workspace_gateway(
            workspace,
            _context(),
            readable=("workspace://input.txt",),
            strict=True,
        )

        assert gateway.read_bytes("input.txt", version=4) == b"authoritative bytes"
        event = gateway.context.events[-1]
        assert event.metadata["version_source"] == "authority"
        assert event.content_hash == expected
    finally:
        os_.close()


def test_agentos_workspace_gateway_does_not_auto_register_unseen_versions(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", b"unregistered").ok

    os_ = AgentOS(":memory:")
    try:
        gateway = os_.workspace_gateway(
            workspace,
            _context(),
            readable=("workspace://input.txt",),
            strict=True,
        )
        with pytest.raises(
            WorkspaceVersionValidationError,
            match="no registered binding",
        ):
            gateway.read_bytes("input.txt", version=9)

        assert os_.workspace_latest_version(workspace, "input.txt") == 0
        assert gateway.read_set == ()
    finally:
        os_.close()


def test_agentos_workspace_gateway_preserves_explicit_compatibility_mode(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", b"compatibility").ok

    os_ = AgentOS(":memory:")
    try:
        gateway = os_.workspace_gateway(
            workspace,
            _context(secure=False),
            readable=("workspace://input.txt",),
            strict=False,
        )
        snapshot = gateway.snapshot("input.txt", version=99)
        assert snapshot.version == 99
        assert gateway.context.events[-1].metadata["version_source"] == "caller"
        assert os_.workspace_latest_version(workspace, "input.txt") == 0
    finally:
        os_.close()


def test_agentos_workspace_gateway_can_derive_task_capabilities(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", b"task input").ok
    os_ = AgentOS(":memory:")
    try:
        os_.register_workspace_artifact(workspace, "input.txt", 2)
        task = Task(
            "task-agentos-workspace",
            inputs=("workspace://input.txt",),
            outputs=("workspace://output.txt",),
            executor_api="context_v1",
        )
        gateway = os_.workspace_gateway(workspace, _context(), task, strict=True)

        assert gateway.read_bytes("input.txt", version=2) == b"task input"
        with pytest.raises(WorkspaceAccessDenied, match="undeclared"):
            gateway.write_text("other.txt", "not declared")
    finally:
        os_.close()


def test_agentos_workspace_gateway_rejects_mixing_task_and_explicit_capabilities(
    tmp_path,
) -> None:
    os_ = AgentOS(":memory:")
    try:
        task = Task("task-agentos-workspace", inputs=("workspace://input.txt",))
        with pytest.raises(
            ConfigurationError,
            match="either task capabilities or explicit",
        ):
            os_.workspace_gateway(
                WorkspaceTool(tmp_path),
                _context(),
                task,
                readable=("workspace://input.txt",),
            )
    finally:
        os_.close()


def test_agentos_commit_revalidates_mediated_workspace_read_set(tmp_path) -> None:
    """Direct byte changes after a mediated read cannot become VERIFIED."""

    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    os_ = AgentOS(":memory:")
    try:
        os_.register_workspace_artifact(workspace, "input.txt", version=1)

        def execute(context, _task_id):
            gateway = os_.workspace_gateway(
                workspace,
                context,
                readable=("workspace://input.txt",),
                strict=True,
            )
            assert gateway.read_text("input.txt", version=1) == "v1"
            # Bypass the gateway to model an external/process-local mutation
            # before semantic commit. Facts still says v1, so this specifically
            # exercises the gateway's point-in-time byte revalidation.
            assert workspace.write("input.txt", "v2").ok

        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
            )
        )
        goal = Goal("workspace-commit-freshness")
        goal.task(
            "task-agentos-workspace",
            agent="worker",
            inputs=("workspace://input.txt",),
            executor_api="context_v1",
            provenance_policy="strict",
            verify=lambda _context: VerificationOutcome(
                passed=True,
                artifact_id="output",
                version=1,
                content="must-not-commit",
            ),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            automatic_rebase=False,
        )

        assert result.goal_state == "open"
        assert result.task_states["task-agentos-workspace"] == "unverified"
        assert os_._facts.latest("output") is None
        attempt = os_.scheduler.attempts[0]
        assert attempt.state is AttemptState.STALE_COGNITION
        assert "workspace read-set changed" in (attempt.error or "")
    finally:
        os_.close()
