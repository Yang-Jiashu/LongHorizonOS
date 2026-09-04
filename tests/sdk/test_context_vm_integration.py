"""Context VM wiring at the public AgentOS execution boundary."""

from __future__ import annotations

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.provenance import ProvenanceOperation
from lhos.runtimes.multi_agent import AttemptState
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome


def test_attempt_materializes_explicit_manifest_and_binds_evidence_identity() -> None:
    seen: dict[str, object] = {}
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor_api="context_v1",
                executor=lambda ctx, task_id: seen.update(
                    {
                        "executor_task": task_id,
                        "executor_content": ctx.loaded_context.ordered_pages[0].content,
                        "executor_snapshot": ctx.context_snapshot_id,
                    }
                ),
            )
        )
        os_._facts.add_version("requirements", 1, "partial refund")
        digest = os_._facts.content_hash("partial refund")
        manifest = ContextManifest(
            manifest_id="requirements-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements",
                    canonical_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=digest,
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=100,
        )
        goal = Goal("context-goal")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            context_manifest=manifest,
            verify=lambda ctx: (
                seen.update({"verifier_snapshot": ctx.context_snapshot_id})
                or VerificationOutcome(
                    passed=True,
                    artifact_id="implementation",
                    version=1,
                    content="done",
                )
            ),
        )

        result = os_.run(goal, max_dispatches=1)

        assert result.goal_state == "closed"
        assert seen["executor_task"] == "implement"
        assert seen["executor_content"] == b"partial refund"
        assert seen["executor_snapshot"] == seen["verifier_snapshot"]

        attempt = os_.scheduler.attempts[0]
        assert attempt.context_snapshot_id == seen["executor_snapshot"]
        assert attempt.context_manifest_id == "requirements-manifest"
        assert attempt.context_materialized_hash

        nodes, _ = os_.vpg.snapshot_projection(os_._goal_gid["context-goal"])
        evidence = next(node for node in nodes.values() if node.node_type.value == "evidence")
        assert evidence.context_snapshot_id == attempt.context_snapshot_id
        assert evidence.metadata["context_snapshot"]["snapshot_id"] == attempt.context_snapshot_id
    finally:
        os_.close()


def test_context_vm_pages_form_strict_provenance_read_set_without_manual_ctx_read() -> None:
    """The Context VM working set is an actual input boundary.

    A context-aware executor should not have to duplicate every page binding
    with a hand-written ``ctx.read`` call.  The runtime records the exact
    version/hash bindings at snapshot bind time and strict coverage can use
    them directly.
    """

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor_api="context_v1",
                executor=lambda _ctx, _task_id: None,
            )
        )
        os_._facts.add_version("requirements", 1, "partial refund")
        digest = os_._facts.content_hash("partial refund")
        manifest = ContextManifest(
            manifest_id="strict-requirements-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements",
                    canonical_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=digest,
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=100,
        )
        goal = Goal("context-strict-goal")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            inputs=("vpg://requirements",),
            provenance_policy="strict",
            context_manifest=manifest,
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="done",
            ),
        )

        # Isolate the commit-time freshness fence; automatic fresh-attempt
        # repair is covered by the dedicated automatic-rebase tests.
        result = os_.run(goal, max_dispatches=1, automatic_rebase=False)

        assert result.goal_state == "closed"
        attempt = os_.scheduler.attempts[0]
        assert attempt.agent_snapshot is not None
        assert [binding.resource_uri for binding in attempt.agent_snapshot.read_set] == [
            "vpg://requirements"
        ]
        assert attempt.agent_snapshot.read_set[0].source == "context_vm"
        assert attempt.agent_snapshot.read_set[0].version == 1
    finally:
        os_.close()


def test_context_vm_read_binding_fences_stale_commit_before_evidence() -> None:
    """A world change after materialization quarantines the old cognition."""

    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version("requirements", 1, "partial refund")
        os_.add_agent(
            Agent(
                "worker",
                executor_api="context_v1",
                executor=lambda _ctx, _task_id: os_._facts.add_version(
                    "requirements", 2, "partial refund with tax"
                ),
            )
        )
        digest = os_._facts.content_hash("partial refund")
        manifest = ContextManifest(
            manifest_id="stale-requirements-manifest",
            owner_pid="template-owner",
            refs=(
                ContentRef(
                    ref_id="requirements",
                    canonical_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=digest,
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=100,
        )
        goal = Goal("context-stale-goal")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            context_manifest=manifest,
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="must-not-commit",
            ),
        )

        # Isolate the commit fence itself; automatic fresh-attempt repair is
        # covered separately in test_automatic_rebase.py.
        result = os_.run(goal, max_dispatches=1, automatic_rebase=False)

        assert result.goal_state == "open"
        assert result.task_states["implement"] == "unverified"
        attempt = os_.scheduler.attempts[0]
        assert attempt.state.value == "stale_cognition"
        nodes, _ = os_.vpg.snapshot_projection(os_._goal_gid[goal.goal_id])
        assert not any(node.node_type.value == "evidence" for node in nodes.values())
    finally:
        os_.close()


def test_secure_mode_unknown_read_is_quarantined_without_strict_task_policy() -> None:
    """Secure runtime policy must fail closed for hidden cognition inputs.

    ``CoveragePolicy.LEGACY`` remains the compatibility default for ordinary
    AgentOS instances, but secure mode is a stronger runtime boundary: an
    explicitly observed unknown read cannot receive a VERIFIED Evidence node
    merely because the task omitted ``provenance_policy="strict"``.
    """

    verifier_called = False

    def execute(ctx, _task_id):
        ctx.observe_unknown(
            op=ProvenanceOperation.READ,
            resource_hint="raw-python://hidden",
        )

    def verify(_ctx):
        nonlocal verifier_called
        verifier_called = True
        return VerificationOutcome(
            passed=True,
            artifact_id="implementation",
            version=1,
            content="must-not-commit",
        )

    os_ = AgentOS(":memory:", secure_mode=True)
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor_api="context_v1",
                executor=execute,
            )
        )
        goal = Goal("secure-unknown-read-goal")
        # Deliberately omit provenance_policy: secure_mode itself must be the
        # fail-closed boundary.
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            verify=verify,
        )

        result = os_.run(goal, max_dispatches=1)

        assert result.goal_state == "open"
        assert result.task_states["implement"] == "unverified"
        assert result.verified == []
        assert verifier_called is True
        attempt = os_.scheduler.attempts[0]
        assert attempt.state is AttemptState.STALE_COGNITION
        assert attempt.error is not None
        assert "READ_SET_UNAVAILABLE" in attempt.error
        assert any(
            event.event_type.value == "execution_stale_cognition"
            and "read_set_unavailable" in (event.reason or "")
            for event in os_.scheduler.events
        )
        nodes, _ = os_.vpg.snapshot_projection(os_._goal_gid[goal.goal_id])
        assert not any(node.node_type.value == "evidence" for node in nodes.values())
    finally:
        os_.close()
