"""Bounded main-path automatic stale-cognition repair tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.runtimes.multi_agent import AgentSnapshot, AttemptState, ResourceBinding
from lhos.runtimes.verified_progress.errors import VPGCode, VPGError
from lhos.runtimes.verified_progress.models import EvidenceNode
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.automatic_rebase import plan_automatic_rebase
from lhos.sdk.errors import ConfigurationError


def _manifest(os_: AgentOS, *, version: int, manifest_id: str) -> ContextManifest:
    content = "requirements-v1" if version == 1 else "requirements-v2"
    return ContextManifest(
        manifest_id=manifest_id,
        owner_pid="template-owner",
        refs=(
            ContentRef(
                ref_id="requirements",
                canonical_uri="vpg://requirements",
                artifact_id="requirements",
                version=version,
                content_hash=os_._facts.content_hash(content),
                media_type="text/plain",
                required=True,
            ),
        ),
        token_budget=128,
    )


async def test_run_async_reopens_stale_cognition_with_fresh_context_attempt() -> None:
    os_ = AgentOS(":memory:")
    calls: list[dict[str, object]] = []
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        manifest = _manifest(os_, version=1, manifest_id="requirements-v1-manifest")

        async def execute(ctx, task_id: str) -> None:
            calls.append(
                {
                    "task_id": task_id,
                    "attempt_id": ctx.attempt_id,
                    "snapshot_id": ctx.context_snapshot_id,
                    "requirements": ctx.loaded_context.ordered_pages[0].content,
                    "rebase_action": getattr(ctx, "automatic_rebase_action", None),
                    "delta": tuple(getattr(ctx, "automatic_rebase_delta_ref_ids", ())),
                }
            )
            if len(calls) == 1:
                os_._facts.add_version("requirements", 2, "requirements-v2")

        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
                max_concurrency=1,
            )
        )
        goal = Goal("automatic-rebase-goal")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            context_manifest=manifest,
            inputs=("vpg://requirements",),
            provenance_policy="strict",
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="implementation-v1",
            ),
        )

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
            automatic_rebase=True,
            max_automatic_rebase_dispatches=1,
        )

        assert result.goal_state == "closed"
        assert result.verified == ["implement"]
        assert result.meta["regular_dispatches"] == 1
        assert result.meta["automatic_rebase_dispatches"] == 1
        assert result.meta["automatic_rebase_pending"] == ()
        records = result.meta["automatic_rebase_records"]
        assert len(records) == 1
        assert records[0]["status"] == "planned"
        assert records[0]["action"] in {"rebase", "full_reload"}

        assert len(calls) == 2
        assert calls[0]["requirements"] == b"requirements-v1"
        assert calls[1]["requirements"] == b"requirements-v2"
        assert calls[1]["rebase_action"] in {"rebase", "full_reload"}
        assert calls[1]["delta"] == ("requirements",)
        assert calls[0]["attempt_id"] != calls[1]["attempt_id"]
        assert calls[0]["snapshot_id"] != calls[1]["snapshot_id"]

        attempts = os_.scheduler.attempts
        assert len(attempts) == 2
        assert attempts[0].state is AttemptState.STALE_COGNITION
        assert attempts[1].state is AttemptState.VERIFIED_SEMANTICALLY
        assert attempts[0].context_snapshot_id != attempts[1].context_snapshot_id
        assert attempts[1].agent_snapshot is not None
        assert attempts[1].agent_snapshot.read_set[0].version == 2

        nodes, _ = os_.vpg.snapshot_projection(os_._goal_gid[goal.goal_id])
        evidence = [node for node in nodes.values() if isinstance(node, EvidenceNode)]
        assert len(evidence) == 1
        assert evidence[0].attempt_id == attempts[1].attempt_id
    finally:
        os_.close()


def test_run_reopens_stale_cognition_with_fresh_context_attempt() -> None:
    """The synchronous facade exposes the same bounded repair contract."""

    os_ = AgentOS(":memory:")
    calls: list[dict[str, object]] = []
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        manifest = _manifest(os_, version=1, manifest_id="requirements-v1-sync-manifest")

        def execute(ctx, task_id: str) -> None:
            calls.append(
                {
                    "task_id": task_id,
                    "attempt_id": ctx.attempt_id,
                    "snapshot_id": ctx.context_snapshot_id,
                    "requirements": ctx.loaded_context.ordered_pages[0].content,
                    "rebase_action": getattr(ctx, "automatic_rebase_action", None),
                    "delta": tuple(getattr(ctx, "automatic_rebase_delta_ref_ids", ())),
                }
            )
            if len(calls) == 1:
                os_._facts.add_version("requirements", 2, "requirements-v2")

        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
                max_concurrency=1,
            )
        )
        goal = Goal("automatic-rebase-sync-goal")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            context_manifest=manifest,
            inputs=("vpg://requirements",),
            provenance_policy="strict",
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="implementation-v1",
            ),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            automatic_rebase=True,
            max_automatic_rebase_dispatches=1,
        )

        assert result.goal_state == "closed"
        assert result.verified == ["implement"]
        assert result.meta["regular_dispatches"] == 1
        assert result.meta["automatic_rebase_dispatches"] == 1
        assert result.meta["automatic_rebase_pending"] == ()
        assert len(result.meta["automatic_rebase_records"]) == 1
        assert len(calls) == 2
        assert calls[0]["requirements"] == b"requirements-v1"
        assert calls[1]["requirements"] == b"requirements-v2"
        assert calls[1]["rebase_action"] in {"rebase", "full_reload"}
        assert calls[1]["delta"] == ("requirements",)
        assert calls[0]["attempt_id"] != calls[1]["attempt_id"]
        assert calls[0]["snapshot_id"] != calls[1]["snapshot_id"]

        attempts = os_.scheduler.attempts
        assert len(attempts) == 2
        assert attempts[0].state is AttemptState.STALE_COGNITION
        assert attempts[1].state is AttemptState.VERIFIED_SEMANTICALLY
        assert attempts[0].context_snapshot_id != attempts[1].context_snapshot_id
    finally:
        os_.close()


def test_sync_automatic_rebase_budget_blocks_a_second_stale_retry() -> None:
    """A repair Attempt cannot enqueue another retry past the explicit budget."""

    os_ = AgentOS(":memory:")
    calls: list[bytes] = []
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        manifest = _manifest(os_, version=1, manifest_id="requirements-v1-manifest")

        def execute(ctx, _task_id: str) -> None:
            calls.append(ctx.loaded_context.ordered_pages[0].content)
            next_version = len(calls) + 1
            os_._facts.add_version(
                "requirements",
                next_version,
                f"requirements-v{next_version}",
            )

        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
                max_concurrency=1,
            )
        )
        goal = Goal("sync-automatic-rebase-budget")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            context_manifest=manifest,
            inputs=("vpg://requirements",),
            provenance_policy="strict",
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="implementation-v1",
            ),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            automatic_rebase=True,
            max_automatic_rebase_dispatches=1,
        )

        assert result.goal_state == "open"
        assert calls == [b"requirements-v1", b"requirements-v2"]
        assert result.meta["regular_dispatches"] == 1
        assert result.meta["automatic_rebase_dispatches"] == 1
        assert result.meta["automatic_rebase_pending"] == ()
        assert any("automatic_rebase_budget_exhausted" in item for item in result.failures)
        assert [attempt.state for attempt in os_.scheduler.attempts] == [
            AttemptState.STALE_COGNITION,
            AttemptState.STALE_COGNITION,
        ]
    finally:
        os_.close()


def test_sync_automatic_rebase_fails_closed_without_explicit_manifest() -> None:
    os_ = AgentOS(":memory:", secure_mode=True)
    calls = 0
    try:

        def execute(ctx, _task_id: str) -> None:
            nonlocal calls
            calls += 1
            ctx.observe_unknown(resource_hint="raw-python://hidden")

        os_.add_agent(Agent("worker", executor=execute, executor_api="context_v1"))
        goal = Goal("automatic-rebase-sync-blocked")
        goal.task(
            "implement",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="implementation",
                version=1,
                content="implementation-v1",
            ),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            automatic_rebase=True,
            max_automatic_rebase_dispatches=1,
        )

        assert result.goal_state == "open"
        assert result.verified == []
        assert calls == 1
        assert result.meta["automatic_rebase_dispatches"] == 0
        assert result.meta["automatic_rebase_pending"] == ()
        assert any("automatic_rebase_blocked" in item for item in result.failures)
        records = result.meta["automatic_rebase_records"]
        assert len(records) == 1
        assert records[0]["status"] == "blocked"
    finally:
        os_.close()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"max_dispatches": -1}, "max_dispatches and max_steps must be >= 0"),
        ({"max_steps": -1}, "max_dispatches and max_steps must be >= 0"),
        ({"max_dispatches": True}, "max_dispatches must be an integer"),
        ({"max_steps": "1"}, "max_steps must be an integer"),
    ),
)
def test_sync_run_rejects_invalid_dispatch_bounds(
    kwargs: dict[str, object],
    message: str,
) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("invalid-sync-bounds")
        with pytest.raises(ConfigurationError, match=message):
            os_.run(goal, **kwargs)
    finally:
        os_.close()


async def test_automatic_rebase_fails_closed_without_explicit_manifest() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        os_._facts.add_version("requirements", 2, "requirements-v2")
        snapshot = AgentSnapshot(
            agent_id="worker",
            process_id="process-worker",
            task_id="implement",
            claim_id="claim-old",
            attempt_id="attempt-old",
            graph_id="graph",
            graph_version=1,
            semantic_epoch=1,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=os_._facts.content_hash("requirements-v1"),
                    source="context_vm",
                ),
            ),
            started_at=datetime.now(UTC),
            state=AttemptState.STALE_COGNITION,
        )
        decision, replacement = plan_automatic_rebase(
            task_id="implement",
            graph_id="graph",
            graph_version=2,
            agent_snapshot=snapshot,
            context_manifest=None,
            facts=os_._facts,
            claim_id="claim-old",
            attempt_id="attempt-old",
        )

        assert replacement is None
        assert decision.blocked
        assert decision.status.value == "blocked"
        assert "ContextManifest" in decision.reason
    finally:
        os_.close()


def test_automatic_rebase_derives_vpg_artifact_from_uri_only_binding() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        os_._facts.add_version("requirements", 2, "requirements-v2")
        snapshot = AgentSnapshot(
            agent_id="worker",
            process_id="process-worker",
            task_id="implement",
            claim_id="claim-old",
            attempt_id="attempt-old",
            graph_id="graph",
            graph_version=1,
            semantic_epoch=1,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://requirements",
                    artifact_id=None,
                    version=1,
                    content_hash=os_._facts.content_hash("requirements-v1"),
                    source="context_vm",
                ),
            ),
            started_at=datetime.now(UTC),
            state=AttemptState.STALE_COGNITION,
        )
        decision, replacement = plan_automatic_rebase(
            task_id="implement",
            graph_id="graph",
            graph_version=2,
            agent_snapshot=snapshot,
            context_manifest=_manifest(os_, version=1, manifest_id="requirements-v1-manifest"),
            facts=os_._facts,
            claim_id="claim-old",
            attempt_id="attempt-old",
        )

        assert decision.redispatchable
        assert replacement is not None
        assert decision.changed_refs[0].artifact_id == "requirements"
    finally:
        os_.close()


def test_automatic_rebase_decision_hash_is_independent_of_manifest_ref_order() -> None:
    os_ = AgentOS(":memory:")
    try:
        for artifact, content in (
            ("api", "api-v1"),
            ("api", "api-v2"),
            ("requirements", "requirements-v1"),
            ("requirements", "requirements-v2"),
        ):
            version = 1 if content.endswith("v1") else 2
            os_._facts.add_version(artifact, version, content)

        def manifest(ref_order: tuple[str, ...]) -> ContextManifest:
            return ContextManifest(
                manifest_id="stable-manifest",
                owner_pid="template-owner",
                refs=tuple(
                    ContentRef(
                        ref_id=ref_id,
                        canonical_uri=f"vpg://{ref_id}",
                        artifact_id=ref_id,
                        version=1,
                        content_hash=os_._facts.content_hash(f"{ref_id}-v1"),
                        media_type="text/plain",
                    )
                    for ref_id in ref_order
                ),
                token_budget=256,
            )

        snapshot = AgentSnapshot(
            agent_id="worker",
            process_id="process-worker",
            task_id="implement",
            claim_id="claim-old",
            attempt_id="attempt-old",
            graph_id="graph",
            graph_version=1,
            semantic_epoch=1,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://api",
                    artifact_id="api",
                    version=1,
                    content_hash=os_._facts.content_hash("api-v1"),
                    source="context_vm",
                ),
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=1,
                    content_hash=os_._facts.content_hash("requirements-v1"),
                    source="context_vm",
                ),
            ),
            started_at=datetime.now(UTC),
            state=AttemptState.STALE_COGNITION,
        )

        first, first_manifest = plan_automatic_rebase(
            task_id="implement",
            graph_id="graph",
            graph_version=1,
            agent_snapshot=snapshot,
            context_manifest=manifest(("api", "requirements")),
            facts=os_._facts,
            claim_id="claim-old",
            attempt_id="attempt-old",
        )
        second, second_manifest = plan_automatic_rebase(
            task_id="implement",
            graph_id="graph",
            graph_version=1,
            agent_snapshot=snapshot,
            context_manifest=manifest(("requirements", "api")),
            facts=os_._facts,
            claim_id="claim-old",
            attempt_id="attempt-old",
        )

        assert first.redispatchable and second.redispatchable
        assert first.decision_hash == second.decision_hash
        assert first.changed_refs == second.changed_refs
        assert first.replacement_manifest_id == second.replacement_manifest_id
        assert first_manifest is not None and second_manifest is not None
        assert first_manifest.manifest_hash() == second_manifest.manifest_hash()
    finally:
        os_.close()


def test_commit_read_guard_derives_vpg_artifact_from_uri_only_binding() -> None:
    """URI-only VPG provenance must still receive a freshness guard."""

    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version("requirements", 1, "requirements-v1")
        snapshot = AgentSnapshot(
            agent_id="worker",
            process_id="process-worker",
            task_id="implement",
            claim_id="claim-old",
            attempt_id="attempt-old",
            graph_id="graph",
            graph_version=1,
            semantic_epoch=1,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://requirements",
                    artifact_id=None,
                    version=1,
                    content_hash=os_._facts.content_hash("requirements-v1"),
                    source="provenance",
                ),
            ),
            started_at=datetime.now(UTC),
            state=AttemptState.VERIFIED_SEMANTICALLY,
        )

        guards = os_._prepare_read_guards(
            task=type("StrictTask", (), {"provenance_policy": "strict"})(),
            snapshot=snapshot,
        )

        assert len(guards) == 1
        assert guards[0].artifact_id == "requirements"
        assert guards[0].canonical_uri == "vpg://requirements"
        os_._preflight_read_guards(guards)

        os_._facts.add_version("requirements", 2, "requirements-v2")
        with pytest.raises(VPGError) as exc_info:
            os_._preflight_read_guards(guards)
        assert exc_info.value.code is VPGCode.STALE_COGNITION
    finally:
        os_.close()


def test_commit_read_guard_does_not_guess_arbitrary_uri_identity() -> None:
    """Strict cognition must fail closed for non-authoritative URI schemes."""

    os_ = AgentOS(":memory:")
    try:
        snapshot = AgentSnapshot(
            agent_id="worker",
            process_id="process-worker",
            task_id="implement",
            claim_id="claim-old",
            attempt_id="attempt-old",
            graph_id="graph",
            graph_version=1,
            semantic_epoch=1,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="https://example.test/requirements",
                    artifact_id=None,
                    version=1,
                    content_hash="a" * 64,
                    source="provenance",
                ),
            ),
            started_at=datetime.now(UTC),
            state=AttemptState.VERIFIED_SEMANTICALLY,
        )

        with pytest.raises(VPGError) as exc_info:
            os_._prepare_read_guards(
                task=type("StrictTask", (), {"provenance_policy": "strict"})(),
                snapshot=snapshot,
            )
        assert exc_info.value.code is VPGCode.READ_SET_UNAVAILABLE
    finally:
        os_.close()
