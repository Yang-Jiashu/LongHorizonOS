"""Focused tests for the bounded Context delta/rebase planner."""

from __future__ import annotations

import hashlib

import pytest
from pydantic import ValidationError

from lhos.agent_os.context.models import ContentRef, ContextManifest, VersionBinding
from lhos.runtimes.multi_agent.models import ResourceBinding
from lhos.sdk.context_delta import (
    ContextBindingRef,
    ContextDelta,
    ContextGraphChange,
    ContextGraphDelta,
    ContextRebaseAction,
    RebasePlan,
    build_context_delta,
    plan_context_rebase,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _ref(
    ref_id: str,
    *,
    version: int = 1,
    required: bool = False,
    uri: str | None = None,
    artifact_id: str | None = None,
) -> ContentRef:
    return ContentRef(
        ref_id=ref_id,
        canonical_uri=uri or f"vpg://{ref_id}",
        artifact_id=artifact_id or ref_id,
        version=version,
        content_hash=_digest(f"{ref_id}@{version}"),
        media_type="text/plain",
        required=required,
    )


def _manifest(*refs: ContentRef) -> ContextManifest:
    return ContextManifest(
        manifest_id="old-context",
        owner_pid="agent-7",
        refs=refs,
        token_budget=1_000,
    )


def test_explicit_change_reloads_only_affected_ref_and_preserves_the_rest() -> None:
    manifest = _manifest(
        _ref("requirements", version=8, required=True),
        _ref("api", version=17, required=True),
        _ref("auth", version=5),
    )
    delta = ContextGraphDelta(
        changes=(
            ContextGraphChange(
                ref_id="api",
                artifact_id="api",
                old_version=17,
                new_version=18,
                new_content_hash=_digest("api@18"),
            ),
        ),
        coverage="complete",
    )

    plan = plan_context_rebase(
        182,
        183,
        old_context_manifest=manifest,
        graph_delta=delta,
    )

    assert plan.action is ContextRebaseAction.REBASE
    assert plan.reload_ref_ids == ("api",)
    assert plan.preserve_ref_ids == ("auth", "requirements")
    assert plan.required_ref_ids == ("api", "requirements")
    assert plan.context_delta.required_affected_ref_ids == ("api",)
    assert plan.context_delta.affected_ref_ids == ("api",)
    assert plan.context_delta.still_valid_ref_ids == ("auth", "requirements")
    assert len(plan.plan_hash) == len(plan.context_delta.delta_hash) == 64


def test_all_affected_bindings_require_full_reload() -> None:
    plan = plan_context_rebase(
        7,
        8,
        _manifest(_ref("api"), _ref("requirements")),
        {"changed_artifact_ids": ("api", "requirements"), "coverage": "complete"},
    )

    assert plan.action is ContextRebaseAction.FULL_RELOAD
    assert plan.preserve_ref_ids == ()
    assert plan.reload_ref_ids == ("api", "requirements")


def test_graph_version_change_without_declared_affected_identity_is_a_noop() -> None:
    plan = plan_context_rebase(
        10,
        11,
        _manifest(_ref("api"), _ref("auth")),
        ContextGraphDelta(coverage="complete"),
    )

    assert plan.action is ContextRebaseAction.REUSE
    assert plan.affected_ref_ids == ()
    assert plan.still_valid_ref_ids == ("api", "auth")


def test_unknown_delta_fails_closed_and_does_not_claim_reloads_are_sufficient() -> None:
    plan = plan_context_rebase(
        10,
        11,
        _manifest(_ref("api", required=True), _ref("auth")),
        ContextGraphDelta(known=False, coverage="unknown", reason="watcher gap"),
    )

    assert plan.action is ContextRebaseAction.BLOCKED
    assert plan.blocked is True
    assert plan.blocked_ref_ids == ("api", "auth")
    assert plan.reload_ref_ids == ()
    assert plan.context_delta.unknown_ref_ids == ("api", "auth")
    assert "graph_delta_unknown" in plan.context_delta.reasons


def test_unknown_old_read_binding_is_blocked_while_known_binding_is_reused() -> None:
    old_reads = (
        ResourceBinding(
            operation="read",
            resource_uri="workspace://src/payment.py",
            artifact_id="payment.py",
            version=17,
            content_hash=_digest("payment.py@17"),
            known=True,
        ),
        ResourceBinding(
            operation="read",
            resource_uri="raw-python://hidden",
            known=False,
        ),
    )

    plan = plan_context_rebase(
        20,
        21,
        old_read_bindings=old_reads,
        graph_delta={"coverage": "complete"},
    )

    assert plan.action is ContextRebaseAction.BLOCKED
    assert plan.preserve_ref_ids == ("workspace://src/payment.py",)
    assert plan.blocked_ref_ids == ("raw-python://hidden",)


def test_version_binding_like_inputs_and_explicit_required_ids_are_supported() -> None:
    bindings = (
        VersionBinding(
            page_id="api-page",
            canonical_uri="vpg://api",
            artifact_id="api",
            version=17,
            content_hash=_digest("api@17"),
        ),
        VersionBinding(
            page_id="auth-page",
            canonical_uri="vpg://auth",
            artifact_id="auth",
            version=5,
            content_hash=_digest("auth@5"),
        ),
    )

    plan = plan_context_rebase(
        3,
        4,
        bindings,
        {"changed_resources": ("vpg://api",), "coverage": "complete"},
        required_refs=("vpg://api",),
    )

    assert plan.reload_ref_ids == ("vpg://api",)
    assert plan.preserve_ref_ids == ("vpg://auth",)
    assert plan.required_ref_ids == ("vpg://api",)
    assert plan.context_delta.required_affected_ref_ids == ("vpg://api",)


def test_replanning_against_the_declared_new_identity_is_idempotent() -> None:
    manifest = _manifest(_ref("api", version=18))
    change = ContextGraphChange(
        artifact_id="api",
        old_version=17,
        new_version=18,
        old_content_hash=_digest("api@17"),
        new_content_hash=_digest("api@18"),
    )

    first = plan_context_rebase(18, 18, manifest, ContextGraphDelta(changes=(change,)))
    second = plan_context_rebase(18, 18, manifest, ContextGraphDelta(changes=(change,)))

    assert first.action is ContextRebaseAction.REUSE
    assert first.context_delta.is_noop is True
    assert first == second
    assert first.plan_hash == second.plan_hash


def test_planner_is_pure_deterministic_and_does_not_mutate_inputs() -> None:
    manifest = _manifest(_ref("z"), _ref("a", required=True))
    before = manifest.model_dump(mode="json")
    delta = {
        "changed_ref_ids": ["z"],
        "changed_resource_keys": ["vpg://z"],
        "coverage": "complete",
    }

    left = plan_context_rebase(1, 2, manifest, delta)
    right = plan_context_rebase(1, 2, manifest, delta)

    assert left == right
    assert left.plan_hash == right.plan_hash
    assert manifest.model_dump(mode="json") == before
    assert isinstance(left, RebasePlan)
    assert isinstance(left.context_delta, ContextDelta)
    with pytest.raises(ValidationError):
        left.action = ContextRebaseAction.REUSE  # type: ignore[misc]


def test_build_context_delta_returns_only_the_immutable_classification() -> None:
    delta = build_context_delta(
        1,
        2,
        (_ref("requirements", required=True),),
        {"changed_ref_ids": ("requirements",), "coverage": "complete"},
    )

    assert isinstance(delta, ContextDelta)
    assert delta.affected_ref_ids == ("requirements",)
    assert delta.required_ref_ids == ("requirements",)
    with pytest.raises(ValidationError):
        delta.reasons = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("old_version", "new_version", "error"),
    [
        (True, 2, TypeError),
        (1, False, TypeError),
        (-1, 0, ValueError),
        (4, 3, ValueError),
    ],
)
def test_invalid_graph_versions_fail_closed(
    old_version: int,
    new_version: int,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        plan_context_rebase(old_version, new_version)


def test_duplicate_ref_identity_and_ambiguous_sources_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate context binding"):
        plan_context_rebase(
            1,
            2,
            (
                ContextBindingRef(ref_id="api", canonical_uri="vpg://api", version=1),
                ContextBindingRef(ref_id="api", canonical_uri="vpg://api", version=2),
            ),
        )
    with pytest.raises(ValueError, match="provide only one"):
        plan_context_rebase(
            1,
            2,
            (_ref("api"),),
            old_read_bindings=(_ref("auth"),),
        )


def test_change_requires_an_explicit_target_and_rejects_version_rollback() -> None:
    with pytest.raises(ValidationError):
        ContextGraphChange()
    with pytest.raises(ValidationError):
        ContextGraphChange(artifact_id="api", old_version=18, new_version=17)


def test_context_delta_api_is_exported_from_public_sdk() -> None:
    import lhos.sdk as public_sdk

    assert public_sdk.CONTEXT_DELTA_SCHEMA_VERSION == "context-delta.v1"
    assert public_sdk.CONTEXT_REBASE_POLICY_ID == "explicit-context-rebase.v1"
    assert public_sdk.ContextBindingRef is ContextBindingRef
    assert public_sdk.ContextDelta is ContextDelta
    assert public_sdk.ContextGraphChange is ContextGraphChange
    assert public_sdk.ContextGraphDelta is ContextGraphDelta
    assert public_sdk.ContextRebaseAction is ContextRebaseAction
    assert public_sdk.GraphDelta is ContextGraphDelta
    assert public_sdk.RebasePlan is RebasePlan
    assert public_sdk.build_context_delta is build_context_delta
    assert public_sdk.plan_context_delta is not None
    assert public_sdk.plan_context_rebase is plan_context_rebase


def test_agentos_context_rebase_facade_is_read_only_and_matches_public_planner() -> None:
    from lhos.sdk import AgentOS

    os_ = AgentOS(":memory:")
    try:
        manifest = _manifest(
            _ref("api", version=17, required=True),
            _ref("auth", version=5),
        )
        graph_delta = ContextGraphDelta(
            changed_ref_ids=("api",),
            coverage="complete",
        )
        before = (
            tuple(os_.scheduler.claims),
            tuple(os_.scheduler.attempts),
            tuple(os_.scheduler.events),
            dict(os_._goal_gid),
        )

        facade_plan = os_.plan_context_rebase(
            17,
            18,
            old_context_manifest=manifest,
            graph_delta=graph_delta,
        )
        direct_plan = plan_context_rebase(
            17,
            18,
            old_context_manifest=manifest,
            graph_delta=graph_delta,
        )

        assert facade_plan == direct_plan
        assert facade_plan.action is ContextRebaseAction.REBASE
        assert facade_plan.reload_ref_ids == ("api",)
        assert facade_plan.preserve_ref_ids == ("auth",)
        assert (
            tuple(os_.scheduler.claims),
            tuple(os_.scheduler.attempts),
            tuple(os_.scheduler.events),
            dict(os_._goal_gid),
        ) == before
    finally:
        os_.close()
