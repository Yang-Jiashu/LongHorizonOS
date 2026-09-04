"""Public export and DTO-contract tests for bounded live Context rebase."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest

import lhos.sdk as sdk
from lhos.runtimes.multi_agent.models import (
    AgentSnapshot,
    AttemptState,
    ContextIdentity,
    ResourceBinding,
)
from lhos.sdk.context_delta import ContextGraphChange, ContextGraphDelta
from lhos.sdk.rebase_runtime import RebaseRuntimeBridge


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _live_plan() -> sdk.LiveContextRebasePlan:
    digest = _hash("ctx-1")
    agent = AgentSnapshot(
        agent_id="agent-backend",
        process_id="process-backend",
        task_id="backend",
        claim_id="claim-backend",
        attempt_id="attempt-backend",
        graph_id="graph-1",
        graph_version=1,
        semantic_epoch=0,
        context_identity=ContextIdentity(
            snapshot_id="ctx-1",
            manifest_id="manifest-1",
            manifest_hash=digest,
            working_set_hash=digest,
            materialized_hash=digest,
        ),
        read_set=(
            ResourceBinding(
                operation="read",
                resource_uri="vpg://api",
                artifact_id="api",
                version=1,
                content_hash=_hash("api@1"),
            ),
        ),
        started_at=datetime.now(UTC),
        state=AttemptState.RUNNING,
    )
    delta = ContextGraphDelta(
        graph_id="graph-1",
        changes=(
            ContextGraphChange(
                artifact_id="api",
                old_version=1,
                new_version=2,
                new_content_hash=_hash("api@2"),
            ),
        ),
        coverage="complete",
    )
    decision = RebaseRuntimeBridge().plan(
        agent,
        graph_delta=delta,
        new_graph_version=2,
    )
    return sdk.LiveContextRebasePlan(
        graph_id="graph-1",
        graph_version=2,
        source_graph_version=1,
        target_semantic_epoch=1,
        task_id=agent.task_id,
        agent_id=agent.agent_id,
        process_id=agent.process_id,
        claim_id=agent.claim_id,
        attempt_id=agent.attempt_id,
        source_semantic_epoch=agent.semantic_epoch,
        context_snapshot_id="ctx-1",
        context_snapshot_hash=digest,
        agent_snapshot_fingerprint=agent.fingerprint(),
        harness_session_id="session-backend",
        harness_revision=0,
        decision=decision,
        graph_delta_hash=decision.plan.context_delta.delta_hash,
        plan_hash=decision.plan.plan_hash,
    )


def test_live_rebase_dtos_are_public_sdk_exports() -> None:
    assert sdk.LiveContextRebasePlan is not None
    assert sdk.LiveContextRebaseApplyResult is not None
    assert "LiveContextRebasePlan" in sdk.__all__
    assert "LiveContextRebaseApplyResult" in sdk.__all__
    assert sdk.LiveContextRebasePlan.__module__ == "lhos.sdk.rebase_runtime"
    assert sdk.LiveContextRebaseApplyResult.__module__ == "lhos.sdk.rebase_runtime"


def test_live_rebase_dtos_round_trip_and_remain_frozen() -> None:
    plan = _live_plan()
    result = sdk.LiveContextRebaseApplyResult(
        plan=plan,
        refused=True,
        reason="REBASE requires an atomic ownership handoff",
    )

    assert plan.schema_version == sdk.REBASE_RUNTIME_SCHEMA_VERSION
    assert result.schema_version == sdk.REBASE_RUNTIME_SCHEMA_VERSION
    assert result.plan.plan_hash == plan.plan_hash
    restored = sdk.LiveContextRebaseApplyResult.model_validate(result.model_dump(mode="json"))
    assert restored == result

    with pytest.raises((TypeError, ValueError)):
        plan.graph_version = 3  # type: ignore[misc]
