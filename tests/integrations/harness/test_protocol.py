"""Harness-neutral phase protocol tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from lhos.integrations.harness import (
    HarnessExecutionBinding,
    HarnessPhaseEvent,
    HarnessPhaseKind,
    HarnessUsage,
)


def test_usage_preserves_provider_buckets_and_adds_monotonically() -> None:
    first = HarnessUsage(
        uncached_input_tokens=10,
        output_tokens=2,
        cache_read_tokens=20,
        model_calls=1,
    )
    second = HarnessUsage(
        uncached_input_tokens=3,
        output_tokens=1,
        cache_write_tokens=5,
        tool_calls=2,
        wall_time_ms=50,
    )

    total = first.plus(second)

    assert total.uncached_input_tokens == 13
    assert total.cache_read_tokens == 20
    assert total.cache_write_tokens == 5
    assert total.input_token_units == 38
    assert total.total_token_units == 41
    assert total.model_calls == 1
    assert total.tool_calls == 2
    assert total.wall_time_ms == 50


def test_binding_from_execution_context_captures_exact_attempt_identity() -> None:
    context = SimpleNamespace(
        graph_id="graph-1",
        graph_version=4,
        semantic_epoch=2,
        task_id="task-1",
        agent_id="agent-1",
        claim_id="claim-1",
        attempt_id="attempt-1",
        process_id="process-1",
        lease_id="lease-1",
        lease_fencing_token=9,
        context_snapshot_id="snapshot-1",
        context_manifest_hash="manifest-hash",
        context_working_set_hash="working-set-hash",
    )

    binding = HarnessExecutionBinding.from_execution_context(
        context,
        workspace_id="workspace-1",
    )

    assert binding.graph_version == 4
    assert binding.semantic_epoch == 2
    assert binding.claim_id == "claim-1"
    assert binding.attempt_id == "attempt-1"
    assert binding.lease_id == "lease-1"
    assert binding.lease_fencing_token == 9
    assert binding.context_snapshot_id == "snapshot-1"
    assert binding.context_manifest_hash == "manifest-hash"
    assert binding.working_set_hash == "working-set-hash"


def test_binding_rejects_partial_ownership_identity() -> None:
    context = SimpleNamespace(
        graph_id="graph-1",
        graph_version=1,
        semantic_epoch=0,
        task_id="task-1",
        agent_id="",
        claim_id="claim-1",
        attempt_id="attempt-1",
    )

    with pytest.raises(ValidationError, match="identity fields must be non-empty"):
        HarnessExecutionBinding.from_execution_context(
            context,
            workspace_id="workspace-1",
        )


def test_event_normalizes_access_sets_and_has_stable_fingerprint() -> None:
    binding = HarnessExecutionBinding(
        graph_id="graph-1",
        graph_version=1,
        semantic_epoch=0,
        task_id="task-1",
        agent_id="agent-1",
        claim_id="claim-1",
        attempt_id="attempt-1",
    )
    event = HarnessPhaseEvent(
        event_id="event-1",
        session_id="session-1",
        phase_seq=3,
        phase=HarnessPhaseKind.TOOL_CALL,
        binding=binding,
        idempotency_key="attempt-1:3:tool_call",
        read_set_delta=("workspace://b.py", "workspace://a.py", "workspace://a.py"),
    )

    assert event.read_set_delta == ("workspace://a.py", "workspace://b.py")
    assert event.fingerprint() == event.model_copy().fingerprint()
