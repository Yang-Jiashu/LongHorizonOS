"""Focused AgentSnapshot and AttemptManager audit tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from lhos.provenance import ProvenanceEvent, ProvenanceOperation
from lhos.runtimes.multi_agent.attempts import AttemptManager
from lhos.runtimes.multi_agent.models import (
    AgentSnapshot,
    AttemptState,
    ComputationCost,
    ContextIdentity,
    ResourceBinding,
    ScheduledExecutionAttempt,
)

NOW = datetime(2026, 8, 13, 1, 0, tzinfo=UTC)


def _attempt(*, with_context: bool = True) -> ScheduledExecutionAttempt:
    context = (
        {
            "context_snapshot_id": "ctx-1",
            "context_manifest_id": "manifest-1",
            "context_manifest_hash": "A" * 64,
            "context_working_set_hash": "B" * 64,
            "context_materialized_hash": "C" * 64,
        }
        if with_context
        else {}
    )
    return ScheduledExecutionAttempt(
        attempt_id="attempt-1",
        graph_id="graph-1",
        graph_version=7,
        semantic_epoch=3,
        task_id="task-1",
        claim_id="claim-1",
        agent_id="agent-1",
        process_id="process-1",
        state=AttemptState.RUNNING,
        started_at=NOW,
        **context,
    )


def _snapshot(
    attempt: ScheduledExecutionAttempt,
    *,
    captured_delta: int = 1,
    progress: float = 0.25,
    reads: tuple[ResourceBinding, ...] = (),
    writes: tuple[ResourceBinding, ...] = (),
    cost: ComputationCost | None = None,
) -> AgentSnapshot:
    return AgentSnapshot.from_attempt(
        attempt,
        progress=progress,
        provenance_events=(),
        captured_at=NOW + timedelta(seconds=captured_delta),
        cost=cost or ComputationCost(input_tokens=10, elapsed_ms=100),
    ).model_copy(update={"read_set": reads, "write_set": writes})


def test_resource_sets_are_frozen_deduplicated_and_deterministic():
    read_b = ResourceBinding(
        operation="read",
        resource_uri="workspace://b.py",
        content_hash="B" * 64,
    )
    read_a = ResourceBinding(
        operation="read",
        resource_uri="workspace://a.py",
        content_hash="A" * 64,
    )
    snapshot = AgentSnapshot.from_attempt(
        _attempt(),
        provenance_events=(),
        captured_at=NOW + timedelta(seconds=1),
    ).model_copy(update={"read_set": (read_b, read_a, read_b)})
    # model_copy does not revalidate, so exercise wire round-trip normalization.
    snapshot = AgentSnapshot.model_validate(snapshot.model_dump())

    assert snapshot.read_set == (read_a, read_b)
    assert hash(snapshot.read_set[0])
    with pytest.raises(ValidationError):
        ResourceBinding(operation="read")


def test_snapshot_builds_from_provenance_and_context_bindings():
    attempt = _attempt()
    events = (
        ProvenanceEvent(
            event_id="read-event",
            graph_id="graph-1",
            task_id="task-1",
            attempt_id="attempt-1",
            semantic_epoch=3,
            op=ProvenanceOperation.READ,
            resource_uri="workspace://input.py",
            content_hash="1" * 64,
            observed_at=NOW,
        ),
        ProvenanceEvent(
            event_id="write-event",
            graph_id="graph-1",
            task_id="task-1",
            attempt_id="attempt-1",
            semantic_epoch=3,
            op=ProvenanceOperation.WRITE,
            resource_uri="workspace://output.py",
            content_hash="2" * 64,
            observed_at=NOW,
        ),
    )
    context = SimpleNamespace(
        context_snapshot_id="ctx-1",
        context_manifest_id="manifest-1",
        context_manifest_hash="A" * 64,
        context_working_set_hash="B" * 64,
        context_materialized_hash="C" * 64,
        events=events,
        loaded_context=SimpleNamespace(
            version_bindings=(
                SimpleNamespace(
                    canonical_uri="artifact://requirements",
                    artifact_id="requirements",
                    version=8,
                    content_hash="3" * 64,
                ),
            )
        ),
    )

    snapshot = AgentSnapshot.from_attempt(
        attempt,
        execution_context=context,
        progress=0.5,
        cost={"input_tokens": 100, "output_tokens": 20, "elapsed_ms": 250},
        captured_at=NOW + timedelta(seconds=1),
    )

    assert snapshot.context_identity is not None
    assert snapshot.context_identity.snapshot_id == "ctx-1"
    assert [item.resource_uri for item in snapshot.read_set] == [
        "artifact://requirements",
        "workspace://input.py",
    ]
    assert [item.resource_uri for item in snapshot.write_set] == ["workspace://output.py"]
    assert snapshot.cost.total_tokens == 120


def test_unknown_hidden_read_binding_survives_snapshot_round_trip() -> None:
    """An unidentifiable read must remain explicit after durable replay.

    ``AgentSnapshot`` is an audit record, not a hidden-read detector.  When an
    adapter does report an unknown/hidden interaction, the ``known=False``
    binding must not be dropped, normalized into a fake URI, or silently
    promoted to a known dependency during JSON replay.
    """

    hidden = ProvenanceEvent(
        event_id="hidden-read",
        graph_id="graph-1",
        task_id="task-1",
        attempt_id="attempt-1",
        semantic_epoch=3,
        op=ProvenanceOperation.EXTERNAL,
        resource_uri="",
        known=False,
        metadata={"hidden": True, "channel": "raw-python"},
        observed_at=NOW,
    )
    snapshot = AgentSnapshot.from_attempt(
        _attempt(),
        provenance_events=(hidden,),
        captured_at=NOW + timedelta(seconds=1),
    )

    assert len(snapshot.read_set) == 1
    binding = snapshot.read_set[0]
    assert binding.operation == "external"
    assert binding.known is False
    assert binding.resource_uri == ""
    assert binding.source_event_id == "hidden-read"

    reopened_attempt = ScheduledExecutionAttempt.model_validate(
        _attempt().model_copy(update={"agent_snapshot": snapshot}).model_dump(mode="json")
    )
    assert reopened_attempt.agent_snapshot is not None
    restored = reopened_attempt.agent_snapshot.read_set[0]
    assert restored.known is False
    assert restored.source_event_id == "hidden-read"
    assert restored.resource_uri == ""


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("graph_id", "graph-other", "graph_id"),
        ("task_id", "task-other", "task_id"),
        ("attempt_id", "attempt-other", "attempt_id"),
        ("semantic_epoch", 4, "semantic_epoch"),
    ),
)
def test_snapshot_rejects_cross_attempt_provenance_identity(
    field: str,
    value: object,
    message: str,
) -> None:
    """A foreign provenance event must never become this attempt's read-set."""

    event_values: dict[str, object] = {
        "event_id": "foreign-event",
        "graph_id": "graph-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "semantic_epoch": 3,
        "op": ProvenanceOperation.READ,
        "resource_uri": "workspace://foreign-input",
        "content_hash": "f" * 64,
        "observed_at": NOW,
    }
    event_values[field] = value
    foreign = ProvenanceEvent(**event_values)

    with pytest.raises(ValueError, match=message):
        AgentSnapshot.from_attempt(
            _attempt(),
            provenance_events=(foreign,),
            captured_at=NOW + timedelta(seconds=1),
        )


def test_snapshot_accepts_legacy_event_without_optional_identity_fields() -> None:
    """Low-level compatibility adapters may omit optional event identities."""

    legacy_event = SimpleNamespace(
        op=ProvenanceOperation.READ,
        resource_uri="workspace://legacy-input",
        artifact_id=None,
        version=None,
        content_hash=None,
        action_id=None,
        idempotency_key=None,
        event_id="legacy-event",
        source="legacy",
        known=True,
        observed_at=NOW,
    )
    snapshot = AgentSnapshot.from_attempt(
        _attempt(),
        provenance_events=(legacy_event,),
        captured_at=NOW + timedelta(seconds=1),
    )

    assert snapshot.read_set[0].resource_uri == "workspace://legacy-input"


def test_bind_is_idempotent_and_rejects_cross_attempt_identity():
    manager = AttemptManager()
    attempt = _attempt()
    manager._book(attempt)
    snapshot = _snapshot(attempt)

    assert manager.bind_agent_snapshot(attempt, snapshot)
    assert manager.bind_agent_snapshot(attempt, snapshot)

    wrong = snapshot.model_copy(update={"claim_id": "claim-other"})
    assert not manager.bind_agent_snapshot(attempt, wrong)
    assert attempt.agent_snapshot == snapshot


def test_update_requires_fingerprint_and_monotonic_progress_cost_and_sets():
    manager = AttemptManager()
    attempt = _attempt()
    read = ResourceBinding(operation="read", resource_uri="workspace://input.py")
    write = ResourceBinding(operation="write", resource_uri="workspace://output.py")
    first = _snapshot(attempt, reads=(read,))
    assert manager.bind_agent_snapshot(attempt, first)

    advanced = _snapshot(
        attempt,
        captured_delta=2,
        progress=0.75,
        reads=(read,),
        writes=(write,),
        cost=ComputationCost(input_tokens=20, output_tokens=5, elapsed_ms=200),
    )
    assert not manager.update_agent_snapshot(
        attempt,
        advanced,
        expected_fingerprint="0" * 64,
    )
    assert manager.update_agent_snapshot(
        attempt,
        advanced,
        expected_fingerprint=first.fingerprint(),
    )

    lost_read = _snapshot(
        attempt,
        captured_delta=3,
        progress=0.8,
        writes=(write,),
        cost=ComputationCost(input_tokens=30, output_tokens=5, elapsed_ms=300),
    )
    assert not manager.update_agent_snapshot(
        attempt,
        lost_read,
        expected_fingerprint=advanced.fingerprint(),
    )

    regressed_cost = _snapshot(
        attempt,
        captured_delta=3,
        progress=0.8,
        reads=(read,),
        writes=(write,),
        cost=ComputationCost(input_tokens=19, output_tokens=5, elapsed_ms=300),
    )
    assert not manager.update_agent_snapshot(
        attempt,
        regressed_cost,
        expected_fingerprint=advanced.fingerprint(),
    )
    assert attempt.agent_snapshot == advanced


def test_attempt_snapshot_is_backward_compatible_and_durable_json_round_trips():
    legacy = _attempt(with_context=False)
    payload = legacy.model_dump(mode="json", exclude={"agent_snapshot"})
    reopened = ScheduledExecutionAttempt.model_validate(payload)
    assert reopened.agent_snapshot is None

    snapshot = _snapshot(legacy)
    legacy.agent_snapshot = snapshot
    reopened = ScheduledExecutionAttempt.model_validate(legacy.model_dump(mode="json"))
    assert reopened.agent_snapshot == snapshot
    assert reopened.agent_snapshot is not None
    assert reopened.agent_snapshot.fingerprint() == snapshot.fingerprint()


def test_context_identity_from_source_rejects_none_instead_of_stringifying():
    """Incomplete Context VM identities must fail closed.

    In particular, a source attribute containing ``None`` must not become the
    literal string ``"None"`` and pass the non-empty validators.
    """

    source = SimpleNamespace(
        context_snapshot_id=None,
        snapshot_id="ctx-1",
        context_manifest_id="manifest-1",
        context_manifest_hash="A" * 64,
        context_working_set_hash="B" * 64,
        context_materialized_hash="C" * 64,
    )
    identity = ContextIdentity.from_source(source)
    assert identity.snapshot_id == "ctx-1"

    source.context_manifest_hash = None
    with pytest.raises(ValueError, match="missing identity fields"):
        ContextIdentity.from_source(source)


def test_mark_stale_cognition_is_a_terminal_audit_state():
    manager = AttemptManager()
    attempt = _attempt()

    manager.mark_stale_cognition(attempt, "PaymentAPI@17 changed")

    assert attempt.state == AttemptState.STALE_COGNITION
    assert attempt.ended_at is not None
    assert attempt.error == "PaymentAPI@17 changed"
