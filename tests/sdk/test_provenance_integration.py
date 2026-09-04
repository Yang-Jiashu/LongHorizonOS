"""Focused SDK/provenance compatibility tests.

These tests exercise the adapter contract independently of AgentOS's
composition root.  The latter can consume the helper functions immediately
before attaching Evidence without changing legacy executor call sites.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.context.models import ContextSnapshot, PageBinding
from lhos.provenance import CoveragePolicy, CoverageStatus, ProvenanceOperation
from lhos.sdk import (
    Agent,
    ConfigurationError,
    Goal,
    Task,
    VerificationError,
    build_coverage_report,
    create_execution_context,
    enforce_verification_coverage,
    evaluate_verification_coverage,
    resolve_executor_api,
)


def test_task_and_goal_provenance_fields_are_optional_and_inherit() -> None:
    goal = Goal(
        "g",
        inputs=("workspace://source.py",),
        outputs=("workspace://report.md",),
        provenance_policy="audit",
        executor_api="context_v1",
    )
    task = goal.task("t")

    assert isinstance(task, Task)
    assert task.inputs == ("workspace://source.py",)
    assert task.outputs == ("workspace://report.md",)
    assert task.provenance_policy is CoveragePolicy.AUDIT
    assert task.executor_api == "context_v1"
    assert goal.inputs == ("workspace://source.py",)


def test_task_fields_normalize_and_validate() -> None:
    task = Task(
        "t",
        inputs={"workspace://b": {}, "workspace://a": {}},
        outputs="workspace://out",
        provenance_policy=CoveragePolicy.STRICT,
        executor_api="CONTEXT_V1",
    )
    assert task.inputs == ("workspace://a", "workspace://b")
    assert task.outputs == ("workspace://out",)
    assert task.provenance_policy is CoveragePolicy.STRICT
    assert task.executor_api == "context_v1"

    with pytest.raises(ConfigurationError, match="executor_api"):
        Task("bad", executor_api="unsupported")
    with pytest.raises(ConfigurationError, match="provenance_policy"):
        Task("bad-policy", provenance_policy="fail_open")


def test_executor_api_precedence() -> None:
    agent = Agent("worker", executor_api="context_v1")
    task = Task("t", executor_api="legacy_task_id")

    assert resolve_executor_api(agent=agent) == "context_v1"
    assert resolve_executor_api(agent=agent, task=task) == "legacy_task_id"
    assert (
        resolve_executor_api(
            agent=agent,
            task=task,
            explicit="context_v1",
        )
        == "context_v1"
    )


def test_legacy_context_is_explicitly_unknown_and_strict_denies() -> None:
    task = Task(
        "t",
        inputs=("workspace://source.py",),
        provenance_policy=CoveragePolicy.STRICT,
        executor_api="legacy_task_id",
    )
    context = create_execution_context(
        "graph-1",
        "t",
        attempt_id="attempt-1",
        claim_id="claim-1",
        executor_api="legacy_task_id",
    )
    report = build_coverage_report(task, context)

    assert report.status is CoverageStatus.UNKNOWN
    assert report.unknown_inputs == ("legacy://executor",)
    decision = evaluate_verification_coverage(task, report)
    assert not decision.allowed
    with pytest.raises(VerificationError):
        enforce_verification_coverage(task, report)


def test_context_v1_complete_trace_is_admitted_in_strict() -> None:
    task = Task(
        "t",
        inputs=("workspace://source.py",),
        provenance_policy="strict",
        executor_api="context_v1",
    )
    context = create_execution_context(
        "graph-1",
        "t",
        attempt_id="attempt-1",
        claim_id="claim-1",
        semantic_epoch=3,
        executor_api="context_v1",
    )
    context.read(
        "workspace://source.py",
        artifact_id="source.py",
        version=7,
        content_hash="sha256:source-v7",
    )
    report = build_coverage_report(task, context)

    assert report.status is CoverageStatus.COMPLETE
    assert report.operation_counts == {ProvenanceOperation.READ.value: 1}
    decision = enforce_verification_coverage(task, report)
    assert decision.allowed


def test_context_v1_missing_declared_read_is_partial_and_audit_allows() -> None:
    task = Task(
        "t",
        inputs=("workspace://source.py",),
        provenance_policy="audit",
        executor_api="context_v1",
    )
    context = create_execution_context("graph-1", "t", executor_api="context_v1")
    report = build_coverage_report(task, context)

    assert report.status is CoverageStatus.PARTIAL
    decision = enforce_verification_coverage(task, report)
    assert decision.allowed
    assert decision.warnings


def test_context_v1_empty_trace_is_not_complete_for_declared_inputs() -> None:
    """No mediated read is evidence of missing coverage, not proof of purity."""

    task = Task(
        "t",
        inputs=("workspace://hidden.txt",),
        provenance_policy="strict",
        executor_api="context_v1",
    )
    context = create_execution_context("graph-1", "t", executor_api="context_v1")
    report = build_coverage_report(task, context)

    assert report.status is CoverageStatus.PARTIAL
    assert report.missing_inputs == ("workspace://hidden.txt",)
    decision = evaluate_verification_coverage(task, report)
    assert not decision.allowed
    assert any("missing declared inputs" in reason for reason in decision.reasons)


def test_context_v1_unknown_hidden_read_is_never_promoted_to_complete() -> None:
    """An explicitly reported hidden read remains UNKNOWN under strict policy."""

    task = Task(
        "t",
        inputs=("workspace://declared.txt",),
        provenance_policy="strict",
        executor_api="context_v1",
    )
    context = create_execution_context("graph-1", "t", executor_api="context_v1")
    context.observe_unknown(resource_hint="", channel="raw-python")
    report = build_coverage_report(task, context)

    assert report.status is CoverageStatus.UNKNOWN
    assert report.unknown_inputs
    assert not evaluate_verification_coverage(task, report).allowed


def test_context_snapshot_bindings_are_automatically_recorded_as_reads() -> None:
    """Context VM materialization must become an auditable read-set.

    This is the first concrete bridge between the Context VM working set and
    provenance: a context-aware executor should not have to remember to repeat
    ``ctx.read(...)`` for every page it was handed.
    """

    context = create_execution_context(
        "graph-1",
        "t",
        attempt_id="attempt-1",
        executor_api="context_v1",
    )
    snapshot = ContextSnapshot(
        snapshot_id="snapshot-1",
        pid="pid-1",
        manifest_hash="a" * 64,
        working_set_hash="b" * 64,
        materialized_hash="c" * 64,
        policy_id="priority_stable_v1",
        estimator_id="byte_x4_utf8_v1",
        page_bindings=(
            PageBinding(
                page_id="page-1",
                canonical_uri="vpg://requirements",
                artifact_id="requirements",
                version=8,
                content_hash="d" * 64,
                page_hash="e" * 64,
                byte_start=0,
                byte_end=12,
            ),
        ),
        tokens_used=3,
        bytes_used=12,
    )

    context.bind_context_snapshot(
        snapshot_id=snapshot.snapshot_id,
        manifest_id="manifest-1",
        manifest_hash=snapshot.manifest_hash,
        working_set_hash=snapshot.working_set_hash,
        materialized_hash=snapshot.materialized_hash,
        snapshot=snapshot,
    )

    reads = [event for event in context.events if event.op is ProvenanceOperation.READ]
    assert len(reads) == 1
    assert reads[0].source == "context_vm"
    assert reads[0].resource_uri == "vpg://requirements"
    assert reads[0].artifact_id == "requirements"
    assert reads[0].version == 8
    assert reads[0].content_hash == "d" * 64
    assert reads[0].metadata["context_snapshot_id"] == "snapshot-1"
