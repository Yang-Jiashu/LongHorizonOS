"""Focused tests for the mediated workspace provenance boundary."""

from __future__ import annotations

import hashlib

import pytest

from lhos.integrations import (
    WorkspaceAccessDenied,
    WorkspaceGatewayError,
    WorkspaceProvenanceGateway,
    WorkspaceReadSetValidationError,
    WorkspaceTool,
)
from lhos.provenance import CoverageStatus, ProvenanceOperation
from lhos.sdk import Task, build_coverage_report, create_execution_context


def _strict_context():
    return create_execution_context(
        "graph-1",
        "task-1",
        claim_id="claim-1",
        attempt_id="attempt-1",
        semantic_epoch=3,
        executor_api="context_v1",
        secure_mode=True,
    )


def test_strict_task_gateway_records_exact_read_bytes_and_complete_coverage(
    tmp_path,
) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("source.py", "print('v1')\n").ok
    task = Task(
        "task-1",
        inputs=("workspace://source.py",),
        outputs=("workspace://report.md",),
        executor_api="context_v1",
        provenance_policy="strict",
    )
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway.for_task(workspace, context, task)

    assert gateway.read_text("source.py") == "print('v1')\n"

    report = build_coverage_report(task, context)
    event = context.events[0]
    assert report.status is CoverageStatus.COMPLETE
    assert event.op is ProvenanceOperation.READ
    assert event.resource_uri == "workspace://source.py"
    assert event.artifact_id == "source.py"
    assert event.content_hash == hashlib.sha256(b"print('v1')\n").hexdigest()
    assert event.attempt_id == "attempt-1"
    assert event.semantic_epoch == 3
    assert event.source == "workspace-gateway"


def test_strict_gateway_rejects_undeclared_and_escaping_reads_before_access(
    tmp_path,
) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("declared.txt", "declared").ok
    assert workspace.write("secret.txt", "secret").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://declared.txt",),
    )

    with pytest.raises(WorkspaceAccessDenied, match="undeclared"):
        gateway.read_text("secret.txt")
    with pytest.raises(WorkspaceAccessDenied):
        gateway.read_text("../outside.txt")

    assert context.events == ()


def test_audit_gateway_makes_an_undeclared_read_visible_to_coverage(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("declared.txt", "declared").ok
    assert workspace.write("extra.txt", "extra").ok
    task = Task(
        "task-1",
        inputs=("workspace://declared.txt",),
        executor_api="context_v1",
        provenance_policy="audit",
    )
    context = create_execution_context(
        "graph-1",
        "task-1",
        executor_api="context_v1",
    )
    gateway = WorkspaceProvenanceGateway.for_task(
        workspace,
        context,
        task,
        strict=False,
    )

    gateway.read_text("declared.txt")
    gateway.read_text("extra.txt")

    report = build_coverage_report(task, context)
    assert report.status is CoverageStatus.PARTIAL
    assert report.undeclared_inputs == ("workspace://extra.txt",)


def test_strict_gateway_writes_only_declared_output_and_records_result(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    task = Task(
        "task-1",
        outputs=("vpg://workspace/report.md",),
        executor_api="context_v1",
    )
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway.for_task(workspace, context, task)

    snapshot = gateway.write_text("report.md", "verified result")

    assert workspace.read("report.md").value == "verified result"
    assert snapshot.resource_uri == "vpg://workspace/report.md"
    assert snapshot.artifact_id == "report.md"
    assert snapshot.content_hash == hashlib.sha256(b"verified result").hexdigest()
    event = context.events[-1]
    assert event.op is ProvenanceOperation.WRITE
    assert event.resource_uri == snapshot.resource_uri
    assert event.content_hash == snapshot.content_hash

    with pytest.raises(WorkspaceAccessDenied, match="undeclared"):
        gateway.write_text("other.md", "denied")
    assert not (tmp_path / "other.md").exists()


def test_gateway_cas_failure_leaves_file_and_provenance_unchanged(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("report.md", "old").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        writable=("workspace://report.md",),
    )

    with pytest.raises(WorkspaceGatewayError, match="CAS failed"):
        gateway.write_text("report.md", "new", expected_hash="0" * 64)

    assert workspace.read("report.md").value == "old"
    assert context.events == ()


def test_secure_context_cannot_disable_gateway_strict_mode(tmp_path) -> None:
    with pytest.raises(WorkspaceGatewayError, match="cannot disable"):
        WorkspaceProvenanceGateway(
            WorkspaceTool(tmp_path),
            _strict_context(),
            strict=False,
        )


def test_gateway_rejects_non_workspace_capability_uri(tmp_path) -> None:
    with pytest.raises(WorkspaceGatewayError, match="unsupported"):
        WorkspaceProvenanceGateway(
            WorkspaceTool(tmp_path),
            _strict_context(),
            readable=("https://example.test/input",),
        )


def test_gateway_binds_optional_authoritative_version_and_exposes_sets(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        writable=("workspace://output.txt",),
        version_validator=lambda snapshot: snapshot.version in {7, 8},
    )

    observed = gateway.snapshot("input.txt", version=7)
    produced = gateway.write_text("output.txt", "result", version=8)

    assert observed.version == 7
    assert produced.version == 8
    assert gateway.read_set == (observed,)
    assert gateway.write_set == (produced,)
    assert context.events[0].version == 7
    assert context.events[-1].version == 8
    assert context.events[-1].metadata["version_source"] == "validator"


def test_gateway_accepts_context_vm_workspace_uri_spelling(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("doc.md", "hello").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace:///doc.md",),
    )

    assert gateway.read_text("workspace:///doc.md") == "hello"
    assert context.events[0].artifact_id == "doc.md"


@pytest.mark.parametrize("bad_version", [True, False, 0, -1, 1.5, "7"])
def test_gateway_rejects_non_authoritative_version_tokens(tmp_path, bad_version) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    gateway = WorkspaceProvenanceGateway(
        workspace,
        _strict_context(),
        readable=("workspace://input.txt",),
    )

    with pytest.raises(WorkspaceGatewayError, match="positive integer"):
        gateway.snapshot("input.txt", version=bad_version)
    assert gateway.context.events == ()


def test_gateway_read_returns_the_same_bytes_that_were_hashed(tmp_path, monkeypatch) -> None:
    """The gateway must not hash one read and return bytes from a second read."""

    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "first").ok
    original_read = workspace.read_bytes
    calls = 0

    def mutate_after_first_read(rel: str) -> bytes:
        nonlocal calls
        calls += 1
        payload = original_read(rel)
        if calls == 1:
            assert workspace.write("input.txt", "second").ok
        return payload

    monkeypatch.setattr(workspace, "read_bytes", mutate_after_first_read)
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
    )

    assert gateway.read_bytes("input.txt") == b"first"
    assert context.events[0].content_hash == hashlib.sha256(b"first").hexdigest()
    assert calls == 1


def test_gateway_records_unknown_for_an_admitted_missing_read(tmp_path) -> None:
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        WorkspaceTool(tmp_path),
        context,
        readable=("workspace://missing.txt",),
    )

    with pytest.raises(WorkspaceGatewayError, match="read failed"):
        gateway.read_bytes("missing.txt")

    assert len(context.events) == 1
    event = context.events[0]
    assert event.op is ProvenanceOperation.READ
    assert event.known is False
    assert event.metadata["reason"] == "workspace_read_failed"


def test_strict_version_validator_rejects_before_write(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        writable=("workspace://output.txt",),
        version_validator=lambda _snapshot: False,
    )

    with pytest.raises(Exception, match="validator rejected"):
        gateway.write_text("output.txt", "result", version=1)
    assert not (tmp_path / "output.txt").exists()
    assert context.events == ()


def test_facts_like_authority_validates_version_and_hash(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    digest = hashlib.sha256(b"v1").hexdigest()

    class Authority:
        def read_hash(self, _pid, uri, version):
            return (
                digest if uri in {"workspace://input.txt", "input.txt"} and version == 3 else None
            )

    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_authority=Authority(),
    )
    snapshot = gateway.snapshot("input.txt", version=3)
    assert snapshot.content_hash == digest
    assert context.events[0].metadata["version_source"] == "authority"


def test_strict_facts_like_authority_rejects_hash_mismatch(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_authority=type(
            "Authority",
            (),
            {"read_hash": lambda _self, _pid, _uri, _version: "0" * 64},
        )(),
    )

    with pytest.raises(Exception, match="hash mismatch"):
        gateway.snapshot("input.txt", version=3)
    assert context.events[-1].known is False
    assert gateway.read_set == ()


def test_unknown_latest_event_hides_earlier_trusted_binding(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_validator=lambda snapshot: snapshot.version == 1,
    )

    assert gateway.snapshot("input.txt", version=1) in gateway.read_set

    # Simulate a later mediated observation that could not be proven.  The
    # current read-set must not continue to expose v1 as authoritative.
    context.record(
        ProvenanceOperation.READ,
        resource_uri="workspace://input.txt",
        artifact_id="input.txt",
        version=2,
        content_hash="0" * 64,
        source="workspace-gateway",
        known=False,
        metadata={"reason": "watcher_gap"},
    )
    assert gateway.read_set == ()


def test_gateway_commit_boundary_detects_direct_workspace_mutation(tmp_path) -> None:
    """A mediated read must be revalidated before semantic commit."""

    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
    )

    assert gateway.read_text("input.txt") == "v1"
    current = gateway.validate_read_set_current()
    assert current.current is True
    assert current.checked_resources == ("input.txt",)

    # Simulate a direct external mutation that no watcher has registered yet.
    assert workspace.write("input.txt", "v2").ok
    report = gateway.validate_read_set_current()
    assert report.current is False
    assert report.stale_resources == ("input.txt",)
    with pytest.raises(WorkspaceReadSetValidationError) as exc_info:
        gateway.require_read_set_current()
    assert exc_info.value.report == report


def test_gateway_commit_boundary_fails_closed_on_deleted_file(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "v1").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
    )
    gateway.read_text("input.txt")
    (tmp_path / "input.txt").unlink()

    report = gateway.validate_read_set_current()
    assert report.current is False
    assert report.unavailable_resources == ("input.txt",)
    with pytest.raises(WorkspaceReadSetValidationError):
        gateway.require_read_set_current()


def test_gateway_commit_boundary_is_bounded_and_fail_closed(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    for name in ("a.txt", "b.txt"):
        assert workspace.write(name, name).ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://a.txt", "workspace://b.txt"),
    )
    gateway.read_text("a.txt")
    gateway.read_text("b.txt")

    report = gateway.validate_read_set_current(max_resources=1)
    assert report.current is False
    assert report.truncated is True
    assert report.resource_limit == 1

    with pytest.raises(WorkspaceGatewayError, match="between 1 and 4096"):
        gateway.validate_read_set_current(max_resources=0)
