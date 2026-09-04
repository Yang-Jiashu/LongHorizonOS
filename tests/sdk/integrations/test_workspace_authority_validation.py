"""Authority-backed version validation for the mediated workspace gateway.

These tests intentionally exercise the trust boundary rather than the normal
workspace read/write happy paths.  A caller-provided integer is not semantic
truth in strict mode: the gateway must either validate the exact
``version + content_hash`` binding or fail closed.
"""

from __future__ import annotations

import hashlib

import pytest

from lhos.integrations import (
    WorkspaceGatewayError,
    WorkspaceProvenanceGateway,
    WorkspaceTool,
)
from lhos.integrations.tools.provenance_workspace import (
    WorkspaceVersionValidationError,
)
from lhos.provenance import ProvenanceOperation
from lhos.sdk import create_execution_context


def _strict_context():
    return create_execution_context(
        "graph-authority",
        "task-authority",
        claim_id="claim-authority",
        attempt_id="attempt-authority",
        semantic_epoch=9,
        executor_api="context_v1",
        secure_mode=True,
    )


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class _HashAuthority:
    """Small FactsProvider-like test double with call auditing."""

    def __init__(self, bindings: dict[tuple[str, int], str | None]):
        self.bindings = bindings
        self.calls: list[tuple[str, str, int]] = []

    def read_hash(self, pid: str, uri: str, version: int) -> str | None:
        self.calls.append((pid, uri, version))
        return self.bindings.get((uri, version))


def test_strict_claimed_version_without_authority_fails_before_payload_read(
    tmp_path, monkeypatch
) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "payload").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
    )

    reads = 0
    original_read = workspace.read_bytes

    def counted_read(relative_path: str) -> bytes:
        nonlocal reads
        reads += 1
        return original_read(relative_path)

    monkeypatch.setattr(workspace, "read_bytes", counted_read)
    with pytest.raises(
        WorkspaceVersionValidationError,
        match="requires a version_validator or version_authority",
    ):
        gateway.read_bytes("input.txt", version=1)

    assert reads == 0
    assert context.events == ()


def test_validator_acceptance_binds_exact_snapshot_and_records_validator_source(
    tmp_path,
) -> None:
    payload = b"v1"
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", payload).ok
    context = _strict_context()
    seen = []

    def validator(snapshot) -> bool:
        seen.append(snapshot)
        return snapshot.version == 7 and snapshot.content_hash == _digest(payload)

    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_validator=validator,
    )

    snapshot = gateway.snapshot("input.txt", version=7)
    assert snapshot.version == 7
    assert seen == [snapshot]
    event = context.events[-1]
    assert event.known is True
    assert event.metadata["version_source"] == "validator"
    assert event.content_hash == _digest(payload)


def test_validator_rejection_records_unknown_read_and_excludes_it_from_read_set(
    tmp_path,
) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "payload").ok
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_validator=lambda _snapshot: False,
    )

    with pytest.raises(WorkspaceVersionValidationError, match="validator rejected"):
        gateway.read_bytes("input.txt", version=4)

    assert len(context.events) == 1
    event = context.events[0]
    assert event.op is ProvenanceOperation.READ
    assert event.known is False
    assert event.metadata["reason"] == "workspace_version_unverified"
    assert gateway.read_set == ()


def test_authority_accepts_declared_uri_and_uses_context_claim_as_pid(tmp_path) -> None:
    payload = b"authoritative"
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", payload).ok
    authority = _HashAuthority({("workspace://input.txt", 11): _digest(payload)})
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_authority=authority,
    )

    gateway.read_bytes("input.txt", version=11)

    assert authority.calls == [("claim-authority", "workspace://input.txt", 11)]
    assert context.events[-1].metadata["version_source"] == "authority"
    assert context.events[-1].known is True


def test_authority_falls_back_to_artifact_id_when_uri_is_unregistered(tmp_path) -> None:
    payload = b"fallback"
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", payload).ok
    authority = _HashAuthority({("input.txt", 12): _digest(payload)})
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("vpg://workspace/input.txt",),
        version_authority=authority,
    )

    gateway.read_bytes("input.txt", version=12)

    assert authority.calls == [
        ("claim-authority", "vpg://workspace/input.txt", 12),
        ("claim-authority", "input.txt", 12),
    ]
    assert context.events[-1].metadata["version_source"] == "authority"


@pytest.mark.parametrize(
    ("binding", "message"),
    [
        (("workspace://input.txt", 13, "0" * 64), "hash mismatch"),
        (("workspace://other.txt", 13, _digest(b"payload")), "no registered binding"),
        (("workspace://input.txt", 13, "not-a-sha256"), "invalid content hash"),
    ],
)
def test_authority_rejection_is_fail_closed_and_audited(tmp_path, binding, message) -> None:
    payload = b"payload"
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", payload).ok
    uri, version, expected = binding
    authority = _HashAuthority({(uri, version): expected})
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        version_authority=authority,
    )

    with pytest.raises(WorkspaceVersionValidationError, match=message):
        gateway.read_bytes("input.txt", version=version)

    assert context.events[-1].op is ProvenanceOperation.READ
    assert context.events[-1].known is False
    assert context.events[-1].metadata["reason"] == "workspace_version_unverified"
    assert gateway.read_set == ()


def test_strict_authority_rejects_output_before_filesystem_write(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    context = _strict_context()
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        writable=("workspace://output.txt",),
        version_authority=_HashAuthority({}),
    )

    with pytest.raises(WorkspaceVersionValidationError, match="no registered binding"):
        gateway.write_text("output.txt", "new output", version=21)

    assert not (tmp_path / "output.txt").exists()
    assert context.events == ()


def test_compatibility_mode_preserves_caller_version_behavior(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    assert workspace.write("input.txt", "payload").ok
    context = create_execution_context(
        "graph-authority",
        "task-authority",
        executor_api="context_v1",
        secure_mode=False,
    )
    gateway = WorkspaceProvenanceGateway(
        workspace,
        context,
        readable=("workspace://input.txt",),
        strict=False,
    )

    snapshot = gateway.snapshot("input.txt", version=5)
    assert snapshot.version == 5
    assert context.events[-1].metadata["version_source"] == "caller"
    assert context.events[-1].known is True


def test_constructor_rejects_ambiguous_or_invalid_authority_configuration(tmp_path) -> None:
    workspace = WorkspaceTool(tmp_path)
    context = _strict_context()

    with pytest.raises(WorkspaceGatewayError, match="either version_validator"):
        WorkspaceProvenanceGateway(
            workspace,
            context,
            version_validator=lambda _snapshot: True,
            version_authority=_HashAuthority({}),
        )

    with pytest.raises(WorkspaceGatewayError, match="must expose read_hash"):
        WorkspaceProvenanceGateway(
            workspace,
            context,
            version_authority=object(),
        )
