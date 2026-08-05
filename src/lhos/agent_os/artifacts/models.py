"""Artifact FS domain models.

All models are pydantic BaseModels, consistent with kernel/models.py conventions.
These models live at L3 (System Service) and must not be imported by the microkernel.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return uuid4().hex


# ── Artifact Record ──────────────────────────────────────────────────────────


class ArtifactRecord(BaseModel):
    """An artifact is a versioned, immutable-after-commit file within a namespace."""

    artifact_id: str = Field(default_factory=_uuid)
    namespace_id: str
    canonical_uri: str

    current_version: int = 0
    artifact_type: str = "file"

    created_by_pid: str
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    deleted: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Artifact Version ─────────────────────────────────────────────────────────


class ArtifactVersion(BaseModel):
    """An immutable, committed version of an artifact.

    Once committed, no field may change.
    Version numbers start at 1 and strictly increment by 1.
    """

    artifact_id: str
    version: int

    content_ref: str
    content_hash: str
    size_bytes: int

    parent_version: int | None = None
    committed_by_pid: str
    committed_action_id: str
    committed_at: datetime = Field(default_factory=_utcnow)


# ── Artifact Handle ──────────────────────────────────────────────────────────


HandleMode = Literal["read", "write", "append"]


class ArtifactHandle(BaseModel):
    """An open handle to an artifact, similar to a file descriptor.

    A handle belongs to exactly one process and cannot be transferred.
    Read handles pin a specific version.
    Write handles hold an exclusive lease.
    """

    handle_id: str = Field(default_factory=_uuid)
    pid: str
    artifact_id: str

    mode: HandleMode
    opened_version: int | None = None
    expected_version: int | None = None

    lease_id: str | None = None
    transaction_id: str | None = None

    opened_at: datetime = Field(default_factory=_utcnow)
    closed_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        return self.closed_at is None


# ── Write Transaction ────────────────────────────────────────────────────────


TransactionState = Literal[
    "open",
    "staged",
    "committed",
    "aborted",
    "conflicted",
    "uncertain",
]

TERMINAL_TRANSACTION_STATES: frozenset[str] = frozenset(
    {"committed", "aborted", "conflicted", "uncertain"}
)


class WriteTransaction(BaseModel):
    """An atomic write transaction for staging and committing artifact content."""

    transaction_id: str = Field(default_factory=_uuid)
    artifact_id: str
    pid: str

    expected_version: int | None = None
    staged_content_ref: str = ""
    staged_content_hash: str = ""
    staged_size_bytes: int = 0

    state: TransactionState = "open"

    idempotency_key: str
    created_at: datetime = Field(default_factory=_utcnow)
    finished_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_TRANSACTION_STATES


# ── Namespace ────────────────────────────────────────────────────────────────


class ArtifactNamespace(BaseModel):
    """A namespace isolates artifact URIs per process.

    Each process gets a private namespace on spawn: ns-<pid>.
    """

    namespace_id: str
    owner_pid: str

    root_uri: str
    quota_bytes: int | None = None
    max_open_handles: int | None = None

    created_at: datetime = Field(default_factory=_utcnow)


# ── Namespace Mount ──────────────────────────────────────────────────────────


MountMode = Literal[
    "private",
    "shared_readonly",
    "shared_readwrite",
    "copy_on_write",
]


class NamespaceMount(BaseModel):
    """A mount maps a source namespace prefix into a target namespace.

    Phase C1 implements: private, shared_readonly, copy_on_write.
    shared_readwrite is defined but not required for the gate.
    """

    mount_id: str = Field(default_factory=_uuid)
    namespace_id: str  # target namespace where mount_point lives

    mount_point: str
    source_namespace_id: str
    source_prefix: str

    mode: MountMode = "private"
    created_at: datetime = Field(default_factory=_utcnow)


# ── Snapshot ─────────────────────────────────────────────────────────────────


class NamespaceSnapshot(BaseModel):
    """An immutable manifest of artifact URI → version for a namespace."""

    snapshot_id: str = Field(default_factory=_uuid)
    namespace_id: str

    artifact_versions: dict[str, int] = Field(default_factory=dict)  # canonical_uri → version
    content_refs: dict[str, str] = Field(default_factory=dict)  # version_key → content_ref

    created_by_pid: str
    created_at: datetime = Field(default_factory=_utcnow)


# ── Artifact Watch ───────────────────────────────────────────────────────────


class ArtifactWatch(BaseModel):
    """A watch registration for artifact change signals."""

    watch_id: str = Field(default_factory=_uuid)
    pid: str
    namespace_id: str
    uri_prefix: str

    created_at: datetime = Field(default_factory=_utcnow)
    active: bool = True


# ── Storage Driver Types ─────────────────────────────────────────────────────


class StagedArtifact(BaseModel):
    """Result of staging content in the storage driver."""

    transaction_id: str
    content_ref: str
    content_hash: str
    size_bytes: int


class StorageCommitResult(BaseModel):
    """Result of committing staged content in the storage driver."""

    transaction_id: str
    content_ref: str
    committed: bool


class StorageTransactionStatus(BaseModel):
    """Inspection result for a storage transaction."""

    transaction_id: str
    status: Literal["unknown", "staged", "committed", "aborted"]
    content_ref: str = ""
    content_hash: str = ""
