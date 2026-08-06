"""Context VM core domain models.

All models are pydantic BaseModels with frozen semantics. Context VM has no
semantic-relevance logic: it takes a version-pinned Manifest and produces
a bounded, deterministic working set under the supplied policy.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field

WORKING_SET_STATES = Literal[
    "created",
    "loading",
    "resident",
    "partially_resident",
    "evicted",
    "closed",
    "failed",
]


# ── deterministic hashing helpers ──────────────────────────────────────────────


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return uuid4().hex


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _deterministic_hash(fields: list[str]) -> str:
    """Stable SHA-256 over a list of stringified field values, joined by NUL."""
    blob = "\x00".join(fields).encode("utf-8")
    return _hash_bytes(blob)


def _content_hash_for(data: bytes) -> str:
    """Identity-style content hash (mirrors Artifact FS CAS)."""
    return _hash_bytes(data)


# ── 6.1 ContentRef ───────────────────────────────────────────────────────────


class ContentRef(BaseModel):
    """A single version-pinned reference to a byte range within one
    committed ArtifactVersion.

    Invariants:
    - version >= 1
    - content_hash must match the authoritative ArtifactVersion
    - if byte range specified: 0 <= start_byte < end_byte <= content_length
    - same ref_id is unique within a Manifest
    """

    ref_id: str

    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str

    media_type: str
    encoding: str = "utf-8"

    priority: int = 0
    required: bool = False
    pinnable: bool = True

    start_byte: int | None = None
    end_byte: int | None = None

    metadata: dict[str, Any] = Field(default_factory=dict)

    def ref_hash(self) -> str:
        """Stable identity hash for this ref's binding."""
        return _deterministic_hash(
            [
                self.ref_id,
                self.canonical_uri,
                self.artifact_id,
                str(self.version),
                self.content_hash,
                self.media_type,
                self.encoding,
                str(self.priority),
                str(self.required),
                str(self.pinnable),
                str(self.start_byte),
                str(self.end_byte),
            ]
        )


# ── 6.2 ContextManifest ──────────────────────────────────────────────────────


class ContextManifest(BaseModel):
    """Materialization request: a set of version-pinned refs plus budgets."""

    manifest_id: str = Field(default_factory=_uuid)
    owner_pid: str

    refs: tuple[ContentRef, ...]

    token_budget: int
    byte_budget: int | None = None
    page_size_bytes: int = 4096

    policy_id: str = "priority_stable_v1"

    created_at: datetime = Field(default_factory=_utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def manifest_hash(self) -> str:
        """Deterministic hash over the full, frozen manifest."""
        ref_strs = [r.ref_hash() for r in self.refs]
        # Sort by ref_id so that input ordering does not change the hash.
        ref_strs.sort()
        return _deterministic_hash(
            [
                self.manifest_id,
                self.owner_pid,
                *ref_strs,
                str(self.token_budget),
                str(self.byte_budget),
                str(self.page_size_bytes),
                self.policy_id,
            ]
        )


# ── 6.3 ContextPage ──────────────────────────────────────────────────────────


class ContextPage(BaseModel):
    """A single, contiguous range of a committed ArtifactVersion."""

    page_id: str

    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str

    byte_start: int
    byte_end: int

    page_hash: str
    estimated_tokens: int
    size_bytes: int

    required: bool
    priority: int

    pinned: bool = False
    resident: bool = False


# ── 6.4 WorkingSet ────────────────────────────────────────────────────────────


class WorkingSet(BaseModel):
    """Snapshot of materialization state for one Manifest load."""

    working_set_id: str = Field(default_factory=_uuid)
    pid: str
    manifest_id: str
    manifest_hash: str

    policy_id: str

    token_budget: int
    byte_budget: int | None

    selected_page_ids: tuple[str, ...]
    omitted_page_ids: tuple[str, ...]

    tokens_used: int = 0
    bytes_used: int = 0

    state: WORKING_SET_STATES = "created"

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


# ── 6.5 ContextHandle ─────────────────────────────────────────────────────────


class ContextHandle(BaseModel):
    """An open binding between a PID and a WorkingSet."""

    handle_id: str = Field(default_factory=_uuid)
    pid: str
    working_set_id: str

    opened_at: datetime = Field(default_factory=_utcnow)
    closed_at: datetime | None = None

    pinned_page_ids: tuple[str, ...]


# ── 6.6 LoadedPage / OmittedRef / VersionBinding / LoadedContext ─────────────


class LoadedPage(BaseModel):
    """Snapshot of a materialized page as exposed to the caller."""

    page_id: str
    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str
    page_hash: str
    byte_start: int
    byte_end: int
    size_bytes: int
    required: bool
    priority: int

    media_type: str
    encoding: str

    estimated_tokens: int

    content: bytes


class OmittedRef(BaseModel):
    """Records that a ref's page(s) were omitted due to budget pressure."""

    ref_id: str
    reason: str  # "budget_exceeded" or "optional_skipped"
    requested_tokens: int | None


class VersionBinding(BaseModel):
    """Each materialized page pins one committed ArtifactVersion."""

    page_id: str
    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str


class LoadedContext(BaseModel):
    """Exact materialized state used by an agent step."""

    context_id: str = Field(default_factory=_uuid)
    pid: str

    manifest_id: str
    manifest_hash: str
    working_set_id: str

    ordered_pages: tuple[LoadedPage, ...]

    token_budget: int
    tokens_used: int

    byte_budget: int | None
    bytes_used: int

    omitted_refs: tuple[OmittedRef, ...]
    version_bindings: tuple[VersionBinding, ...]

    materialized_hash: str
    created_at: datetime = Field(default_factory=_utcnow)


# ── 6.7 PageBinding / ContextSnapshot ─────────────────────────────────────────


class PageBinding(BaseModel):
    """Exact materialization detail captured at snapshot time."""

    page_id: str
    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str
    page_hash: str
    byte_start: int
    byte_end: int


class ContextSnapshot(BaseModel):
    """Immutable record of a materialized working set."""

    snapshot_id: str = Field(default_factory=_uuid)
    pid: str

    manifest_hash: str
    working_set_hash: str
    materialized_hash: str

    policy_id: str
    estimator_id: str

    page_bindings: tuple[PageBinding, ...]

    tokens_used: int
    bytes_used: int

    created_at: datetime = Field(default_factory=_utcnow)
