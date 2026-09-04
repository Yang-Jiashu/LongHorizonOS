"""Small, dependency-light models for runtime provenance.

The provenance layer is deliberately an *observation* layer.  A
``ProvenanceEvent`` records what an executor observed or touched; it does not
itself assert that an artifact is semantically valid.  The VPG/D3 verifier
remains the authority for validity and invalidation.

Events are immutable Pydantic models.  Stores add a monotonically increasing
sequence number and a hash-chain link before persisting an event.  Keeping the
chain fields on the model makes a JSONL trace independently auditable while
still allowing callers to construct uncommitted events conveniently.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ProvenanceOperation(StrEnum):
    """Operation classes captured by the v0.2 recorder."""

    READ = "read"
    WRITE = "write"
    TOOL = "tool"
    NETWORK = "network"
    MODEL = "model"
    EXTERNAL = "external"


# Short alias used by the public API and in design documents.
ProvenanceOp = ProvenanceOperation

# Operations which normally introduce an input dependency.  Writes are
# intentionally excluded: a produced output is bound by the VPG separately.
INPUT_OPERATIONS: frozenset[ProvenanceOperation] = frozenset(
    {
        ProvenanceOperation.READ,
        ProvenanceOperation.TOOL,
        ProvenanceOperation.NETWORK,
        ProvenanceOperation.MODEL,
        ProvenanceOperation.EXTERNAL,
    }
)


class CoverageStatus(StrEnum):
    """Completeness level of the observed provenance trace.

    ``UNKNOWN`` is deliberately distinct from ``PARTIAL``: the former means
    at least one input was observed but could not be identified, while the
    latter means all observations are identifiable but the declaration and
    trace do not match exactly.
    """

    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"


def canonical_json(value: Any) -> str:
    """Return the one canonical JSON representation used for hashes.

    ``default=str`` is intentional for user metadata.  Event fields are
    validated and serialised in JSON mode before reaching this helper, so the
    fallback only applies to opaque metadata values.
    """

    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


class ProvenanceEvent(BaseModel):
    """Immutable observation of an execution/resource interaction.

    ``event_id``, ``sequence``, ``previous_hash`` and ``event_hash`` are
    assigned/verified by a :mod:`lhos.provenance.store` implementation.  An
    empty ``event_id`` means "let the store derive a deterministic id".
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str = ""
    graph_id: str
    task_id: str = ""
    attempt_id: str = ""
    semantic_epoch: int = 0

    op: ProvenanceOperation
    resource_uri: str = ""
    artifact_id: str | None = None
    version: int | None = None
    content_hash: str | None = None
    action_id: str | None = None

    source: str = "runtime"
    confidence: float = 1.0
    # ``known=False`` is an explicit fail-closed marker for an input that the
    # executor could not identify (for example a hidden environment read).
    known: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_at: datetime = Field(default_factory=_utcnow)
    idempotency_key: str | None = None

    sequence: int = 0
    previous_hash: str = ""
    event_hash: str = ""

    _hash_excluded_fields: ClassVar[frozenset[str]] = frozenset({"previous_hash", "event_hash"})

    @field_validator("graph_id")
    @classmethod
    def _graph_id_non_empty(cls, value: str) -> str:
        value = str(value).strip()
        if not value:
            raise ValueError("graph_id must be non-empty")
        return value

    @field_validator("event_id", "task_id", "attempt_id", "resource_uri", "source")
    @classmethod
    def _trim_strings(cls, value: str) -> str:
        return str(value).strip()

    @field_validator("semantic_epoch", "sequence", "version")
    @classmethod
    def _non_negative_ints(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("version/sequence/semantic_epoch must be >= 0")
        return value

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, value: float) -> float:
        value = float(value)
        if not 0.0 <= value <= 1.0:
            raise ValueError("confidence must be between 0 and 1")
        return value

    @property
    def is_input(self) -> bool:
        """Whether this operation is treated as an input dependency."""

        return self.op in INPUT_OPERATIONS or bool(self.metadata.get("input", False))

    @property
    def resource_key(self) -> str:
        """Canonical key used by coverage matching.

        Artifact IDs are accepted as a useful fallback when a lower-level
        adapter has no URI.  The prefix prevents collisions with URI strings.
        """

        if self.resource_uri:
            return self.resource_uri
        if self.artifact_id:
            return f"artifact:{self.artifact_id}"
        return ""

    def body_dict(self, *, include_event_id: bool = True) -> dict[str, Any]:
        """JSON-ready semantic body, excluding hash-chain bookkeeping."""

        data = self.model_dump(mode="json", exclude=set(self._hash_excluded_fields))
        if not include_event_id:
            data.pop("event_id", None)
        return data

    def canonical_json(
        self,
        *,
        include_chain: bool = False,
        include_event_id: bool = True,
    ) -> str:
        """Canonical JSON used for persistence and event hashing."""

        if include_chain:
            data = self.model_dump(mode="json")
            if not include_event_id:
                data.pop("event_id", None)
        else:
            data = self.body_dict(include_event_id=include_event_id)
        return canonical_json(data)

    def fingerprint(self) -> str:
        """Hash of semantic content, independent of hash-chain links."""

        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def deterministic_id(self, sequence: int | None = None) -> str:
        """Derive a stable ID for an otherwise unassigned event.

        Sequence is included so repeated identical observations remain
        distinguishable in an append-only trace.
        """

        body = self.model_copy(
            update={
                "event_id": "",
                "sequence": self.sequence if sequence is None else sequence,
                "previous_hash": "",
                "event_hash": "",
            }
        )
        digest = hashlib.sha256(body.canonical_json(include_event_id=False).encode()).hexdigest()
        return f"prov-{digest[:32]}"

    def compute_hash(self, previous_hash: str = "") -> str:
        """Compute the hash-chain digest for this event body."""

        payload = f"{previous_hash}:{self.canonical_json()}".encode()
        return hashlib.sha256(payload).hexdigest()

    def bind_chain(
        self,
        *,
        sequence: int,
        previous_hash: str,
        event_id: str | None = None,
    ) -> ProvenanceEvent:
        """Return a copy bound to a store sequence/hash-chain position."""

        if sequence < 1:
            raise ValueError("persisted provenance sequence must start at 1")
        resolved_id = event_id or self.event_id or self.deterministic_id(sequence)
        candidate = self.model_copy(
            update={
                "event_id": resolved_id,
                "sequence": sequence,
                "previous_hash": previous_hash,
                "event_hash": "",
            }
        )
        return candidate.model_copy(update={"event_hash": candidate.compute_hash(previous_hash)})


class CoverageReport(BaseModel):
    """Deterministic comparison of declared and observed input provenance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    graph_id: str = ""
    task_id: str = ""
    status: CoverageStatus

    declared_inputs: tuple[str, ...] = ()
    observed_inputs: tuple[str, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    undeclared_inputs: tuple[str, ...] = ()
    unknown_inputs: tuple[str, ...] = ()
    missing_operations: tuple[str, ...] = ()

    event_count: int = 0
    input_event_count: int = 0
    operation_counts: dict[str, int] = Field(default_factory=dict)
    coverage_ratio: float = 0.0
    warnings: tuple[str, ...] = ()
    report_hash: str = ""

    @field_validator("coverage_ratio")
    @classmethod
    def _ratio_range(cls, value: float) -> float:
        value = float(value)
        if not 0.0 <= value <= 1.0:
            raise ValueError("coverage_ratio must be between 0 and 1")
        return value

    def with_hash(self) -> CoverageReport:
        """Return a copy with a deterministic report digest."""

        body = self.model_copy(update={"report_hash": ""})
        digest = hashlib.sha256(body.canonical_json().encode("utf-8")).hexdigest()
        return body.model_copy(update={"report_hash": digest})

    def canonical_json(self) -> str:
        return canonical_json(self.model_dump(mode="json"))


__all__ = [
    "INPUT_OPERATIONS",
    "CoverageReport",
    "CoverageStatus",
    "ProvenanceEvent",
    "ProvenanceOp",
    "ProvenanceOperation",
    "canonical_json",
]
