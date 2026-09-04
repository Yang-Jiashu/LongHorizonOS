"""Authority-issued observation tokens for safe semantic repair.

An observation token is a small, immutable capability describing one exact
artifact observation.  It is intentionally *not* a semantic conclusion: the
VPG still decides whether an artifact change invalidates a task.  The token
only proves that the caller is referring to an observation registered by the
FactsProvider, including its graph binding and content digest.

The token is backed by an append-only row in the FactsProvider database.  This
is preferable to trusting a caller-supplied ``dict`` (or a bare integer
version): changing any field causes validation to fail because the durable
authority row no longer matches.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ObservationToken(BaseModel):
    """Immutable, authority-issued artifact observation.

    ``token_digest`` is a deterministic digest of all fields except itself.
    The FactsProvider additionally stores the complete token row, so the
    digest is an integrity check while the row is the authority check.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = "observation-v1"
    token_id: str = Field(default_factory=lambda: uuid4().hex, min_length=1)
    artifact_id: str = Field(min_length=1)
    version: int = Field(gt=0)
    content_hash: str = Field(
        min_length=64,
        max_length=64,
        validation_alias=AliasChoices("content_hash", "hash"),
    )
    graph_id: str = ""
    issued_at: datetime = Field(default_factory=_utcnow)
    token_digest: str = Field(min_length=64, max_length=64)

    @field_validator("artifact_id", "graph_id")
    @classmethod
    def _strip_ids(cls, value: str) -> str:
        return str(value).strip()

    @field_validator("content_hash", "token_digest")
    @classmethod
    def _lower_hex_digest(cls, value: str) -> str:
        normalized = str(value).strip().lower()
        if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
            raise ValueError("observation token digests must be 64-character SHA-256 hex")
        return normalized

    @property
    def hash(self) -> str:
        """Compatibility alias for callers that call the digest ``hash``."""

        return self.content_hash

    @property
    def canonical_uri(self) -> str:
        return f"vpg://{self.artifact_id}"

    def payload(self) -> dict[str, Any]:
        """Return fields covered by ``token_digest`` in canonical form."""

        return {
            "schema_version": self.schema_version,
            "token_id": self.token_id,
            "artifact_id": self.artifact_id,
            "version": int(self.version),
            "content_hash": self.content_hash,
            "graph_id": self.graph_id,
            "issued_at": self.issued_at.astimezone(UTC).isoformat(),
        }

    def computed_digest(self) -> str:
        encoded = json.dumps(
            self.payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def is_self_consistent(self) -> bool:
        return self.computed_digest() == self.token_digest

    def as_dict(self) -> dict[str, Any]:
        """JSON-friendly representation suitable for manifests and IPC."""

        return self.model_dump(mode="json")

    def serialize(self) -> str:
        """Serialize to deterministic JSON for transport/storage."""

        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def parse(cls, value: ObservationToken | Mapping[str, Any] | str) -> ObservationToken:
        """Coerce a token from an instance, mapping, or serialized JSON."""

        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                raw = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError("invalid observation token JSON") from exc
            if not isinstance(raw, Mapping):
                raise ValueError("observation token JSON must encode an object")
            return cls.model_validate(dict(raw))
        if isinstance(value, Mapping):
            return cls.model_validate(dict(value))
        raise TypeError(
            "observation token must be an ObservationToken, mapping, or serialized JSON string"
        )


def make_observation_token(
    *,
    token_id: str,
    artifact_id: str,
    version: int,
    content_hash: str,
    graph_id: str,
    issued_at: datetime | None = None,
) -> ObservationToken:
    """Construct a token and compute its integrity digest."""

    # Build once with a placeholder digest, then replace it with the digest
    # covered by the immutable payload.  ``model_copy`` is used rather than a
    # mutable assignment because token models are frozen by design.
    first = ObservationToken(
        token_id=token_id,
        artifact_id=artifact_id,
        version=version,
        content_hash=content_hash,
        graph_id=graph_id,
        issued_at=issued_at or _utcnow(),
        token_digest="0" * 64,
    )
    return first.model_copy(update={"token_digest": first.computed_digest()})


# A descriptive alias keeps the API discoverable for users who call these
# records "artifact observations" rather than "tokens".
ArtifactObservation = ObservationToken

__all__ = ["ArtifactObservation", "ObservationToken", "make_observation_token"]
