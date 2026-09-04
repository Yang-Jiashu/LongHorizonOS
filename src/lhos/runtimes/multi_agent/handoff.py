"""Bounded single-host ownership-handoff transaction DTOs.

The Scheduler, Kernel lease authority, and Harness adapter are separate
authorities.  A real distributed transaction is therefore out of scope for
this module.  These immutable DTOs instead provide a durable intent and a
small, fail-closed recovery protocol for one in-process Scheduler instance.

The intent is deliberately *not* an ownership capability.  It authenticates
the exact Claim/Attempt/Agent tuple that a caller observed and gives retries a
stable idempotency identity.  A successful commit still relies on the normal
Scheduler ``handoff_task`` fencing path.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, field_validator

from .models import ClaimHandoffResult

HANDOFF_SCHEMA_VERSION: Final[Literal["ownership-handoff.v1"]] = "ownership-handoff.v1"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OwnershipHandoffPhase(StrEnum):
    """Durable intent phase.

    ``COMMITTING`` means the release-first operation may have crossed the
    Kernel boundary.  Recovery must consequently distinguish an active source
    from an already-fenced source and must never guess an owner.
    """

    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTED = "aborted"
    FAILED_CLOSED = "failed_closed"


class OwnershipHandoffStatus(StrEnum):
    """Public outcome of the bounded transaction facade."""

    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    REPLAYED = "replayed"
    ABORTED = "aborted"
    REFUSED = "refused"
    FAILED_CLOSED = "failed_closed"
    IN_DOUBT = "in_doubt"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class OwnershipHandoffIntent(_FrozenModel):
    """Exact durable handoff intent prepared before ownership mutation."""

    schema_version: Literal["ownership-handoff.v1"] = HANDOFF_SCHEMA_VERSION
    handoff_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    source_claim_id: str = Field(min_length=1)
    source_attempt_id: str = Field(min_length=1)
    source_agent_id: str = Field(min_length=1)
    replacement_agent_id: str = Field(min_length=1)
    source_graph_version: StrictInt = Field(ge=0)
    source_semantic_epoch: StrictInt = Field(ge=0)
    source_fencing_token: StrictInt | None = Field(default=None, ge=0)
    source_lease_id: str | None = None
    action: Literal["preempt", "rebase"]
    reason: str = ""
    created_at: datetime = Field(default_factory=_utcnow)

    @field_validator(
        "handoff_id",
        "graph_id",
        "task_id",
        "source_claim_id",
        "source_attempt_id",
        "source_agent_id",
        "replacement_agent_id",
        mode="before",
    )
    @classmethod
    def _non_empty(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("handoff identity fields must be non-empty")
        return normalized

    @field_validator("source_lease_id", mode="before")
    @classmethod
    def _optional_lease(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("reason", mode="before")
    @classmethod
    def _bounded_reason(cls, value: Any) -> str:
        return str(value).replace("\r", " ").replace("\n", " ").strip()[:512]

    def fingerprint(self) -> str:
        """Stable request identity used for idempotency/replay checks."""

        payload = self.model_dump(mode="json")
        payload.pop("created_at", None)
        return hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


class OwnershipHandoffResult(_FrozenModel):
    """Bounded result returned by prepare/commit/recover operations."""

    schema_version: Literal["ownership-handoff.v1"] = HANDOFF_SCHEMA_VERSION
    intent: OwnershipHandoffIntent
    phase: OwnershipHandoffPhase
    status: OwnershipHandoffStatus
    durable: StrictBool = True
    source_lease_released: StrictBool | None = None
    replacement_claim_id: str | None = None
    replacement_attempt_id: str | None = None
    claim_result: ClaimHandoffResult | None = None
    reason: str = ""

    @property
    def transferred(self) -> bool:
        return self.status in {
            OwnershipHandoffStatus.COMMITTED,
            OwnershipHandoffStatus.REPLAYED,
        }

    @property
    def fail_closed(self) -> bool:
        return self.status in {
            OwnershipHandoffStatus.FAILED_CLOSED,
            OwnershipHandoffStatus.IN_DOUBT,
        }
