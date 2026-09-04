"""Typed runtime events shared by external Agent Harness adapters.

The existing ``harness-session.v1`` protocol controls lifecycle transitions.
This module adds an observation contract for what happened inside one external
Harness attempt. Events are operational telemetry and provenance inputs; they
are never semantic Evidence by themselves.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt, field_validator

HARNESS_PHASE_SCHEMA_VERSION: Final[Literal["harness-phase.v1"]] = "harness-phase.v1"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return uuid4().hex


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HarnessPhaseKind(StrEnum):
    STARTING = "starting"
    READY = "ready"
    MODEL_CALL = "model_call"
    TOOL_CALL = "tool_call"
    VERIFYING = "verifying"
    CHECKPOINTED = "checkpointed"
    REBASED = "rebased"
    PREEMPT_REQUESTED = "preempt_requested"
    PREEMPTED = "preempted"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STALE = "stale"


class HarnessFailureClass(StrEnum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    CONTENT_POLICY = "content_policy"
    INVALID_REQUEST = "invalid_request"
    NETWORK_TRANSIENT = "network_transient"
    PROVIDER_5XX = "provider_5xx"
    TOOL_ERROR = "tool_error"
    SANDBOX_ERROR = "sandbox_error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"
    STALE_COGNITION = "stale_cognition"
    VERIFIER_FAILED = "verifier_failed"
    PROTOCOL_MALFORMED = "protocol_malformed"
    EXIT_NONZERO = "exit_nonzero"
    UNKNOWN = "unknown"


class HarnessRetryScope(StrEnum):
    NONE = "none"
    SESSION = "session"
    ATTEMPT = "attempt"
    TASK = "task"
    PROVIDER = "provider"


class HarnessUsage(_FrozenModel):
    """Monotonic or delta usage with provider token buckets kept separate."""

    schema_version: Literal["harness-phase.v1"] = HARNESS_PHASE_SCHEMA_VERSION
    uncached_input_tokens: StrictInt = Field(default=0, ge=0)
    output_tokens: StrictInt = Field(default=0, ge=0)
    reasoning_tokens: StrictInt = Field(default=0, ge=0)
    cache_read_tokens: StrictInt = Field(default=0, ge=0)
    cache_write_tokens: StrictInt = Field(default=0, ge=0)
    verification_tokens: StrictInt = Field(default=0, ge=0)
    model_calls: StrictInt = Field(default=0, ge=0)
    tool_calls: StrictInt = Field(default=0, ge=0)
    wall_time_ms: StrictInt = Field(default=0, ge=0)
    cpu_time_ms: StrictInt | None = Field(default=None, ge=0)
    monetary_microusd: StrictInt = Field(default=0, ge=0)

    @property
    def input_token_units(self) -> int:
        return self.uncached_input_tokens + self.cache_read_tokens + self.cache_write_tokens

    @property
    def total_token_units(self) -> int:
        return self.input_token_units + self.output_tokens + self.verification_tokens

    def plus(self, other: HarnessUsage) -> HarnessUsage:
        cpu_time_ms = (
            None
            if self.cpu_time_ms is None or other.cpu_time_ms is None
            else self.cpu_time_ms + other.cpu_time_ms
        )
        return HarnessUsage(
            uncached_input_tokens=self.uncached_input_tokens + other.uncached_input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            reasoning_tokens=self.reasoning_tokens + other.reasoning_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cache_write_tokens=self.cache_write_tokens + other.cache_write_tokens,
            verification_tokens=self.verification_tokens + other.verification_tokens,
            model_calls=self.model_calls + other.model_calls,
            tool_calls=self.tool_calls + other.tool_calls,
            wall_time_ms=self.wall_time_ms + other.wall_time_ms,
            cpu_time_ms=cpu_time_ms,
            monetary_microusd=self.monetary_microusd + other.monetary_microusd,
        )


class HarnessFailure(_FrozenModel):
    schema_version: Literal["harness-phase.v1"] = HARNESS_PHASE_SCHEMA_VERSION
    failure_class: HarnessFailureClass
    retryable: bool = False
    retry_scope: HarnessRetryScope = HarnessRetryScope.NONE
    status_code: StrictInt | None = Field(default=None, ge=100, le=599)
    retry_after_seconds: float | None = Field(default=None, ge=0.0)
    provider_request_id: str | None = None
    summary: str = ""
    message_digest: str = Field(min_length=64, max_length=64)

    @classmethod
    def from_message(
        cls,
        failure_class: HarnessFailureClass,
        message: str,
        *,
        retryable: bool = False,
        retry_scope: HarnessRetryScope = HarnessRetryScope.NONE,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        provider_request_id: str | None = None,
    ) -> HarnessFailure:
        normalized = str(message).strip()
        return cls(
            failure_class=failure_class,
            retryable=retryable,
            retry_scope=retry_scope,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
            provider_request_id=provider_request_id,
            summary=normalized[-512:],
            message_digest=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        )

    @field_validator("provider_request_id", mode="before")
    @classmethod
    def _optional_string(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class HarnessExecutionBinding(_FrozenModel):
    """Exact Scheduler/Context identity visible to an external Harness."""

    schema_version: Literal["harness-phase.v1"] = HARNESS_PHASE_SCHEMA_VERSION
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    semantic_epoch: StrictInt = Field(ge=0)
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    process_id: str = ""
    lease_id: str | None = None
    lease_fencing_token: StrictInt | None = Field(default=None, ge=0)
    context_snapshot_id: str = ""
    context_manifest_hash: str = ""
    working_set_hash: str = ""
    workspace_id: str = ""
    session_cursor: str | None = None

    @field_validator(
        "graph_id",
        "task_id",
        "agent_id",
        "claim_id",
        "attempt_id",
        mode="before",
    )
    @classmethod
    def _required_string(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Harness execution identity fields must be non-empty")
        return normalized

    @field_validator(
        "process_id",
        "context_snapshot_id",
        "context_manifest_hash",
        "working_set_hash",
        "workspace_id",
        mode="before",
    )
    @classmethod
    def _optional_empty_string(cls, value: Any) -> str:
        return "" if value is None else str(value).strip()

    @field_validator("lease_id", "session_cursor", mode="before")
    @classmethod
    def _optional_identity(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def from_execution_context(
        cls,
        context: Any,
        *,
        workspace_id: str,
        session_cursor: str | None = None,
    ) -> HarnessExecutionBinding:
        return cls(
            graph_id=str(getattr(context, "graph_id", "") or ""),
            graph_version=int(getattr(context, "graph_version", 0) or 0),
            semantic_epoch=int(getattr(context, "semantic_epoch", 0) or 0),
            task_id=str(getattr(context, "task_id", "") or ""),
            agent_id=str(getattr(context, "agent_id", "") or ""),
            claim_id=str(getattr(context, "claim_id", "") or ""),
            attempt_id=str(getattr(context, "attempt_id", "") or ""),
            process_id=str(getattr(context, "process_id", "") or ""),
            lease_id=getattr(context, "lease_id", None),
            lease_fencing_token=getattr(context, "lease_fencing_token", None),
            context_snapshot_id=str(getattr(context, "context_snapshot_id", "") or ""),
            context_manifest_hash=str(getattr(context, "context_manifest_hash", "") or ""),
            working_set_hash=str(getattr(context, "context_working_set_hash", "") or ""),
            workspace_id=workspace_id,
            session_cursor=session_cursor,
        )


class HarnessPhaseEvent(_FrozenModel):
    schema_version: Literal["harness-phase.v1"] = HARNESS_PHASE_SCHEMA_VERSION
    event_id: str = Field(default_factory=_uuid, min_length=1)
    session_id: str = Field(min_length=1)
    phase_seq: StrictInt = Field(ge=0)
    emitted_at: datetime = Field(default_factory=_utcnow)
    phase: HarnessPhaseKind
    binding: HarnessExecutionBinding
    usage_delta: HarnessUsage = Field(default_factory=HarnessUsage)
    usage_cumulative: HarnessUsage = Field(default_factory=HarnessUsage)
    read_set_delta: tuple[str, ...] = ()
    write_set_delta: tuple[str, ...] = ()
    checkpoint_ref: str | None = None
    artifact_refs: tuple[str, ...] = ()
    failure: HarnessFailure | None = None
    idempotency_key: str = Field(min_length=1)
    parent_event_id: str | None = None
    details: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator(
        "read_set_delta",
        "write_set_delta",
        "artifact_refs",
        mode="before",
    )
    @classmethod
    def _sorted_unique(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "HARNESS_PHASE_SCHEMA_VERSION",
    "HarnessExecutionBinding",
    "HarnessFailure",
    "HarnessFailureClass",
    "HarnessPhaseEvent",
    "HarnessPhaseKind",
    "HarnessRetryScope",
    "HarnessUsage",
]
