"""Core kernel domain models — Process, Action, KernelRequest, KernelEvent,
Signal, Message, Capability, ResourceLease, Clock.

All objects are frozen per the Canonical Spec.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _require_expires_at() -> datetime:
    """Force callers to supply an expiry — leases must never default to
    already-expired (MOD-02)."""
    raise TypeError("ResourceLease.expires_at must be supplied explicitly; there is no default.")


def _uuid() -> str:
    return uuid4().hex


# ── Process ──────────────────────────────────────────────────────────────────


class ProcessState(StrEnum):
    CREATED = "created"
    READY = "ready"
    RUNNING = "running"
    BLOCKED = "blocked"
    SUSPENDED = "suspended"
    EXITED = "exited"
    FAILED = "failed"


TERMINAL_PROCESS_STATES = frozenset({ProcessState.EXITED, ProcessState.FAILED})


class ProcessControlBlock(BaseModel):
    """Process Control Block — the kernel's view of a process."""

    pid: str
    parent_pid: str | None = None
    program_id: str

    state: ProcessState = ProcessState.CREATED
    priority: int = 10
    effective_priority: int = 10

    capability_set_id: str
    namespace_id: str
    resource_group_id: str = "default"

    program_state_ref: str = ""
    pending_request_id: str | None = None
    wait_condition: dict[str, Any] | None = None

    checkpoint_ref: str | None = None
    exit_code: str | None = None
    result_ref: str | None = None

    event_cursor: int = 0
    created_at: datetime = Field(default_factory=_utcnow)


# ── Action ───────────────────────────────────────────────────────────────────


class ActionState(StrEnum):
    SUBMITTED = "submitted"
    ADMITTED = "admitted"
    RUNNING = "running"
    COMMITTED = "committed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    UNCERTAIN = "uncertain"


TERMINAL_ACTION_STATES = frozenset(
    {
        ActionState.COMMITTED,
        ActionState.FAILED,
        ActionState.CANCELLED,
        ActionState.TIMED_OUT,
        ActionState.UNCERTAIN,
    }
)


class SideEffectClass(StrEnum):
    PURE = "pure"
    IDEMPOTENT = "idempotent"
    NON_REVERSIBLE = "non_reversible"
    UNKNOWN = "unknown"


class ActionControlBlock(BaseModel):
    """Action Control Block — the kernel's view of an external action."""

    action_id: str = Field(default_factory=_uuid)
    pid: str

    device_type: str
    operation: str
    arguments: dict[str, Any] = Field(default_factory=dict)

    state: ActionState = ActionState.SUBMITTED

    resource_claims: list[dict[str, Any]] = Field(default_factory=list)
    lease_ids: list[str] = Field(default_factory=list)
    fencing_tokens: dict[str, int] = Field(default_factory=dict)
    idempotency_key: str | None = None
    side_effect_class: SideEffectClass = SideEffectClass.PURE
    recovery_policy: str = "retry"

    timeout_seconds: int | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    submitted_at: datetime = Field(default_factory=_utcnow)
    finished_at: datetime | None = None


# ── Kernel Request ───────────────────────────────────────────────────────────


class KernelRequest(BaseModel):
    """Base class for all kernel requests (syscalls)."""

    request_id: str = Field(default_factory=_uuid)
    pid: str
    request_type: str


class SpawnRequest(KernelRequest):
    request_type: str = "spawn"
    pid: str = ""  # empty → kernel assigns
    program_id: str = ""
    parent_pid: str | None = None
    capabilities: list[str] = Field(default_factory=list)
    namespace_id: str = ""
    resource_group_id: str = "default"
    initial_state: dict[str, Any] = Field(default_factory=dict)


class ProgramStepRequest(KernelRequest):
    request_type: str = "step"


class SubmitActionRequest(KernelRequest):
    request_type: str = "submit_action"
    device_type: str
    operation: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    resource_claims: list[dict[str, Any]] = Field(default_factory=list)
    side_effect_class: SideEffectClass = SideEffectClass.PURE
    idempotency_key: str | None = None
    timeout_seconds: int | None = None


class InspectActionRequest(KernelRequest):
    request_type: str = "inspect_action"
    action_id: str


class CancelActionRequest(KernelRequest):
    request_type: str = "cancel_action"
    action_id: str


class AcquireResourceRequest(KernelRequest):
    request_type: str = "acquire"
    claims: list[dict[str, Any]] = Field(default_factory=list)


class ReleaseResourceRequest(KernelRequest):
    request_type: str = "release"
    lease_ids: list[str] = Field(default_factory=list)


class WaitRequest(KernelRequest):
    request_type: str = "wait"
    condition: dict[str, Any]
    timeout_seconds: int = 300


class SignalRequest(KernelRequest):
    request_type: str = "signal"
    target_pid: str
    signal_type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class CheckpointRequest(KernelRequest):
    request_type: str = "checkpoint"


class RestoreRequest(KernelRequest):
    request_type: str = "restore"
    checkpoint_id: str


class ExitRequest(KernelRequest):
    request_type: str = "exit"
    exit_code: str = "ok"
    result_ref: str | None = None


# ── Kernel Event ─────────────────────────────────────────────────────────────


class KernelEvent(BaseModel):
    """Append-only journal entry — the source of truth."""

    event_id: str = Field(default_factory=_uuid)
    journal_offset: int = 0
    pid: str
    process_sequence: int = 0

    event_type: str
    causation_id: str | None = None
    correlation_id: str | None = None

    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)


# ── Signal ───────────────────────────────────────────────────────────────────


class Signal(BaseModel):
    signal_id: str = Field(default_factory=_uuid)
    target_pid: str
    signal_type: str
    source_pid: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)
    consumed: bool = False


# ── Message ──────────────────────────────────────────────────────────────────


class Message(BaseModel):
    message_id: str = Field(default_factory=_uuid)
    source_pid: str
    target_pid: str
    schema_name: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)


# ── Capability ───────────────────────────────────────────────────────────────


class Capability(BaseModel):
    capability_id: str = Field(default_factory=_uuid)
    resource_pattern: str
    operations: set[str] = Field(default_factory=set)
    constraints: dict[str, Any] = Field(default_factory=dict)


class CapabilitySet(BaseModel):
    set_id: str = Field(default_factory=_uuid)
    pid: str
    capabilities: list[Capability] = Field(default_factory=list)

    def check(self, resource: str, operation: str) -> bool:
        import fnmatch

        for cap in self.capabilities:
            if fnmatch.fnmatch(resource, cap.resource_pattern) and operation in cap.operations:
                return True
        return False


# ── ResourceLease ────────────────────────────────────────────────────────────


class ResourceLease(BaseModel):
    lease_id: str = Field(default_factory=_uuid)
    resource_id: str
    owner_pid: str

    mode: Literal["shared", "exclusive"] = "exclusive"
    # Monotonically increases for every acquisition of a logical resource.
    # A stale owner can retain its old lease_id in memory, but it can never
    # regain authority once a newer fencing token has been issued.
    # ``0`` is an explicitly unfenced compatibility value for callers that
    # construct a descriptive lease model outside LeaseService. Authoritative
    # acquisitions always persist a strictly positive token.
    fencing_token: int = Field(default=0, ge=0)

    acquired_at: datetime = Field(default_factory=_utcnow)
    expires_at: datetime = Field(default_factory=_require_expires_at)

    renewable: bool = True
    revocable: bool = True


# ── Clock ────────────────────────────────────────────────────────────────────


class Clock:
    """Logical + wall-clock time source."""

    def __init__(self) -> None:
        self._logical: int = 0

    def now(self) -> datetime:
        return _utcnow()

    def tick(self) -> int:
        self._logical += 1
        return self._logical

    @property
    def logical(self) -> int:
        return self._logical


# ── Checkpoint ───────────────────────────────────────────────────────────────


class ProcessCheckpoint(BaseModel):
    checkpoint_id: str = Field(default_factory=_uuid)
    pid: str
    journal_offset: int
    process_sequence: int
    pcb_snapshot: dict[str, Any]
    program_state_ref: str
    wait_condition: dict[str, Any] | None = None
    mailbox_cursor: int = 0
    created_at: datetime = Field(default_factory=_utcnow)
