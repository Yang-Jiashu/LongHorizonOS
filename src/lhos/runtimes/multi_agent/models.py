"""D2 Multi-Agent Scheduler domain models.

Immutable-by-default Pydantic models for the scheduling domain.  All
snapshots used for audit / hashing are deterministic: sets are sorted
before encoding, tuples stored in insertion-stable order.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ── UUID / clock helpers (same convention as Kernel + VPG) ─────────────────
def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(UTC)


# ── Enums ────────────────────────────────────────────────────────────────────
class ClaimState(StrEnum):
    PROPOSED = "proposed"
    ACQUIRING = "acquiring"
    ACTIVE = "active"
    RELEASED = "released"
    LOST = "lost"
    COMPLETED = "completed"
    REJECTED = "rejected"


TERMINAL_CLAIM_STATES = frozenset(
    {
        ClaimState.RELEASED,
        ClaimState.LOST,
        ClaimState.COMPLETED,
        ClaimState.REJECTED,
    }
)


class AttemptState(StrEnum):
    DISPATCHED = "dispatched"
    RUNNING = "running"
    FAILED = "failed"
    CRASHED = "crashed"
    SUCCEEDED_OPERATIONALLY = "succeeded_operationally"
    VERIFIED_SEMANTICALLY = "verified_semantically"


# ── Resource accounting ─────────────────────────────────────────────────────
class ResourceVector(BaseModel):
    """A schedulable, additive resource vector.

    CPU is expressed in millicores and memory in bytes so comparisons remain
    integer-only and deterministic. ``model_slots`` are keyed by model/pool
    name; a task must acquire its entire vector atomically before execution.
    """

    model_config = ConfigDict(frozen=True)

    cpu_millis: int = 0
    ram_bytes: int = 0
    gpu_count: int = 0
    vram_bytes: int = 0
    model_slots: dict[str, int] = Field(default_factory=dict)

    @field_validator("cpu_millis", "ram_bytes", "gpu_count", "vram_bytes")
    @classmethod
    def _scalar_non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("resource quantities must be >= 0")
        return value

    @field_validator("model_slots")
    @classmethod
    def _model_slots_valid(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for name, count in value.items():
            key = str(name).strip()
            if not key:
                raise ValueError("model slot names must be non-empty")
            if count < 0:
                raise ValueError("model slot quantities must be >= 0")
            if count:
                normalized[key] = int(count)
        return dict(sorted(normalized.items()))

    @property
    def is_zero(self) -> bool:
        return (
            self.cpu_millis == 0
            and self.ram_bytes == 0
            and self.gpu_count == 0
            and self.vram_bytes == 0
            and not self.model_slots
        )

    def fits_within(self, capacity: ResourceVector) -> bool:
        return not self.shortages(capacity)

    def shortages(self, capacity: ResourceVector) -> dict[str, int]:
        shortages: dict[str, int] = {}
        for name in ("cpu_millis", "ram_bytes", "gpu_count", "vram_bytes"):
            missing = getattr(self, name) - getattr(capacity, name)
            if missing > 0:
                shortages[name] = missing
        for name, requested in self.model_slots.items():
            missing = requested - capacity.model_slots.get(name, 0)
            if missing > 0:
                shortages[f"model_slots.{name}"] = missing
        return shortages

    def plus(self, other: ResourceVector) -> ResourceVector:
        slot_names = set(self.model_slots) | set(other.model_slots)
        return ResourceVector(
            cpu_millis=self.cpu_millis + other.cpu_millis,
            ram_bytes=self.ram_bytes + other.ram_bytes,
            gpu_count=self.gpu_count + other.gpu_count,
            vram_bytes=self.vram_bytes + other.vram_bytes,
            model_slots={
                name: self.model_slots.get(name, 0) + other.model_slots.get(name, 0)
                for name in slot_names
            },
        )

    def minus(self, other: ResourceVector) -> ResourceVector:
        result = ResourceVector(
            cpu_millis=self.cpu_millis - other.cpu_millis,
            ram_bytes=self.ram_bytes - other.ram_bytes,
            gpu_count=self.gpu_count - other.gpu_count,
            vram_bytes=self.vram_bytes - other.vram_bytes,
            model_slots={
                name: self.model_slots.get(name, 0) - other.model_slots.get(name, 0)
                for name in set(self.model_slots) | set(other.model_slots)
            },
        )
        return result


# ── AgentDescriptor ──────────────────────────────────────────────────────────
class AgentDescriptor(BaseModel):
    """Scheduling authority's knowledge of an Agent process.

    Fields ``agent_id`` / ``process_id`` MUST be non-empty.  There is NO
    authoritative ``alive`` / ``running`` field here — real liveness is
    learned from the Kernel via the injected ProcessProvider.
    """

    agent_id: str
    process_id: str

    specializations: tuple[str, ...] = ()
    supported_task_kinds: tuple[str, ...] = ()
    supported_tools: tuple[str, ...] = ()

    max_concurrency: int = 1
    cost_weight: int = 100
    resource_capacity: ResourceVector = Field(default_factory=ResourceVector)

    enabled: bool = True

    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("agent_id", "process_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("must be non-empty")
        return v

    @field_validator("max_concurrency")
    @classmethod
    def _max_concurrency_ge_zero(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_concurrency must be >= 0")
        return v

    @field_validator("cost_weight")
    @classmethod
    def _cost_weight_ge_zero(cls, v: int) -> int:
        if v < 0:
            raise ValueError("cost_weight must be >= 0")
        return v


# ── TaskRequirements ────────────────────────────────────────────────────────
class TaskRequirements(BaseModel):
    """Structured scheduling contract decoded from a TaskNode's metadata.

    Per Section 10 the Scheduler does NOT read description text or use
    models to guess fit — it only reads these typed requirements.
    """

    task_id: str
    task_kind: str = ""
    preferred_agent: str = ""

    required_specializations: tuple[str, ...] = ()
    preferred_specializations: tuple[str, ...] = ()

    required_tools: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()

    priority: int = 0
    estimated_cost: int = 0
    resources: ResourceVector = Field(default_factory=ResourceVector)

    max_attempts: int | None = None


# ── AgentCapabilitySnapshot ─────────────────────────────────────────────────
class AgentCapabilitySnapshot(BaseModel):
    """A point-in-time audit record of an Agent's Kernel capability grants.

    Built by querying the CapabilityProvider at eligibility time, stored
    with EligibilityResult so later audit can reconstruct exactly what the
    Scheduler saw.
    """

    agent_id: str
    captured_at: datetime = Field(default_factory=_utcnow)
    capabilities: tuple[str, ...] = ()

    @classmethod
    def from_provider(
        cls,
        agent_id: str,
        provider: Any,
    ) -> AgentCapabilitySnapshot:
        raw = provider.capabilities_for(agent_id) if provider is not None else []
        flat: list[str] = []
        for c in raw:
            rp = getattr(c, "resource_pattern", None)
            ops = getattr(c, "operations", None)
            if rp is not None and ops is not None:
                for op in sorted(ops):
                    flat.append(f"{rp}:{op}")
            elif isinstance(c, str):
                flat.append(c)
        return cls(agent_id=agent_id, capabilities=tuple(sorted(set(flat))))


# ── EligibilityResult ────────────────────────────────────────────────────────
class EligibilityResult(BaseModel):
    """Outcome of the eligibility predicate for a (graph, task, agent) triple."""

    graph_id: str
    graph_version: int

    task_id: str
    agent_id: str

    eligible: bool
    reasons: tuple[str, ...] = ()

    @property
    def reason_text(self) -> str:
        return "; ".join(self.reasons) if self.reasons else ""


# ── Matching ─────────────────────────────────────────────────────────────────
class AgentMatchScore(BaseModel):
    agent_id: str
    score: int
    reasons: tuple[str, ...] = ()


class MatchDecision(BaseModel):
    """Deterministic record of WHY agent A was chosen over B.

    ``decision_hash`` is a content hash of the full candidate vector so an
    auditor can re-derive the identical decision offline.
    """

    graph_id: str
    graph_version: int
    task_id: str

    selected_agent_id: str
    candidates: tuple[AgentMatchScore, ...] = ()

    policy_id: str = "deterministic_best_fit_v1"
    decision_hash: str = ""


# ── TaskClaim ───────────────────────────────────────────────────────────────
class TaskClaim(BaseModel):
    """An exclusive task ownership claim.

    Real ownership ONLY linearizes when ``lease_id`` is non-null AND the
    backing Kernel ResourceLease is live.  This record is never the
    ownership authority — the Kernel Lease is.
    """

    claim_id: str = Field(default_factory=_uuid)

    graph_id: str
    graph_version: int

    task_id: str

    agent_id: str
    process_id: str

    lease_resource: str
    lease_id: str | None = None
    lease_owner_pid: str | None = None
    lease_fencing_token: int | None = None
    lease_expires_at: datetime | None = None

    resource_reservation_id: str | None = None
    reserved_resources: ResourceVector = Field(default_factory=ResourceVector)

    state: ClaimState = ClaimState.PROPOSED

    attempt_number: int = 0
    reason: str | None = None

    created_at: datetime = Field(default_factory=_utcnow)
    activated_at: datetime | None = None
    released_at: datetime | None = None


# ── ScheduledExecutionAttempt ────────────────────────────────────────────────
class ScheduledExecutionAttempt(BaseModel):
    """One execution attempt for a Task under a TaskClaim.

    Operational success (!= semantic verification) is the boundary between
    "the agent's action committed" and "the VPG derived VERIFIED".
    """

    attempt_id: str = Field(default_factory=_uuid)

    graph_id: str = ""
    graph_version: int = 0
    semantic_epoch: int = 0

    task_id: str
    claim_id: str

    agent_id: str
    process_id: str
    attempt_number: int = 0

    state: AttemptState = AttemptState.DISPATCHED
    action_ids: tuple[str, ...] = ()

    started_at: datetime = Field(default_factory=_utcnow)
    ended_at: datetime | None = None
    error: str | None = None
