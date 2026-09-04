"""D2 Multi-Agent Scheduler domain models.

Immutable-by-default Pydantic models for the scheduling domain.  All
snapshots used for audit / hashing are deterministic: sets are sorted
before encoding, tuples stored in insertion-stable order.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
    PREEMPTED = "preempted"
    FAILED = "failed"
    CRASHED = "crashed"
    STALE_COGNITION = "stale_cognition"
    SUCCEEDED_OPERATIONALLY = "succeeded_operationally"
    VERIFIED_SEMANTICALLY = "verified_semantically"


# ── Resource accounting ─────────────────────────────────────────────────────
class ClaimHandoffStatus(StrEnum):
    """Bounded outcome of one exact-claim ownership handoff."""

    TRANSFERRED = "transferred"
    REPLAYED = "replayed"
    REFUSED = "refused"
    FAILED_CLOSED = "failed_closed"


class ClaimHandoffResult(BaseModel):
    """Audit-friendly result returned by ``handoff_task``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    handoff_id: str
    status: ClaimHandoffStatus
    graph_id: str
    task_id: str
    source_claim_id: str
    source_attempt_id: str
    replacement_agent_id: str
    replacement_claim_id: str | None = None
    replacement_attempt_id: str | None = None
    source_fencing_token: int | None = None
    replacement_fencing_token: int | None = None
    action: str
    reason: str = ""

    @property
    def transferred(self) -> bool:
        return self.status in {
            ClaimHandoffStatus.TRANSFERRED,
            ClaimHandoffStatus.REPLAYED,
        }


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
    # Optional durable identity for a bounded PREEMPT/REBASE handoff.  A
    # handoff id is never an ownership authority; it only makes retries
    # auditable and lets the Scheduler recognize an already-created
    # replacement claim.
    handoff_id: str | None = None

    state: ClaimState = ClaimState.PROPOSED

    attempt_number: int = 0
    reason: str | None = None

    created_at: datetime = Field(default_factory=_utcnow)
    activated_at: datetime | None = None
    released_at: datetime | None = None


# ── ScheduledExecutionAttempt ────────────────────────────────────────────────
class ResourceBinding(BaseModel):
    """Immutable identity of one resource observed by an Attempt."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: str
    resource_uri: str = ""
    artifact_id: str | None = None
    version: int | None = None
    content_hash: str | None = None
    action_id: str | None = None
    idempotency_key: str | None = None
    source_event_id: str | None = None
    source: str = "runtime"
    known: bool = True
    observed_at: datetime | None = None

    @field_validator("operation", "resource_uri", "source", mode="before")
    @classmethod
    def _normalize_strings(cls, value: Any) -> str:
        return str(value).strip()

    @field_validator(
        "artifact_id",
        "content_hash",
        "action_id",
        "idempotency_key",
        "source_event_id",
        mode="before",
    )
    @classmethod
    def _normalize_optional_strings(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("content_hash")
    @classmethod
    def _normalize_content_hash(cls, value: str | None) -> str | None:
        return None if value is None else value.lower()

    @field_validator("version")
    @classmethod
    def _version_non_negative(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("resource version must be >= 0")
        return value

    @model_validator(mode="after")
    def _identity_is_auditable(self) -> ResourceBinding:
        if not self.operation:
            raise ValueError("resource binding operation must be non-empty")
        if self.known and not any(
            (
                self.resource_uri,
                self.artifact_id,
                self.action_id,
                self.source_event_id,
            )
        ):
            raise ValueError("known resource binding must carry an auditable identity")
        return self

    @property
    def identity(self) -> str | None:
        """Best exact identity of the bound resource, or None if unauditable.

        This is the key space that task-side declarations (``Task.inputs`` /
        ``Task.outputs``) also live in, so agent-side residency and graph-side
        declarations can be intersected without a translation table.
        """
        for field in ("resource_uri", "artifact_id", "action_id", "source_event_id"):
            value = str(getattr(self, field, "") or "").strip()
            if value:
                return value
        return None

    @property
    def sort_key(self) -> tuple[str, str, str, int, str, str, str, str, str]:
        return (
            self.operation,
            self.resource_uri,
            self.artifact_id or "",
            -1 if self.version is None else self.version,
            self.content_hash or "",
            self.action_id or "",
            self.idempotency_key or "",
            self.source_event_id or "",
            "" if self.observed_at is None else self.observed_at.isoformat(),
        )

    @classmethod
    def from_provenance_event(cls, event: Any) -> ResourceBinding:
        """Project a dependency-light ProvenanceEvent-like object."""

        operation = getattr(getattr(event, "op", ""), "value", getattr(event, "op", ""))
        return cls(
            operation=str(operation),
            resource_uri=getattr(event, "resource_uri", ""),
            artifact_id=getattr(event, "artifact_id", None),
            version=getattr(event, "version", None),
            content_hash=getattr(event, "content_hash", None),
            action_id=getattr(event, "action_id", None),
            idempotency_key=getattr(event, "idempotency_key", None),
            source_event_id=getattr(event, "event_id", None),
            source=getattr(event, "source", "runtime"),
            known=bool(getattr(event, "known", True)),
            observed_at=getattr(event, "observed_at", None),
        )

    @classmethod
    def from_context_binding(cls, binding: Any) -> ResourceBinding:
        """Project a Context VM VersionBinding/PageBinding-like object."""

        return cls(
            operation="read",
            resource_uri=getattr(binding, "canonical_uri", ""),
            artifact_id=getattr(binding, "artifact_id", None),
            version=getattr(binding, "version", None),
            content_hash=getattr(binding, "content_hash", None),
            source="context-vm",
        )


class ContextIdentity(BaseModel):
    """Exact Context VM materialization behind an Agent computation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    snapshot_id: str
    manifest_id: str
    manifest_hash: str
    working_set_hash: str
    materialized_hash: str

    @field_validator(
        "snapshot_id",
        "manifest_id",
        "manifest_hash",
        "working_set_hash",
        "materialized_hash",
        mode="before",
    )
    @classmethod
    def _non_empty(cls, value: Any) -> str:
        if value is None:
            raise ValueError("context identity fields must be non-empty")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("context identity fields must be non-empty")
        return normalized

    @field_validator("manifest_hash", "working_set_hash", "materialized_hash")
    @classmethod
    def _normalize_hashes(cls, value: str) -> str:
        return value.lower()

    @classmethod
    def from_source(
        cls,
        source: Any,
        *,
        manifest_id: str | None = None,
    ) -> ContextIdentity:
        """Build from an Attempt, ExecutionContext, or ContextSnapshot."""

        # Do not stringify values before validation.  ``str(None)`` would
        # produce the non-empty literal ``"None"`` and silently turn an
        # incomplete context identity into an apparently valid one.  This
        # helper also treats an explicitly present-but-None primary alias as
        # absent, allowing the ContextSnapshot aliases to be used safely.
        def _first_non_none(*names: str) -> Any:
            for name in names:
                value = getattr(source, name, None)
                if value is not None:
                    return value
            return None

        resolved_manifest_id = (
            manifest_id
            if manifest_id is not None
            else _first_non_none("context_manifest_id", "manifest_id")
        )
        values = {
            "snapshot_id": _first_non_none("context_snapshot_id", "snapshot_id"),
            "manifest_id": resolved_manifest_id,
            "manifest_hash": _first_non_none("context_manifest_hash", "manifest_hash"),
            "working_set_hash": _first_non_none("context_working_set_hash", "working_set_hash"),
            "materialized_hash": _first_non_none("context_materialized_hash", "materialized_hash"),
        }
        missing = [name for name, value in values.items() if value is None]
        if missing:
            raise ValueError("context source is missing identity fields: " + ", ".join(missing))
        return cls.model_validate(values)

    @classmethod
    def from_attempt(cls, attempt: Any) -> ContextIdentity | None:
        values = (
            getattr(attempt, "context_snapshot_id", None),
            getattr(attempt, "context_manifest_id", None),
            getattr(attempt, "context_manifest_hash", None),
            getattr(attempt, "context_working_set_hash", None),
            getattr(attempt, "context_materialized_hash", None),
        )
        if not any(value is not None for value in values):
            return None
        if not all(value is not None and str(value).strip() for value in values):
            raise ValueError("attempt carries a partial context snapshot identity")
        return cls(
            snapshot_id=str(values[0]),
            manifest_id=str(values[1]),
            manifest_hash=str(values[2]),
            working_set_hash=str(values[3]),
            materialized_hash=str(values[4]),
        )


class ComputationCost(BaseModel):
    """Monotonic, integer-only cumulative cost counters."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    model_calls: int = 0
    tool_calls: int = 0
    elapsed_ms: int = 0
    monetary_micros: int = 0

    @field_validator(
        "input_tokens",
        "output_tokens",
        "cached_input_tokens",
        "model_calls",
        "tool_calls",
        "elapsed_ms",
        "monetary_micros",
    )
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("computation cost counters must be >= 0")
        return value

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def dominates(self, earlier: ComputationCost) -> bool:
        fields = (
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "model_calls",
            "tool_calls",
            "elapsed_ms",
            "monetary_micros",
        )
        return all(getattr(self, field) >= getattr(earlier, field) for field in fields)


class AgentSnapshot(BaseModel):
    """Immutable point-in-time cognition/runtime state for one Attempt."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_id: str
    process_id: str
    task_id: str
    claim_id: str
    attempt_id: str
    graph_id: str
    graph_version: int
    semantic_epoch: int

    context_identity: ContextIdentity | None = None
    read_set: tuple[ResourceBinding, ...] = ()
    write_set: tuple[ResourceBinding, ...] = ()
    progress: float = 0.0
    cost: ComputationCost = Field(default_factory=ComputationCost)

    started_at: datetime
    captured_at: datetime = Field(default_factory=_utcnow)
    ended_at: datetime | None = None
    state: AttemptState

    @field_validator(
        "agent_id",
        "process_id",
        "task_id",
        "claim_id",
        "attempt_id",
        "graph_id",
        mode="before",
    )
    @classmethod
    def _identity_non_empty(cls, value: Any) -> str:
        if value is None:
            raise ValueError("AgentSnapshot identity fields must be non-empty")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("AgentSnapshot identity fields must be non-empty")
        return normalized

    @field_validator("graph_version", "semantic_epoch")
    @classmethod
    def _version_non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("graph_version and semantic_epoch must be >= 0")
        return value

    @field_validator("progress")
    @classmethod
    def _progress_range(cls, value: float) -> float:
        normalized = float(value)
        if not 0.0 <= normalized <= 1.0:
            raise ValueError("AgentSnapshot progress must be between 0 and 1")
        return normalized

    @field_validator("read_set", "write_set", mode="before")
    @classmethod
    def _normalize_resource_sets(cls, value: Any) -> tuple[ResourceBinding, ...]:
        if value is None:
            return ()
        if isinstance(value, (ResourceBinding, dict)):
            value = (value,)
        bindings = {
            item if isinstance(item, ResourceBinding) else ResourceBinding.model_validate(item)
            for item in value
        }
        return tuple(sorted(bindings, key=lambda item: item.sort_key))

    @model_validator(mode="after")
    def _timestamps_are_ordered(self) -> AgentSnapshot:
        if self.captured_at < self.started_at:
            raise ValueError("AgentSnapshot captured_at cannot precede started_at")
        if self.ended_at is not None:
            if self.ended_at < self.started_at:
                raise ValueError("AgentSnapshot ended_at cannot precede started_at")
            if self.captured_at < self.ended_at:
                raise ValueError("AgentSnapshot captured_at cannot precede ended_at")
        return self

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def from_attempt(
        cls,
        attempt: Any,
        *,
        execution_context: Any | None = None,
        provenance_events: Iterable[Any] | None = None,
        context_bindings: Iterable[Any] | Any | None = None,
        progress: float = 0.0,
        cost: ComputationCost | dict[str, int] | None = None,
        captured_at: datetime | None = None,
        state: AttemptState | str | None = None,
    ) -> AgentSnapshot:
        """Build from a durable Attempt plus mediated context/provenance."""

        sealed_context = ContextIdentity.from_attempt(attempt)
        observed_context: ContextIdentity | None = None
        if execution_context is not None and getattr(
            execution_context,
            "context_snapshot_id",
            None,
        ):
            observed_context = ContextIdentity.from_source(execution_context)
        if (
            sealed_context is not None
            and observed_context is not None
            and sealed_context != observed_context
        ):
            raise ValueError("execution context identity disagrees with durable attempt")

        events = provenance_events
        if events is None and execution_context is not None:
            events = getattr(execution_context, "events", ())
        reads: list[ResourceBinding] = []
        writes: list[ResourceBinding] = []
        for event in events or ():
            # Provenance is an observation attached to one exact Attempt, not
            # a free-floating bag of resource facts.  A malformed adapter
            # must not inject an event captured under another graph/task/
            # attempt (or another semantic epoch) and thereby manufacture
            # read/write bindings for this snapshot.  ``task_id`` and
            # ``attempt_id`` were historically optional on low-level events;
            # preserve that compatibility for genuinely legacy/unknown
            # observations, but reject any explicit non-empty identity
            # mismatch.  ``graph_id`` is mandatory on ProvenanceEvent and the
            # semantic epoch is always part of its durable identity.
            event_graph_id = str(getattr(event, "graph_id", "") or "").strip()
            event_task_id = str(getattr(event, "task_id", "") or "").strip()
            event_attempt_id = str(getattr(event, "attempt_id", "") or "").strip()
            event_epoch = getattr(event, "semantic_epoch", None)
            if event_graph_id and event_graph_id != str(attempt.graph_id):
                raise ValueError("provenance event graph_id disagrees with execution attempt")
            if event_task_id and event_task_id != str(attempt.task_id):
                raise ValueError("provenance event task_id disagrees with execution attempt")
            if event_attempt_id and event_attempt_id != str(attempt.attempt_id):
                raise ValueError("provenance event attempt_id disagrees with execution attempt")
            if event_epoch is not None and int(event_epoch) != int(attempt.semantic_epoch):
                raise ValueError("provenance event semantic_epoch disagrees with execution attempt")
            binding = ResourceBinding.from_provenance_event(event)
            if binding.operation == "write":
                writes.append(binding)
            elif binding.operation in {"read", "tool", "network", "model", "external"}:
                reads.append(binding)

        resolved_context_bindings = context_bindings
        if resolved_context_bindings is None and execution_context is not None:
            loaded = getattr(execution_context, "loaded_context", None)
            resolved_context_bindings = getattr(loaded, "version_bindings", ())
        if resolved_context_bindings is not None:
            if hasattr(resolved_context_bindings, "version_bindings"):
                resolved_context_bindings = resolved_context_bindings.version_bindings
            reads.extend(
                ResourceBinding.from_context_binding(binding)
                for binding in resolved_context_bindings
            )

        # Context VM bindings are now recorded directly by
        # ``ExecutionContext.bind_context_snapshot``.  Older adapters (and
        # lightweight integrations) may also expose the same bindings through
        # ``loaded_context.version_bindings``.  Merge those observations by
        # exact resource identity so one page cannot appear twice in the
        # durable read-set merely because it crossed two instrumentation
        # surfaces.  Prefer the event-backed binding because it retains the
        # durable source event id and exact observation timestamp.
        reads = _deduplicate_resource_bindings(reads)
        writes = _deduplicate_resource_bindings(writes)

        resolved_cost = (
            cost
            if isinstance(cost, ComputationCost)
            else ComputationCost.model_validate(cost or {})
        )
        resolved_state = (
            state if isinstance(state, AttemptState) else AttemptState(state or attempt.state)
        )
        return cls(
            agent_id=attempt.agent_id,
            process_id=attempt.process_id,
            task_id=attempt.task_id,
            claim_id=attempt.claim_id,
            attempt_id=attempt.attempt_id,
            graph_id=attempt.graph_id,
            graph_version=attempt.graph_version,
            semantic_epoch=attempt.semantic_epoch,
            context_identity=observed_context or sealed_context,
            read_set=tuple(reads),
            write_set=tuple(writes),
            progress=progress,
            cost=resolved_cost,
            started_at=attempt.started_at,
            captured_at=captured_at or _utcnow(),
            ended_at=attempt.ended_at,
            state=resolved_state,
        )


def _deduplicate_resource_bindings(
    bindings: Iterable[ResourceBinding],
) -> list[ResourceBinding]:
    """Deduplicate equivalent observations while retaining strongest proof."""

    selected: dict[
        tuple[str, str, str, int | None, str, str, str, bool],
        ResourceBinding,
    ] = {}
    for binding in bindings:
        key = (
            binding.operation,
            binding.resource_uri,
            binding.artifact_id or "",
            binding.version,
            binding.content_hash or "",
            binding.action_id or "",
            binding.idempotency_key or "",
            binding.known,
        )
        current = selected.get(key)
        if current is None:
            selected[key] = binding
            continue
        # Event-backed observations carry a durable source_event_id and are
        # stronger than a reconstructed Context VM binding.  If both are
        # equally strong, retain the deterministic earliest sort key.
        current_score = (
            bool(current.source_event_id),
            bool(current.observed_at),
            current.source == "context_vm",
        )
        candidate_score = (
            bool(binding.source_event_id),
            bool(binding.observed_at),
            binding.source == "context_vm",
        )
        if candidate_score > current_score:
            selected[key] = binding
    return sorted(selected.values(), key=lambda item: item.sort_key)


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
    # Canonical provenance/coverage digest sealed by the SDK before semantic
    # Evidence commit.  It is optional for legacy scheduler integrations, but
    # once present it is immutable for the lifetime of the attempt.
    provenance_digest: str | None = None
    # Exact Context VM materialization used by this attempt. These fields are
    # bound atomically under the scheduler lifecycle lock before user code
    # runs and remain immutable for the lifetime of the attempt.
    context_snapshot_id: str | None = None
    context_manifest_id: str | None = None
    context_manifest_hash: str | None = None
    context_working_set_hash: str | None = None
    context_materialized_hash: str | None = None
    # Optional for backwards-compatible durable replay of pre-snapshot state.
    agent_snapshot: AgentSnapshot | None = None

    started_at: datetime = Field(default_factory=_utcnow)
    ended_at: datetime | None = None
    error: str | None = None

    @model_validator(mode="after")
    def _agent_snapshot_matches_attempt(self) -> ScheduledExecutionAttempt:
        snapshot = self.agent_snapshot
        if snapshot is None:
            return self
        attempt_identity = (
            self.agent_id,
            self.process_id,
            self.task_id,
            self.claim_id,
            self.attempt_id,
            self.graph_id,
            self.graph_version,
            self.semantic_epoch,
        )
        snapshot_identity = (
            snapshot.agent_id,
            snapshot.process_id,
            snapshot.task_id,
            snapshot.claim_id,
            snapshot.attempt_id,
            snapshot.graph_id,
            snapshot.graph_version,
            snapshot.semantic_epoch,
        )
        if snapshot_identity != attempt_identity:
            raise ValueError("agent snapshot identity disagrees with execution attempt")
        if snapshot.started_at != self.started_at:
            raise ValueError("agent snapshot start time disagrees with execution attempt")
        if snapshot.context_identity != ContextIdentity.from_attempt(self):
            raise ValueError("agent snapshot context disagrees with execution attempt")
        return self
