"""Typed results for one Scheduler-backed online computation epoch.

The online controller decides *what/when* should run.  The authoritative
Scheduler still decides *who/where* and is the only component allowed to
create Claims, Attempts, resource reservations, and Kernel Leases.

These immutable DTOs make that boundary explicit.  In particular, an
``OnlineEpochDispatch`` can only be constructed after the Scheduler has
returned an exact live Claim/Attempt binding; an ownerless controller
``START`` proposal is never represented as an executable Harness session.
"""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

from .computation_control import ComputationControlAudit

ONLINE_EPOCH_SCHEMA_VERSION: Final[Literal["online-epoch.v1"]] = "online-epoch.v1"
ONLINE_EPOCH_HARNESS_SCHEMA_VERSION: Final[Literal["online-epoch-harness.v1"]] = (
    "online-epoch-harness.v1"
)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class OnlineEpochStatus(StrEnum):
    """Terminal status of one bounded online scheduling epoch."""

    PLANNED_ONLY = "planned_only"
    CLAIMS_ACQUIRED = "claims_acquired"
    CLAIMS_RELEASED = "claims_released"
    CLEANUP_REQUIRED = "cleanup_required"
    NOOP = "noop"
    GRAPH_CHANGED = "graph_changed"
    POLICY_REJECTED = "policy_rejected"


class OnlineEpochDispatch(_FrozenModel):
    """Exact Scheduler/Kernel ownership produced for one selected task."""

    schema_version: Literal["online-epoch.v1"] = ONLINE_EPOCH_SCHEMA_VERSION
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    semantic_epoch: StrictInt = Field(ge=0)
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    process_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    lease_fencing_token: StrictInt = Field(ge=1)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class OnlineEpochSkip(_FrozenModel):
    """One authoritative Scheduler rejection/defer reason."""

    task_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class OnlineEpochScheduleResult(_FrozenModel):
    """Immutable transcript of policy planning plus Scheduler admission.

    ``claims_retained`` means the caller now owns cleanup of the returned
    exact Claim identities.  The result itself never executes user code and
    never implies semantic verification.
    """

    schema_version: Literal["online-epoch.v1"] = ONLINE_EPOCH_SCHEMA_VERSION
    status: OnlineEpochStatus
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    control_audit: ComputationControlAudit
    selected_task_ids: tuple[str, ...] = ()
    dispatches: tuple[OnlineEpochDispatch, ...] = ()
    skipped: tuple[OnlineEpochSkip, ...] = ()
    released_claim_ids: tuple[str, ...] = ()
    retained_claim_ids: tuple[str, ...] = ()
    scheduler_invoked: StrictBool = False
    claims_retained: StrictBool = False
    scheduling_audit_persisted: StrictBool = False
    reason: str = ""
    result_hash: str = Field(min_length=64, max_length=64)

    @model_validator(mode="after")
    def _ownership_sets_are_coherent(self) -> OnlineEpochScheduleResult:
        dispatch_ids = tuple(item.claim_id for item in self.dispatches)
        dispatch_set = set(dispatch_ids)
        dispatch_task_ids = tuple(item.task_id for item in self.dispatches)
        dispatch_task_set = set(dispatch_task_ids)
        selected = set(self.selected_task_ids)
        released = set(self.released_claim_ids)
        retained = set(self.retained_claim_ids)
        if len(dispatch_ids) != len(dispatch_set):
            raise ValueError("online epoch dispatch claim ids must be unique")
        if len(dispatch_task_ids) != len(dispatch_task_set):
            raise ValueError("online epoch dispatch task ids must be unique")
        if len(self.selected_task_ids) != len(selected):
            raise ValueError("online epoch selected task ids must be unique")
        if not dispatch_task_set <= selected:
            raise ValueError("online epoch dispatch tasks must be selected by the policy")
        if any(item.graph_id != self.graph_id for item in self.dispatches):
            raise ValueError("online epoch dispatch graph_id must match result graph_id")
        if self.control_audit.graph_id != self.graph_id:
            raise ValueError("online epoch control audit graph_id must match result graph_id")
        if len(self.released_claim_ids) != len(released):
            raise ValueError("online epoch released Claim ids must be unique")
        if len(self.retained_claim_ids) != len(retained):
            raise ValueError("online epoch retained Claim ids must be unique")
        if released & retained:
            raise ValueError("released and retained Claim ids must be disjoint")
        if not (released | retained) <= dispatch_set:
            raise ValueError("released/retained Claim ids must come from dispatches")
        if dispatch_set and not self.scheduler_invoked:
            raise ValueError("epochs with dispatches must invoke the Scheduler")
        if dispatch_set and (released | retained) != dispatch_set:
            raise ValueError("released/retained Claim ids must partition dispatched Claims")
        if not dispatch_set and (released or retained):
            raise ValueError("ownership Claim ids require at least one dispatch")
        if self.claims_retained != bool(retained):
            raise ValueError("claims_retained must match retained_claim_ids")
        if not self.scheduler_invoked and (self.dispatches or released or retained):
            raise ValueError("non-scheduled epochs cannot carry ownership")
        if self.status is OnlineEpochStatus.PLANNED_ONLY and self.scheduler_invoked:
            raise ValueError("planned-only epochs cannot invoke the Scheduler")
        if self.status is OnlineEpochStatus.POLICY_REJECTED and self.scheduler_invoked:
            raise ValueError("policy-rejected epochs cannot invoke the Scheduler")
        if (
            self.status
            in {
                OnlineEpochStatus.PLANNED_ONLY,
                OnlineEpochStatus.POLICY_REJECTED,
                OnlineEpochStatus.NOOP,
            }
            and dispatch_set
        ):
            raise ValueError(f"{self.status.value} epochs cannot carry Scheduler dispatches")
        if self.status is OnlineEpochStatus.CLAIMS_ACQUIRED and not retained:
            raise ValueError("claims-acquired status requires retained ownership")
        if self.status is OnlineEpochStatus.CLAIMS_ACQUIRED and (
            not self.scheduler_invoked or not dispatch_set or retained != dispatch_set or released
        ):
            raise ValueError("claims-acquired status requires all dispatched Claims to be retained")
        if self.status is OnlineEpochStatus.CLAIMS_RELEASED and (not released or retained):
            raise ValueError(
                "claims-released status requires released ownership and no retained Claim"
            )
        if self.status is OnlineEpochStatus.CLAIMS_RELEASED and (
            not self.scheduler_invoked or released != dispatch_set
        ):
            raise ValueError(
                "claims-released status requires every dispatched Claim to be released"
            )
        if self.status is OnlineEpochStatus.CLEANUP_REQUIRED and not retained:
            raise ValueError("cleanup-required status requires retained ownership")
        if self.status is OnlineEpochStatus.CLEANUP_REQUIRED and (
            not self.scheduler_invoked or not dispatch_set
        ):
            raise ValueError("cleanup-required status requires Scheduler dispatches")
        return self

    @property
    def release_required(self) -> bool:
        return self.claims_retained and bool(self.retained_claim_ids)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def create(
        cls,
        *,
        status: OnlineEpochStatus,
        graph_id: str,
        graph_version: int,
        control_audit: ComputationControlAudit,
        selected_task_ids: tuple[str, ...] = (),
        dispatches: tuple[OnlineEpochDispatch, ...] = (),
        skipped: tuple[OnlineEpochSkip, ...] = (),
        released_claim_ids: tuple[str, ...] = (),
        retained_claim_ids: tuple[str, ...] = (),
        scheduler_invoked: bool = False,
        claims_retained: bool = False,
        scheduling_audit_persisted: bool = False,
        reason: str = "",
    ) -> OnlineEpochScheduleResult:
        payload = {
            "schema_version": ONLINE_EPOCH_SCHEMA_VERSION,
            "status": status.value,
            "graph_id": graph_id,
            "graph_version": graph_version,
            "control_audit": control_audit,
            "selected_task_ids": selected_task_ids,
            "dispatches": dispatches,
            "skipped": skipped,
            "released_claim_ids": released_claim_ids,
            "retained_claim_ids": retained_claim_ids,
            "scheduler_invoked": scheduler_invoked,
            "claims_retained": claims_retained,
            "scheduling_audit_persisted": scheduling_audit_persisted,
            "reason": reason,
        }
        return cls(
            status=status,
            graph_id=graph_id,
            graph_version=graph_version,
            control_audit=control_audit,
            selected_task_ids=selected_task_ids,
            dispatches=dispatches,
            skipped=skipped,
            released_claim_ids=released_claim_ids,
            retained_claim_ids=retained_claim_ids,
            scheduler_invoked=scheduler_invoked,
            claims_retained=claims_retained,
            scheduling_audit_persisted=scheduling_audit_persisted,
            reason=reason,
            result_hash=_hash_payload(payload),
        )


class OnlineEpochReleaseResult(_FrozenModel):
    """Exact-claim cleanup result for a retained online epoch."""

    schema_version: Literal["online-epoch.v1"] = ONLINE_EPOCH_SCHEMA_VERSION
    graph_id: str = Field(min_length=1)
    requested_claim_ids: tuple[str, ...] = ()
    released_claim_ids: tuple[str, ...] = ()
    already_terminal_claim_ids: tuple[str, ...] = ()
    not_released_claim_ids: tuple[str, ...] = ()
    reason: str = Field(min_length=1)
    result_hash: str = Field(min_length=64, max_length=64)

    @model_validator(mode="after")
    def _release_sets_are_coherent(self) -> OnlineEpochReleaseResult:
        requested = set(self.requested_claim_ids)
        released = set(self.released_claim_ids)
        already_terminal = set(self.already_terminal_claim_ids)
        not_released = set(self.not_released_claim_ids)
        if len(requested) != len(self.requested_claim_ids):
            raise ValueError("requested Claim ids must be unique")
        if len(already_terminal) != len(self.already_terminal_claim_ids):
            raise ValueError("already-terminal Claim ids must be unique")
        if (
            released & already_terminal
            or released & not_released
            or already_terminal & not_released
        ):
            raise ValueError("release outcome Claim ids must be pairwise disjoint")
        if released | already_terminal | not_released != requested:
            raise ValueError("release outcomes must partition the requested Claim ids")
        return self

    @property
    def complete(self) -> bool:
        return not self.not_released_claim_ids

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def create(
        cls,
        *,
        graph_id: str,
        requested_claim_ids: tuple[str, ...],
        released_claim_ids: tuple[str, ...],
        not_released_claim_ids: tuple[str, ...],
        reason: str,
        already_terminal_claim_ids: tuple[str, ...] = (),
    ) -> OnlineEpochReleaseResult:
        payload = {
            "schema_version": ONLINE_EPOCH_SCHEMA_VERSION,
            "graph_id": graph_id,
            "requested_claim_ids": requested_claim_ids,
            "released_claim_ids": released_claim_ids,
            "already_terminal_claim_ids": already_terminal_claim_ids,
            "not_released_claim_ids": not_released_claim_ids,
            "reason": reason,
        }
        return cls(
            graph_id=graph_id,
            requested_claim_ids=requested_claim_ids,
            released_claim_ids=released_claim_ids,
            already_terminal_claim_ids=already_terminal_claim_ids,
            not_released_claim_ids=not_released_claim_ids,
            reason=reason,
            result_hash=_hash_payload(payload),
        )


class OnlineEpochHarnessHandoffStatus(StrEnum):
    """Outcome of binding retained Scheduler ownership to Harness sessions."""

    BOUND = "bound"
    REPLAYED = "replayed"
    PARTIAL = "partial"
    REFUSED = "refused"
    FAILED_CLOSED = "failed_closed"


class OnlineEpochHarnessBinding(_FrozenModel):
    """Auditable exact Claim/Attempt/Lease identity bound to one Harness."""

    schema_version: Literal["online-epoch-harness.v1"] = ONLINE_EPOCH_HARNESS_SCHEMA_VERSION
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    semantic_epoch: StrictInt = Field(ge=0)
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    process_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    lease_fencing_token: StrictInt = Field(ge=1)
    session_id: str = Field(min_length=1)
    snapshot_revision: StrictInt = Field(ge=0)
    snapshot_state: str = Field(min_length=1)
    outcome: Literal["bound", "replayed"]


class OnlineEpochHarnessHandoffResult(_FrozenModel):
    """Immutable transcript for a retained online-epoch Harness handoff.

    The DTO records only ownership identities and bounded diagnostics.  It
    does not claim that a Harness has executed, verified, or committed VPG
    Evidence; those remain separate lifecycle operations.
    """

    schema_version: Literal["online-epoch-harness.v1"] = ONLINE_EPOCH_HARNESS_SCHEMA_VERSION
    status: OnlineEpochHarnessHandoffStatus
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    source_result_hash: str = Field(min_length=64, max_length=64)
    requested_claim_ids: tuple[str, ...] = ()
    bound_claim_ids: tuple[str, ...] = ()
    replayed_claim_ids: tuple[str, ...] = ()
    refused_claim_ids: tuple[str, ...] = ()
    bindings: tuple[OnlineEpochHarnessBinding, ...] = ()
    errors: tuple[str, ...] = ()
    reason: str = Field(min_length=1)
    result_hash: str = Field(min_length=64, max_length=64)

    @model_validator(mode="after")
    def _coherent(self) -> OnlineEpochHarnessHandoffResult:
        requested = set(self.requested_claim_ids)
        bound = set(self.bound_claim_ids)
        replayed = set(self.replayed_claim_ids)
        refused = set(self.refused_claim_ids)
        if len(requested) != len(self.requested_claim_ids):
            raise ValueError("requested Harness Claim ids must be unique")
        if len(bound) != len(self.bound_claim_ids):
            raise ValueError("bound Harness Claim ids must be unique")
        if len(replayed) != len(self.replayed_claim_ids):
            raise ValueError("replayed Harness Claim ids must be unique")
        if len(refused) != len(self.refused_claim_ids):
            raise ValueError("refused Harness Claim ids must be unique")
        if bound & replayed or bound & refused or replayed & refused:
            raise ValueError("Harness handoff Claim outcomes must be disjoint")
        if not (bound | replayed | refused) <= requested:
            raise ValueError("Harness handoff outcomes must reference requested Claims")
        binding_ids = tuple(item.claim_id for item in self.bindings)
        if len(binding_ids) != len(set(binding_ids)):
            raise ValueError("Harness handoff bindings must have unique Claim ids")
        if set(binding_ids) != bound | replayed:
            raise ValueError("Harness handoff bindings must match successful Claims")
        if any(item.graph_id != self.graph_id for item in self.bindings):
            raise ValueError("Harness binding graph_id must match handoff graph_id")
        if any(item.graph_version != self.graph_version for item in self.bindings):
            raise ValueError("Harness binding graph_version must match handoff graph_version")
        if self.status is OnlineEpochHarnessHandoffStatus.BOUND and (
            not requested or bound != requested or replayed or refused
        ):
            raise ValueError("bound status requires every requested Claim to be successfully bound")
        if self.status is OnlineEpochHarnessHandoffStatus.REPLAYED and (
            not requested or replayed != requested or bound or refused
        ):
            raise ValueError("replayed status requires every requested Claim to replay")
        if self.status is OnlineEpochHarnessHandoffStatus.PARTIAL and (
            not bound or not replayed or refused or (bound | replayed) != requested
        ):
            raise ValueError("partial status requires a complete mixed new/replayed binding")
        if self.status in {
            OnlineEpochHarnessHandoffStatus.REFUSED,
            OnlineEpochHarnessHandoffStatus.FAILED_CLOSED,
        } and (bound or replayed):
            raise ValueError("refused/failed-closed status cannot carry successful bindings")
        return self

    @property
    def complete(self) -> bool:
        return bool(self.requested_claim_ids) and not self.refused_claim_ids

    @classmethod
    def create(
        cls,
        *,
        status: OnlineEpochHarnessHandoffStatus,
        graph_id: str,
        graph_version: int,
        source_result_hash: str,
        requested_claim_ids: tuple[str, ...] = (),
        bound_claim_ids: tuple[str, ...] = (),
        replayed_claim_ids: tuple[str, ...] = (),
        refused_claim_ids: tuple[str, ...] = (),
        bindings: tuple[OnlineEpochHarnessBinding, ...] = (),
        errors: tuple[str, ...] = (),
        reason: str,
    ) -> OnlineEpochHarnessHandoffResult:
        payload = {
            "schema_version": ONLINE_EPOCH_HARNESS_SCHEMA_VERSION,
            "status": status.value,
            "graph_id": graph_id,
            "graph_version": graph_version,
            "source_result_hash": source_result_hash,
            "requested_claim_ids": requested_claim_ids,
            "bound_claim_ids": bound_claim_ids,
            "replayed_claim_ids": replayed_claim_ids,
            "refused_claim_ids": refused_claim_ids,
            "bindings": bindings,
            "errors": errors,
            "reason": reason,
        }
        return cls(
            status=status,
            graph_id=graph_id,
            graph_version=graph_version,
            source_result_hash=source_result_hash,
            requested_claim_ids=requested_claim_ids,
            bound_claim_ids=bound_claim_ids,
            replayed_claim_ids=replayed_claim_ids,
            refused_claim_ids=refused_claim_ids,
            bindings=bindings,
            errors=errors,
            reason=reason,
            result_hash=_hash_payload(payload),
        )


def _hash_payload(value: Any) -> str:
    canonical = json.dumps(
        _json_compatible(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, tuple):
        return [_json_compatible(item) for item in value]
    if isinstance(value, list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _json_compatible(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    return value


__all__ = [
    "ONLINE_EPOCH_HARNESS_SCHEMA_VERSION",
    "ONLINE_EPOCH_SCHEMA_VERSION",
    "OnlineEpochDispatch",
    "OnlineEpochHarnessBinding",
    "OnlineEpochHarnessHandoffResult",
    "OnlineEpochHarnessHandoffStatus",
    "OnlineEpochReleaseResult",
    "OnlineEpochScheduleResult",
    "OnlineEpochSkip",
    "OnlineEpochStatus",
]
