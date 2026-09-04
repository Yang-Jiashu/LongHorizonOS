"""Bounded Context-rebase runtime bridge.

The Context delta planner, semantic-interrupt router, and Harness protocol are
useful independently, but a long-running Agent needs one small seam that wires
them together.  This module provides that seam without taking ownership away
from the Scheduler/Kernel:

``AgentSnapshot + ContextSnapshot + GraphDelta``
    -> read-set freshness check
    -> Context rebase classification
    -> semantic interrupt observation
    -> fenced Harness control request

The bridge is deliberately conservative.  It never discovers hidden
dependencies, mutates the VPG, creates Claims/Leases, or pretends that a claim
handoff is atomic.  An optional handoff callback is invoked by ``apply`` only
after the Harness request has been acknowledged; callers must treat the
callback and Harness transition as a release-then-acquire/non-atomic boundary.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Final, Literal, Protocol, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    ValidationError,
)

from lhos.agent_os.context.models import ContextManifest, ContextSnapshot
from lhos.runtimes.multi_agent.models import AgentSnapshot

from .context_delta import (
    ContextGraphDelta,
    ContextRebaseAction,
    RebasePlan,
    plan_context_rebase,
)
from .harness import (
    HarnessControlRequest,
    HarnessControlResult,
    HarnessOperation,
    HarnessSessionAdapter,
    HarnessSessionSnapshot,
    HarnessSessionState,
)
from .semantic_interrupt import (
    SemanticInterrupt,
    SemanticInterruptKind,
)

REBASE_RUNTIME_SCHEMA_VERSION: Final[Literal["rebase-runtime.v1"]] = "rebase-runtime.v1"
REBASE_RUNTIME_POLICY_ID: Final[str] = "context-rebase-runtime.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CommitFreshnessStatus(StrEnum):
    """Outcome of the pre-commit read-set check."""

    FRESH = "fresh"
    STALE = "stale"
    BLOCKED = "blocked"


class CommitFreshness(_FrozenModel):
    """Auditable result of validating one Agent read-set against a delta."""

    schema_version: str = REBASE_RUNTIME_SCHEMA_VERSION
    status: CommitFreshnessStatus
    graph_id: str
    observed_graph_version: StrictInt = Field(ge=0)
    current_graph_version: StrictInt = Field(ge=0)
    stale_ref_ids: tuple[str, ...] = ()
    unknown_ref_ids: tuple[str, ...] = ()
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.status is CommitFreshnessStatus.FRESH

    @property
    def rejected(self) -> bool:
        return not self.allowed

    @property
    def is_stale(self) -> bool:
        return self.status is CommitFreshnessStatus.STALE


class RebaseRuntimeDecision(_FrozenModel):
    """One deterministic decision for a live Agent/Harness session."""

    schema_version: str = REBASE_RUNTIME_SCHEMA_VERSION
    policy_id: str = REBASE_RUNTIME_POLICY_ID
    graph_id: str
    task_id: str
    agent_id: str
    attempt_id: str
    action: ContextRebaseAction
    plan: RebasePlan
    freshness: CommitFreshness
    interrupt: SemanticInterrupt | None = None
    control_request: HarnessControlRequest | None = None
    reason: str = ""
    non_atomic_handoff: bool = True

    @property
    def blocked(self) -> bool:
        return self.action is ContextRebaseAction.BLOCKED

    @property
    def request(self) -> HarnessControlRequest | None:
        """Compatibility alias for callers that call it simply ``request``."""

        return self.control_request

    @property
    def affected_ref_ids(self) -> tuple[str, ...]:
        return self.plan.affected_ref_ids


class RebaseRuntimeApplyResult(_FrozenModel):
    """Result of optionally applying a decision to a Harness and handoff hook."""

    schema_version: str = REBASE_RUNTIME_SCHEMA_VERSION
    decision: RebaseRuntimeDecision
    harness_result: HarnessControlResult | None = None
    handoff_result: Any = None
    handoff_attempted: bool = False
    non_atomic_handoff: bool = True
    reason: str = ""

    @property
    def applied(self) -> bool:
        return bool(self.harness_result and self.harness_result.applied)


class LiveContextRebasePlan(_FrozenModel):
    """Authority-backed plan for one exact live Claim/Harness session.

    ``decision`` is produced by :class:`RebaseRuntimeBridge`, while the
    surrounding identities are copied from the Scheduler, durable
    ``AgentSnapshot``, Context VM, and registered Harness at plan time.  They
    are intentionally redundant: :meth:`AgentOS.apply_live_context_rebase`
    revalidates every field before entering Harness code.
    """

    schema_version: Literal["rebase-runtime.v1"] = REBASE_RUNTIME_SCHEMA_VERSION
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    source_graph_version: StrictInt = Field(ge=0)
    target_semantic_epoch: StrictInt = Field(ge=0)
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    process_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    source_semantic_epoch: StrictInt = Field(ge=0)
    context_snapshot_id: str = Field(min_length=1)
    context_snapshot_hash: str = Field(min_length=64, max_length=64)
    agent_snapshot_fingerprint: str = Field(min_length=64, max_length=64)
    harness_session_id: str = Field(min_length=1)
    harness_revision: StrictInt = Field(ge=0)
    decision: RebaseRuntimeDecision
    graph_delta_hash: str = Field(min_length=64, max_length=64)
    plan_hash: str = Field(min_length=64, max_length=64)
    bounded: StrictBool = True
    non_atomic_handoff: StrictBool = True


class LiveContextRebaseApplyResult(_FrozenModel):
    """Bounded result of applying an authority-backed live rebase plan."""

    schema_version: Literal["rebase-runtime.v1"] = REBASE_RUNTIME_SCHEMA_VERSION
    plan: LiveContextRebasePlan
    harness_result: HarnessControlResult | None = None
    # ``handoff_result`` is intentionally untyped at this boundary to avoid
    # coupling the SDK DTO to one Scheduler implementation.  When
    # ``handoff_attempted`` is true it is an
    # ``OwnershipHandoffResult``-compatible object (or a bounded recovery
    # witness) returned by the authoritative Scheduler.
    handoff_result: Any = None
    handoff_id: str | None = None
    handoff_attempted: StrictBool = False
    handoff_replayed: StrictBool = False
    replacement_claim_id: str | None = None
    replacement_attempt_id: str | None = None
    recovery_required: StrictBool = False
    applied: StrictBool = False
    replayed: StrictBool = False
    refused: StrictBool = False
    ownership_unchanged: StrictBool = True
    bounded: StrictBool = True
    non_atomic_handoff: StrictBool = True
    reason: str = ""


class HandoffCallback(Protocol):
    """Optional ownership callback.

    Implementations may synchronously or asynchronously release the old claim
    and admit a replacement.  The callback receives the immutable decision;
    it is intentionally not part of the Harness/VPG transaction.
    """

    def __call__(self, decision: RebaseRuntimeDecision) -> Any: ...


def _coerce_agent(value: AgentSnapshot | Mapping[str, Any]) -> AgentSnapshot:
    if isinstance(value, AgentSnapshot):
        return value
    return AgentSnapshot.model_validate(value)


def _coerce_context(
    value: ContextSnapshot | ContextManifest | Mapping[str, Any] | None,
) -> ContextSnapshot | ContextManifest | None:
    if value is None or isinstance(value, (ContextSnapshot, ContextManifest)):
        return value
    # ContextSnapshot and ContextManifest have disjoint required fields.  Try
    # the snapshot first, then the manifest for friendly mapping callers.
    try:
        return ContextSnapshot.model_validate(value)
    except ValidationError:
        return ContextManifest.model_validate(value)


def _coerce_version(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _context_bindings(
    agent: AgentSnapshot,
    context: ContextSnapshot | ContextManifest | None,
) -> tuple[Any, ...]:
    """Merge mediated Agent read bindings with Context VM page bindings."""

    values: list[Any] = list(agent.read_set)
    if context is not None:
        if isinstance(context, ContextSnapshot):
            values.extend(context.page_bindings)
        else:
            values.extend(context.refs)
    # Keep exact identities once.  The planner performs a second identity
    # check and will fail closed if the same ref id carries conflicting data.
    result: list[Any] = []
    seen: set[tuple[str, str, int | None, str]] = set()
    for value in values:
        uri = str(getattr(value, "canonical_uri", getattr(value, "resource_uri", "")) or "").strip()
        artifact = str(getattr(value, "artifact_id", "") or "").strip()
        version = getattr(value, "version", None)
        digest = str(getattr(value, "content_hash", "") or "").strip().lower()
        key = (uri, artifact, version, digest)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return tuple(result)


def _context_identity_matches(
    agent: AgentSnapshot,
    context: ContextSnapshot | ContextManifest | None,
) -> bool:
    """Check fields that both AgentSnapshot and ContextSnapshot expose."""

    identity = agent.context_identity
    if context is None or isinstance(context, ContextManifest):
        return True
    if identity is None:
        # A caller supplied a concrete ContextSnapshot but the Agent did not
        # seal its identity.  We cannot prove that the snapshot is what the
        # Agent actually reasoned over, so fail closed.
        return False
    checks = (
        ("snapshot_id", "snapshot_id"),
        ("manifest_hash", "manifest_hash"),
        ("working_set_hash", "working_set_hash"),
        ("materialized_hash", "materialized_hash"),
    )
    return all(
        str(getattr(identity, left, "")).strip().lower()
        == str(getattr(context, right, "")).strip().lower()
        for left, right in checks
    )


def _delta_has_explicit_changes(delta: ContextGraphDelta) -> bool:
    return bool(
        delta.effective_ref_ids or delta.effective_resource_keys or delta.effective_artifact_ids
    )


def _strict_read_set_unknown_ids(agent: AgentSnapshot) -> tuple[str, ...]:
    """Return read bindings that cannot carry an exact freshness guard."""

    unknown: list[str] = []
    for index, binding in enumerate(agent.read_set):
        identity = str(binding.resource_uri or binding.artifact_id or "").strip() or f"read-{index}"
        if not binding.known:
            unknown.append(identity)
            continue
        # A version/hash pair is required to prove cognition freshness.  Tool
        # observations with no artifact identity are intentionally conservative.
        if (
            not str(binding.artifact_id or "").strip()
            or binding.version is None
            or binding.version < 1
            or not str(binding.content_hash or "").strip()
        ):
            unknown.append(identity)
    return tuple(sorted(set(unknown)))


def validate_read_set_freshness(
    agent_snapshot: AgentSnapshot | Mapping[str, Any],
    *,
    current_graph_version: int,
    graph_delta: Any = None,
    context_snapshot: ContextSnapshot | ContextManifest | Mapping[str, Any] | None = None,
) -> CommitFreshness:
    """Validate cognition before semantic commit.

    This is a control-plane guard, not a replacement for the VPG/FactsProvider
    transactional guard.  A caller should run this check before staging output
    and still invoke the authoritative Scheduler/VPG commit fence.

    A graph-version advance can be declared fresh only from a ``complete``
    delta.  ``partial`` coverage describes the changes that were observed, not
    proof that every unlisted read binding remained unchanged.  Same-version
    empty partial observations remain accepted for backwards compatibility.
    """

    agent = _coerce_agent(agent_snapshot)
    current = _coerce_version(current_graph_version, name="current_graph_version")
    delta = ContextGraphDelta.from_any(graph_delta)
    graph_id = str(agent.graph_id).strip()
    old = int(agent.graph_version)

    if delta.graph_id and delta.graph_id != graph_id:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="graph delta belongs to a different graph",
        )
    if current < old:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="AgentSnapshot graph version is newer than current graph",
        )
    if not _context_identity_matches(agent, _coerce_context(context_snapshot)):
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="ContextSnapshot identity disagrees with AgentSnapshot",
        )

    explicit = _delta_has_explicit_changes(delta)
    # Keep the missing-delta case distinct in the audit trail.  ``from_any``
    # normalizes ``None`` into a partial delta, so this must run before the
    # generic incomplete-coverage guard below.
    if current > old and graph_delta is None:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="graph advanced without an explicit delta",
        )
    # Unknown coverage is unsafe even at the same graph version: the producer
    # explicitly says it cannot enumerate what may have changed.
    if not delta.known or delta.coverage == "unknown":
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            unknown_ref_ids=_strict_read_set_unknown_ids(agent),
            reason="graph delta coverage is unknown",
        )
    if current > old and delta.coverage != "complete":
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="graph delta coverage is not complete for commit freshness",
        )
    if current == old and explicit:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason="delta reports changes without advancing graph version",
        )
    unknown = _strict_read_set_unknown_ids(agent)
    if unknown:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            unknown_ref_ids=unknown,
            reason="one or more read bindings lack an exact version/hash",
        )
    try:
        plan = plan_context_rebase(
            old,
            current,
            old_bindings=_context_bindings(agent, _coerce_context(context_snapshot)),
            graph_delta=delta,
        )
    except (TypeError, ValueError) as exc:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            reason=f"context delta could not be validated: {exc}",
        )
    unknown_ids = tuple(plan.context_delta.unknown_ref_ids)
    if unknown_ids:
        return CommitFreshness(
            status=CommitFreshnessStatus.BLOCKED,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            unknown_ref_ids=unknown_ids,
            reason="one or more read bindings cannot be matched authoritatively",
        )
    if plan.affected_ref_ids:
        return CommitFreshness(
            status=CommitFreshnessStatus.STALE,
            graph_id=graph_id,
            observed_graph_version=old,
            current_graph_version=current,
            stale_ref_ids=plan.affected_ref_ids,
            reason="one or more read bindings changed since cognition began",
        )
    return CommitFreshness(
        status=CommitFreshnessStatus.FRESH,
        graph_id=graph_id,
        observed_graph_version=old,
        current_graph_version=current,
        reason="all mediated read bindings remain current",
    )


def _make_interrupt(
    agent: AgentSnapshot,
    delta: ContextGraphDelta,
    *,
    graph_version: int,
    reason: str,
) -> SemanticInterrupt:
    return SemanticInterrupt(
        graph_id=agent.graph_id,
        graph_version=graph_version,
        kind=SemanticInterruptKind.ARTIFACT_CHANGED,
        reason=reason or delta.reason or "context bindings changed",
        source_task_id=agent.task_id,
        affected_task_ids=(agent.task_id,),
        metadata={
            "delta_coverage": delta.coverage,
            "delta_known": delta.known,
            "changed_ref_ids": sorted(delta.effective_ref_ids),
            "changed_resource_keys": sorted(delta.effective_resource_keys),
            "changed_artifact_ids": sorted(delta.effective_artifact_ids),
        },
    )


def _identity_matches_agent(
    session: HarnessSessionSnapshot,
    agent: AgentSnapshot,
) -> bool:
    identity = session.identity
    return (
        identity.graph_id == agent.graph_id
        and identity.graph_version == agent.graph_version
        and identity.task_id == agent.task_id
        and identity.agent_id == agent.agent_id
        and identity.claim_id == agent.claim_id
        and identity.attempt_id == agent.attempt_id
        and identity.semantic_epoch == agent.semantic_epoch
    )


def _build_control_request(
    harness: HarnessSessionAdapter,
    agent: AgentSnapshot,
    *,
    action: ContextRebaseAction,
    plan: RebasePlan,
    new_graph_version: int,
    target_semantic_epoch: int | None,
    reason: str,
) -> HarnessControlRequest | None:
    snapshot = harness.snapshot
    if not _identity_matches_agent(snapshot, agent):
        return None
    if action is ContextRebaseAction.REUSE:
        if snapshot.state is HarnessSessionState.CREATED:
            operation = HarnessOperation.START
        elif snapshot.state in {
            HarnessSessionState.RUNNING,
            HarnessSessionState.CHECKPOINTED,
        }:
            operation = HarnessOperation.CONTINUE
        else:
            return None
        if not harness.capabilities.supports(operation):
            return None
        expected_checkpoint = (
            snapshot.checkpoint_id if snapshot.state is HarnessSessionState.CHECKPOINTED else None
        )
        return HarnessControlRequest(
            operation=operation,
            session=snapshot.identity,
            expected_revision=snapshot.revision,
            expected_checkpoint_id=expected_checkpoint,
            reason=reason or "reuse current cognition",
            payload={
                "decision": action.value,
                "preserve_ref_ids": list(plan.preserve_ref_ids),
            },
        )

    if action not in {
        ContextRebaseAction.REBASE,
        ContextRebaseAction.FULL_RELOAD,
    } or not harness.capabilities.supports(HarnessOperation.REBASE):
        return None
    target_epoch = (
        agent.semantic_epoch + 1
        if target_semantic_epoch is None
        else _coerce_version(target_semantic_epoch, name="target_semantic_epoch")
    )
    if new_graph_version < snapshot.identity.graph_version:
        return None
    if (
        new_graph_version == snapshot.identity.graph_version
        and target_epoch <= snapshot.identity.semantic_epoch
    ):
        return None
    expected_checkpoint = (
        snapshot.checkpoint_id if snapshot.state is HarnessSessionState.CHECKPOINTED else None
    )
    return HarnessControlRequest(
        operation=HarnessOperation.REBASE,
        session=snapshot.identity,
        expected_revision=snapshot.revision,
        expected_checkpoint_id=expected_checkpoint,
        target_graph_version=new_graph_version,
        target_semantic_epoch=target_epoch,
        reason=reason or plan.reason,
        payload={
            "decision": action.value,
            "preserve_ref_ids": list(plan.preserve_ref_ids),
            "reload_ref_ids": list(plan.reload_ref_ids),
            "blocked_ref_ids": list(plan.blocked_ref_ids),
            "context_delta": plan.context_delta.as_dict(),
            "non_atomic_handoff": True,
        },
    )


class RebaseRuntimeBridge:
    """Compose context planning, interrupt creation, and Harness requests."""

    def __init__(self, *, handoff_callback: HandoffCallback | None = None) -> None:
        self._handoff_callback = handoff_callback

    def plan(
        self,
        agent_snapshot: AgentSnapshot | Mapping[str, Any],
        context_snapshot: ContextSnapshot | ContextManifest | Mapping[str, Any] | None = None,
        graph_delta: Any = None,
        *,
        current_graph_version: int | None = None,
        new_graph_version: int | None = None,
        target_semantic_epoch: int | None = None,
        harness: HarnessSessionAdapter | None = None,
        reason: str = "",
    ) -> RebaseRuntimeDecision:
        agent = _coerce_agent(agent_snapshot)
        context = _coerce_context(context_snapshot)
        if (
            current_graph_version is not None
            and new_graph_version is not None
            and current_graph_version != new_graph_version
        ):
            raise ValueError("current_graph_version and new_graph_version disagree")
        current = current_graph_version if current_graph_version is not None else new_graph_version
        if current is None:
            raise ValueError("current_graph_version/new_graph_version is required")
        current = _coerce_version(current, name="current_graph_version")
        delta = ContextGraphDelta.from_any(graph_delta)
        freshness = validate_read_set_freshness(
            agent,
            current_graph_version=current,
            graph_delta=graph_delta,
            context_snapshot=context,
        )
        # Build a plan even for blocked input, so callers receive a complete
        # immutable audit object rather than an exception.
        try:
            context_plan = plan_context_rebase(
                agent.graph_version,
                current,
                old_bindings=_context_bindings(agent, context),
                graph_delta=delta,
            )
        except (TypeError, ValueError):
            unknown_delta = ContextGraphDelta(
                graph_id=agent.graph_id,
                known=False,
                coverage="unknown",
                reason="invalid graph delta",
            )
            context_plan = plan_context_rebase(
                agent.graph_version,
                max(agent.graph_version, current),
                old_bindings=_context_bindings(agent, context),
                graph_delta=unknown_delta,
            )

        action = context_plan.action
        if freshness.status is CommitFreshnessStatus.BLOCKED:
            action = ContextRebaseAction.BLOCKED
        elif (
            freshness.status is CommitFreshnessStatus.STALE and action is ContextRebaseAction.REUSE
        ):
            action = ContextRebaseAction.REBASE
        interrupt = (
            _make_interrupt(
                agent,
                delta,
                graph_version=current,
                reason=reason or freshness.reason or context_plan.reason,
            )
            if action in {ContextRebaseAction.REBASE, ContextRebaseAction.FULL_RELOAD}
            else None
        )
        request = None
        if action is not ContextRebaseAction.BLOCKED and harness is not None:
            request = _build_control_request(
                harness,
                agent,
                action=action,
                plan=context_plan,
                new_graph_version=current,
                target_semantic_epoch=target_semantic_epoch,
                reason=reason,
            )
            if request is None:
                action = ContextRebaseAction.BLOCKED
        final_reason = (
            freshness.reason
            if action is ContextRebaseAction.BLOCKED and freshness.reason
            else context_plan.reason
        )
        return RebaseRuntimeDecision(
            graph_id=agent.graph_id,
            task_id=agent.task_id,
            agent_id=agent.agent_id,
            attempt_id=agent.attempt_id,
            action=action,
            plan=context_plan,
            freshness=freshness,
            interrupt=interrupt,
            control_request=request,
            reason=final_reason,
        )

    async def apply(
        self,
        decision: RebaseRuntimeDecision,
        *,
        harness: HarnessSessionAdapter | None = None,
        invoke_handoff: bool = True,
    ) -> RebaseRuntimeApplyResult:
        """Apply a prepared request and optionally invoke non-atomic handoff."""

        if not isinstance(decision, RebaseRuntimeDecision):
            raise TypeError("decision must be a RebaseRuntimeDecision")
        harness_result: HarnessControlResult | None = None
        if decision.control_request is not None:
            if harness is None:
                return RebaseRuntimeApplyResult(
                    decision=decision,
                    reason="a Harness adapter is required to apply control_request",
                )
            harness_result = await harness.control(decision.control_request)
            if not harness_result.applied:
                return RebaseRuntimeApplyResult(
                    decision=decision,
                    harness_result=harness_result,
                    reason="Harness rejected or failed the control request",
                )
        should_handoff = (
            invoke_handoff
            and self._handoff_callback is not None
            and decision.action in {ContextRebaseAction.REBASE, ContextRebaseAction.FULL_RELOAD}
        )
        if not should_handoff:
            return RebaseRuntimeApplyResult(
                decision=decision,
                harness_result=harness_result,
                reason="Harness control applied; no ownership handoff requested",
            )
        callback = cast(HandoffCallback, self._handoff_callback)
        raw = callback(decision)
        handoff_result = await raw if inspect.isawaitable(raw) else raw
        return RebaseRuntimeApplyResult(
            decision=decision,
            harness_result=harness_result,
            handoff_result=handoff_result,
            handoff_attempted=True,
            reason=("Harness control and handoff callback completed; handoff is non-atomic"),
        )


def plan_rebase_runtime(*args: Any, **kwargs: Any) -> RebaseRuntimeDecision:
    """Functional convenience wrapper around :class:`RebaseRuntimeBridge`."""

    handoff_callback = kwargs.pop("handoff_callback", None)
    return RebaseRuntimeBridge(handoff_callback=handoff_callback).plan(*args, **kwargs)


async def apply_rebase_runtime(
    decision: RebaseRuntimeDecision,
    *,
    harness: HarnessSessionAdapter | None = None,
    handoff_callback: HandoffCallback | None = None,
    invoke_handoff: bool = True,
) -> RebaseRuntimeApplyResult:
    """Functional convenience wrapper for applying a prepared decision."""

    return await RebaseRuntimeBridge(handoff_callback=handoff_callback).apply(
        decision,
        harness=harness,
        invoke_handoff=invoke_handoff,
    )


__all__ = [
    "REBASE_RUNTIME_POLICY_ID",
    "REBASE_RUNTIME_SCHEMA_VERSION",
    "CommitFreshness",
    "CommitFreshnessStatus",
    "HandoffCallback",
    "LiveContextRebaseApplyResult",
    "LiveContextRebasePlan",
    "RebaseRuntimeApplyResult",
    "RebaseRuntimeBridge",
    "RebaseRuntimeDecision",
    "apply_rebase_runtime",
    "plan_rebase_runtime",
    "validate_read_set_freshness",
]
