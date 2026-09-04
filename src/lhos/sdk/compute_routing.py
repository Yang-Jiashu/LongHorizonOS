"""Auditable compute-routing primitives for LongHorizonOS.

This module implements the *bounded* Stage-8 policy primitive described in
``Mind-VLA-笔记.md``.  It does not call a model provider, create a process,
claim work, or mutate the Scheduler.  Given an immutable
``GlobalRuntimeState`` plus explicit task/context/agent metadata, it computes
one deterministic recommendation:

* reuse a warm Agent process or start a fresh one;
* request a cheap, standard, or strong model tier;
* choose a bounded context-token budget; and
* choose light, standard, or strong verification.

The policy only gives credit for exact version-pinned resource bindings.  An
unknown/hidden binding, stale binding, or missing Context VM snapshot prevents
the policy from claiming a warm-context benefit.  This is intentionally
fail-closed: a routing recommendation is not provenance discovery and is not
an assertion that a downstream execution path can satisfy the recommendation.
The existing Scheduler/Kernel remains authoritative for eligibility, resource
admission, Claims, Leases, fencing, and actual execution.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any, Final, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    field_validator,
    model_validator,
)

from lhos.agent_os.context.models import ContextManifest

from .runtime_state import (
    GlobalRuntimeState,
    ResourceBindingState,
    UnavailableField,
)

COMPUTE_ROUTING_SCHEMA_VERSION: Final[Literal["compute-routing.v1"]] = "compute-routing.v1"
COMPUTE_ROUTING_POLICY_ID: Final[str] = "cognitive-locality-routing.v1"


class AgentReuseAction(StrEnum):
    """Warm-process decision emitted by the policy."""

    REUSE_AGENT = "reuse_agent"
    FRESH_AGENT = "fresh_agent"


class ModelTier(StrEnum):
    """Provider-independent model tier recommendation.

    These are *policy labels*, not model names and not proof that a provider
    exposes a corresponding model.
    """

    CHEAP = "cheap"
    STANDARD = "standard"
    STRONG = "strong"


class VerificationStrength(StrEnum):
    """Amount of independent verification effort to request."""

    LIGHT = "light"
    STANDARD = "standard"
    STRONG = "strong"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class RoutingBinding(_FrozenModel):
    """Explicit version-pinned context/provenance binding.

    ``known=False`` or ``stale=True`` is deliberately retained in the input
    rather than silently dropped.  A binding is eligible for exact overlap only
    when URI, version, and content hash are all present.
    """

    operation: str = "read"
    resource_uri: str = ""
    artifact_id: str | None = None
    version: StrictInt | None = None
    content_hash: str | None = None
    known: StrictBool = True
    stale: StrictBool = False
    token_cost: StrictInt = Field(default=0, ge=0)

    @field_validator("operation", "resource_uri", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        return str(value).strip()

    @field_validator("artifact_id", "content_hash", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("content_hash")
    @classmethod
    def _normalize_hash(cls, value: str | None) -> str | None:
        return value.lower() if value else None

    @field_validator("version", "token_cost")
    @classmethod
    def _non_negative_int(cls, value: int | None) -> int | None:
        if value is not None and (isinstance(value, bool) or value < 0):
            raise ValueError("binding integer fields must be non-negative integers")
        return value

    @model_validator(mode="after")
    def _known_binding_identity(self) -> RoutingBinding:
        if self.known and self.operation == "":
            raise ValueError("known binding operation must be non-empty")
        return self

    @property
    def exact_identity(self) -> tuple[str, str, int, str] | None:
        """Return an exact identity, or ``None`` when it is not authoritative."""

        if not self.known or self.stale:
            return None
        if not self.resource_uri or self.version is None or not self.content_hash:
            return None
        return (
            self.resource_uri,
            self.artifact_id or "",
            int(self.version),
            self.content_hash.lower(),
        )

    @property
    def identity_key(self) -> str:
        """Stable human/audit key including incomplete bindings."""

        return "|".join(
            (
                self.resource_uri,
                self.artifact_id or "",
                "" if self.version is None else str(self.version),
                self.content_hash or "",
            )
        )


class RoutingContextManifest(_FrozenModel):
    """Immutable policy input distilled from a Context VM manifest."""

    manifest_id: str = Field(min_length=1)
    token_budget: StrictInt = Field(ge=0)
    bindings: tuple[RoutingBinding, ...] = ()

    @field_validator("bindings", mode="before")
    @classmethod
    def _coerce_bindings(cls, value: Any) -> tuple[RoutingBinding, ...]:
        return _coerce_binding_tuple(value)

    @model_validator(mode="after")
    def _unique_bindings(self) -> RoutingContextManifest:
        return _dedupe_bindings(self)

    @classmethod
    def from_context_manifest(cls, manifest: ContextManifest) -> RoutingContextManifest:
        if not isinstance(manifest, ContextManifest):
            raise TypeError("manifest must be a ContextManifest")
        return cls(
            manifest_id=manifest.manifest_id,
            token_budget=manifest.token_budget,
            bindings=tuple(
                RoutingBinding(
                    operation="read",
                    resource_uri=ref.canonical_uri,
                    artifact_id=ref.artifact_id,
                    version=ref.version,
                    content_hash=ref.content_hash,
                    known=True,
                )
                for ref in manifest.refs
            ),
        )


class CandidateTaskMetadata(_FrozenModel):
    """Explicit metadata used to route one candidate task.

    The policy never parses task descriptions or guesses hidden dependencies.
    ``None`` means the corresponding signal was not supplied; the decision
    records that fact in ``reasons``/``unavailable``.
    """

    task_id: str = Field(min_length=1)
    criticality: StrictInt | None = Field(default=None, ge=0)
    downstream_fanout: StrictInt | None = Field(default=None, ge=0)
    failure_blast_radius: StrictInt | None = Field(default=None, ge=0)
    input_stability: StrictFloat | None = Field(default=None, ge=0.0, le=1.0)
    preferred_agent_id: str | None = None
    required_bindings: tuple[RoutingBinding, ...] = ()
    context_manifest: RoutingContextManifest | None = None
    context_budget_tokens: StrictInt | None = Field(default=None, ge=0)
    estimated_context_tokens: StrictInt | None = Field(default=None, ge=0)
    estimated_remaining_tokens: StrictInt | None = Field(default=None, ge=0)
    requested_model_tier: ModelTier | None = None
    requested_verification_strength: VerificationStrength | None = None
    hidden_context: StrictBool = False

    @field_validator("required_bindings", mode="before")
    @classmethod
    def _coerce_required_bindings(cls, value: Any) -> tuple[RoutingBinding, ...]:
        return _coerce_binding_tuple(value)

    @field_validator("context_manifest", mode="before")
    @classmethod
    def _coerce_context_manifest(cls, value: Any) -> RoutingContextManifest | None:
        if value is None:
            return None
        if isinstance(value, RoutingContextManifest):
            return value
        if isinstance(value, ContextManifest):
            return RoutingContextManifest.from_context_manifest(value)
        if isinstance(value, Mapping):
            if "bindings" in value:
                return cast(
                    RoutingContextManifest,
                    RoutingContextManifest.model_validate(value),
                )
            manifest = cast(ContextManifest, ContextManifest.model_validate(dict(value)))
            return RoutingContextManifest.from_context_manifest(manifest)
        raise TypeError("context_manifest must be a ContextManifest or mapping")

    @field_validator("preferred_agent_id", mode="before")
    @classmethod
    def _normalize_preferred_agent(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def _merge_manifest_bindings(self) -> CandidateTaskMetadata:
        # Keep both fields visible for audit.  ``required_bindings`` is the
        # explicit union consumed by the policy; do not mutate the caller.
        manifest_bindings = () if self.context_manifest is None else self.context_manifest.bindings
        merged = _dedupe_binding_values((*self.required_bindings, *manifest_bindings))
        object.__setattr__(self, "required_bindings", merged)
        return self


class RegisteredAgentMetadata(_FrozenModel):
    """Explicit, point-in-time metadata for a reusable Agent process."""

    agent_id: str = Field(min_length=1)
    model: str | None = None
    model_tier: ModelTier | None = None
    supported_model_tiers: tuple[ModelTier, ...] | None = None
    cost_weight: StrictFloat = Field(default=1.0, ge=0.0)
    available: StrictBool = True
    context_snapshot_id: str | None = None
    read_set: tuple[RoutingBinding, ...] = ()
    context_bindings: tuple[RoutingBinding, ...] = ()
    reconstruction_token_cost: StrictInt | None = Field(default=None, ge=0)
    stale_binding_count: StrictInt = Field(default=0, ge=0)
    hidden_context: StrictBool = False

    @field_validator("read_set", "context_bindings", mode="before")
    @classmethod
    def _coerce_agent_bindings(cls, value: Any) -> tuple[RoutingBinding, ...]:
        return _coerce_binding_tuple(value)

    @field_validator("model", "context_snapshot_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("supported_model_tiers", mode="before")
    @classmethod
    def _normalize_tiers(cls, value: Any) -> tuple[ModelTier, ...] | None:
        if value is None:
            return None
        return tuple(sorted({ModelTier(item) for item in value}, key=lambda item: item.value))

    @model_validator(mode="after")
    def _non_empty_agent(self) -> RegisteredAgentMetadata:
        if not self.agent_id.strip():
            raise ValueError("agent_id must be non-empty")
        return self

    @property
    def effective_bindings(self) -> tuple[RoutingBinding, ...]:
        return _dedupe_binding_values((*self.read_set, *self.context_bindings))


class AgentLocalityScore(_FrozenModel):
    """Auditable score for one candidate Agent."""

    agent_id: str = Field(min_length=1)
    locality_score: StrictFloat = Field(ge=0.0, le=1.0)
    exact_overlap_count: StrictInt = Field(ge=0)
    required_binding_count: StrictInt = Field(ge=0)
    stale_binding_count: StrictInt = Field(ge=0)
    unknown_binding_count: StrictInt = Field(ge=0)
    reconstruction_token_cost: StrictInt | None = Field(default=None, ge=0)
    fresh_context_token_cost: StrictInt | None = Field(default=None, ge=0)
    estimated_context_token_savings: StrictInt | None = Field(default=None, ge=0)
    reusable: StrictBool
    reasons: tuple[str, ...] = ()


class ComputeRoutingDecision(_FrozenModel):
    """Immutable output of one Stage-8 policy pass."""

    schema_version: Literal["compute-routing.v1"] = COMPUTE_ROUTING_SCHEMA_VERSION
    policy_id: str = COMPUTE_ROUTING_POLICY_ID
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    selected_agent_id: str | None = None
    action: AgentReuseAction
    locality_score: StrictFloat = Field(ge=0.0, le=1.0)
    exact_overlap_count: StrictInt = Field(ge=0)
    required_binding_count: StrictInt = Field(ge=0)
    stale_binding_count: StrictInt = Field(ge=0)
    unknown_binding_count: StrictInt = Field(ge=0)
    reconstruction_token_cost: StrictInt | None = Field(default=None, ge=0)
    fresh_context_token_cost: StrictInt | None = Field(default=None, ge=0)
    estimated_context_token_savings: StrictInt | None = Field(default=None, ge=0)
    model_tier: ModelTier
    context_budget_tokens: StrictInt = Field(ge=0)
    verification_strength: VerificationStrength
    criticality: StrictInt = Field(ge=0)
    downstream_fanout: StrictInt = Field(ge=0)
    failure_blast_radius: StrictInt = Field(ge=0)
    input_stability: StrictFloat | None = Field(default=None, ge=0.0, le=1.0)
    # Graph-derived verification audit: the downstream fan-out (blast-radius
    # proxy) and critical-path membership that set the verification floor.
    # ``verification_fanout`` is ``None`` when graph structure was not
    # observable for this node (fail-closed; see ``_graph_verification_floor``).
    verification_fanout: StrictInt | None = Field(default=None, ge=0)
    verification_on_critical_path: StrictBool = False
    # Graph-derived model-tier audit: the same observed structure that sets the
    # model-tier floor (see ``_graph_model_floor``).  ``model_tier_fanout`` is
    # the downstream fan-out (blast radius of a wrong *output*); it is ``None``
    # when the node's structure was not observable (fail-closed to STANDARD).
    # ``model_tier_rework_observed`` is ``True`` when the task is being redone
    # because its prior Evidence went STALE/INVALID -- observed rework, not a
    # declared opinion.
    model_tier_fanout: StrictInt | None = Field(default=None, ge=0)
    model_tier_on_critical_path: StrictBool = False
    model_tier_rework_observed: StrictBool = False
    eligible: StrictBool = True
    reasons: tuple[str, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()
    candidate_scores: tuple[AgentLocalityScore, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    def as_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.model_dump(mode="json"))


class ComputeRoutingPolicy(_FrozenModel):
    """Deterministic, side-effect-free compute-routing policy."""

    reuse_threshold: StrictFloat = Field(default=0.60, ge=0.0, le=1.0)
    max_context_budget_tokens: StrictInt = Field(default=128_000, ge=0)
    policy_id: str = COMPUTE_ROUTING_POLICY_ID

    def route(
        self,
        state: GlobalRuntimeState,
        candidate: CandidateTaskMetadata | Mapping[str, Any],
        agents: Iterable[RegisteredAgentMetadata | Mapping[str, Any]] | Mapping[str, Any] = (),
        task_outcomes: Mapping[str, Any] | None = None,
    ) -> ComputeRoutingDecision:
        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if not isinstance(candidate, CandidateTaskMetadata):
            candidate = CandidateTaskMetadata.model_validate(candidate)
        agent_values = _normalize_agents(agents)
        # If callers omit explicit registry metadata, only currently observed
        # attempts may be considered.  Missing model/cost/context fields stay
        # unknown and never receive fabricated locality credit.
        if not agent_values:
            agent_values = _agents_from_state(state)

        criticality, fanout, blast, signal_reasons = _task_signals(state, candidate)
        (
            graph_floor,
            graph_fanout,
            on_critical_path,
            floor_reasons,
            floor_unavailable,
        ) = _graph_verification_floor(state, candidate.task_id)
        (
            model_floor,
            model_fanout,
            model_on_critical_path,
            model_rework_observed,
            model_floor_reasons,
            model_floor_unavailable,
        ) = _graph_model_floor(state, candidate.task_id)
        measured_floor, measured_success_bp, measured_samples = _measured_failure_floor(
            (task_outcomes or {}).get(str(candidate.task_id))
        )
        if measured_floor is not None:
            model_floor_reasons = (
                *model_floor_reasons,
                f"model_tier_floor_measured_success_bp={measured_success_bp}"
                f"_over_{measured_samples}_attempts",
            )
        desired_model = _model_tier(
            candidate, criticality, fanout, blast, model_floor, measured_floor
        )
        desired_verify = _verification_strength(candidate, criticality, fanout, blast, graph_floor)
        budget, budget_reasons = _context_budget(candidate, self.max_context_budget_tokens)
        fresh_context_cost = _fresh_context_token_cost(candidate, budget)
        required = candidate.required_bindings
        candidate_scores = tuple(
            _score_agent(
                agent,
                required=required,
                candidate=candidate,
                fresh_context_cost=fresh_context_cost,
                threshold=self.reuse_threshold,
            )
            for agent in agent_values
        )
        selected_score = _select_score(candidate_scores, candidate.preferred_agent_id)
        unavailable: list[UnavailableField] = []
        reasons = list(signal_reasons) + list(floor_reasons) + list(model_floor_reasons)
        reasons += list(budget_reasons)
        if floor_unavailable is not None:
            unavailable.append(floor_unavailable)
        if model_floor_unavailable is not None:
            unavailable.append(model_floor_unavailable)
        # Record whether declared risk raised effort above the graph-derived
        # floor so the composition (fan-out floor vs. declared risk) is
        # auditable directly from the decision.
        reasons.append(
            "verification_strength_raised_above_graph_floor"
            if _VERIFICATION_ORDER[desired_verify] > _VERIFICATION_ORDER[graph_floor]
            else "verification_strength_at_graph_floor"
        )
        reasons.append(
            "model_tier_raised_above_graph_floor"
            if _MODEL_TIER_ORDER[desired_model] > _MODEL_TIER_ORDER[model_floor]
            else "model_tier_at_graph_floor"
        )

        if state.progress.graph_closed or state.progress.goal_closed:
            eligible = False
            reasons.append("goal_or_graph_closed")
        elif candidate.task_id not in set(state.progress.ready_frontier) | set(
            state.progress.repair_ready_frontier
        ):
            eligible = False
            reasons.append("task_not_on_observed_frontier")
        else:
            eligible = True
        if not state.agent_cognition.available:
            eligible = False
            unavailable.append(
                UnavailableField(
                    name="agent_cognition",
                    reason=state.agent_cognition.reason or "Agent cognition state unavailable",
                )
            )
            reasons.append("agent_cognition_unavailable")
        if not state.resources.available:
            eligible = False
            unavailable.append(
                UnavailableField(
                    name="resources",
                    reason=state.resources.reason or "logical resource state unavailable",
                )
            )
            reasons.append("resources_unavailable")

        if candidate.hidden_context:
            reasons.append("candidate_hidden_context")
            unavailable.append(
                UnavailableField(
                    name="candidate.context",
                    reason="hidden context is not observable by this policy",
                )
            )

        if selected_score is None:
            action = AgentReuseAction.FRESH_AGENT
            selected_agent_id = None
            locality = 0.0
            overlap = required_count = stale = unknown = 0
            reconstruction = None
            estimated_savings = None
            reasons.append("no_registered_agent")
        else:
            selected_agent_id = selected_score.agent_id
            action = (
                AgentReuseAction.REUSE_AGENT
                if selected_score.reusable and not candidate.hidden_context
                else AgentReuseAction.FRESH_AGENT
            )
            locality = selected_score.locality_score
            overlap = selected_score.exact_overlap_count
            required_count = selected_score.required_binding_count
            stale = selected_score.stale_binding_count
            unknown = selected_score.unknown_binding_count
            reconstruction = selected_score.reconstruction_token_cost
            fresh_context_cost = selected_score.fresh_context_token_cost
            estimated_savings = selected_score.estimated_context_token_savings
            reasons.extend(selected_score.reasons)

        if not required:
            reasons.append("no_declared_context_bindings")
        if any(not _binding_complete(binding) for binding in required):
            unavailable.append(
                UnavailableField(
                    name="candidate.required_bindings",
                    reason="one or more required bindings are unknown or incomplete",
                )
            )
        if selected_score is not None and selected_score.reconstruction_token_cost is None:
            unavailable.append(
                UnavailableField(
                    name="agent.reconstruction_token_cost",
                    reason="Agent did not report context reconstruction cost",
                )
            )
        if selected_score is not None and not selected_score.reusable:
            reasons.append("exact_context_overlap_below_safe_threshold")

        supported_tiers = _agent_supported_tiers(selected_score, agent_values)
        if (
            selected_score is not None
            and supported_tiers is not None
            and desired_model not in supported_tiers
        ):
            # A missing/unknown capability is not silently converted into a
            # cheaper model.  The recommendation remains auditable.
            unavailable.append(
                UnavailableField(
                    name="agent.model_tier",
                    reason=f"selected Agent did not declare support for {desired_model.value}",
                )
            )
        payload: dict[str, Any] = {
            "schema_version": COMPUTE_ROUTING_SCHEMA_VERSION,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "task_id": candidate.task_id,
            "selected_agent_id": selected_agent_id,
            "action": action,
            "locality_score": locality,
            "exact_overlap_count": overlap,
            "required_binding_count": required_count,
            "stale_binding_count": stale,
            "unknown_binding_count": unknown,
            "reconstruction_token_cost": reconstruction,
            "fresh_context_token_cost": fresh_context_cost,
            "estimated_context_token_savings": estimated_savings,
            "model_tier": desired_model,
            "context_budget_tokens": budget,
            "verification_strength": desired_verify,
            "criticality": criticality,
            "downstream_fanout": fanout,
            "failure_blast_radius": blast,
            "input_stability": candidate.input_stability,
            "verification_fanout": graph_fanout,
            "verification_on_critical_path": on_critical_path,
            "model_tier_fanout": model_fanout,
            "model_tier_on_critical_path": model_on_critical_path,
            "model_tier_rework_observed": model_rework_observed,
            "eligible": eligible,
            "reasons": tuple(reasons),
            "unavailable": tuple(unavailable),
            "candidate_scores": candidate_scores,
        }
        return ComputeRoutingDecision(
            **payload,
            decision_hash=_decision_hash(payload),
        )


def plan_compute_routing(
    state: GlobalRuntimeState,
    candidate: CandidateTaskMetadata | Mapping[str, Any],
    agents: Iterable[RegisteredAgentMetadata | Mapping[str, Any]] = (),
    *,
    reuse_threshold: float = 0.60,
    max_context_budget_tokens: int = 128_000,
) -> ComputeRoutingDecision:
    """Convenience wrapper for one deterministic policy pass."""

    return ComputeRoutingPolicy(
        reuse_threshold=reuse_threshold,
        max_context_budget_tokens=max_context_budget_tokens,
    ).route(
        state,
        candidate
        if isinstance(candidate, CandidateTaskMetadata)
        else CandidateTaskMetadata.model_validate(candidate),
        agents,
    )


# Friendly aliases for papers/examples that use the shorter terminology.
CognitiveLocalityPolicy = ComputeRoutingPolicy
CognitiveLocalityDecision = ComputeRoutingDecision


def _coerce_binding(value: Any) -> RoutingBinding:
    if isinstance(value, RoutingBinding):
        return value
    if isinstance(value, ResourceBindingState):
        return RoutingBinding(
            operation=value.operation,
            resource_uri=value.resource_uri,
            artifact_id=value.artifact_id,
            version=value.version,
            content_hash=value.content_hash,
            known=value.known,
        )
    if isinstance(value, Mapping):
        return cast(RoutingBinding, RoutingBinding.model_validate(dict(value)))
    # ResourceBinding from the scheduler has a compatible model_dump method.
    if isinstance(value, BaseModel):
        raw = value.model_dump(mode="python")
        return RoutingBinding(
            operation=raw.get("operation", "read"),
            resource_uri=raw.get("resource_uri", ""),
            artifact_id=raw.get("artifact_id"),
            version=raw.get("version"),
            content_hash=raw.get("content_hash"),
            known=raw.get("known", True),
            stale=raw.get("stale", False),
            token_cost=raw.get("token_cost", 0),
        )
    raise TypeError("bindings must be RoutingBinding, ResourceBindingState, or mapping")


def _coerce_binding_tuple(value: Any) -> tuple[RoutingBinding, ...]:
    if value is None:
        return ()
    if isinstance(value, (RoutingBinding, ResourceBindingState, BaseModel, Mapping)):
        value = (value,)
    return _dedupe_binding_values(tuple(_coerce_binding(item) for item in value))


def _dedupe_binding_values(values: Iterable[RoutingBinding]) -> tuple[RoutingBinding, ...]:
    by_key: dict[tuple[Any, ...], RoutingBinding] = {}
    for binding in values:
        key = (
            binding.operation,
            binding.resource_uri,
            binding.artifact_id or "",
            binding.version,
            binding.content_hash or "",
            binding.known,
            binding.stale,
        )
        by_key.setdefault(key, binding)
    return tuple(sorted(by_key.values(), key=lambda item: (item.operation, item.identity_key)))


def _dedupe_bindings(manifest: RoutingContextManifest) -> RoutingContextManifest:
    object.__setattr__(manifest, "bindings", _dedupe_binding_values(manifest.bindings))
    return manifest


def _binding_complete(binding: RoutingBinding) -> bool:
    return binding.known and binding.exact_identity is not None


def _normalize_agents(
    values: Iterable[RegisteredAgentMetadata | Mapping[str, Any]] | Mapping[str, Any],
) -> tuple[RegisteredAgentMetadata, ...]:
    if isinstance(values, Mapping):
        # Accept either one metadata DTO mapping or a registry mapping
        # ``agent_id -> metadata``.  Both forms remain explicit inputs.
        values = (values,) if "agent_id" in values else values.values()
    result: list[RegisteredAgentMetadata] = []
    seen: set[str] = set()
    for value in values:
        agent = (
            value
            if isinstance(value, RegisteredAgentMetadata)
            else RegisteredAgentMetadata.model_validate(value)
        )
        if agent.agent_id in seen:
            raise ValueError(f"duplicate registered agent metadata: {agent.agent_id!r}")
        seen.add(agent.agent_id)
        result.append(agent)
    return tuple(sorted(result, key=lambda item: item.agent_id))


def _agents_from_state(state: GlobalRuntimeState) -> tuple[RegisteredAgentMetadata, ...]:
    # A single Agent may own several concurrent attempts.  The runtime state
    # therefore legitimately contains duplicate ``agent_id`` values; feeding
    # those rows directly to ``_normalize_agents`` would turn a normal
    # concurrent batch into a routing failure.  Do not merge read-sets from
    # different attempts (that could manufacture a false locality overlap).
    # For duplicate rows retain one conservative, context-free profile so the
    # policy remains usable while giving no warm-context credit.
    grouped: dict[str, list[Any]] = {}
    for attempt in state.agent_cognition.current_attempts:
        agent_id = str(attempt.agent_id).strip()
        if agent_id:
            grouped.setdefault(agent_id, []).append(attempt)

    result: list[RegisteredAgentMetadata] = []
    for agent_id in sorted(grouped):
        attempts = grouped[agent_id]
        if len(attempts) == 1:
            attempt = attempts[0]
            result.append(
                RegisteredAgentMetadata(
                    agent_id=agent_id,
                    available=True,
                    context_snapshot_id=attempt.context_snapshot_id,
                    read_set=tuple(_coerce_binding(binding) for binding in attempt.read_set),
                )
            )
            continue
        result.append(
            RegisteredAgentMetadata(
                agent_id=agent_id,
                available=True,
                # Multiple live attempts do not expose one authoritative
                # context snapshot.  Keeping these fields empty is
                # deliberately fail-closed for cognitive-locality reuse.
                context_snapshot_id=None,
                read_set=(),
                context_bindings=(),
                hidden_context=True,
            )
        )
    return _normalize_agents(result)


def _task_signals(
    state: GlobalRuntimeState,
    candidate: CandidateTaskMetadata,
) -> tuple[int, int, int, tuple[str, ...]]:
    reasons: list[str] = []
    criticality = candidate.criticality
    if criticality is None:
        criticality = 1 if candidate.task_id in state.progress.critical_path else 0
        reasons.append("criticality_derived_from_critical_path")
    fanout = candidate.downstream_fanout
    if fanout is None:
        unlock = next(
            (
                item.unlock_value
                for item in state.progress.downstream_unlock_values
                if item.task_id == candidate.task_id
            ),
            0,
        )
        fanout = unlock
        reasons.append("downstream_fanout_derived_from_unlock_value")
    blast = candidate.failure_blast_radius
    if blast is None:
        blast = fanout
        reasons.append("failure_blast_radius_derived_from_fanout")
    if candidate.input_stability is None:
        reasons.append("input_stability_unavailable")
    return int(criticality), int(fanout), int(blast), tuple(reasons)


MEASURED_FAILURE_STRONG_BASIS_POINTS: Final[int] = 5_000
MEASURED_FAILURE_STANDARD_BASIS_POINTS: Final[int] = 8_000
MEASURED_MIN_SAMPLES: Final[int] = 2


def _measured_failure_floor(outcomes: Any) -> tuple[ModelTier | None, int, int]:
    """Raise the tier floor from a task's *measured* verification rate.

    A task that keeps failing is evidence, not an opinion: repeated failure on a
    cheap tier is the one signal here that comes from outcomes rather than from
    structure or declaration.  Requires at least ``MEASURED_MIN_SAMPLES``
    terminal attempts, because a single failure is noise and would otherwise
    escalate every task that stumbled once.

    Returns ``(floor, success_basis_points, samples)``; ``floor`` is ``None``
    when there is not enough measured history to justify raising anything.
    """

    if outcomes is None:
        return None, -1, 0
    samples = int(getattr(outcomes, "success_samples", 0) or 0)
    verified = int(getattr(outcomes, "verified", 0) or 0)
    if samples < MEASURED_MIN_SAMPLES:
        return None, -1, samples
    success_bp = verified * 10_000 // samples
    if success_bp < MEASURED_FAILURE_STRONG_BASIS_POINTS:
        return ModelTier.STRONG, success_bp, samples
    if success_bp < MEASURED_FAILURE_STANDARD_BASIS_POINTS:
        return ModelTier.STANDARD, success_bp, samples
    return None, success_bp, samples


def _model_tier(
    candidate: CandidateTaskMetadata,
    criticality: int,
    fanout: int,
    blast: int,
    graph_floor: ModelTier,
    measured_floor: ModelTier | None = None,
) -> ModelTier:
    """Compose the graph-derived model-tier floor with caller-declared preference.

    The ordering is deliberate and one-directional, mirroring
    ``_verification_strength``: ``graph_floor`` (downstream blast radius +
    critical-path membership + observed rework, all derived purely from observed
    graph structure) and ``measured_floor`` (the task's measured verification
    rate) are hard lower bounds.  The declared-risk tier and an
    explicit ``requested_model_tier`` are folded in with ``max`` so a caller may
    only RAISE the tier above the floor, never pull it below the strength the
    observed consequence justifies.  This is why the floor is passed in already
    computed from the graph and is never recomputed from the declared signals.
    """

    requested = candidate.requested_model_tier
    risk = _risk_score(criticality, fanout, blast, candidate.input_stability)
    declared = (
        ModelTier.STRONG if risk >= 50 else ModelTier.STANDARD if risk >= 20 else ModelTier.CHEAP
    )
    return _max_model_tier(graph_floor, measured_floor, declared, requested)


_MODEL_TIER_ORDER: Final[dict[ModelTier, int]] = {
    ModelTier.CHEAP: 0,
    ModelTier.STANDARD: 1,
    ModelTier.STRONG: 2,
}
# Fan-out thresholds for the graph-derived model-tier floor.  These share the
# blast-radius reasoning of the verification floor but answer a different
# question: a hub whose *output* is wrong forces rework across everything
# downstream, so model strength -- not just verification effort -- scales with
# observed downstream consequence rather than a caller's declared opinion.  The
# thresholds are named separately so the two policies can diverge without a
# silent magic-number coupling.
_MODEL_TIER_STRONG_FANOUT: Final[int] = 8
_MODEL_TIER_STANDARD_FANOUT: Final[int] = 2


def _max_model_tier(*values: ModelTier | None) -> ModelTier:
    best = ModelTier.CHEAP
    for value in values:
        if value is not None and _MODEL_TIER_ORDER[value] > _MODEL_TIER_ORDER[best]:
            best = value
    return best


def _graph_model_floor(
    state: GlobalRuntimeState,
    task_id: str,
) -> tuple[ModelTier, int | None, bool, bool, tuple[str, ...], UnavailableField | None]:
    """Derive the model-tier floor from *observed graph structure* only.

    Three signals, all read directly from the immutable ``GlobalRuntimeState``
    projection and none requiring a model call:

    * downstream fan-out -- reusing the ``DownstreamUnlock`` value
      ``derive_graph_analysis`` already computed; the blast radius of a wrong
      *output* (a hub many tasks consume is expensive to get wrong);
    * critical-path membership -- a wrong or slow critical-path task delays the
      whole goal, so it earns at least the safe middle tier;
    * observed rework -- the task is being redone because its prior Evidence
      went STALE/INVALID (``stale_task_ids``/``invalid_task_ids``).  That is a
      measured fact about this task, not a declared opinion, and re-spending the
      cheapest model on work that already had to be thrown away compounds waste.

    The floor is intentionally independent of caller-declared risk: declared
    preference is composed on top in ``_model_tier`` and may only raise the
    result, never lower the tier the observed consequence justifies.

    Returns ``(floor, fanout, on_critical_path, rework_observed, reasons,
    unavailable)``.
    """

    on_critical_path = task_id in state.progress.critical_path
    rework_observed = (
        task_id in state.progress.stale_task_ids or task_id in state.progress.invalid_task_ids
    )
    fanout: int | None = None
    for item in state.progress.downstream_unlock_values:
        if item.task_id == task_id:
            fanout = int(item.unlock_value)
            break

    reasons: list[str] = []
    if on_critical_path:
        reasons.append("model_tier_floor_on_critical_path")
    if rework_observed:
        reasons.append("model_tier_floor_rework_observed")
    # Critical-path membership and observed rework each justify at least the
    # safe middle tier regardless of fan-out.
    structural_floor = ModelTier.CHEAP
    if on_critical_path or rework_observed:
        structural_floor = ModelTier.STANDARD

    if fanout is None:
        # Fail-closed: the graph structure for this node was not observable.
        # Treating an unread node as a leaf would spend the *cheapest* model on
        # exactly the high-fan-out nodes whose structure we failed to read, so
        # the floor is STANDARD (never CHEAP) and the gap is recorded via
        # UnavailableField.
        reasons.append("model_tier_floor_fanout_unobservable")
        return (
            _max_model_tier(ModelTier.STANDARD, structural_floor),
            None,
            on_critical_path,
            rework_observed,
            tuple(reasons),
            UnavailableField(
                name="graph.downstream_fanout",
                reason=(
                    f"task {task_id!r} has no observable downstream fan-out in the "
                    "graph projection; model-tier floor raised to standard"
                ),
            ),
        )

    if fanout >= _MODEL_TIER_STRONG_FANOUT:
        fanout_floor = ModelTier.STRONG
    elif fanout >= _MODEL_TIER_STANDARD_FANOUT:
        fanout_floor = ModelTier.STANDARD
    else:
        fanout_floor = ModelTier.CHEAP
    reasons.append(f"model_tier_floor_fanout={fanout}")
    return (
        _max_model_tier(fanout_floor, structural_floor),
        fanout,
        on_critical_path,
        rework_observed,
        tuple(reasons),
        None,
    )


_VERIFICATION_ORDER: Final[dict[VerificationStrength, int]] = {
    VerificationStrength.LIGHT: 0,
    VerificationStrength.STANDARD: 1,
    VerificationStrength.STRONG: 2,
}
# Fan-out thresholds for the graph-derived verification floor.  ``fan-out`` is
# the number of downstream tasks this node structurally unblocks -- i.e. the
# blast radius of a false VERIFIED.  A node many tasks depend on is expensive
# to get wrong, so verification effort scales with observed downstream
# consequence, not with a caller's declared opinion.
_VERIFICATION_STRONG_FANOUT: Final[int] = 8
_VERIFICATION_STANDARD_FANOUT: Final[int] = 2


def _max_verification(*values: VerificationStrength | None) -> VerificationStrength:
    best = VerificationStrength.LIGHT
    for value in values:
        if value is not None and _VERIFICATION_ORDER[value] > _VERIFICATION_ORDER[best]:
            best = value
    return best


def _graph_verification_floor(
    state: GlobalRuntimeState,
    task_id: str,
) -> tuple[VerificationStrength, int | None, bool, tuple[str, ...], UnavailableField | None]:
    """Derive the verification floor from *observed graph structure* only.

    The floor scales with downstream fan-out -- reusing the ``DownstreamUnlock``
    signal ``derive_graph_analysis`` already computed rather than recomputing
    reachability -- and with critical-path membership.  It is intentionally
    independent of any caller-declared risk: declared risk is composed on top
    in ``_verification_strength`` and may only raise the result, never lower the
    effort the blast radius justifies.

    Returns ``(floor, fanout, on_critical_path, reasons, unavailable)``.
    """

    on_critical_path = task_id in state.progress.critical_path
    fanout: int | None = None
    for item in state.progress.downstream_unlock_values:
        if item.task_id == task_id:
            fanout = int(item.unlock_value)
            break

    reasons: list[str] = []
    if fanout is None:
        # Fail-closed: the graph structure for this node was not observable.
        # Treating an unread node as a leaf would under-verify exactly the
        # high-fan-out nodes whose structure we failed to read, so the floor is
        # STANDARD (never LIGHT) and the gap is recorded via UnavailableField.
        reasons.append("verification_floor_fanout_unobservable")
        if on_critical_path:
            reasons.append("verification_floor_on_critical_path")
        return (
            VerificationStrength.STANDARD,
            None,
            on_critical_path,
            tuple(reasons),
            UnavailableField(
                name="graph.downstream_fanout",
                reason=(
                    f"task {task_id!r} has no observable downstream fan-out in the "
                    "graph projection; verification floor raised to standard"
                ),
            ),
        )

    if fanout >= _VERIFICATION_STRONG_FANOUT:
        fanout_floor = VerificationStrength.STRONG
    elif fanout >= _VERIFICATION_STANDARD_FANOUT:
        fanout_floor = VerificationStrength.STANDARD
    else:
        fanout_floor = VerificationStrength.LIGHT
    reasons.append(f"verification_floor_fanout={fanout}")
    critical_floor = VerificationStrength.LIGHT
    if on_critical_path:
        critical_floor = VerificationStrength.STANDARD
        reasons.append("verification_floor_on_critical_path")
    return (
        _max_verification(fanout_floor, critical_floor),
        fanout,
        on_critical_path,
        tuple(reasons),
        None,
    )


def _verification_strength(
    candidate: CandidateTaskMetadata,
    criticality: int,
    fanout: int,
    blast: int,
    graph_floor: VerificationStrength,
) -> VerificationStrength:
    """Compose the graph-derived floor with caller-declared risk.

    The ordering is deliberate and one-directional: ``graph_floor`` (downstream
    blast radius + critical-path membership, derived purely from observed graph
    structure) is a hard lower bound.  Declared risk and an explicit requested
    strength are folded in with ``max`` so they can only RAISE verification
    effort -- a caller can never de-escalate below the effort the observed
    fan-out justifies.  This is why the floor is passed in already computed
    from the graph and is never recomputed from the caller-declared signals.
    """

    requested = candidate.requested_verification_strength
    risk = _risk_score(criticality, fanout, blast, candidate.input_stability)
    declared = (
        VerificationStrength.STRONG
        if risk >= 50
        else VerificationStrength.STANDARD
        if risk >= 20
        else VerificationStrength.LIGHT
    )
    return _max_verification(graph_floor, declared, requested)


def _risk_score(
    criticality: int,
    fanout: int,
    blast: int,
    input_stability: float | None = None,
) -> int:
    score = criticality * 5 + fanout * 3 + blast * 4
    if input_stability is not None:
        # Low stability increases expected rework/risk.  The policy uses a
        # bounded deterministic penalty rather than pretending to predict
        # provider success probabilities.
        score += round((1.0 - input_stability) * 20)
    return min(100, score)


def _context_budget(
    candidate: CandidateTaskMetadata,
    maximum: int,
) -> tuple[int, tuple[str, ...]]:
    reasons: list[str] = []
    budget = candidate.context_budget_tokens
    if budget is None and candidate.context_manifest is not None:
        budget = candidate.context_manifest.token_budget
        reasons.append("context_budget_from_manifest")
    if budget is None:
        budget = candidate.estimated_context_tokens
        if budget is not None:
            reasons.append("context_budget_from_explicit_estimate")
    if budget is None:
        budget = 0
        reasons.append("context_budget_unavailable")
    if budget > maximum:
        budget = maximum
        reasons.append("context_budget_clamped_to_policy_maximum")
    return int(budget), tuple(reasons)


def _fresh_context_token_cost(
    candidate: CandidateTaskMetadata,
    _bounded_budget: int,
) -> int | None:
    """Estimate fresh-context reconstruction cost from explicit inputs only.

    A manifest token budget is an explicit caller-provided upper bound and is
    usable as a conservative estimate.  Binding-level ``token_cost`` defaults
    are intentionally ignored because they are often synthetic/defaulted and
    would manufacture a locality benefit.  If no explicit estimate is
    available, returning ``None`` is safer than pretending to know savings.
    """

    if candidate.estimated_context_tokens is not None:
        return int(candidate.estimated_context_tokens)
    if candidate.context_manifest is not None and candidate.context_manifest.token_budget > 0:
        return int(candidate.context_manifest.token_budget)
    if candidate.context_budget_tokens is not None and candidate.context_budget_tokens > 0:
        return int(candidate.context_budget_tokens)
    return None


def _score_agent(
    agent: RegisteredAgentMetadata,
    *,
    required: tuple[RoutingBinding, ...],
    candidate: CandidateTaskMetadata,
    fresh_context_cost: int | None,
    threshold: float,
) -> AgentLocalityScore:
    agent_bindings = agent.effective_bindings
    required_count = len(required)
    unknown = sum(1 for item in required if not _binding_complete(item))
    unknown += sum(1 for item in agent_bindings if not item.known or not _binding_complete(item))
    stale = (
        agent.stale_binding_count
        + sum(1 for item in required if item.stale)
        + sum(1 for item in agent_bindings if item.stale)
    )
    exact_keys = {
        item.exact_identity
        for item in agent_bindings
        if item.exact_identity is not None and item.operation == "read"
    }
    overlap = sum(
        1
        for item in required
        if item.exact_identity is not None
        and item.operation == "read"
        and item.exact_identity in exact_keys
    )
    overlap_ratio = overlap / required_count if required_count else 0.0
    reconstruction = agent.reconstruction_token_cost
    estimated_savings = (
        None
        if reconstruction is None or fresh_context_cost is None
        else max(0, fresh_context_cost - reconstruction)
    )
    # Locality itself is the exact version-pinned overlap ratio.  Reconstruction
    # economics are reported separately and gate reuse, rather than silently
    # changing the semantic overlap metric.
    ratio = overlap_ratio
    # Unknown/hidden bindings and stale cognition remove all positive reuse
    # evidence.  A partial exact overlap is still reported, but cannot cross
    # the reuse threshold unless every binding is authoritative.
    if candidate.hidden_context or agent.hidden_context:
        ratio = 0.0
    low_stability = candidate.input_stability is not None and candidate.input_stability < 0.25
    if (
        unknown
        or stale
        or agent.context_snapshot_id is None
        or low_stability
        or reconstruction is None
        or (fresh_context_cost is not None and reconstruction >= fresh_context_cost)
    ):
        reusable = False
    else:
        reusable = bool(required_count and ratio >= threshold and agent.available)
    reasons: list[str] = []
    if not agent.available:
        reasons.append("agent_unavailable")
    if agent.context_snapshot_id is None:
        reasons.append("missing_context_snapshot")
    if unknown:
        reasons.append("unknown_or_incomplete_binding")
    if stale:
        reasons.append("stale_binding_present")
    if low_stability:
        reasons.append("input_stability_low")
    if overlap:
        reasons.append(f"exact_version_overlap={overlap}/{required_count}")
    if reconstruction is None:
        reasons.append("reconstruction_cost_unavailable")
    elif reconstruction == 0:
        reasons.append("zero_reconstruction_cost_reported")
    if fresh_context_cost is None:
        reasons.append("fresh_context_cost_unavailable")
    elif reconstruction is not None and reconstruction >= fresh_context_cost:
        reasons.append("reconstruction_cost_not_better_than_fresh")
    elif estimated_savings is not None:
        reasons.append(f"estimated_context_token_savings={estimated_savings}")
    if reusable:
        reasons.append("locality_threshold_met")
    return AgentLocalityScore(
        agent_id=agent.agent_id,
        locality_score=round(max(0.0, min(1.0, ratio)), 6),
        exact_overlap_count=overlap,
        required_binding_count=required_count,
        stale_binding_count=stale,
        unknown_binding_count=unknown,
        reconstruction_token_cost=reconstruction,
        fresh_context_token_cost=fresh_context_cost,
        estimated_context_token_savings=estimated_savings,
        reusable=reusable,
        reasons=tuple(reasons),
    )


def _select_score(
    scores: tuple[AgentLocalityScore, ...],
    preferred_agent_id: str | None,
) -> AgentLocalityScore | None:
    if not scores:
        return None
    # Prefer available/reusable evidence first; a fresh route may still use
    # the best available profile, but unavailable Agents are never selected.
    available = [item for item in scores if "agent_unavailable" not in item.reasons]
    if not available:
        return None
    return min(
        available,
        key=lambda item: (
            0 if preferred_agent_id and item.agent_id == preferred_agent_id else 1,
            -int(item.reusable),
            -item.locality_score,
            item.reconstruction_token_cost
            if item.reconstruction_token_cost is not None
            else 2**31 - 1,
            item.agent_id,
        ),
    )


def _agent_supported_tiers(
    selected: AgentLocalityScore | None,
    agents: tuple[RegisteredAgentMetadata, ...],
) -> set[ModelTier] | None:
    if selected is None:
        return None
    agent = next((item for item in agents if item.agent_id == selected.agent_id), None)
    if agent is None:
        return None
    if agent.supported_model_tiers is not None:
        return set(agent.supported_model_tiers)
    if agent.model_tier is not None:
        return {agent.model_tier}
    return None


def _decision_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _json_compatible(payload),
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
        return {str(key): _json_compatible(item) for key, item in value.items()}
    return value


__all__ = [
    "COMPUTE_ROUTING_POLICY_ID",
    "COMPUTE_ROUTING_SCHEMA_VERSION",
    "AgentLocalityScore",
    "AgentReuseAction",
    "CandidateTaskMetadata",
    "CognitiveLocalityDecision",
    "CognitiveLocalityPolicy",
    "ComputeRoutingDecision",
    "ComputeRoutingPolicy",
    "ModelTier",
    "RegisteredAgentMetadata",
    "RoutingBinding",
    "RoutingContextManifest",
    "VerificationStrength",
    "plan_compute_routing",
]
