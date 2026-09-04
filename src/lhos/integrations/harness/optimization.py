"""Harness-neutral continuation policy for semantic context working sets.

The policy consumes only bounded, typed phase observations.  It does not
inspect provider payloads or Harness-specific transcripts.  ``usage`` on each
observation is the delta for that phase, while ``event_count`` is the Harness
session's observed event cursor.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from statistics import median
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from .protocol import HarnessUsage

_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[\s_-]*key|access[\s_-]*token|auth[\s_-]*token|"
    r"credential|password|passwd|secret|token)\b"
    r"(\s*[:=]\s*)([\"']?)[^\s,;\"']+([\"']?)"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{10,}")
_PREFIXED_SECRET = re.compile(
    r"(?i)\b(?:"
    r"sk-[A-Za-z0-9_-]{12,}|"
    r"1s[A-Za-z0-9]{30,}|"
    r"gh[opsu]_[A-Za-z0-9]{20,}|"
    r"github_pat_[A-Za-z0-9_]{20,}|"
    r"AKIA[A-Z0-9]{16}|"
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
    r")\b"
)
_URI_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*\Z")


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HarnessContinuationAction(StrEnum):
    """Wrapper-level action taken before the next Harness phase."""

    RESUME = "resume"
    RESTART_COMPACTED = "restart_compacted"
    # Halt the task: stop resuming and let the harness score the current
    # state.  Emitted only by policies that detected semantic convergence or
    # exhausted marginal yield (see optimization_v2); v1 never emits it.
    TERMINATE = "terminate"


class HarnessEfficiencyDisposition(StrEnum):
    """Control-plane result for the optional efficiency guard.

    ``APPLY`` leaves the candidate continuation decision in charge.  The
    other dispositions deliberately do not add a new Harness action: they
    tell an adapter to hand control back to its native implementation or to
    restore a checkpoint before doing so.
    """

    APPLY = "apply"
    BYPASS_NATIVE = "bypass_native"
    FALLBACK_NATIVE = "fallback_native"
    RESTORE_BEST_CHECKPOINT = "restore_best_checkpoint"


class HarnessQualityProbe(_FrozenModel):
    """Zero-cost runtime quality proxy extracted from the agent's tool trace.

    Unlike token/event/cache counters (which cannot separate a spinning agent
    from a progressing one -- see offline replay: grammar-fuzz 47M/11inv vs
    microscopy 55M/10inv are indistinguishable on those signals), this probe
    captures *what the agent actually did* in the phase: whether it wrote
    distinct files, whether it ran tests/builds, and whether those commands
    failed. It is computed purely from the durable tool-call trace, so it costs
    zero extra tokens. ``verifier_passed``/test outcomes are runtime signals
    the OS uses to decide whether an agent is making semantic progress.
    """

    write_calls: StrictInt = Field(default=0, ge=0)
    distinct_writes: StrictInt = Field(default=0, ge=0)
    test_calls: StrictInt = Field(default=0, ge=0)
    error_calls: StrictInt = Field(default=0, ge=0)
    tool_calls: StrictInt = Field(default=0, ge=0)
    write_repeat_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    error_ratio: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("write_repeat_ratio", "error_ratio", mode="before")
    @classmethod
    def _finite_ratio(cls, value: Any) -> float:
        normalized = float(value)
        if not math.isfinite(normalized):
            raise ValueError("quality ratios must be finite")
        return normalized

    @classmethod
    def empty(cls) -> "HarnessQualityProbe":
        return cls()


class HarnessPhaseObservation(_FrozenModel):
    """Safe phase projection used by the generic continuation policy.

    ``usage`` contains usage incurred only during this phase, not cumulative
    session usage.  Read/write sets contain semantic artifact URIs, never tool
    request or response payloads.
    """

    phase_index: StrictInt = Field(ge=0)
    usage: HarnessUsage = Field(default_factory=HarnessUsage)
    event_count: StrictInt = Field(default=0, ge=0)
    read_set: tuple[str, ...] = ()
    write_set: tuple[str, ...] = ()
    verifier_passed: bool = False
    max_tokens_checkpoint: bool = False
    harness_completed: bool = False
    # Harness-verifier telemetry (additive; the v1 policy never reads these).
    # ``verifier_rejection_repeat_streak`` counts consecutive byte-identical
    # verifier-rejection digests: a long streak means the agent is in a
    # failure loop (a restart can help), while 1 means failures are still
    # evolving (continuity is valuable -- do not interrupt).
    verifier_rejection_count: StrictInt = Field(default=0, ge=0)
    verifier_rejection_repeat_streak: StrictInt = Field(default=0, ge=0)
    # Time-slice telemetry (additive; the v1 policy never reads these).
    # ``slice_preempted`` marks a phase that ended because its control slice
    # expired -- the next resume pays a full context reload.  The consecutive
    # counter lets policies tell a long reload chain (structural burn) from
    # natural invocation boundaries.
    slice_preempted: bool = False
    consecutive_slice_preemptions: StrictInt = Field(default=0, ge=0)
    unknown_io: bool = False
    session_id: str = Field(min_length=1)
    elapsed_ms: StrictInt = Field(default=0, ge=0)
    quality_probe: HarnessQualityProbe = Field(
        default_factory=HarnessQualityProbe.empty
    )

    @field_validator("session_id", mode="before")
    @classmethod
    def _non_empty_session_id(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("session_id must be non-empty")
        return normalized

    @field_validator("read_set", "write_set", mode="before")
    @classmethod
    def _sorted_unique_access_set(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


class HarnessContinuationDecision(_FrozenModel):
    """Deterministic continuation result.

    Handoff items are deliberately excluded from normal serialization and
    ``repr`` so decision telemetry cannot accidentally persist prompt text.
    The tuple remains directly accessible to the wrapper that starts the
    compacted replacement session.
    """

    action: HarnessContinuationAction
    reason: str = Field(min_length=1)
    context_score: float = Field(ge=0.0)
    cache_tokens_per_call: float = Field(ge=0.0)
    cumulative_session_cache_read_tokens: StrictInt = Field(default=0, ge=0)
    consecutive_max_tokens: StrictInt = Field(default=0, ge=0)
    event_progress_ratio: float | None = Field(default=None, ge=0.0)
    completed_without_verification: bool = False
    guard_triggers: tuple[str, ...] = ()
    bounded_handoff_items: tuple[str, ...] = Field(default=(), exclude=True, repr=False)
    decision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("guard_triggers", mode="before")
    @classmethod
    def _sorted_unique_guard_triggers(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


class HarnessEfficiencyEstimate(_FrozenModel):
    """Expected work avoided and control overhead for one optimization.

    ``avoided`` is the work the optimized path is expected to skip relative
    to native Harness execution.  ``overhead`` is extra work introduced by
    observation, compaction, checkpointing, or a restart.  Keeping the two
    buckets separate makes a benchmark report explain *why* an optimization
    was accepted or rejected instead of hiding the cost in one total.
    """

    baseline: HarnessUsage = Field(default_factory=HarnessUsage)
    avoided: HarnessUsage = Field(default_factory=HarnessUsage)
    overhead: HarnessUsage = Field(default_factory=HarnessUsage)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("confidence")
    @classmethod
    def _finite_confidence(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("confidence must be finite")
        return float(value)

    @property
    def net_token_units(self) -> int:
        return self.avoided.total_token_units - self.overhead.total_token_units

    @property
    def net_wall_time_ms(self) -> int:
        return self.avoided.wall_time_ms - self.overhead.wall_time_ms

    @property
    def net_tool_calls(self) -> int:
        return self.avoided.tool_calls - self.overhead.tool_calls

    @property
    def net_monetary_microusd(self) -> int:
        return self.avoided.monetary_microusd - self.overhead.monetary_microusd


class HarnessQualityObservation(_FrozenModel):
    """Optional normalized quality signal used by the guard.

    Scores are intentionally not restricted to ``[0, 1]`` because production
    Harnesses commonly expose rewards, test counts, or percentages.  The
    caller must use the same scale for ``reference_score`` and
    ``candidate_score``.  ``uncertainty`` is a conservative absolute margin;
    the lower confidence bound is candidate minus uncertainty minus reference.
    """

    reference_score: float | None = None
    candidate_score: float | None = None
    uncertainty: float = Field(default=0.0, ge=0.0)
    verifier_passed: bool | None = None

    @field_validator("reference_score", "candidate_score", mode="before")
    @classmethod
    def _finite_optional_score(cls, value: Any) -> float | None:
        if value is None:
            return None
        normalized = float(value)
        if not math.isfinite(normalized):
            raise ValueError("quality scores must be finite")
        return normalized

    @field_validator("uncertainty")
    @classmethod
    def _finite_uncertainty(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("uncertainty must be finite")
        return float(value)

    @property
    def lower_bound_delta(self) -> float | None:
        if self.reference_score is None or self.candidate_score is None:
            return None
        return self.candidate_score - self.uncertainty - self.reference_score


class HarnessBestCheckpoint(_FrozenModel):
    """Metadata for a verified checkpoint selected by a control plane.

    This is deliberately only a reference.  The adapter that owns the
    workspace performs the actual restore after validating its lease and
    manifest.  Keeping restore side effects out of this pure policy makes the
    guard safe to replay in an offline trace.
    """

    checkpoint_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    quality_score: float | None = None
    graph_version: StrictInt | None = Field(default=None, ge=0)
    verified: bool = True

    @field_validator("checkpoint_id", "task_id", mode="before")
    @classmethod
    def _non_empty_identity(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("checkpoint identity must be non-empty")
        return normalized

    @field_validator("quality_score", mode="before")
    @classmethod
    def _finite_optional_quality(cls, value: Any) -> float | None:
        if value is None:
            return None
        normalized = float(value)
        if not math.isfinite(normalized):
            raise ValueError("checkpoint quality_score must be finite")
        return normalized


class HarnessBestCheckpointStore(Protocol):
    """Minimal checkpoint lookup port for quality-aware rollback.

    Implementations may use the existing ``CheckpointManager`` for restore.
    The guard only asks for metadata; it never invokes filesystem/container
    side effects.
    """

    def get_best_checkpoint(
        self,
        task_id: str,
        *,
        graph_version: int | None = None,
    ) -> HarnessBestCheckpoint | None:
        """Return the best verified checkpoint compatible with a graph version."""


class HarnessEfficiencyGuardObservation(_FrozenModel):
    """Typed task capabilities and estimates consumed by the optional guard."""

    task_id: str = Field(min_length=1)
    one_shot: bool = False
    expected_phase_count: StrictInt | None = Field(default=None, ge=1)
    continuation_supported: bool = True
    graph_version: StrictInt | None = Field(default=None, ge=0)
    estimate: HarnessEfficiencyEstimate | None = None
    predicted_quality: HarnessQualityObservation | None = None
    observed_quality: HarnessQualityObservation | None = None
    best_checkpoint: HarnessBestCheckpoint | None = None

    @field_validator("task_id", mode="before")
    @classmethod
    def _non_empty_task_id(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("task_id must be non-empty")
        return normalized


class HarnessEfficiencyGuardDecision(_FrozenModel):
    """Auditable result of :class:`QualityConstrainedEfficiencyGuard`."""

    disposition: HarnessEfficiencyDisposition
    candidate_action: HarnessContinuationAction
    effective_action: HarnessContinuationAction | None = None
    reason: str = Field(min_length=1)
    efficiency_score: float = 0.0
    net_token_units: StrictInt = 0
    net_wall_time_ms: StrictInt = 0
    net_tool_calls: StrictInt = 0
    quality_delta_lower_bound: float | None = None
    checkpoint_id: str | None = None
    decision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("efficiency_score")
    @classmethod
    def _finite_efficiency_score(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("efficiency_score must be finite")
        return float(value)

    @field_validator("quality_delta_lower_bound", mode="before")
    @classmethod
    def _finite_optional_delta(cls, value: Any) -> float | None:
        if value is None:
            return None
        normalized = float(value)
        if not math.isfinite(normalized):
            raise ValueError("quality_delta_lower_bound must be finite")
        return normalized


def _efficiency_decision_hash(
    *,
    disposition: HarnessEfficiencyDisposition,
    candidate_action: HarnessContinuationAction,
    effective_action: HarnessContinuationAction | None,
    reason: str,
    efficiency_score: float,
    net_token_units: int,
    net_wall_time_ms: int,
    net_tool_calls: int,
    quality_delta_lower_bound: float | None,
    checkpoint_id: str | None,
    candidate_hash: str,
) -> str:
    payload = {
        "disposition": disposition.value,
        "candidate_action": candidate_action.value,
        "effective_action": effective_action.value if effective_action is not None else None,
        "reason": reason,
        "efficiency_score": efficiency_score,
        "net_token_units": net_token_units,
        "net_wall_time_ms": net_wall_time_ms,
        "net_tool_calls": net_tool_calls,
        "quality_delta_lower_bound": quality_delta_lower_bound,
        "checkpoint_id": checkpoint_id,
        "candidate_hash": candidate_hash,
    }
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _make_efficiency_guard_decision(
    *,
    disposition: HarnessEfficiencyDisposition,
    candidate: HarnessContinuationDecision,
    reason: str,
    efficiency_score: float = 0.0,
    net_token_units: int = 0,
    net_wall_time_ms: int = 0,
    net_tool_calls: int = 0,
    quality_delta_lower_bound: float | None = None,
    checkpoint_id: str | None = None,
) -> HarnessEfficiencyGuardDecision:
    score = round(float(efficiency_score), 6)
    quality_delta = (
        None if quality_delta_lower_bound is None else round(float(quality_delta_lower_bound), 6)
    )
    effective_action = (
        candidate.action if disposition is HarnessEfficiencyDisposition.APPLY else None
    )
    return HarnessEfficiencyGuardDecision(
        disposition=disposition,
        candidate_action=candidate.action,
        effective_action=effective_action,
        reason=reason,
        efficiency_score=score,
        net_token_units=int(net_token_units),
        net_wall_time_ms=int(net_wall_time_ms),
        net_tool_calls=int(net_tool_calls),
        quality_delta_lower_bound=quality_delta,
        checkpoint_id=checkpoint_id,
        decision_hash=_efficiency_decision_hash(
            disposition=disposition,
            candidate_action=candidate.action,
            effective_action=effective_action,
            reason=reason,
            efficiency_score=score,
            net_token_units=int(net_token_units),
            net_wall_time_ms=int(net_wall_time_ms),
            net_tool_calls=int(net_tool_calls),
            quality_delta_lower_bound=quality_delta,
            checkpoint_id=checkpoint_id,
            candidate_hash=candidate.decision_hash,
        ),
    )


def _efficiency_score(estimate: HarnessEfficiencyEstimate, weights: tuple[float, ...]) -> float:
    """Return a dimensionless weighted saving ratio.

    The calculation is constant time and skips dimensions for which the
    native baseline has no measurable work.  Token and wall-time ratios are
    kept separate until normalization, avoiding meaningless token/ms sums.
    """

    candidates: list[tuple[float, float]] = []
    metrics = (
        (estimate.net_token_units, estimate.baseline.total_token_units, weights[0]),
        (estimate.net_wall_time_ms, estimate.baseline.wall_time_ms, weights[1]),
        (estimate.net_tool_calls, estimate.baseline.tool_calls, weights[2]),
    )
    for net, baseline, weight in metrics:
        if weight > 0 and baseline > 0:
            candidates.append((weight, net / baseline))
    if not candidates:
        return 0.0
    return sum(weight * ratio for weight, ratio in candidates) / sum(
        weight for weight, _ in candidates
    )


@dataclass(frozen=True, slots=True)
class QualityConstrainedEfficiencyGuard:
    """Optional safety gate for applying continuation optimizations.

    The guard is intentionally disabled by default.  When enabled, it is a
    constant-time decision layer around an existing
    :class:`HarnessContinuationDecision`:

    * one-shot or unsupported Harnesses bypass semantic control;
    * an explicit avoided-work/overhead estimate must predict a non-negative
      token and wall-time result;
    * predicted or observed quality regression falls back to native execution;
    * an observed regression can request restoration of a compatible verified
      checkpoint, but the adapter performs that side effect.

    This keeps the current R4 behavior byte-for-byte at the continuation
    policy boundary unless a caller opts in with ``enabled=True``.
    """

    enabled: bool = False
    min_estimate_confidence: float = 0.70
    min_efficiency_score: float = 0.0
    token_weight: float = 0.5
    wall_time_weight: float = 0.5
    tool_weight: float = 0.0
    require_token_non_regression: bool = True
    require_wall_time_non_regression: bool = True
    require_tool_non_regression: bool = False
    max_quality_regression: float = 0.0
    one_shot_bypass: bool = True
    allow_checkpoint_restore: bool = True

    def __post_init__(self) -> None:
        for name, value in (
            ("min_estimate_confidence", self.min_estimate_confidence),
            ("min_efficiency_score", self.min_efficiency_score),
            ("token_weight", self.token_weight),
            ("wall_time_weight", self.wall_time_weight),
            ("tool_weight", self.tool_weight),
            ("max_quality_regression", self.max_quality_regression),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not 0 <= self.min_estimate_confidence <= 1:
            raise ValueError("min_estimate_confidence must be between zero and one")
        if self.token_weight < 0 or self.wall_time_weight < 0 or self.tool_weight < 0:
            raise ValueError("efficiency weights must not be negative")
        if self.token_weight + self.wall_time_weight + self.tool_weight <= 0:
            raise ValueError("at least one efficiency weight must be positive")
        if self.max_quality_regression < 0:
            raise ValueError("max_quality_regression must not be negative")

    @staticmethod
    def _checkpoint_compatible(
        checkpoint: HarnessBestCheckpoint | None,
        observation: HarnessEfficiencyGuardObservation,
    ) -> bool:
        if checkpoint is None or not checkpoint.verified:
            return False
        if checkpoint.task_id != observation.task_id:
            return False
        return not (
            observation.graph_version is not None
            and (
                checkpoint.graph_version is not None
                and checkpoint.graph_version != observation.graph_version
            )
        )

    def _resolve_checkpoint(
        self,
        observation: HarnessEfficiencyGuardObservation,
        checkpoint_store: HarnessBestCheckpointStore | None,
    ) -> HarnessBestCheckpoint | None:
        checkpoint = observation.best_checkpoint
        if checkpoint is None and checkpoint_store is not None:
            try:
                checkpoint = checkpoint_store.get_best_checkpoint(
                    observation.task_id,
                    graph_version=observation.graph_version,
                )
            except Exception:
                # A lookup failure must never turn into a destructive restore.
                return None
        if not self._checkpoint_compatible(checkpoint, observation):
            return None
        if checkpoint is None:
            return None
        reference_score = next(
            (
                signal.reference_score
                for signal in (
                    observation.observed_quality,
                    observation.predicted_quality,
                )
                if signal is not None and signal.reference_score is not None
            ),
            None,
        )
        if (
            checkpoint.quality_score is not None
            and reference_score is not None
            and checkpoint.quality_score < reference_score - self.max_quality_regression
        ):
            return None
        return checkpoint

    def _quality_regression(
        self,
        signal: HarnessQualityObservation | None,
    ) -> tuple[bool, float | None]:
        if signal is None or signal.verifier_passed is True:
            return False, None
        lower_bound = signal.lower_bound_delta
        if lower_bound is None:
            return False, None
        return lower_bound < -self.max_quality_regression, lower_bound

    def decide(
        self,
        candidate: HarnessContinuationDecision,
        observation: HarnessEfficiencyGuardObservation,
        *,
        checkpoint_store: HarnessBestCheckpointStore | None = None,
    ) -> HarnessEfficiencyGuardDecision:
        """Gate a candidate decision without executing Harness side effects.

        The guard is safe to call at every phase boundary.  All telemetry is
        bounded typed data, and the work per call is ``O(1)``: a fixed number
        of metric and quality comparisons plus an optional O(1) checkpoint
        lookup.  A caller can therefore place it on the hot path without
        rescanning the semantic graph or full transcript.
        """

        if not self.enabled:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.APPLY,
                candidate=candidate,
                reason="guard_disabled",
            )

        # A verifier-passed candidate is already a durable success boundary.
        # Never replace it with a speculative optimization action.
        if candidate.reason == "verifier_passed":
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.APPLY,
                candidate=candidate,
                reason="verifier_passed",
            )
        if not observation.continuation_supported:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.BYPASS_NATIVE,
                candidate=candidate,
                reason="continuation_unsupported",
            )
        if self.one_shot_bypass and (observation.one_shot or observation.expected_phase_count == 1):
            reason = "one_shot_bypass" if observation.one_shot else "single_phase_bypass"
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.BYPASS_NATIVE,
                candidate=candidate,
                reason=reason,
            )

        checkpoint = self._resolve_checkpoint(observation, checkpoint_store)
        for label, signal in (
            ("observed", observation.observed_quality),
            ("predicted", observation.predicted_quality),
        ):
            regressed, lower_bound = self._quality_regression(signal)
            if not regressed:
                continue
            if label == "observed" and self.allow_checkpoint_restore and checkpoint is not None:
                return _make_efficiency_guard_decision(
                    disposition=HarnessEfficiencyDisposition.RESTORE_BEST_CHECKPOINT,
                    candidate=candidate,
                    reason="quality_regression_restore_checkpoint",
                    quality_delta_lower_bound=lower_bound,
                    checkpoint_id=checkpoint.checkpoint_id,
                )
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason=f"quality_regression_{label}",
                quality_delta_lower_bound=lower_bound,
            )

        estimate = observation.estimate
        if estimate is None:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="efficiency_estimate_missing",
            )
        if estimate.confidence < self.min_estimate_confidence:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="efficiency_estimate_low_confidence",
            )

        net_tokens = estimate.net_token_units
        net_wall = estimate.net_wall_time_ms
        net_tools = estimate.net_tool_calls
        if self.require_token_non_regression and net_tokens < 0:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="estimated_token_regression",
                net_token_units=net_tokens,
                net_wall_time_ms=net_wall,
                net_tool_calls=net_tools,
            )
        if self.require_wall_time_non_regression and net_wall < 0:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="estimated_wall_time_regression",
                net_token_units=net_tokens,
                net_wall_time_ms=net_wall,
                net_tool_calls=net_tools,
            )
        if self.require_tool_non_regression and net_tools < 0:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="estimated_tool_regression",
                net_token_units=net_tokens,
                net_wall_time_ms=net_wall,
                net_tool_calls=net_tools,
            )

        score = _efficiency_score(
            estimate,
            (self.token_weight, self.wall_time_weight, self.tool_weight),
        )
        if score < self.min_efficiency_score or score <= 0:
            return _make_efficiency_guard_decision(
                disposition=HarnessEfficiencyDisposition.FALLBACK_NATIVE,
                candidate=candidate,
                reason="estimated_efficiency_insufficient",
                efficiency_score=score,
                net_token_units=net_tokens,
                net_wall_time_ms=net_wall,
                net_tool_calls=net_tools,
            )
        return _make_efficiency_guard_decision(
            disposition=HarnessEfficiencyDisposition.APPLY,
            candidate=candidate,
            reason="estimated_efficiency_accepted",
            efficiency_score=score,
            net_token_units=net_tokens,
            net_wall_time_ms=net_wall,
            net_tool_calls=net_tools,
        )


def _redact_secrets(value: str) -> str:
    redacted = _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        value,
    )
    redacted = _BEARER_TOKEN.sub("Bearer [REDACTED]", redacted)
    return _PREFIXED_SECRET.sub("[REDACTED]", redacted)


def _normalized_instruction(value: str) -> str:
    return _redact_secrets(" ".join(str(value).split()))


def _safe_artifact_uri(value: str) -> str | None:
    """Return a payload-free URI or ``None`` for an unsafe access-set item."""

    candidate = _redact_secrets(str(value).strip())
    if not candidate or any(character in candidate for character in ("\r", "\n", "\x00")):
        return None
    parsed = urlsplit(candidate)
    if not parsed.scheme or not _URI_SCHEME.fullmatch(parsed.scheme):
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    # Query strings and fragments often carry credentials or request payloads.
    bounded = urlunsplit((parsed.scheme.lower(), parsed.netloc, parsed.path, "", ""))
    return bounded or None


def _normalize_artifact_uri(value: str) -> str:
    """Normalize an artifact URI for deduplication.

    Strips fragments, trailing slashes, and collapses duplicate separators so
    two URIs pointing at the same artifact are deduped even when their string
    forms differ.  Deterministic: pure string normalization.
    """
    uri = value.strip()
    if "#" in uri:
        uri = uri[: uri.index("#")]
    scheme_sep = "://"
    if scheme_sep in uri:
        scheme, rest = uri.split(scheme_sep, 1)
        rest = rest.replace("//", "/")
        uri = scheme + scheme_sep + rest
    else:
        uri = uri.replace("//", "/")
    if len(uri) > 1 and uri.endswith("/"):
        uri = uri.rstrip("/")
    return uri


def _append_bounded(
    result: list[str],
    item: str,
    *,
    max_items: int,
    max_chars: int,
    allow_truncate: bool = True,
) -> bool:
    if len(result) >= max_items:
        return False
    used = sum(len(existing) for existing in result)
    remaining = max_chars - used
    if remaining <= 0:
        return False
    if len(item) > remaining and not allow_truncate:
        return True
    bounded = item[:remaining]
    if not bounded:
        return False
    result.append(bounded)
    return len(result) < max_items and sum(len(existing) for existing in result) < max_chars


def _middle_truncated(value: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(value) <= limit:
        return value
    marker = " ... [TRUNCATED] ... "
    if limit <= len(marker):
        return value[:limit]
    remaining = limit - len(marker)
    head = (remaining + 1) // 2
    tail = remaining - head
    return value[:head] + marker + (value[-tail:] if tail else "")


def build_semantic_handoff(
    original_instruction: str,
    observations: Sequence[HarnessPhaseObservation],
    *,
    max_items: int = 12,
    max_chars: int = 2048,
    recent_phases: int = 3,
) -> tuple[str, ...]:
    """Build a deterministic, redacted handoff for a compacted restart.

    Only a bounded instruction projection and artifact URIs from recent typed
    observations are admitted.  Session IDs, event payloads, model messages,
    tool arguments, and tool results are not part of this interface.
    """

    if max_items < 1:
        raise ValueError("max_items must be at least 1")
    if max_chars < 1:
        raise ValueError("max_chars must be at least 1")
    if recent_phases < 1:
        raise ValueError("recent_phases must be at least 1")

    instruction = str(original_instruction)
    instruction_digest = hashlib.sha256(instruction.encode("utf-8")).hexdigest()
    normalized = _normalized_instruction(instruction)
    instruction_item = f"instruction_sha256:{instruction_digest}"
    if normalized:
        instruction_item += f"; instruction:{normalized}"
    # A long task prompt must not evict the semantic working set it describes.
    # Reserve at least half of the character budget for recent artifact URIs
    # whenever the item budget permits them.
    instruction_limit = max_chars if max_items == 1 else min(1024, max(1, max_chars // 2))
    instruction_item = _middle_truncated(instruction_item, instruction_limit)

    result: list[str] = []
    if not _append_bounded(
        result,
        instruction_item,
        max_items=max_items,
        max_chars=max_chars,
    ):
        return tuple(result)

    recent = tuple(observations)[-recent_phases:]
    seen_uris: set[str] = set()
    seen_normalized: set[str] = set()
    # Preserve writes before reads across the whole recent window. A compacted
    # replacement can rediscover inputs, but losing the identity of a newly
    # produced artifact risks destructive recomputation.
    for access_kind, attribute in (("write_uri", "write_set"), ("read_uri", "read_set")):
        for observation in reversed(recent):
            if not isinstance(observation, HarnessPhaseObservation):
                continue
            access_set = getattr(observation, attribute)
            for raw_uri in access_set:
                uri = _safe_artifact_uri(raw_uri)
                if uri is None or uri in seen_uris:
                    continue
                normalized = _normalize_artifact_uri(uri)
                if normalized in seen_normalized:
                    continue
                seen_uris.add(uri)
                seen_normalized.add(normalized)
                if not _append_bounded(
                    result,
                    f"{access_kind}:{uri}",
                    max_items=max_items,
                    max_chars=max_chars,
                    allow_truncate=False,
                ):
                    return tuple(result)
    return tuple(result)


def _decision_hash(
    *,
    action: HarnessContinuationAction,
    reason: str,
    context_score: float,
    cache_tokens_per_call: float,
    cumulative_session_cache_read_tokens: int,
    consecutive_max_tokens: int,
    event_progress_ratio: float | None,
    completed_without_verification: bool,
    guard_triggers: tuple[str, ...],
    handoff_items: tuple[str, ...],
) -> str:
    payload = {
        "action": action.value,
        "reason": reason,
        "context_score": context_score,
        "cache_tokens_per_call": cache_tokens_per_call,
        "cumulative_session_cache_read_tokens": cumulative_session_cache_read_tokens,
        "consecutive_max_tokens": consecutive_max_tokens,
        "event_progress_ratio": event_progress_ratio,
        "completed_without_verification": completed_without_verification,
        "guard_triggers": list(guard_triggers),
        # Hashing rather than serializing handoff text keeps prompts out of
        # audit logs while binding the decision to the exact restart context.
        "handoff_item_hashes": [
            hashlib.sha256(item.encode("utf-8")).hexdigest() for item in handoff_items
        ],
    }
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _make_decision(
    *,
    action: HarnessContinuationAction,
    reason: str,
    context_score: float,
    cache_tokens_per_call: float,
    cumulative_session_cache_read_tokens: int = 0,
    consecutive_max_tokens: int = 0,
    event_progress_ratio: float | None = None,
    completed_without_verification: bool = False,
    guard_triggers: tuple[str, ...] = (),
    handoff_items: tuple[str, ...] = (),
) -> HarnessContinuationDecision:
    bounded_score = round(max(0.0, context_score), 6)
    bounded_cache_rate = round(max(0.0, cache_tokens_per_call), 6)
    bounded_event_ratio = (
        None
        if event_progress_ratio is None
        else round(max(0.0, event_progress_ratio), 6)
    )
    bounded_cumulative_cache = max(0, int(cumulative_session_cache_read_tokens))
    bounded_max_tokens = max(0, int(consecutive_max_tokens))
    bounded_triggers = tuple(
        sorted({str(item).strip() for item in guard_triggers if str(item).strip()})
    )
    return HarnessContinuationDecision(
        action=action,
        reason=reason,
        context_score=bounded_score,
        cache_tokens_per_call=bounded_cache_rate,
        cumulative_session_cache_read_tokens=bounded_cumulative_cache,
        consecutive_max_tokens=bounded_max_tokens,
        event_progress_ratio=bounded_event_ratio,
        completed_without_verification=bool(completed_without_verification),
        guard_triggers=bounded_triggers,
        bounded_handoff_items=handoff_items,
        decision_hash=_decision_hash(
            action=action,
            reason=reason,
            context_score=bounded_score,
            cache_tokens_per_call=bounded_cache_rate,
            cumulative_session_cache_read_tokens=bounded_cumulative_cache,
            consecutive_max_tokens=bounded_max_tokens,
            event_progress_ratio=bounded_event_ratio,
            completed_without_verification=bool(completed_without_verification),
            guard_triggers=bounded_triggers,
            handoff_items=handoff_items,
        ),
    )


def _validated_timeline(
    history: Sequence[HarnessPhaseObservation],
    current: HarnessPhaseObservation,
) -> tuple[HarnessPhaseObservation, ...] | None:
    try:
        prior = tuple(history)
    except (TypeError, ValueError):
        return None
    if not isinstance(current, HarnessPhaseObservation):
        return None
    if any(not isinstance(item, HarnessPhaseObservation) for item in prior):
        return None
    timeline = (*prior, current)
    if any(left.phase_index >= right.phase_index for left, right in pairwise(timeline)):
        return None

    closed_sessions: set[str] = set()
    active_session = timeline[0].session_id
    previous = timeline[0]
    for observation in timeline[1:]:
        if observation.session_id == active_session:
            if observation.event_count < previous.event_count:
                return None
            previous = observation
            continue
        closed_sessions.add(active_session)
        active_session = observation.session_id
        if active_session in closed_sessions:
            return None
        previous = observation
    return timeline


@dataclass(frozen=True, slots=True)
class SemanticContextPolicy:
    """Pure wrapper-level policy for resume versus compacted restart."""

    min_phases_before_restart: int = 2
    cache_tokens_per_call_threshold: float = 16_000.0
    cache_growth_ratio_threshold: float = 1.75
    progress_event_ratio_threshold: float = 0.8
    force_restart_context_score: float = 1.5
    max_consecutive_max_tokens: int = 2
    cumulative_cache_read_tokens_threshold: int = 96_000
    cumulative_cache_requires_max_tokens: int = 2
    no_progress_phases: int = 2
    no_progress_event_ratio_threshold: float = 0.10
    # --- Progressive-quality stall (漏洞3): OS senses whether the agent is
    # actually advancing or just spinning.  Zero-cost probe from the tool
    # trace; triggers a compacted restart only when tests are run and keep
    # failing while the distinct artifact set stops growing.  Conservative by
    # default so a hard-working (if unlucky) agent is never mis-killed.
    quality_stall_phases: int = 3
    quality_stall_error_ratio: float = 0.5
    quality_stall_min_test_calls: int = 1
    quality_stall_write_regression_phases: int = 2
    cooldown_phases: int = 2
    max_restarts: int = 6
    # --- Restart must pay for itself (漏洞A fix) ---------------------------
    # A compacted restart is only justified when the replacement session
    # produces new artifacts.  When the last N consecutive restarts each ended
    # without growing the cumulative write set, restarting yet again only burns
    # wall clock and tokens (observed matpower regression: 4 restarts on a
    # one-restart task pushed it into AgentTimeout and dropped reward 1.0 ->
    # 0.917).  This switches to RESUME once that threshold is reached so a
    # high-reward task is never churned to timeout by unproductive restarts.
    # Conservative: a restart whose session adds any new write resets the
    # counter, so a genuinely progressing agent is never blocked.
    restart_unproductive_restarts: int = 2
    # --- Restart payoff: cache-rebound aware suppression (漏洞B fix) --------
    # A compacted restart is only worthwhile when the compressed context keeps
    # the per-call cache low for a meaningful number of phases.  On tool-output
    # heavy tasks the context working set exceeds what compaction can shrink:
    # the cache rebounds to its pre-restart level within one or two phases, so
    # every restart only pays the fixed cost (summarisation call + re-reading
    # the compressed context + warm-up) and burns tokens for nothing.  We learn
    # this from the actual observation timeline: a restart is deemed a failure
    # when the cache of the replacement session reaches
    # restart_payoff_rebloat_ratio x the pre-restart cache within
    # restart_payoff_window_phases phases.  After restart_payoff_failures
    # consecutive failed restarts the policy stops restarting altogether and
    # lets the current session run to its natural end.  Purely a system-level
    # context signal (no reward, no artifact set), so it transfers to any long-
    # horizon harness workload.
    restart_payoff_window_phases: int = 3
    # 漏洞B fix v2：rebloat_ratio 1.0 -> 1.5。读密集任务（OpenROAD/spot/
    # 污染7 等反复读大文件/日志/flow 状态）restart 后 per-call cache 必然
    # 回弹到旧水平，1.0 会把每次 restart 都误判为 payoff 失败从而提前抑制
    # restart、context 无限膨胀到 timeout。只有 cache 涨到旧水平 1.5 倍
    # 以上才算真正回弹失败。
    restart_payoff_rebloat_ratio: float = 1.5
    restart_payoff_failures: int = 2
    max_handoff_items: int = 12
    max_handoff_chars: int = 2048
    recent_handoff_phases: int = 3

    def __post_init__(self) -> None:
        if self.min_phases_before_restart < 1:
            raise ValueError("min_phases_before_restart must be at least 1")
        if not math.isfinite(self.cache_tokens_per_call_threshold):
            raise ValueError("cache_tokens_per_call_threshold must be finite")
        if self.cache_tokens_per_call_threshold <= 0:
            raise ValueError("cache_tokens_per_call_threshold must be positive")
        if not math.isfinite(self.cache_growth_ratio_threshold):
            raise ValueError("cache_growth_ratio_threshold must be finite")
        if self.cache_growth_ratio_threshold <= 1:
            raise ValueError("cache_growth_ratio_threshold must be greater than 1")
        if not math.isfinite(self.progress_event_ratio_threshold):
            raise ValueError("progress_event_ratio_threshold must be finite")
        if self.progress_event_ratio_threshold < 0:
            raise ValueError("progress_event_ratio_threshold must not be negative")
        if not math.isfinite(self.force_restart_context_score):
            raise ValueError("force_restart_context_score must be finite")
        if self.force_restart_context_score <= 1:
            raise ValueError("force_restart_context_score must be greater than 1")
        if self.max_consecutive_max_tokens < 1:
            raise ValueError("max_consecutive_max_tokens must be at least 1")
        if self.cumulative_cache_read_tokens_threshold < 1:
            raise ValueError("cumulative_cache_read_tokens_threshold must be positive")
        if self.cumulative_cache_requires_max_tokens < 1:
            raise ValueError("cumulative_cache_requires_max_tokens must be at least 1")
        if self.no_progress_phases < 2:
            raise ValueError("no_progress_phases must be at least 2")
        if not math.isfinite(self.no_progress_event_ratio_threshold):
            raise ValueError("no_progress_event_ratio_threshold must be finite")
        if not 0 <= self.no_progress_event_ratio_threshold <= 1:
            raise ValueError(
                "no_progress_event_ratio_threshold must be between zero and one"
            )
        if self.quality_stall_phases < 2:
            raise ValueError("quality_stall_phases must be at least 2")
        if not math.isfinite(self.quality_stall_error_ratio):
            raise ValueError("quality_stall_error_ratio must be finite")
        if not 0 <= self.quality_stall_error_ratio <= 1:
            raise ValueError("quality_stall_error_ratio must be between zero and one")
        if self.quality_stall_min_test_calls < 1:
            raise ValueError("quality_stall_min_test_calls must be at least 1")
        if self.quality_stall_write_regression_phases < 1:
            raise ValueError(
                "quality_stall_write_regression_phases must be at least 1"
            )
        if self.cooldown_phases < 0:
            raise ValueError("cooldown_phases must not be negative")
        if self.max_restarts < 0:
            raise ValueError("max_restarts must not be negative")
        if self.restart_unproductive_restarts < 1:
            raise ValueError("restart_unproductive_restarts must be at least 1")
        if self.restart_payoff_window_phases < 1:
            raise ValueError("restart_payoff_window_phases must be at least 1")
        if not math.isfinite(self.restart_payoff_rebloat_ratio):
            raise ValueError("restart_payoff_rebloat_ratio must be finite")
        if self.restart_payoff_rebloat_ratio <= 0:
            raise ValueError("restart_payoff_rebloat_ratio must be positive")
        if self.restart_payoff_failures < 1:
            raise ValueError("restart_payoff_failures must be at least 1")
        if self.max_handoff_items < 1:
            raise ValueError("max_handoff_items must be at least 1")
        if self.max_handoff_chars < 1:
            raise ValueError("max_handoff_chars must be at least 1")
        if self.recent_handoff_phases < 1:
            raise ValueError("recent_handoff_phases must be at least 1")

    def decide(
        self,
        history: Sequence[HarnessPhaseObservation],
        current: HarnessPhaseObservation,
        *,
        original_instruction: str = "",
        critical_path_priority: float = 0.0,
    ) -> HarnessContinuationDecision:
        """Choose a continuation action without Harness-specific state.

        Invalid or ambiguous telemetry fails closed to ``RESUME``: a compacted
        restart is destructive to cognitive locality and therefore requires a
        complete, ordered observation timeline.
        """

        timeline = _validated_timeline(history, current)
        if timeline is None:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="invalid_observation_timeline",
                context_score=0.0,
                cache_tokens_per_call=0.0,
            )

        model_calls = current.usage.model_calls
        cache_tokens_per_call = (
            current.usage.cache_read_tokens / model_calls if model_calls > 0 else 0.0
        )
        same_session = tuple(
            observation for observation in timeline if observation.session_id == current.session_id
        )
        cumulative_session_cache = sum(
            observation.usage.cache_read_tokens for observation in same_session
        )
        consecutive_max_tokens = 0
        for observation in reversed(same_session):
            if not observation.max_tokens_checkpoint:
                break
            consecutive_max_tokens += 1

        event_deltas: list[int] = []
        previous_event_count = 0
        for observation in same_session:
            event_deltas.append(observation.event_count - previous_event_count)
            previous_event_count = observation.event_count
        latest_event_progress_ratio: float | None = None
        if len(event_deltas) >= 2:
            latest_event_progress_ratio = event_deltas[-1] / max(1, event_deltas[-2])

        prior_rates = tuple(
            observation.usage.cache_read_tokens / observation.usage.model_calls
            for observation in same_session[:-1]
            if observation.usage.model_calls > 0
        )
        baseline_rate = median(prior_rates) if prior_rates else 0.0
        growth_ratio = (
            cache_tokens_per_call / baseline_rate
            if baseline_rate > 0 and cache_tokens_per_call > 0
            else 0.0
        )
        # Critical-path tasks get elevated compaction thresholds.
        _cp = max(0.0, min(1.0, critical_path_priority))
        _cp_boost = 1.0 + _cp * 0.5
        _effective_cache_threshold = self.cache_tokens_per_call_threshold * _cp_boost
        _effective_cumulative_threshold = self.cumulative_cache_read_tokens_threshold * _cp_boost
        cache_pressure = cache_tokens_per_call / _effective_cache_threshold
        growth_pressure = (
            growth_ratio / self.cache_growth_ratio_threshold if growth_ratio > 0 else 0.0
        )
        checkpoint_pressure = (
            consecutive_max_tokens / self.max_consecutive_max_tokens
        )
        cumulative_cache_pressure = (
            cumulative_session_cache / _effective_cumulative_threshold
            if consecutive_max_tokens >= self.cumulative_cache_requires_max_tokens
            else 0.0
        )

        no_progress_window = False
        if len(same_session) >= self.no_progress_phases:
            observations = same_session[-self.no_progress_phases :]
            deltas = event_deltas[-self.no_progress_phases :]
            no_artifact_write = all(not observation.write_set for observation in observations)
            progress_ratios = tuple(
                right / max(1, left) for left, right in pairwise(deltas)
            )
            no_progress_window = bool(progress_ratios) and no_artifact_write and all(
                ratio <= self.no_progress_event_ratio_threshold for ratio in progress_ratios
            )
        progress_collapse_pressure = (
            1.0
            + (
                self.no_progress_event_ratio_threshold
                - (latest_event_progress_ratio or 0.0)
            )
            / max(self.no_progress_event_ratio_threshold, 1e-9)
            if no_progress_window
            else 0.0
        )
        context_score = max(
            cache_pressure,
            growth_pressure,
            checkpoint_pressure,
            cumulative_cache_pressure,
            progress_collapse_pressure,
        )
        completed_without_verification = bool(
            current.harness_completed and not current.verifier_passed
        )

        if current.verifier_passed:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="verifier_passed",
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
            )

        guard_triggers: list[str] = []
        if consecutive_max_tokens >= self.max_consecutive_max_tokens:
            guard_triggers.append("consecutive_max_tokens")
        if (
            cumulative_session_cache >= _effective_cumulative_threshold
            and consecutive_max_tokens >= self.cumulative_cache_requires_max_tokens
        ):
            guard_triggers.append("cumulative_session_cache")
        if no_progress_window:
            guard_triggers.append("semantic_progress_collapse")
        # Ordinary context-bloat path: when per-call cache input already
        # exceeds the threshold, compact immediately.  The predictive branch
        # below only handles the still-below-threshold case.
        if cache_tokens_per_call >= _effective_cache_threshold:
            guard_triggers.append("context_cache_bloat")

        # Predictive compaction: only when current value is still below the
        # threshold but the observed growth rate projects the next phase over it.
        # If the threshold is already exceeded, the ordinary context_bloat path
        # owns the decision so existing reason codes and tests stay stable.
        if (
            len(same_session) >= self.min_phases_before_restart
            and growth_ratio > 1.25
            and cache_tokens_per_call < _effective_cache_threshold
            and cache_tokens_per_call * growth_ratio > _effective_cache_threshold
            and "consecutive_max_tokens" not in guard_triggers
            and "predictive_cache_overflow" not in guard_triggers
        ):
            guard_triggers.append("predictive_cache_overflow")

        # --- Progressive-quality stall (漏洞3) ---------------------------
        # OS senses semantic stall: the agent keeps running tests/builds but
        # they keep failing AND the distinct artifact set stopped growing.
        # Uses only the zero-cost probe; never fires while writes grow, so a
        # progressing agent (even one with red tests mid-flight) is safe.
        if len(same_session) >= self.quality_stall_phases:
            window = same_session[-self.quality_stall_phases :]
            probes = [obs.quality_probe for obs in window if obs.quality_probe is not None]
            if len(probes) >= self.quality_stall_phases and all(
                probe.test_calls >= self.quality_stall_min_test_calls
                and probe.error_ratio > self.quality_stall_error_ratio
                for probe in probes
            ):
                write_window = same_session[
                    -self.quality_stall_write_regression_phases :
                ]
                prior_write_union: set[str] = set()
                for obs in window[: -self.quality_stall_write_regression_phases]:
                    prior_write_union.update(obs.write_set)
                latest_write_union: set[str] = set()
                for obs in write_window:
                    latest_write_union.update(obs.write_set)
                write_growth = latest_write_union - prior_write_union
                if not write_growth:
                    guard_triggers.append("quality_stall_no_progress")

        restart_count = sum(
            left.session_id != right.session_id for left, right in pairwise(timeline)
        )
        # --- Restart must pay for itself (漏洞A: restart 无收益停止) ------
        # Group observations by session; for each session after the first one
        # (i.e. each session born from a restart), check whether it grew the
        # cumulative write set.  Consecutive sessions that added no new writes
        # mean restarts are not paying off; once that run reaches
        # restart_unproductive_restarts, further restarts are suppressed and
        # we let the current session run to its natural end instead of churning
        # it into an AgentTimeout.  Any session with a new write resets the
        # counter, so a progressing agent is never blocked.
        restart_unproductive_stall = False
        if restart_count > 0:
            _session_unions: list[set[str]] = []
            _sid: str | None = None
            _writes: set[str] = set()
            for _obs in timeline:
                if _obs.session_id != _sid:
                    if _sid is not None:
                        _session_unions.append(_writes)
                    _sid = _obs.session_id
                    _writes = set(_obs.write_set or ())
                else:
                    _writes.update(_obs.write_set or ())
            if _sid is not None:
                _session_unions.append(_writes)
            _cum_writes: set[str] = set()
            _unproductive = 0
            for _idx, _union in enumerate(_session_unions):
                if _idx > 0:
                    if _union - _cum_writes:
                        _unproductive = 0
                    else:
                        _unproductive += 1
                _cum_writes.update(_union)
            restart_unproductive_stall = _unproductive >= self.restart_unproductive_restarts
        # --- Restart payoff: cache-rebound aware suppression (漏洞B) -------
        # Rebuild per-session per-call cache traces from the observation
        # timeline.  A restart is a payoff failure when the replacement
        # session's cache reaches rebloat_ratio x the pre-restart cache within
        # restart_payoff_window_phases phases (the compressed context did not
        # actually shrink the working set).  Once restart_payoff_failures
        # consecutive restarts have failed, suppress restarting entirely.
        restart_payoff_stall = False
        if restart_count > 0:
            _sess_caches: list[list[float]] = []
            _sid2: str | None = None
            _caches: list[float] = []
            for _obs in timeline:
                _c = (
                    _obs.usage.cache_read_tokens / _obs.usage.model_calls
                    if _obs.usage.model_calls > 0
                    else 0.0
                )
                if _obs.session_id != _sid2:
                    if _sid2 is not None:
                        _sess_caches.append(_caches)
                    _sid2 = _obs.session_id
                    _caches = [_c]
                else:
                    _caches.append(_c)
            if _sid2 is not None:
                _sess_caches.append(_caches)
            _payoff_failures = 0
            for _i in range(len(_sess_caches) - 1):
                _before = _sess_caches[_i][-1] if _sess_caches[_i] else 0.0
                _after = _sess_caches[_i + 1]
                _rebounded = any(
                    c >= _before * self.restart_payoff_rebloat_ratio
                    for c in _after[: self.restart_payoff_window_phases]
                )
                # 漏洞B fix v2：读密集豁免。restart 后 replacement session
                # 相对前 session 累计写集有新增（agent 仍在推进产出新文件）时，
                # per-call cache 回弹是读密集任务的正常现象（要重新读同一批
                # 大文件/日志），不代表 compaction 无效，不判 payoff 失败。
                # 只有"无新产出 + cache 回弹"才说明 restart 白做。
                _has_write_progress = False
                if _i + 1 < len(_session_unions):
                    _has_write_progress = bool(
                        _session_unions[_i + 1] - _session_unions[_i]
                    )
                if _rebounded and not _has_write_progress:
                    _payoff_failures += 1
                else:
                    _payoff_failures = 0
            restart_payoff_stall = _payoff_failures >= self.restart_payoff_failures
        # 漏洞B fix v2 / context 严重膨胀兜底（C）：cache 已达 2 倍阈值时
        # restart 必须发生，否则 context 无限膨胀拖到 timeout（实测 super-mario
        # cache 4.8x 被 unproductive 抑制、spot 4.6x 被 limit 抑制后全灭）。
        # 严重膨胀时绕过 unproductive/payoff stall 强制 restart；limit 处允许
        # 最多 2 次超限 emergency restart 给 context 一次压缩机会。
        _context_severe = (
            cache_tokens_per_call >= self.cache_tokens_per_call_threshold * 2.0
            and (
                consecutive_max_tokens >= 1
                or cache_tokens_per_call
                >= self.cache_tokens_per_call_threshold * 4.0
            )
        )
        if guard_triggers:
            trigger_tuple = tuple(guard_triggers)
            if restart_count >= self.max_restarts and not (
                _context_severe and restart_count < self.max_restarts + 2
            ):
                return _make_decision(
                    action=HarnessContinuationAction.RESUME,
                    reason="semantic_guard_restart_limit_reached",
                    context_score=context_score,
                    cache_tokens_per_call=cache_tokens_per_call,
                    cumulative_session_cache_read_tokens=cumulative_session_cache,
                    consecutive_max_tokens=consecutive_max_tokens,
                    event_progress_ratio=latest_event_progress_ratio,
                    completed_without_verification=completed_without_verification,
                    guard_triggers=trigger_tuple,
                )
            # Hard guards bypass the ordinary minimum window.  A cooldown only
            # protects the first phase of a replacement session; once the
            # replacement itself has accumulated two checkpoint/progress
            # observations it may be compacted again.
            if restart_count > 0 and len(same_session) < self.cooldown_phases:
                return _make_decision(
                    action=HarnessContinuationAction.RESUME,
                    reason="semantic_guard_restart_cooldown_active",
                    context_score=context_score,
                    cache_tokens_per_call=cache_tokens_per_call,
                    cumulative_session_cache_read_tokens=cumulative_session_cache,
                    consecutive_max_tokens=consecutive_max_tokens,
                    event_progress_ratio=latest_event_progress_ratio,
                    completed_without_verification=completed_without_verification,
                    guard_triggers=trigger_tuple,
                )
            if restart_unproductive_stall and not _context_severe:
                return _make_decision(
                    action=HarnessContinuationAction.RESUME,
                    reason="semantic_guard_restart_unproductive",
                    context_score=context_score,
                    cache_tokens_per_call=cache_tokens_per_call,
                    cumulative_session_cache_read_tokens=cumulative_session_cache,
                    consecutive_max_tokens=consecutive_max_tokens,
                    event_progress_ratio=latest_event_progress_ratio,
                    completed_without_verification=completed_without_verification,
                    guard_triggers=trigger_tuple,
                )
            if restart_payoff_stall and not _context_severe:
                return _make_decision(
                    action=HarnessContinuationAction.RESUME,
                    reason="semantic_guard_restart_payoff_failure",
                    context_score=context_score,
                    cache_tokens_per_call=cache_tokens_per_call,
                    cumulative_session_cache_read_tokens=cumulative_session_cache,
                    consecutive_max_tokens=consecutive_max_tokens,
                    event_progress_ratio=latest_event_progress_ratio,
                    completed_without_verification=completed_without_verification,
                    guard_triggers=trigger_tuple,
                )
            handoff = build_semantic_handoff(
                original_instruction,
                timeline,
                max_items=self.max_handoff_items,
                max_chars=self.max_handoff_chars,
                recent_phases=self.recent_handoff_phases,
            )
            return _make_decision(
                action=HarnessContinuationAction.RESTART_COMPACTED,
                reason="semantic_guard:" + "+".join(guard_triggers),
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
                guard_triggers=trigger_tuple,
                handoff_items=handoff,
            )

        if len(same_session) < self.min_phases_before_restart:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason=(
                    "harness_completed_not_verified"
                    if completed_without_verification
                    else "minimum_phase_window_not_reached"
                ),
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )

        if restart_count >= self.max_restarts and not (
            _context_severe and restart_count < self.max_restarts + 2
        ):
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="restart_limit_reached",
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )
        if restart_count > 0 and len(same_session) <= self.cooldown_phases:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="restart_cooldown_active",
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )

        high_cache_rate = (
            model_calls > 0 and cache_tokens_per_call >= self.cache_tokens_per_call_threshold
        )
        high_growth = bool(prior_rates) and growth_ratio >= self.cache_growth_ratio_threshold
        if not high_cache_rate and not high_growth:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason=(
                    "harness_completed_not_verified"
                    if completed_without_verification
                    else "context_within_limits"
                ),
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )

        prior_event_deltas = tuple(delta for delta in event_deltas[:-1] if delta > 0)
        event_progress_ratio = (
            event_deltas[-1] / median(prior_event_deltas)
            if prior_event_deltas and event_deltas[-1] > 0
            else 0.0
        )
        semantic_write_progress = bool(current.write_set)
        productive_event_activity = (
            bool(prior_event_deltas) and event_progress_ratio >= self.progress_event_ratio_threshold
        )
        force_restart = context_score >= self.force_restart_context_score
        if not force_restart and (semantic_write_progress or productive_event_activity):
            guarded_by = []
            if semantic_write_progress:
                guarded_by.append("artifact_write")
            if productive_event_activity:
                guarded_by.append("event_progress")
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="context_bloat_deferred:" + "+".join(guarded_by),
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )

        triggers = []
        if high_cache_rate:
            triggers.append("cache_tokens_per_call")
        if high_growth:
            triggers.append("cache_growth")
        if restart_unproductive_stall and not _context_severe:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="restart_unproductive_stall",
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )
        if restart_payoff_stall and not _context_severe:
            return _make_decision(
                action=HarnessContinuationAction.RESUME,
                reason="restart_payoff_stall",
                context_score=context_score,
                cache_tokens_per_call=cache_tokens_per_call,
                cumulative_session_cache_read_tokens=cumulative_session_cache,
                consecutive_max_tokens=consecutive_max_tokens,
                event_progress_ratio=latest_event_progress_ratio,
                completed_without_verification=completed_without_verification,
            )
        handoff = build_semantic_handoff(
            original_instruction,
            timeline,
            max_items=self.max_handoff_items,
            max_chars=self.max_handoff_chars,
            recent_phases=self.recent_handoff_phases,
        )
        return _make_decision(
            action=HarnessContinuationAction.RESTART_COMPACTED,
            reason="context_bloat:" + "+".join(triggers),
            context_score=context_score,
            cache_tokens_per_call=cache_tokens_per_call,
            cumulative_session_cache_read_tokens=cumulative_session_cache,
            consecutive_max_tokens=consecutive_max_tokens,
            event_progress_ratio=latest_event_progress_ratio,
            completed_without_verification=completed_without_verification,
            handoff_items=handoff,
        )


__all__ = [
    "HarnessBestCheckpoint",
    "HarnessBestCheckpointStore",
    "HarnessContinuationAction",
    "HarnessContinuationDecision",
    "HarnessEfficiencyDisposition",
    "HarnessEfficiencyEstimate",
    "HarnessEfficiencyGuardDecision",
    "HarnessEfficiencyGuardObservation",
    "HarnessPhaseObservation",
    "HarnessQualityObservation",
    "QualityConstrainedEfficiencyGuard",
    "SemanticContextPolicy",
    "build_semantic_handoff",
]

