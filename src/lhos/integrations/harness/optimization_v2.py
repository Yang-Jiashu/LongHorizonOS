"""Semantic continuation policy v2 -- restart as an investment decision.

Isolated from ``optimization.SemanticContextPolicy`` (v1), which is left
untouched; both policies can be replayed over the same decision logs.  Selected
per run via the ``semantic_policy_version`` harness-agent option.

Design rationale (evidence from the LHTB drop-fix batches):

* A restart is an *investment*, not a water-level alarm.  Three distinct
  failure modes get three distinct gates instead of one shared trigger list:

  1. LOOP ESCAPE -- the harness verifier repeats the byte-identical rejection
     (``verifier_rejection_repeat_streak``) and nothing new is being written.
     A restart helps even with a thin handoff: the current context *is* the
     problem.
  2. SEVERE VALVE -- per-call cache crosses ``hard_cache_ratio`` x the soft
     threshold.  Rate-limited (``restart_rate_limit_phases``) rather than
     absolutely capped, so a runaway session always has an out but cannot
     churn (observed: restart-cap exhaustion left one session riding at 11.5x
     the soft line for 130 phases).
  3. COST INVESTMENT -- the soft threshold is crossed.  Restart only when the
     restart can actually carry state (handoff item floor), the agent is not
     visibly converging (writes landing, or failure digests still evolving),
     and the previous restart demonstrably paid off (no instant re-bloat
     without new writes).

* ``min_phases_before_restart`` is honoured by *every* restart path (v1's
  guard branch bypassed it, beheading sessions at phases 1-3).

Second-round additions (evidence: the 46-case LHTB architecture review):

* TERMINATE gates (P1/P2) -- v1 had two governance levels (nothing, then
  the 80M hard cap) and no completion/diminishing-returns primitive, so
  every task rode its full budget.  ``v2_stop_loss_halt`` (default ON)
  fires when burn since the last progress marker crosses
  ``stop_loss_cache_tokens`` (debt survives restarts).
  ``v2_converged_halt`` (default OFF pending live-telemetry validation)
  fires on a quiet window (no writes/tests/verifier engagement, flat
  events); replay showed its signal cannot distinguish a brain-dead
  session from a long solver call in flight.  Both are fail-closed so
  productive work is never interrupted.
* Deferral cap (P0/P1) -- the converging-write vote may defer a restart at
  most ``max_converging_deferrals`` times; each deferred slice pays a full
  reload at the bloated rate, so the cap takes the rich handoff instead of
  riding the soft line (the apex-style 130-phase ride pattern).
* Early error spin (P6) -- a young session spinning on tool errors below
  the cache line is restarted early; v1/v2-classic were blind there.
* Adaptive slice sizing (P0) -- ``recommended_slice_seconds`` shares policy
  state with the time slicer: shrink near decision points, grow to amortize
  reloads on quiet clean sessions.

Fail-closed like v1: invalid or ambiguous telemetry resolves to RESUME.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from itertools import pairwise
from typing import Any, Sequence

from .optimization import (
    HarnessContinuationAction,
    HarnessContinuationDecision,
    HarnessPhaseObservation,
    build_semantic_handoff,
)


def _rate(observation: HarnessPhaseObservation) -> float:
    calls = observation.usage.model_calls
    return observation.usage.cache_read_tokens / calls if calls > 0 else 0.0


def _decision_hash_v2(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SemanticContextPolicyV2:
    """Restart-as-investment continuation policy (see module docstring)."""

    soft_cache_tokens_per_call: float = 16_000.0
    hard_cache_ratio: float = 4.0
    min_phases_before_restart: int = 2
    cooldown_phases: int = 2
    restart_rate_limit_phases: int = 4
    max_restarts: int = 24  # sanity backstop, not the operating constraint
    min_handoff_items: int = 4
    loop_repeat_streak: int = 3
    loop_no_write_phases: int = 2
    payoff_rebloat_ratio: float = 1.5
    payoff_window_phases: int = 3
    max_handoff_items: int = 12
    max_handoff_chars: int = 2048
    recent_handoff_phases: int = 3
    # --- halt gates (P1/P2): TERMINATE instead of riding the budget ---
    # v1 had exactly two governance levels: nothing, then the 80M hard cap.
    # These gates add the missing middle: halt when burn since the last
    # progress marker exceeds the stop-loss (default ON: 8M of zero-progress
    # cache burn is an unambiguous economic signal), or when the task has
    # semantically converged (default OFF: replay showed the quiet-window
    # pattern cannot distinguish "brain-dead session" from "a long solver
    # call is still running" at the observation level -- enable only after
    # live telemetry validation).  Both are fail-closed: any write, test
    # call, verifier-digest evolution, or unclassifiable I/O resets/blocks
    # them, so productive work is never interrupted (reward red line).
    convergence_halt_enabled: bool = False
    stop_loss_halt_enabled: bool = True
    convergence_window_phases: int = 4
    convergence_min_session_phases: int = 6
    convergence_event_ratio: float = 0.02
    stop_loss_cache_tokens: float = 8_000_000.0
    stop_loss_min_session_phases: int = 4
    # --- deferral cap (P0/P1): "converging" must not ride the soft line ---
    # Writes landing above the soft line defer a restart, but each deferred
    # slice also pays a full reload at the bloated rate.  After this many
    # consecutive deferrals the policy takes the (rich) handoff and restarts.
    max_converging_deferrals: int = 3
    # --- early blind zone (P6): error-spin escape below the cache line ---
    # v1/v2 both went blind below the soft cache line, so sessions that spun
    # on tool errors from phase 1 were never helped.  A young session whose
    # phases are all error-dominated with zero writes is restarted early;
    # the context is the problem, so a thin handoff is acceptable.
    early_spin_phases: int = 6
    early_spin_min_tool_calls: int = 10
    early_spin_error_ratio: float = 0.8
    # --- adaptive control slice (P0): share policy state with the slicer ---
    # The time slicer and the restart policy used to be independent, so their
    # costs multiplied (every slice pays a full reload; restarts add
    # re-exploration).  recommended_slice_seconds() lets the slicer amortize
    # reloads on quiet sessions and tighten control near decision points.
    #
    # Downscale defaults OFF (factor 1.0): the 2026-09-04 dropfix batch showed
    # a fatal interaction with the slice no-progress watchdog -- a downscaled
    # (37.5s) slice preempts a slow big-context model call mid-flight, records
    # zero durable events, and the watchdog kills the whole task
    # (apex-openroad/apexmgmt both died on their first downscaled slice).
    # Downscale also fires exactly when calls are slowest (cache rate near the
    # soft line), so it cannot be made safe without call-duration telemetry.
    slice_control_soft_ratio: float = 0.75
    slice_relax_soft_ratio: float = 0.5
    slice_upscale_factor: float = 2.0
    slice_downscale_factor: float = 1.0
    slice_min_seconds: float = 30.0
    slice_max_seconds: float = 240.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.soft_cache_tokens_per_call) or self.soft_cache_tokens_per_call <= 0:
            raise ValueError("soft_cache_tokens_per_call must be positive and finite")
        if not math.isfinite(self.hard_cache_ratio) or self.hard_cache_ratio <= 1:
            raise ValueError("hard_cache_ratio must be greater than 1")
        if self.min_phases_before_restart < 1:
            raise ValueError("min_phases_before_restart must be at least 1")
        if self.cooldown_phases < 0:
            raise ValueError("cooldown_phases must not be negative")
        if self.restart_rate_limit_phases < 1:
            raise ValueError("restart_rate_limit_phases must be at least 1")
        if self.max_restarts < 0:
            raise ValueError("max_restarts must not be negative")
        if self.min_handoff_items < 1:
            raise ValueError("min_handoff_items must be at least 1")
        if self.loop_repeat_streak < 2:
            raise ValueError("loop_repeat_streak must be at least 2")
        if self.loop_no_write_phases < 1:
            raise ValueError("loop_no_write_phases must be at least 1")
        if not math.isfinite(self.payoff_rebloat_ratio) or self.payoff_rebloat_ratio <= 0:
            raise ValueError("payoff_rebloat_ratio must be positive and finite")
        if self.payoff_window_phases < 1:
            raise ValueError("payoff_window_phases must be at least 1")
        if self.max_handoff_items < 1:
            raise ValueError("max_handoff_items must be at least 1")
        if self.max_handoff_chars < 1:
            raise ValueError("max_handoff_chars must be at least 1")
        if self.recent_handoff_phases < 1:
            raise ValueError("recent_handoff_phases must be at least 1")
        if self.convergence_window_phases < 2:
            raise ValueError("convergence_window_phases must be at least 2")
        if self.convergence_min_session_phases < self.convergence_window_phases:
            raise ValueError(
                "convergence_min_session_phases must be >= convergence_window_phases"
            )
        if not math.isfinite(self.convergence_event_ratio) or not 0.0 <= self.convergence_event_ratio <= 1.0:
            raise ValueError("convergence_event_ratio must be in [0, 1]")
        if not math.isfinite(self.stop_loss_cache_tokens) or self.stop_loss_cache_tokens <= 0:
            raise ValueError("stop_loss_cache_tokens must be positive and finite")
        if self.stop_loss_min_session_phases < 1:
            raise ValueError("stop_loss_min_session_phases must be at least 1")
        if self.max_converging_deferrals < 1:
            raise ValueError("max_converging_deferrals must be at least 1")
        if self.early_spin_phases < self.min_phases_before_restart:
            raise ValueError("early_spin_phases must be >= min_phases_before_restart")
        if self.early_spin_min_tool_calls < 1:
            raise ValueError("early_spin_min_tool_calls must be at least 1")
        if not math.isfinite(self.early_spin_error_ratio) or not 0.0 < self.early_spin_error_ratio <= 1.0:
            raise ValueError("early_spin_error_ratio must be in (0, 1]")
        if not math.isfinite(self.slice_control_soft_ratio) or not 0.0 < self.slice_control_soft_ratio <= 1.0:
            raise ValueError("slice_control_soft_ratio must be in (0, 1]")
        if not math.isfinite(self.slice_relax_soft_ratio) or not 0.0 < self.slice_relax_soft_ratio < 1.0:
            raise ValueError("slice_relax_soft_ratio must be in (0, 1)")
        if self.slice_relax_soft_ratio >= self.slice_control_soft_ratio:
            raise ValueError("slice_relax_soft_ratio must be < slice_control_soft_ratio")
        if not math.isfinite(self.slice_upscale_factor) or self.slice_upscale_factor <= 1.0:
            raise ValueError("slice_upscale_factor must be greater than 1")
        if not math.isfinite(self.slice_downscale_factor) or not 0.0 < self.slice_downscale_factor <= 1.0:
            raise ValueError("slice_downscale_factor must be in (0, 1]; 1.0 disables downscale")
        if not math.isfinite(self.slice_min_seconds) or self.slice_min_seconds <= 0:
            raise ValueError("slice_min_seconds must be positive and finite")
        if not math.isfinite(self.slice_max_seconds) or self.slice_max_seconds < self.slice_min_seconds:
            raise ValueError("slice_max_seconds must be >= slice_min_seconds")

    # ------------------------------------------------------------------ utils

    def _resume(
        self,
        reason: str,
        current: HarnessPhaseObservation,
        cache_rate: float,
        same_session: list[HarnessPhaseObservation],
        consecutive_max_tokens: int,
    ) -> HarnessContinuationDecision:
        return self._decision(
            HarnessContinuationAction.RESUME,
            reason,
            current,
            cache_rate,
            same_session,
            consecutive_max_tokens,
            (),
            (),
        )

    def _decision(
        self,
        action: HarnessContinuationAction,
        reason: str,
        current: HarnessPhaseObservation,
        cache_rate: float,
        same_session: list[HarnessPhaseObservation],
        consecutive_max_tokens: int,
        guard_triggers: tuple[str, ...],
        handoff: tuple[str, ...],
    ) -> HarnessContinuationDecision:
        cumulative_cache = sum(o.usage.cache_read_tokens for o in same_session)
        score = (
            round(max(0.0, cache_rate / self.soft_cache_tokens_per_call), 6)
            if self.soft_cache_tokens_per_call > 0
            else 0.0
        )
        return HarnessContinuationDecision(
            action=action,
            reason=reason,
            context_score=score,
            cache_tokens_per_call=round(max(0.0, cache_rate), 6),
            cumulative_session_cache_read_tokens=max(0, int(cumulative_cache)),
            consecutive_max_tokens=max(0, int(consecutive_max_tokens)),
            event_progress_ratio=None,
            completed_without_verification=bool(
                current.harness_completed and not current.verifier_passed
            ),
            guard_triggers=guard_triggers,
            bounded_handoff_items=handoff,
            decision_hash=_decision_hash_v2(
                {
                    "policy": "v2",
                    "action": action.value,
                    "reason": reason,
                    "context_score": score,
                    "cache_tokens_per_call": round(max(0.0, cache_rate), 6),
                    "guard_triggers": list(guard_triggers),
                    "handoff_item_hashes": [
                        hashlib.sha256(item.encode("utf-8")).hexdigest()
                        for item in handoff
                    ],
                }
            ),
        )

    def _restart(
        self,
        reason: str,
        trigger: str,
        original_instruction: str,
        timeline: list[HarnessPhaseObservation],
        current: HarnessPhaseObservation,
        cache_rate: float,
        same_session: list[HarnessPhaseObservation],
        consecutive_max_tokens: int,
    ) -> HarnessContinuationDecision:
        handoff = build_semantic_handoff(
            original_instruction,
            timeline,
            max_items=self.max_handoff_items,
            max_chars=self.max_handoff_chars,
            recent_phases=self.recent_handoff_phases,
        )
        return self._decision(
            HarnessContinuationAction.RESTART_COMPACTED,
            reason,
            current,
            cache_rate,
            same_session,
            consecutive_max_tokens,
            (trigger,),
            handoff,
        )

    def _terminate(
        self,
        reason: str,
        trigger: str,
        current: HarnessPhaseObservation,
        cache_rate: float,
        same_session: list[HarnessPhaseObservation],
        consecutive_max_tokens: int,
    ) -> HarnessContinuationDecision:
        return self._decision(
            HarnessContinuationAction.TERMINATE,
            reason,
            current,
            cache_rate,
            same_session,
            consecutive_max_tokens,
            (trigger,),
            (),
        )

    @staticmethod
    def _session_groups(
        timeline: list[HarnessPhaseObservation],
    ) -> list[list[HarnessPhaseObservation]]:
        groups: list[list[HarnessPhaseObservation]] = []
        current_session: str | None = None
        for obs in timeline:
            if obs.session_id != current_session:
                groups.append([])
                current_session = obs.session_id
            groups[-1].append(obs)
        return groups

    def _last_restart_unpaid(
        self, groups: list[list[HarnessPhaseObservation]]
    ) -> bool:
        """True when the previous restart failed its payoff test.

        The current generation (born from the last restart) re-bloated to
        ``payoff_rebloat_ratio`` x the pre-restart per-call cache within its
        first ``payoff_window_phases`` phases *and* produced no new writes so
        far.  Only the most recent restart is judged; older debts expire.
        """

        if len(groups) < 2:
            return False
        previous, current = groups[-2], groups[-1]
        pre_rate = _rate(previous[-1])
        if pre_rate <= 0:
            return False
        window_rates = [
            _rate(obs)
            for obs in current[: self.payoff_window_phases]
            if _rate(obs) > 0
        ]
        rebloated = any(
            rate >= pre_rate * self.payoff_rebloat_ratio for rate in window_rates
        )
        new_writes = any(obs.write_set for obs in current)
        return rebloated and not new_writes

    # ------------------------------------------------------- halt gate helpers

    @staticmethod
    def _is_progress_phase(
        previous: HarnessPhaseObservation | None,
        current: HarnessPhaseObservation,
    ) -> bool:
        """A phase counts as progress when it lands writes, runs tests, or
        moves the verifier: a rejection whose digest differs from the last
        one (repeat streak reset to 1) means the harness verifier is still
        seeing evolving state -- e.g. a game score changing.  Identical
        digests (streak >= 2) are a stall, not progress.
        """
        if current.write_set or current.quality_probe.distinct_writes > 0:
            return True
        if current.quality_probe.test_calls > 0:
            return True
        prior_rejections = previous.verifier_rejection_count if previous else 0
        if (
            current.verifier_rejection_count > prior_rejections
            and current.verifier_rejection_repeat_streak <= 1
        ):
            return True
        return False

    def _burn_since_last_progress(
        self, timeline: list[HarnessPhaseObservation]
    ) -> int:
        """Cumulative cache-read tokens burned after the most recent progress
        phase, across session boundaries.  Restarting does not clear the
        debt: a restart that fails to restore productivity must not buy
        unlimited budget (the missing middle rung between no governance and
        the hard cap).
        """
        burn = 0
        previous: HarnessPhaseObservation | None = None
        for obs in timeline:
            if self._is_progress_phase(previous, obs):
                burn = 0
            else:
                burn += max(0, int(obs.usage.cache_read_tokens))
            previous = obs
        return burn

    def _converged(self, same_session: list[HarnessPhaseObservation]) -> bool:
        """True when the trailing window shows a semantically quiet session:
        no writes, no tests, no verifier engagement, and flat event growth --
        while the model was actually active (an all-zero window is a tracing
        gap, which fails closed).
        """
        if len(same_session) < self.convergence_min_session_phases:
            return False
        window = same_session[-self.convergence_window_phases :]
        if any(o.unknown_io for o in window):
            return False
        if any(
            o.write_set
            or o.quality_probe.distinct_writes > 0
            or o.quality_probe.test_calls > 0
            for o in window
        ):
            return False
        if window[-1].verifier_rejection_count != window[0].verifier_rejection_count:
            return False
        first, last = window[0], window[-1]
        growth = last.event_count - first.event_count
        if growth > max(1, int(first.event_count * self.convergence_event_ratio)):
            return False
        if sum(o.usage.model_calls for o in window) < 1:
            return False
        return True

    def _early_error_spin(self, same_session: list[HarnessPhaseObservation]) -> bool:
        """True when a young session is spinning on tool errors below the
        cache line: every active phase is error-dominated, nothing has been
        written, and enough tool calls have accrued to rule out a slow start.
        """
        if not self.min_phases_before_restart <= len(same_session) <= self.early_spin_phases:
            return False
        total_tool_calls = 0
        for obs in same_session:
            if obs.unknown_io:
                return False
            if obs.write_set or obs.quality_probe.distinct_writes > 0:
                return False
            probe = obs.quality_probe
            total_tool_calls += probe.tool_calls
            if probe.tool_calls > 0 and probe.error_ratio < self.early_spin_error_ratio:
                return False
        return total_tool_calls >= self.early_spin_min_tool_calls

    def _converging_deferrals(self, same_session: list[HarnessPhaseObservation]) -> int:
        """Trailing count of phases that would have deferred a restart via
        the converging-write vote: writes landing while the per-call cache
        rate sat between the soft and hard lines.  The policy is a pure
        function, so the count is reconstructed from the observation
        timeline rather than stored.
        """
        hard_line = self.soft_cache_tokens_per_call * self.hard_cache_ratio
        count = 0
        for obs in reversed(same_session[:-1]):
            rate = _rate(obs)
            if obs.write_set and self.soft_cache_tokens_per_call <= rate < hard_line:
                count += 1
            else:
                break
        return count

    # ------------------------------------------------------------ slice sizing

    def recommended_slice_seconds(
        self,
        history: Sequence[HarnessPhaseObservation],
        current: HarnessPhaseObservation,
        base_seconds: float,
    ) -> float:
        """Policy-aware control-slice length (P0: the slicer and the restart
        policy share state instead of multiplying each other's costs).

        * Near a decision point (cache rate approaching the soft line, fresh
          verifier rejections, max-tokens checkpoint, or error-heavy phase)
          the slice shrinks so the policy regains control sooner.
        * On a quiet, clean, sub-threshold session the slice grows so fewer
          resumes pay the full context reload.
        * Anything ambiguous fails closed to ``base_seconds``.
        """
        if not math.isfinite(base_seconds) or base_seconds <= 0:
            return base_seconds
        timeline = list(history) + [current]
        if not all(isinstance(o, HarnessPhaseObservation) for o in timeline):
            return float(base_seconds)
        rate = _rate(current)
        probe = current.quality_probe
        window_start = timeline[-3] if len(timeline) >= 3 else timeline[0]
        recent_rejections = (
            current.verifier_rejection_count - window_start.verifier_rejection_count
        )
        if (
            rate >= self.slice_control_soft_ratio * self.soft_cache_tokens_per_call
            or current.verifier_rejection_repeat_streak >= 2
            or recent_rejections > 0
            or current.max_tokens_checkpoint
            or probe.error_ratio >= 0.5
        ):
            # Downscale must never exceed base_seconds (factor 1.0 = off must
            # be an exact no-op), and never go below slice_min_seconds.
            return min(
                float(base_seconds),
                max(
                    self.slice_min_seconds,
                    float(base_seconds) * self.slice_downscale_factor,
                ),
            )
        if (
            0 < rate <= self.slice_relax_soft_ratio * self.soft_cache_tokens_per_call
            and probe.error_ratio <= 0.2
            and recent_rejections == 0
            and not current.unknown_io
        ):
            return min(
                self.slice_max_seconds,
                float(base_seconds) * self.slice_upscale_factor,
            )
        return float(base_seconds)

    # ------------------------------------------------------------------ decide

    def decide(
        self,
        history: Sequence[HarnessPhaseObservation],
        current: HarnessPhaseObservation,
        *,
        original_instruction: str = "",
        critical_path_priority: float = 0.0,
    ) -> HarnessContinuationDecision:
        timeline = list(history) + [current]
        if not all(isinstance(o, HarnessPhaseObservation) for o in timeline):
            raise ValueError("timeline must contain HarnessPhaseObservation")

        model_calls = current.usage.model_calls
        cache_rate = (
            current.usage.cache_read_tokens / model_calls if model_calls > 0 else 0.0
        )
        same_session = [
            o for o in timeline if o.session_id == current.session_id
        ]
        restart_count = sum(
            1 for left, right in pairwise(timeline)
            if left.session_id != right.session_id
        )
        consecutive_max_tokens = 0
        for obs in reversed(same_session):
            if not obs.max_tokens_checkpoint:
                break
            consecutive_max_tokens += 1
        phases_in_session = len(same_session)
        hard_line = self.soft_cache_tokens_per_call * self.hard_cache_ratio

        if current.verifier_passed:
            return self._resume(
                "v2_verifier_passed",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        # Warm-up and cooldown are honoured by EVERY restart path: v1's guard
        # branch bypassed them, beheading sessions at phases 1-3.
        if phases_in_session < self.min_phases_before_restart:
            return self._resume(
                "v2_warmup",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        if restart_count > 0 and phases_in_session <= self.cooldown_phases:
            return self._resume(
                "v2_cooldown",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        if restart_count >= self.max_restarts:
            return self._resume(
                "v2_restart_cap",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )

        groups = self._session_groups(timeline)
        rate_limited = phases_in_session < self.restart_rate_limit_phases

        # (1) Loop escape: byte-identical verifier failures, nothing new
        # written.  A restart helps even with a thin handoff because the
        # current context is the problem.
        window = same_session[-self.loop_no_write_phases :]
        no_recent_writes = all(not obs.write_set for obs in window)
        if (
            current.verifier_rejection_repeat_streak >= self.loop_repeat_streak
            and no_recent_writes
            and not rate_limited
        ):
            return self._restart(
                "v2_loop_escape",
                "verifier_failure_loop",
                original_instruction,
                timeline,
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )

        # (1b) Early error spin (P6 blind zone): a young session spinning on
        # tool errors below the cache line gets the same escape as a
        # verifier loop -- the context is the problem, thin handoff is fine.
        if self._early_error_spin(same_session) and not rate_limited:
            return self._restart(
                "v2_early_error_spin",
                "early_error_spin",
                original_instruction,
                timeline,
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )

        # (2) Severe valve: rate-limited, so a runaway session always has an
        # out but can never churn.
        if cache_rate >= hard_line and not rate_limited:
            return self._restart(
                "v2_severe_valve",
                "severe_cache_bloat",
                original_instruction,
                timeline,
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )

        # (2b) Halt gates (P1/P2): the middle rung between "no governance"
        # and the 80M hard cap.  Both fail closed on any sign of life, so
        # productive work is never interrupted; they only stop burn that was
        # buying nothing.  Burn debt survives restarts, but a fresh
        # generation gets stop_loss_min_session_phases phases to prove life.
        if self.convergence_halt_enabled and self._converged(same_session):
            return self._terminate(
                "v2_converged_halt",
                "semantic_convergence",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        if (
            self.stop_loss_halt_enabled
            and len(same_session) >= self.stop_loss_min_session_phases
            and self._burn_since_last_progress(timeline)
            >= self.stop_loss_cache_tokens
        ):
            return self._terminate(
                "v2_stop_loss_halt",
                "stop_loss_burn",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )

        # (3) Cost investment below the hard line.
        if cache_rate < self.soft_cache_tokens_per_call:
            return self._resume(
                "v2_within_limits",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        # Converging agents are never interrupted: writes landing this phase,
        # or the verifier failure digest still evolving (streak == 1 means the
        # latest rejection differs from the previous one).  The deferral is
        # capped (P0/P1): each deferred slice pays a full reload at the
        # bloated rate, so after max_converging_deferrals the policy takes
        # the rich handoff instead of riding the soft line.
        if current.write_set and (
            self._converging_deferrals(same_session) < self.max_converging_deferrals
        ):
            return self._resume(
                "v2_converging_write",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        if (
            current.verifier_rejection_count > 0
            and current.verifier_rejection_repeat_streak == 1
        ):
            return self._resume(
                "v2_converging_failures",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        # Amnesia gate: a restart whose handoff cannot carry state is pure
        # re-exploration cost.
        handoff = build_semantic_handoff(
            original_instruction,
            timeline,
            max_items=self.max_handoff_items,
            max_chars=self.max_handoff_chars,
            recent_phases=self.recent_handoff_phases,
        )
        if len(handoff) < self.min_handoff_items:
            return self._resume(
                "v2_thin_handoff",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        # The previous restart must have paid for itself.
        if self._last_restart_unpaid(groups):
            return self._resume(
                "v2_unpaid_restart",
                current,
                cache_rate,
                same_session,
                consecutive_max_tokens,
            )
        return self._restart(
            "v2_cost_investment",
            "cost_investment",
            original_instruction,
            timeline,
            current,
            cache_rate,
            same_session,
            consecutive_max_tokens,
        )


__all__ = ["SemanticContextPolicyV2"]