"""Offline replay: compare SemanticContextPolicy (v1) vs SemanticContextPolicyV2
over recorded dsh-semantic-control.json decision sequences.

CAVEATS (read before trusting numbers):
* The recorded timeline was SHAPED by v1's decisions (session boundaries,
  handoffs).  Replaying v2 over the same timeline answers "what would v2 do
  facing the same situations", not "how would the run have unfolded".
* write/read URIs are not persisted in decision logs -- they are synthesised
  from sizes (distinct placeholder per URI), which slightly overstates handoff
  richness.
* Old logs predate verifier-rejection telemetry, so the v2 loop-escape gate
  never fires here.

Usage: python scripts/replay_semantic_policy_v2.py <dsh-semantic-control.json> [...]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from lhos.integrations.harness.optimization import (
    HarnessContinuationAction,
    HarnessPhaseObservation,
    HarnessQualityProbe,
    SemanticContextPolicy,
)
from lhos.integrations.harness.optimization_v2 import SemanticContextPolicyV2
from lhos.integrations.harness.protocol import HarnessUsage


def _load_observations(path: Path) -> tuple[list[HarnessPhaseObservation], dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    decisions = payload.get("decisions") or []
    observations: list[HarnessPhaseObservation] = []
    for index, record in enumerate(decisions, start=1):
        usage_delta = record.get("usage_delta") or {}
        usage = HarnessUsage(
            **{
                key: int(usage_delta.get(key, 0) or 0)
                for key in (
                    "cache_read_tokens",
                    "cache_write_tokens",
                    "uncached_input_tokens",
                    "output_tokens",
                    "model_calls",
                    "tool_calls",
                )
            }
        )
        write_size = int(record.get("write_set_size") or 0)
        read_size = int(record.get("read_set_size") or 0)
        probe_record = record.get("quality_probe") or {}
        observations.append(
            HarnessPhaseObservation(
                phase_index=int(record.get("phase_index") or index),
                usage=usage,
                event_count=int(record.get("event_count") or 0),
                write_set=tuple(
                    f"file:///app/.replay/w{index}-{i}" for i in range(write_size)
                ),
                read_set=tuple(
                    f"file:///app/.replay/r{index}-{i}" for i in range(read_size)
                ),
                max_tokens_checkpoint=bool(record.get("max_tokens_checkpoint")),
                session_id=str(record.get("session_id") or f"session-{index}"),
                unknown_io=bool(record.get("unknown_io")),
                verifier_rejection_count=int(
                    record.get("verifier_rejection_count") or 0
                ),
                verifier_rejection_repeat_streak=int(
                    record.get("verifier_rejection_repeat_streak") or 0
                ),
                quality_probe=HarnessQualityProbe(
                    write_calls=int(probe_record.get("write_calls") or 0),
                    distinct_writes=int(probe_record.get("distinct_writes") or 0),
                    test_calls=int(probe_record.get("test_calls") or 0),
                    error_calls=int(probe_record.get("error_calls") or 0),
                    tool_calls=int(probe_record.get("tool_calls") or 0),
                    write_repeat_ratio=float(
                        probe_record.get("write_repeat_ratio") or 0.0
                    ),
                    error_ratio=float(probe_record.get("error_ratio") or 0.0),
                ),
            )
        )
    return observations, payload.get("config") or {}


def _policies(config: dict) -> tuple[SemanticContextPolicy, SemanticContextPolicyV2]:
    v1 = SemanticContextPolicy(**config)
    key_map = {
        "soft_cache_tokens_per_call": "cache_tokens_per_call_threshold",
        "min_phases_before_restart": "min_phases_before_restart",
        "cooldown_phases": "cooldown_phases",
        "payoff_rebloat_ratio": "restart_payoff_rebloat_ratio",
        "payoff_window_phases": "restart_payoff_window_phases",
        "max_handoff_items": "max_handoff_items",
        "max_handoff_chars": "max_handoff_chars",
        "recent_handoff_phases": "recent_handoff_phases",
    }
    v2 = SemanticContextPolicyV2(
        **{v2k: config[v1k] for v2k, v1k in key_map.items() if v1k in config}
    )
    return v1, v2


def replay(path: Path) -> dict | None:
    observations, config = _load_observations(path)
    v1, v2 = _policies(config)
    print(f"\n=== {path.parent.parent.parent.name} ===")
    print(f"phases: {len(observations)}")

    # Estimate this run's post-restart landing rate and within-session growth
    # slope from the recorded trajectory, so a counterfactual restart can
    # reset the cache rate the way real restarts did in this run.
    def _rate_of(recorded: HarnessPhaseObservation) -> float:
        calls = recorded.usage.model_calls
        return recorded.usage.cache_read_tokens / calls if calls > 0 else 0.0

    recorded_rates = [_rate_of(o) for o in observations]
    landing_rates: list[float] = []
    slopes: list[float] = []
    for i in range(1, len(observations)):
        prev, cur = observations[i - 1], observations[i]
        if cur.session_id != prev.session_id:
            if recorded_rates[i] > 0:
                landing_rates.append(recorded_rates[i])
        elif recorded_rates[i] > 0 and recorded_rates[i - 1] > 0:
            delta = recorded_rates[i] - recorded_rates[i - 1]
            if delta > 0:
                slopes.append(delta)
    from statistics import median

    landing = median(landing_rates) if landing_rates else (
        median(recorded_rates) if recorded_rates else 0.0
    )
    slope = median(slopes) if slopes else 0.0
    print(
        f"rate model: post-restart landing={landing:,.0f}  "
        f"slope={slope:,.0f}/phase"
    )

    def run_counterfactual(policy, *, label: str = ""):
        """Replay with the policy driving its own session boundaries AND a
        modelled cache trajectory: a restart resets the per-call rate to this
        run's observed post-restart landing rate; afterwards the rate grows
        by the observed median within-session slope.  Everything else (writes,
        probes, event growth) follows the recorded run.  A TERMINATE decision
        ends the counterfactual run, as it would live.
        """
        restarts = 0
        halted_at: int | None = None
        reasons: dict[str, int] = {}
        timeline: list[HarnessPhaseObservation] = []
        session = "cf-session-0"
        rate = recorded_rates[0] if recorded_rates else 0.0
        slice_total = 0.0
        # Event counterfactual: preserve the recorded *within-generation*
        # event deltas (flat stays flat, growth adds up) and rebase across
        # recorded generation resets, so a counterfactual session that skips
        # a recorded restart still sees the true growth series.  The result
        # is non-decreasing within any counterfactual session.
        cf_events = 0
        prev_recorded = None
        for i, recorded in enumerate(observations):
            if prev_recorded is None or recorded.session_id != prev_recorded.session_id:
                cf_events += recorded.event_count
            else:
                cf_events += max(0, recorded.event_count - prev_recorded.event_count)
            prev_recorded = recorded
            modelled = recorded.model_copy(
                update={
                    "event_count": cf_events,
                    "session_id": session,
                    "usage": recorded.usage.model_copy(
                        update={
                            # Model the rate as an absolute so the policy's
                            # per-call division recovers it exactly; preserve
                            # recorded model_calls (0 stays 0: dead phases
                            # burn nothing and fail activity checks closed).
                            "cache_read_tokens": int(
                                max(0.0, rate)
                                * max(1, recorded.usage.model_calls)
                            ),
                        }
                    ),
                }
            )
            decision = policy.decide(
                tuple(timeline), modelled, original_instruction="replay"
            )
            timeline.append(modelled)
            reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
            if hasattr(policy, "recommended_slice_seconds"):
                slice_total += policy.recommended_slice_seconds(
                    tuple(timeline[:-1]), modelled, 75.0
                )
            if decision.action == HarnessContinuationAction.TERMINATE:
                halted_at = i + 1
                break
            if decision.action == "restart_compacted":
                restarts += 1
                session = f"cf-session-{restarts}"
                rate = landing
            else:
                rate += slope
        saved_phases = len(observations) - (halted_at or len(observations))
        # Upper bound: the recorded burn of the phases a halt would have
        # skipped (modelled rates make the counterfactual diverge, so report
        # the recorded trajectory's remaining cache reads).
        saved_cache = (
            sum(int(o.usage.cache_read_tokens) for o in observations[halted_at:])
            if halted_at is not None
            else 0
        )
        return restarts, reasons, halted_at, saved_phases, saved_cache, slice_total

    v1_restarts, v1_reasons, _, _, _, _ = run_counterfactual(v1, label="v1")
    v2_restarts, v2_reasons, v2_halt, v2_saved_phases, v2_saved_cache, v2_slices = (
        run_counterfactual(v2, label="v2")
    )
    print(f"restarts: v1(current code)={v1_restarts}  v2={v2_restarts}")
    print(f"  v1 top reasons: {sorted(v1_reasons.items(), key=lambda kv: -kv[1])[:5]}")
    print(f"  v2 top reasons: {sorted(v2_reasons.items(), key=lambda kv: -kv[1])[:5]}")
    if v2_halt is not None:
        print(
            f"  v2 HALTED at phase {v2_halt}/{len(observations)}: "
            f"would skip {v2_saved_phases} phases, "
            f"~{v2_saved_cache / 1e6:.1f}M recorded cache reads"
        )
    else:
        print("  v2 did not halt")
    print(
        f"  v2 adaptive slice: {v2_slices:,.0f}s recommended vs "
        f"{75.0 * len(observations):,.0f}s fixed (upper-bound reload proxy)"
    )
    return {
        "phases": len(observations),
        "v1_restarts": v1_restarts,
        "v2_restarts": v2_restarts,
        "v1_reasons": v1_reasons,
        "v2_reasons": v2_reasons,
        "v2_halted_at": v2_halt,
        "v2_halt_saved_phases": v2_saved_phases,
        "v2_halt_saved_cache_read_tokens": v2_saved_cache,
        "v2_adaptive_slice_seconds": v2_slices,
        "fixed_slice_seconds": 75.0 * len(observations),
    }

    v1_restarts = v2_restarts = 0
    divergences = 0
    for i, current in enumerate(observations):
        history = tuple(observations[:i])
        d1 = v1.decide(history, current, original_instruction="replay")
        d2 = v2.decide(history, current, original_instruction="replay")
        r1 = d1.action == "restart_compacted"
        r2 = d2.action == "restart_compacted"
        v1_restarts += int(r1)
        v2_restarts += int(r2)



def main() -> None:
    args = [a for a in sys.argv[1:]]
    json_out: Path | None = None
    if args and args[0] == "--json":
        if len(args) < 2:
            raise SystemExit("--json requires an output path")
        json_out = Path(args[1])
        args = args[2:]
    results = {}
    for arg in args:
        result = replay(Path(arg))
        if result is not None:
            results[Path(arg).parent.parent.parent.name] = result
    if json_out is not None:
        json_out.write_text(
            json.dumps(results, indent=1, ensure_ascii=False), encoding="utf-8"
        )
        print(f"\nwrote {json_out}")


if __name__ == "__main__":
    main()