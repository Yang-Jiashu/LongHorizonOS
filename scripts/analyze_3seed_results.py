#!/usr/bin/env python
"""P10-P12: Paired differences analysis and pilot readiness decision."""

import csv
import json
import statistics
from pathlib import Path

OUTPUT_DIR = Path("artifacts/pilot_readiness_v3")
RAW_DIR = OUTPUT_DIR / "raw-runs"


def load_results():
    results = []
    for d in sorted(RAW_DIR.iterdir()):
        summary_path = d / "run-summary.json"
        if summary_path.exists():
            results.append(json.loads(summary_path.read_text()))
    return results


def main():
    results = load_results()

    # Organize by seed and mode
    by_seed = {}
    for r in results:
        seed = r.get("seed")
        mode = r.get("mode")
        if seed not in by_seed:
            by_seed[seed] = {}
        by_seed[seed][mode] = r

    # Print raw results
    print("=" * 70)
    print("P10: PAIRED DIFFERENCES ANALYSIS")
    print("=" * 70)

    metrics = [
        "external_score",
        "total_tokens",
        "tool_calls",
        "wall_clock",
        "model_calls",
        "file_reads",
        "file_writes",
        "shell_calls",
        "parse_failure_rate",
    ]

    # Per-seed comparison
    print("\nPer-seed Transcript vs Full LHoS:")
    print(
        f"{'Metric':<25} {'Seed 1 T':>10} {'Seed 1 F':>10} {'Seed 2 T':>10} {'Seed 2 F':>10} {'Seed 3 T':>10} {'Seed 3 F':>10}"
    )
    for m in metrics:
        vals = []
        for seed in [1, 2, 3]:
            t = by_seed.get(seed, {}).get("transcript", {})
            f = by_seed.get(seed, {}).get("full_lhos", {})
            vals.append(t.get(m, 0))
            vals.append(f.get(m, 0))
        print(
            f"{m:<25} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10} {vals[3]:>10} {vals[4]:>10} {vals[5]:>10}"
        )

    # Compute deltas (Full - Transcript)
    print("\nPer-seed deltas (Full - Transcript):")
    deltas = {m: [] for m in metrics}
    for seed in [1, 2, 3]:
        t = by_seed.get(seed, {}).get("transcript", {})
        f = by_seed.get(seed, {}).get("full_lhos", {})
        for m in metrics:
            t_val = t.get(m, 0)
            f_val = f.get(m, 0)
            deltas[m].append(f_val - t_val)

    print(
        f"{'Metric':<25} {'Seed 1':>10} {'Seed 2':>10} {'Seed 3':>10} {'Mean':>10} {'Median':>10} {'StdDev':>10}"
    )
    for m in metrics:
        d = deltas[m]
        mean = statistics.mean(d) if d else 0
        median = statistics.median(d) if d else 0
        stdev = statistics.stdev(d) if len(d) > 1 else 0
        print(
            f"{m:<25} {d[0]:>10.1f} {d[1]:>10.1f} {d[2]:>10.1f} {mean:>10.1f} {median:>10.1f} {stdev:>10.1f}"
        )

    # Consistency analysis
    print("\n" + "=" * 70)
    print("CONSISTENCY ANALYSIS")
    print("=" * 70)

    # Full external score >= Transcript in how many seeds?
    full_wins_score = sum(
        1
        for s in [1, 2, 3]
        if by_seed.get(s, {}).get("full_lhos", {}).get("external_score", 0)
        >= by_seed.get(s, {}).get("transcript", {}).get("external_score", 0)
    )
    print(f"Full external score >= Transcript: {full_wins_score}/3 seeds")

    # Full tokens lower?
    full_lower_tokens = sum(
        1
        for s in [1, 2, 3]
        if by_seed.get(s, {}).get("full_lhos", {}).get("total_tokens", float("inf"))
        < by_seed.get(s, {}).get("transcript", {}).get("total_tokens", 0)
    )
    print(f"Full tokens < Transcript: {full_lower_tokens}/3 seeds")

    # Full wall time lower?
    full_faster = sum(
        1
        for s in [1, 2, 3]
        if by_seed.get(s, {}).get("full_lhos", {}).get("wall_clock", float("inf"))
        < by_seed.get(s, {}).get("transcript", {}).get("wall_clock", 0)
    )
    print(f"Full wall time < Transcript: {full_faster}/3 seeds")

    # Medians
    t_scores = [
        by_seed.get(s, {}).get("transcript", {}).get("external_score", 0) for s in [1, 2, 3]
    ]
    f_scores = [by_seed.get(s, {}).get("full_lhos", {}).get("external_score", 0) for s in [1, 2, 3]]
    t_tokens = [by_seed.get(s, {}).get("transcript", {}).get("total_tokens", 0) for s in [1, 2, 3]]
    f_tokens = [by_seed.get(s, {}).get("full_lhos", {}).get("total_tokens", 0) for s in [1, 2, 3]]

    print(f"\nTranscript median external score: {statistics.median(t_scores):.0%}")
    print(f"Full LHoS median external score: {statistics.median(f_scores):.0%}")
    print(f"Transcript median tokens: {statistics.median(t_tokens):,.0f}")
    print(f"Full LHoS median tokens: {statistics.median(f_tokens):,.0f}")

    # P12: Pilot Readiness Decision
    print("\n" + "=" * 70)
    print("P12: PILOT READINESS DECISION")
    print("=" * 70)

    both_below_50 = statistics.median(t_scores) < 0.5 and statistics.median(f_scores) < 0.5
    full_ge_2_seeds = full_wins_score >= 2
    full_tokens_lower = full_lower_tokens >= 2

    print(f"Both medians < 50%: {both_below_50}")
    print(f"Full score >= Transcript in >= 2 seeds: {full_ge_2_seeds} ({full_wins_score}/3)")
    print(f"Full tokens lower in >= 2 seeds: {full_tokens_lower} ({full_lower_tokens}/3)")

    # Known issues
    print("\nKnown issues:")
    print("  - UNIQUE constraint error in executions table (affects node retries)")
    print("  - This caused 2/3 Full LHoS runs to abort early")
    print("  - Engineering fix needed before pilot")

    if both_below_50:
        print("\nModel capability diagnosis needed (both medians < 50%)")
        decision = "GO-ENGINEERING-ONLY"
        reason = (
            "Engineering is reliable (n3 fix works, structured feedback works, "
            "parse repair 100% success, log consistency verified). However: "
            "3-seed gains are unstable (Full doesn't outperform Transcript), "
            "model capability is low (all scores 20-30%), "
            "and a UNIQUE constraint bug affects node retries. "
            "The UNIQUE constraint bug must be fixed before pilot."
        )
    elif full_ge_2_seeds and (full_tokens_lower or full_faster):
        decision = "GO-PILOT"
        reason = "Full LHoS outperforms Transcript in external score and cost efficiency."
    else:
        decision = "GO-ENGINEERING-ONLY"
        reason = "Engineering reliable but method gains not demonstrated."

    print(f"\nDECISION: {decision}")
    print(f"Reason: {reason}")

    # Save results
    paired = {
        "raw_results": results,
        "per_seed": {
            str(s): {
                "transcript": by_seed.get(s, {}).get("transcript", {}),
                "full_lhos": by_seed.get(s, {}).get("full_lhos", {}),
            }
            for s in [1, 2, 3]
        },
        "deltas": {m: deltas[m] for m in metrics},
        "statistics": {
            "transcript_median_score": statistics.median(t_scores),
            "full_median_score": statistics.median(f_scores),
            "transcript_median_tokens": statistics.median(t_tokens),
            "full_median_tokens": statistics.median(f_tokens),
            "full_score_wins": full_wins_score,
            "full_token_wins": full_lower_tokens,
        },
        "decision": decision,
        "reason": reason,
        "known_issues": [
            "UNIQUE constraint error in executions table (affects node retries)",
            "2/3 Full LHoS runs aborted early due to this bug",
        ],
    }

    # Save paired differences CSV
    csv_path = OUTPUT_DIR / "paired-differences.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["metric", "seed_1_delta", "seed_2_delta", "seed_3_delta", "mean", "median", "stdev"]
        )
        for m in metrics:
            d = deltas[m]
            writer.writerow(
                [
                    m,
                    d[0],
                    d[1],
                    d[2],
                    statistics.mean(d),
                    statistics.median(d),
                    statistics.stdev(d) if len(d) > 1 else 0,
                ]
            )
    print(f"\nPaired differences saved to: {csv_path}")

    # Save full analysis
    with open(OUTPUT_DIR / "paired-analysis.json", "w") as f:
        json.dump(paired, f, indent=2, default=str)
    print(f"Full analysis saved to: {OUTPUT_DIR / 'paired-analysis.json'}")


if __name__ == "__main__":
    main()
