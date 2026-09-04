#!/usr/bin/env python3
"""Generate exclusive token accounting and overlapping causal diagnostics.

Exclusive categories: every token belongs to exactly ONE role.
Overlapping causes: diagnostic labels that may overlap (not additive).
"""

import csv
import sqlite3
from pathlib import Path

BASE = Path("artifacts/pilot_readiness_v3/raw-runs")
OUTPUT = Path("artifacts/milestone_2_3")

# Mutually exclusive roles — every token must be in exactly one
EXCLUSIVE_ROLES = [
    "planner",
    "worker",
    "reconciler",
    "parse_repair",
    "llm_verifier",
    "context_summarizer",
    "failure_analyzer",
    "other",
]


def analyze():
    exclusive_rows = []
    causal_rows = []
    assertions = []

    for seed in [1, 2, 3]:
        for mode in ["transcript", "full_lhos"]:
            db_path = BASE / f"seed-{seed}-{mode}" / "state.db"
            if not db_path.exists():
                continue
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row

            # Get all LLM calls
            calls = conn.execute("""
                SELECT role, input_tokens, output_tokens, total_tokens,
                       status, causation_id
                FROM llm_calls
            """).fetchall()

            # Exclusive accounting: each token in exactly one role bucket
            role_tokens = {r: 0 for r in EXCLUSIVE_ROLES}
            for c in calls:
                role = c["role"] if c["role"] in role_tokens else "other"
                # If this call has a causation_id and status=parse_failed,
                # classify as parse_repair (not worker)
                if c["causation_id"] and c["status"] == "parse_failed":
                    role = "parse_repair"
                elif c["status"] == "parse_failed" and role == "worker":
                    # A parse-failed worker call is still a worker call
                    # (the repair would be a separate call with causation_id)
                    pass
                role_tokens[role] += c["total_tokens"] or 0

            total_logged = sum(c["total_tokens"] or 0 for c in calls)
            total_classified = sum(role_tokens.values())

            for role in EXCLUSIVE_ROLES:
                exclusive_rows.append(
                    {
                        "seed": seed,
                        "mode": mode,
                        "role": role,
                        "tokens": role_tokens[role],
                        "pct_of_total": round(role_tokens[role] / max(total_logged, 1) * 100, 2),
                    }
                )

            # Assertion: sum of exclusive categories == total logged tokens
            diff = total_logged - total_classified
            assertions.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "total_logged": total_logged,
                    "total_classified": total_classified,
                    "difference": diff,
                    "passes": diff == 0,
                }
            )

            # Overlapping causal diagnostics
            # These labels may overlap — they are NOT additive
            parse_fail_tokens = sum(
                c["total_tokens"] or 0 for c in calls if c["status"] == "parse_failed"
            )
            repair_tokens = sum(
                c["total_tokens"] or 0 for c in calls if c["causation_id"] is not None
            )
            worker_tokens = sum(c["total_tokens"] or 0 for c in calls if c["role"] == "worker")
            planner_tokens = sum(c["total_tokens"] or 0 for c in calls if c["role"] == "planner")

            # Node-level causal data
            nodes = conn.execute(
                """
                SELECT id, state, attempt_count, actual_token_cost
                FROM nodes WHERE run_id = ?
            """,
                (f"seed-{seed}-{mode}",),
            ).fetchall()

            retry_tokens = sum(n["actual_token_cost"] or 0 for n in nodes if n["attempt_count"] > 1)

            causal_rows.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "cause": "more_worker_calls",
                    "tokens_attributed": worker_tokens,
                    "note": "Total worker tokens (overlaps with parse_fail and retry)",
                    "additive": False,
                }
            )
            causal_rows.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "cause": "parse_failure_retries",
                    "tokens_attributed": parse_fail_tokens,
                    "note": "Tokens from parse-failed calls (subset of worker tokens)",
                    "additive": False,
                }
            )
            causal_rows.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "cause": "repair_calls",
                    "tokens_attributed": repair_tokens,
                    "note": "Tokens from repair calls (causation_id != NULL)",
                    "additive": False,
                }
            )
            causal_rows.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "cause": "node_retry_cost",
                    "tokens_attributed": retry_tokens,
                    "note": "Token cost of nodes with >1 attempt (overlaps with worker)",
                    "additive": False,
                }
            )
            causal_rows.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "cause": "planner_overhead",
                    "tokens_attributed": planner_tokens,
                    "note": "Planner tokens (additive, does not overlap with worker)",
                    "additive": True,
                }
            )

            conn.close()

    # Write CSVs
    with open(OUTPUT / "exclusive-token-accounting.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["seed", "mode", "role", "tokens", "pct_of_total"])
        w.writeheader()
        w.writerows(exclusive_rows)

    with open(OUTPUT / "overlapping-causal-diagnostics.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["seed", "mode", "cause", "tokens_attributed", "note", "additive"]
        )
        w.writeheader()
        w.writerows(causal_rows)

    # Print assertions
    print("=== EXCLUSIVE TOKEN ACCOUNTING ===")
    for a in assertions:
        status = "PASS" if a["passes"] else "FAIL"
        print(
            f"  seed={a['seed']} mode={a['mode']}: logged={a['total_logged']} "
            f"classified={a['total_classified']} diff={a['difference']} [{status}]"
        )

    print("\n=== EXCLUSIVE BREAKDOWN ===")
    for mode in ["transcript", "full_lhos"]:
        print(f"\n--- {mode} ---")
        for role in EXCLUSIVE_ROLES:
            total = sum(
                r["tokens"] for r in exclusive_rows if r["mode"] == mode and r["role"] == role
            )
            if total > 0:
                print(f"  {role:20s}: {total:8d}")

    print("\n=== OVERLAPPING CAUSAL DIAGNOSTICS (NOT ADDITIVE) ===")
    for mode in ["transcript", "full_lhos"]:
        print(f"\n--- {mode} ---")
        for r in causal_rows:
            if r["mode"] == mode:
                tag = "[additive]" if r["additive"] else "[overlap]"
                print(f"  {r['cause']:25s} {tag:12s}: {r['tokens_attributed']:8d}")

    print("\nFiles written:")
    print(f"  {OUTPUT / 'exclusive-token-accounting.csv'}")
    print(f"  {OUTPUT / 'overlapping-causal-diagnostics.csv'}")


if __name__ == "__main__":
    analyze()
