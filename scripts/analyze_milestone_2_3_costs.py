#!/usr/bin/env python3
"""Analyze token costs for Milestone 2.3 Part G/H."""

import csv
import sqlite3
from pathlib import Path

base = Path("artifacts/pilot_readiness_v3/raw-runs")
output = Path("artifacts/milestone_2_3")
output.mkdir(parents=True, exist_ok=True)

all_role_data = []
per_node_data = []

for seed in [1, 2, 3]:
    for mode in ["transcript", "full_lhos"]:
        db_path = base / f"seed-{seed}-{mode}" / "state.db"
        if not db_path.exists():
            continue
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Per-role breakdown
        rows = conn.execute("""
            SELECT role,
                   COUNT(*) as calls,
                   SUM(input_tokens) as input_tk,
                   SUM(output_tokens) as output_tk,
                   SUM(total_tokens) as total_tk,
                   SUM(CASE WHEN status='parse_failed' THEN 1 ELSE 0 END) as parse_fails
            FROM llm_calls
            GROUP BY role
        """).fetchall()
        for r in rows:
            all_role_data.append(
                {
                    "seed": seed,
                    "mode": mode,
                    "role": r["role"],
                    "calls": r["calls"],
                    "input_tokens": r["input_tk"],
                    "output_tokens": r["output_tk"],
                    "total_tokens": r["total_tk"],
                    "parse_fails": r["parse_fails"],
                }
            )

        # Per-node breakdown (for full_lhos only)
        if mode == "full_lhos":
            run_id = f"seed-{seed}-{mode}"
            nodes = conn.execute(
                "SELECT id, title, state, attempt_count, verification_attempts, parse_attempts, actual_token_cost, actual_tool_calls FROM nodes WHERE run_id=?",
                (run_id,),
            ).fetchall()
            for n in nodes:
                execs = conn.execute(
                    "SELECT attempt_number, status, input_tokens, output_tokens, tool_calls FROM executions WHERE run_id=? AND node_id=? ORDER BY attempt_number",
                    (run_id, n["id"]),
                ).fetchall()
                exec_tokens = sum(e["input_tokens"] + e["output_tokens"] for e in execs)
                per_node_data.append(
                    {
                        "seed": seed,
                        "node_id": n["id"],
                        "title": n["title"],
                        "state": n["state"],
                        "attempt_count": n["attempt_count"],
                        "verification_attempts": n["verification_attempts"],
                        "parse_attempts": n["parse_attempts"],
                        "actual_token_cost": n["actual_token_cost"],
                        "actual_tool_calls": n["actual_tool_calls"],
                        "execution_count": len(execs),
                        "execution_tokens": exec_tokens,
                        "exec_statuses": [e["status"] for e in execs],
                    }
                )
        conn.close()

# Write cost-attribution.csv
with open(output / "cost-attribution.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "seed",
            "mode",
            "role",
            "calls",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "parse_fails",
        ],
    )
    writer.writeheader()
    writer.writerows(all_role_data)

# Write per-node-costs.csv
with open(output / "per-node-costs.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "seed",
            "node_id",
            "title",
            "state",
            "attempt_count",
            "verification_attempts",
            "parse_attempts",
            "actual_token_cost",
            "actual_tool_calls",
            "execution_count",
            "execution_tokens",
            "exec_statuses",
        ],
    )
    writer.writeheader()
    for row in per_node_data:
        row["exec_statuses"] = "|".join(row["exec_statuses"])
        writer.writerow(row)

# Compute context novelty for full_lhos
novelty_data = []
for seed in [1, 2, 3]:
    db_path = base / f"seed-{seed}-full_lhos" / "state.db"
    if not db_path.exists():
        continue
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    calls = conn.execute("""
        SELECT node_id, input_tokens, output_tokens, role, timestamp
        FROM llm_calls WHERE role='worker'
        ORDER BY timestamp
    """).fetchall()
    # Simple novelty proxy: ratio of output_tokens to input_tokens
    # High output/input ratio = more novel work; low ratio = more repetition
    for c in calls:
        novelty_data.append(
            {
                "seed": seed,
                "node_id": c["node_id"],
                "input_tokens": c["input_tokens"],
                "output_tokens": c["output_tokens"],
                "novelty_ratio": round(c["output_tokens"] / max(c["input_tokens"], 1), 4),
            }
        )
    conn.close()

with open(output / "context-novelty-analysis.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f, fieldnames=["seed", "node_id", "input_tokens", "output_tokens", "novelty_ratio"]
    )
    writer.writeheader()
    writer.writerows(novelty_data)

# Print summary
print("=== COST ATTRIBUTION SUMMARY ===")
for mode in ["transcript", "full_lhos"]:
    print(f"\n--- {mode} ---")
    mode_data = [d for d in all_role_data if d["mode"] == mode]
    for role in sorted(set(d["role"] for d in mode_data)):
        role_data = [d for d in mode_data if d["role"] == role]
        total_tk = sum(d["total_tokens"] for d in role_data)
        total_calls = sum(d["calls"] for d in role_data)
        total_parse = sum(d["parse_fails"] for d in role_data)
        print(
            f"  {role:15s}: calls={total_calls:4d}  tokens={total_tk:8d}  parse_fails={total_parse}"
        )

print("\n=== PER-NODE COSTS (Full LHoS) ===")
for seed in [1, 2, 3]:
    seed_nodes = [n for n in per_node_data if n["seed"] == seed]
    print(f"\n--- seed {seed} ---")
    for n in seed_nodes:
        print(
            f"  {n['node_id'][-20:]:20s}  state={n['state']:10s}  attempts={n['attempt_count']}  "
            f"tokens={n['actual_token_cost']:7d}  tools={n['actual_tool_calls']:3d}  "
            f"execs={n['execution_count']}  exec_tk={n['execution_tokens']:7d}"
        )

print("\n=== CONTEXT NOVELTY (Full LHoS worker calls) ===")
for seed in [1, 2, 3]:
    seed_novelty = [n for n in novelty_data if n["seed"] == seed]
    if seed_novelty:
        avg_ratio = sum(n["novelty_ratio"] for n in seed_novelty) / len(seed_novelty)
        avg_input = sum(n["input_tokens"] for n in seed_novelty) / len(seed_novelty)
        print(f"  seed {seed}: avg_input={avg_input:.0f}  avg_novelty_ratio={avg_ratio:.4f}")

print("\nFiles written:")
print(f"  {output / 'cost-attribution.csv'}")
print(f"  {output / 'per-node-costs.csv'}")
print(f"  {output / 'context-novelty-analysis.csv'}")
