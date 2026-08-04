#!/usr/bin/env python
"""Update run-summary.json with comprehensive data from the v3 database."""

import json
import sqlite3
from pathlib import Path

OUTPUT_DIR = Path("artifacts/stuck_recovery_debug_v3")
DB_PATH = OUTPUT_DIR / "state.db"
RUN_ID = "stuck-recovery-v3"


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    nodes = [dict(r) for r in conn.execute("SELECT * FROM nodes WHERE run_id=?", (RUN_ID,))]
    executions = [dict(r) for r in conn.execute("SELECT * FROM executions WHERE run_id=?", (RUN_ID,))]
    llm_calls = [dict(r) for r in conn.execute("SELECT * FROM llm_calls WHERE run_id=?", (RUN_ID,))]
    events = [dict(r) for r in conn.execute("SELECT * FROM events WHERE run_id=? ORDER BY sequence", (RUN_ID,))]

    tool_requested = sum(1 for e in events if e["event_type"] == "TOOL_CALL_REQUESTED")
    tool_completed = sum(1 for e in events if e["event_type"] == "TOOL_CALL_COMPLETED")
    tool_failed = sum(1 for e in events if e["event_type"] == "TOOL_CALL_FAILED")

    parse_fail = sum(1 for c in llm_calls if c.get("status") == "parse_failed")
    success = sum(1 for c in llm_calls if c.get("status") == "success")

    node_stats = {}
    for n in nodes:
        nid = n["id"]
        node_execs = [e for e in executions if e.get("node_id") == nid]
        node_stats[nid] = {
            "title": n["title"],
            "state": n["state"],
            "attempt_count": n["attempt_count"],
            "max_attempts": n["max_attempts"],
            "execution_count": len(node_execs),
            "input_tokens": sum(e.get("input_tokens") or 0 for e in node_execs),
            "output_tokens": sum(e.get("output_tokens") or 0 for e in node_execs),
            "tool_calls": sum(e.get("tool_calls") or 0 for e in node_execs),
        }

    external_score = json.loads((OUTPUT_DIR / "external-score.json").read_text())

    total_input = sum(c.get("input_tokens") or 0 for c in llm_calls)
    total_output = sum(c.get("output_tokens") or 0 for c in llm_calls)

    summary = {
        "run_id": RUN_ID,
        "task": "config_loader",
        "mode": "full_lhos",
        "model_id": "sensenova-6.7-flash-lite",
        "seed": 1,
        "run_status": "failed",
        "primary_failure_code": "run_stuck",
        "termination_reason": "no ready nodes and no waiting nodes: run is stuck (n6 blocked, token budget exhausted)",
        "wall_time_seconds": 526.2,
        "node_count": len(nodes),
        "verified_nodes": sum(1 for n in nodes if n["state"] == "verified"),
        "failed_nodes": sum(1 for n in nodes if n["state"] == "failed"),
        "pending_nodes": sum(1 for n in nodes if n["state"] == "pending"),
        "execution_count": len(executions),
        "llm_call_count": len(llm_calls),
        "llm_calls_by_role": {"planner": 1, "worker": 62},
        "llm_calls_by_status": {"success": success, "parse_failed": parse_fail},
        "parse_failure_rate": round(parse_fail / len(llm_calls) * 100, 1) if llm_calls else 0,
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_tokens": total_input + total_output,
        "tool_call_events": {"requested": tool_requested, "completed": tool_completed, "failed": tool_failed},
        "node_stats": node_stats,
        "external_score": external_score,
        "n3_status": "VERIFIED on attempt 2/3 (first attempt failed verification, structured feedback enabled successful retry)",
        "workspace_files": ["src/sample_app/config_loader.py", "tests/test_config_loader.py", "README.md"],
    }

    with open(OUTPUT_DIR / "run-summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print("run-summary.json updated")
    for k, v in summary.items():
        if k not in ("node_stats", "external_score"):
            print(f"  {k}: {v}")
    print("\nNode stats:")
    for nid, stats in node_stats.items():
        print(f"  {nid}: state={stats['state']}, attempts={stats['attempt_count']}/{stats['max_attempts']}, tokens={stats['input_tokens']+stats['output_tokens']}, tool_calls={stats['tool_calls']}")

    conn.close()


if __name__ == "__main__":
    main()
