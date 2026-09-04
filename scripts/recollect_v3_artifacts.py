#!/usr/bin/env python
"""Re-collect artifacts from the v3 debug run database (fix attribute names)."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "stuck_recovery_debug_v3"
DB_PATH = OUTPUT_DIR / "state.db"
RUN_ID = "stuck-recovery-v3"


def save_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def main() -> int:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # graph-after.json
    nodes = conn.execute(
        "SELECT * FROM nodes WHERE run_id = ? ORDER BY created_at", (RUN_ID,)
    ).fetchall()
    edges = conn.execute("SELECT * FROM edges WHERE run_id = ?", (RUN_ID,)).fetchall()

    graph_after = {
        "nodes": [
            {
                "id": n["id"],
                "title": n["title"],
                "state": n["state"],
                "attempt_count": n["attempt_count"],
                "max_attempts": n["max_attempts"],
                "verification_attempts": n.get("verification_attempts", 0),
                "parse_attempts": n.get("parse_attempts", 0),
                "tool_attempts": n.get("tool_attempts", 0),
            }
            for n in nodes
        ],
        "edges": [
            {"source": e["source_node_id"], "target": e["target_node_id"], "kind": e["kind"]}
            for e in edges
        ],
    }
    save_json(OUTPUT_DIR / "graph-after.json", graph_after)
    print(f"graph-after.json: {len(nodes)} nodes, {len(edges)} edges")

    # events.jsonl
    events = conn.execute(
        "SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (RUN_ID,)
    ).fetchall()
    with open(OUTPUT_DIR / "events.jsonl", "w") as f:
        for e in events:
            f.write(
                json.dumps(
                    {
                        "event_type": e["event_type"],
                        "sequence": e["sequence"],
                        "created_at": e["created_at"],
                        "actor_type": e["actor_type"],
                        "actor_id": e["actor_id"],
                        "payload": json.loads(e["payload_json"]) if e["payload_json"] else {},
                    },
                    default=str,
                )
                + "\n"
            )
    print(f"events.jsonl: {len(events)} events")

    # llm-calls.jsonl
    try:
        llm_calls = conn.execute(
            "SELECT * FROM llm_calls WHERE run_id = ? ORDER BY timestamp", (RUN_ID,)
        ).fetchall()
        with open(OUTPUT_DIR / "llm-calls.jsonl", "w") as f:
            for row in llm_calls:
                f.write(json.dumps(dict(row), default=str) + "\n")
        print(f"llm-calls.jsonl: {len(llm_calls)} calls")
    except Exception as exc:
        print(f"llm-calls error: {exc}")

    # tool-calls.jsonl
    tool_events = []
    for e in events:
        payload = json.loads(e["payload_json"]) if e["payload_json"] else {}
        if e["event_type"] in {"TOOL_CALL_REQUESTED", "TOOL_CALL_COMPLETED", "TOOL_CALL_FAILED"}:
            tool_events.append(
                {
                    "event_type": e["event_type"],
                    "node_id": payload.get("node_id"),
                    "tool_name": payload.get("tool_name"),
                    "arguments": payload.get("arguments"),
                }
            )
    with open(OUTPUT_DIR / "tool-calls.jsonl", "w") as f:
        for t in tool_events:
            f.write(json.dumps(t, default=str) + "\n")
    print(f"tool-calls.jsonl: {len(tool_events)} events")

    # worker-iterations.jsonl
    worker_events = []
    for e in events:
        payload = json.loads(e["payload_json"]) if e["payload_json"] else {}
        if e["event_type"] in {
            "NODE_CLAIMED",
            "NODE_LEASE_RELEASED",
            "NODE_VERIFIED",
            "NODE_FAILED",
            "NODE_RETRY_SCHEDULED",
            "NODE_BUDGET_EXHAUSTED",
            "LOCAL_REPAIR_TRIGGERED",
        }:
            worker_events.append(
                {
                    "event_type": e["event_type"],
                    "node_id": payload.get("node_id"),
                    "payload": payload,
                }
            )
    with open(OUTPUT_DIR / "worker-iterations.jsonl", "w") as f:
        for w in worker_events:
            f.write(json.dumps(w, default=str) + "\n")
    print(f"worker-iterations.jsonl: {len(worker_events)} events")

    # Print node states
    print("\nNode states:")
    for n in nodes:
        print(f"  {n['id']}: state={n['state']}, attempts={n['attempt_count']}/{n['max_attempts']}")

    # LLM call statistics
    if llm_calls:
        by_role: dict[str, int] = {}
        by_status: dict[str, int] = {}
        total_input = 0
        total_output = 0
        for c in llm_calls:
            role = c.get("role", "unknown")
            status = c.get("status", "unknown")
            by_role[role] = by_role.get(role, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1
            total_input += c["input_tokens"] if c.get("input_tokens") else 0
            total_output += c["output_tokens"] if c.get("output_tokens") else 0
        print(f"\nLLM calls by role: {by_role}")
        print(f"LLM calls by status: {by_status}")
        print(f"Total input tokens: {total_input}")
        print(f"Total output tokens: {total_output}")
        print(f"Total tokens: {total_input + total_output}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
