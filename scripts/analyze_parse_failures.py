#!/usr/bin/env python
"""Analyze parse failures in detail to determine final unparsable rate."""

import sqlite3

conn = sqlite3.connect("artifacts/stuck_recovery_debug_v3/state.db")
conn.row_factory = sqlite3.Row

calls = [
    dict(r)
    for r in conn.execute(
        "SELECT * FROM llm_calls WHERE run_id=? ORDER BY timestamp",
        ("stuck-recovery-v3",),
    )
]

parse_failed = [c for c in calls if c.get("status") == "parse_failed"]
success = [c for c in calls if c.get("status") == "success"]

print(f"Total calls: {len(calls)}")
print(f"Success: {len(success)}")
print(f"Parse failed: {len(parse_failed)}")
print(f"First parse failure rate: {len(parse_failed) / len(calls) * 100:.1f}%")
print()

# Check if parse failures were followed by successful repair calls
final_unparsable = 0
for i, c in enumerate(parse_failed):
    idx = calls.index(c)
    node_id = c.get("node_id", "?")
    error_type = c.get("error_type", "?")

    # Check next call
    next_status = "N/A"
    is_repaired = False
    if idx + 1 < len(calls):
        next_call = calls[idx + 1]
        next_status = next_call.get("status", "?")
        # If next call is success with same node, it was repaired
        if next_status == "success":
            is_repaired = True

    if not is_repaired:
        final_unparsable += 1

    print(
        f"  Fail {i + 1}: node={node_id}, error={error_type}, next_status={next_status}, repaired={is_repaired}"
    )

print()
print(f"Final unparsable (not repaired): {final_unparsable}")
print(f"Final unparsable rate: {final_unparsable / len(calls) * 100:.1f}%")
print("Threshold: < 5%")
print(f"Result: {'PASS' if final_unparsable / len(calls) < 0.05 else 'FAIL'}")

# Also check error_type distribution
print()
print("Error type distribution:")
from collections import Counter

errors = Counter(c.get("error_type", "unknown") for c in parse_failed)
for err, count in errors.items():
    print(f"  {err}: {count}")

conn.close()
