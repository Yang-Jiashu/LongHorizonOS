#!/usr/bin/env python
"""Reconstruct n3 execution timeline from Vertical Slice v2 artifacts."""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path("artifacts/real_llm_vertical_slice/full_lhos/state.db")
RUN_ID = "vertical-slice-full-lhos"


def safe_json(s):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # 1. All nodes
    nodes = conn.execute(
        "SELECT * FROM nodes WHERE run_id=? ORDER BY created_at", (RUN_ID,)
    ).fetchall()
    print("=== All Nodes ===")
    for n in nodes:
        print(f"  {n['id']}: state={n['state']}, attempts={n['attempt_count']}/{n['max_attempts']}")
        print(f"    title: {n['title']}")

    # 2. n3 details
    n3 = conn.execute("SELECT * FROM nodes WHERE run_id=? AND id LIKE '%:n3'", (RUN_ID,)).fetchone()
    if n3:
        print("\n=== n3 Full Details ===")
        print(f"  id: {n3['id']}")
        print(f"  title: {n3['title']}")
        print(f"  specification: {n3['specification']}")
        vspec = safe_json(n3["verification_spec_json"])
        print(f"  verification_spec: {json.dumps(vspec, indent=2) if vspec else 'N/A'}")
        print(f"  state: {n3['state']}")
        print(f"  attempt_count: {n3['attempt_count']}")
        print(f"  max_attempts: {n3['max_attempts']}")
        meta = safe_json(n3["metadata_json"])
        print(f"  metadata: {json.dumps(meta, indent=2)[:500] if meta else 'N/A'}")
        print(f"  actual_token_cost: {n3['actual_token_cost']}")
        print(f"  actual_tool_calls: {n3['actual_tool_calls']}")
        print(f"  actual_time_ms: {n3['actual_time_ms']}")

    # 3. Edges
    edges = conn.execute("SELECT * FROM edges WHERE run_id=?", (RUN_ID,)).fetchall()
    print("\n=== Edges ===")
    for e in edges:
        print(f"  {e['source_node_id']} -> {e['target_node_id']} ({e['kind']})")

    # 4. n3 executions
    executions = conn.execute(
        "SELECT * FROM executions WHERE run_id=? AND node_id LIKE '%:n3' ORDER BY attempt_number",
        (RUN_ID,),
    ).fetchall()
    print(f"\n=== n3 Executions ({len(executions)}) ===")
    for ex in executions:
        result = safe_json(ex["result_json"]) if ex["result_json"] else {}
        error = safe_json(ex["error_json"]) if ex["error_json"] else None
        print(
            f"  Attempt {ex['attempt_number']}: status={ex['status']}, tokens={ex['input_tokens']}+{ex['output_tokens']}, tools={ex['tool_calls']}"
        )
        if result and isinstance(result, dict):
            print(f"    summary: {str(result.get('summary', ''))[:200]}")
            print(f"    produced_artifacts: {result.get('produced_artifacts', [])}")
            print(f"    tool_call_count: {result.get('tool_call_count', 0)}")
        if error:
            print(f"    error: {json.dumps(error)[:200]}")

    # 5. n3 events
    events = conn.execute(
        "SELECT * FROM events WHERE run_id=? AND (actor_id LIKE '%:n3' OR payload_json LIKE '%n3%') ORDER BY rowid",
        (RUN_ID,),
    ).fetchall()
    print(f"\n=== n3 Events ({len(events)}) ===")
    for e in events:
        payload = safe_json(e["payload_json"]) if e["payload_json"] else {}
        eid = e["actor_id"] or ""
        if ":n3" not in eid and "n3" not in str(payload):
            continue
        parts = [f"  {e['event_type']}"]
        if e["actor_id"]:
            parts.append(f"actor={e['actor_id']}")
        if isinstance(payload, dict):
            if "attempt" in payload:
                parts.append(f"attempt={payload['attempt']}")
            if "summary" in payload:
                parts.append(f"summary={str(payload['summary'])[:150]}")
            if "verification" in payload:
                parts.append(f"verification={str(payload['verification'])[:200]}")
            if "from_state" in payload:
                parts.append(f"from={payload['from_state']}")
            if "node_id" in payload:
                parts.append(f"node={payload['node_id']}")
        print(" ".join(parts))

    # 6. n3 LLM calls
    llm_calls = conn.execute(
        "SELECT * FROM llm_calls WHERE run_id=? AND node_id LIKE '%:n3' ORDER BY timestamp",
        (RUN_ID,),
    ).fetchall()
    print(f"\n=== n3 LLM Calls ({len(llm_calls)}) ===")
    for i, c in enumerate(llm_calls):
        print(
            f"  [{i}] role={c['role']} status={c['status']} tokens={c['input_tokens']}+{c['output_tokens']} latency={c['latency_ms']}ms parse_fails={c['parse_failure_count']}"
        )
        resp = safe_json(c["response_body_json"])
        if resp and isinstance(resp, dict):
            text = resp.get("text", "")
            if text:
                print(f"      text[:150]: {text[:150]}")

    # 7. n3 tool events
    tool_events = conn.execute(
        "SELECT * FROM events WHERE run_id=? AND event_type IN ('TOOL_CALL_REQUESTED','TOOL_CALL_COMPLETED','TOOL_CALL_FAILED') ORDER BY rowid",
        (RUN_ID,),
    ).fetchall()
    n3_tools = []
    for e in tool_events:
        payload = safe_json(e["payload_json"]) if e["payload_json"] else {}
        if ":n3" not in str(payload.get("node_id", "") if isinstance(payload, dict) else ""):
            continue
        n3_tools.append((e["event_type"], payload))
    print(f"\n=== n3 Tool Events ({len(n3_tools)}) ===")
    for etype, p in n3_tools:
        tool = p.get("tool_name", "?")
        if etype == "TOOL_CALL_REQUESTED":
            args = p.get("arguments", {})
            op = args.get("op", args.get("command", ""))[:80]
            print(f"  REQ tool={tool} op={op}")
        elif etype == "TOOL_CALL_COMPLETED":
            result = p.get("result", {})
            print(
                f"  DONE tool={tool} exit={result.get('exit_code')} success={result.get('success')}"
            )
        elif etype == "TOOL_CALL_FAILED":
            print(f"  FAIL tool={tool} error={p.get('error', '')[:100]}")

    # 8. Context per execution
    context_events = conn.execute(
        "SELECT * FROM events WHERE run_id=? AND event_type='EXECUTION_STARTED' AND actor_id LIKE '%:n3' ORDER BY rowid",
        (RUN_ID,),
    ).fetchall()
    print("\n=== n3 Context per Execution ===")
    for e in context_events:
        payload = safe_json(e["payload_json"]) if e["payload_json"] else {}
        if isinstance(payload, dict):
            print(
                f"  attempt={payload.get('attempt')} context_hash={str(payload.get('context_hash', ''))[:32]} from_state={payload.get('from_state', '')} retry_reason={payload.get('retry_reason', 'N/A')}"
            )

    # 9. Run failure tree
    run_events = conn.execute(
        "SELECT * FROM events WHERE run_id=? AND event_type IN ('RUN_COMPLETED','RUN_FAILED','RUN_PAUSED') ORDER BY rowid",
        (RUN_ID,),
    ).fetchall()
    print("\n=== Run Terminal Event ===")
    for e in run_events:
        payload = safe_json(e["payload_json"]) if e["payload_json"] else {}
        if isinstance(payload, dict):
            print(
                f"  {e['event_type']}: failure_code={payload.get('primary_failure_code')} reason={payload.get('termination_reason', '')[:200]}"
            )
            print(
                f"    failed_nodes={payload.get('failed_node_ids')} blocked={payload.get('blocked_node_ids')}"
            )

    conn.close()


if __name__ == "__main__":
    main()
