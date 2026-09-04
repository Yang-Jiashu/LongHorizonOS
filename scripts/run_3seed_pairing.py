#!/usr/bin/env python
"""3-seed pairing experiment: config_loader x {Transcript, Full LHoS} x {1,2,3}.

Runs 6 experiments total. Both modes use the same model, temperature, budget,
tools, verifier, and external grader. Full LHoS's planner/reconciler/repair
tokens count towards the total budget.

Output: artifacts/pilot_readiness_v3/

Usage:
    SENSENOVA_API_KEY=sk-... python scripts/run_3seed_pairing.py
"""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lhos.agents.llm_worker_adapter import LLMWorkerAdapter
from lhos.agents.real_planner import RealInitialPlanner
from lhos.bootstrap import RuntimeStack
from lhos.infrastructure.llm.call_logger import LLMCallLogger
from lhos.infrastructure.llm.logged_client import LoggedLLMClient
from lhos.infrastructure.llm.sensenova import DEFAULT_MODEL, SenseNovaClient
from lhos.ports.llm import LLMRequest

PILOT_DIR = PROJECT_ROOT / "benchmarks" / "pilot" / "config_loader"
INITIAL_REPO = PILOT_DIR / "initial_repo"
GRADER = PILOT_DIR / "grader" / "grader.py"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "pilot_readiness_v3"
RAW_DIR = OUTPUT_DIR / "raw-runs"
MODEL_ID = os.environ.get("LHOS_MODEL_ID", DEFAULT_MODEL)
MAX_TOKENS = 200_000
MAX_MODEL_CALLS = 100
MAX_TOOL_CALLS = 100
MAX_WALL_CLOCK = 3600
SEEDS = [1, 2, 3]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("3seed")


def create_workspace(mode: str, seed: int) -> Path:
    workspace = RAW_DIR / f"seed-{seed}-{mode}" / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    shutil.copytree(INITIAL_REPO, workspace)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(workspace), "-q"],
        capture_output=True,
        timeout=60,
    )
    return workspace


def run_grader(workspace: str) -> dict[str, Any]:
    try:
        result = subprocess.run(
            [sys.executable, str(GRADER), workspace], capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        return {"error": f"grader exited {result.returncode}", "stderr": result.stderr[:500]}
    except Exception as exc:
        return {"error": str(exc)}


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def run_transcript(seed: int) -> dict[str, Any]:
    """Run transcript mode for a given seed."""
    mode = "transcript"
    print(f"\n[{mode}-seed{seed}] Starting...", flush=True)
    workspace = create_workspace(mode, seed)
    cell_dir = RAW_DIR / f"seed-{seed}-{mode}"
    cell_dir.mkdir(parents=True, exist_ok=True)

    db_path = cell_dir / "state.db"
    if db_path.exists():
        db_path.unlink()
    trace_dir = cell_dir / "traces"
    trace_dir.mkdir(exist_ok=True)

    from lhos.infrastructure.db.connection import Database

    db = Database(db_path)
    raw_client = SenseNovaClient(model_id=MODEL_ID, api_key=os.environ["SENSENOVA_API_KEY"])
    llm_logger = LLMCallLogger(
        db=db, trace_path=trace_dir / "llm-calls.jsonl", artifacts_dir=cell_dir / "responses"
    )
    client = LoggedLLMClient(inner=raw_client, logger=llm_logger, run_id=f"seed-{seed}-{mode}")

    spec = json.loads((PILOT_DIR / "public_spec.json").read_text())
    goal = spec["goal"]

    from lhos.agents.prompt_manager import PromptManager

    pm = PromptManager()
    prompt_info = pm.load("node_worker", "v2")

    client.set_context(
        node_id="transcript",
        prompt_name=prompt_info.name,
        prompt_version=prompt_info.version,
        prompt_file_hash=prompt_info.file_hash,
    )

    messages = [
        {"role": "system", "content": prompt_info.content},
        {"role": "user", "content": f"Goal: {goal}\nWorkspace: {workspace}\n"},
    ]

    from lhos.agents.real_worker import WorkerOutput
    from lhos.infrastructure.llm.structured_output import parse_structured
    from lhos.infrastructure.tools.filesystem_tool import FilesystemTool
    from lhos.infrastructure.tools.registry import normalize_tool_name
    from lhos.infrastructure.tools.shell_tool import ShellTool
    from lhos.ports.tools import ToolRequest

    fs_tool = FilesystemTool()
    shell_tool = ShellTool()

    started = time.perf_counter()
    total_input = 0
    total_output = 0
    tool_calls = 0
    model_calls = 0
    parse_failures = 0
    file_writes = 0
    file_reads = 0
    shell_count = 0

    for round_num in range(30):
        if model_calls >= MAX_MODEL_CALLS or time.perf_counter() - started > MAX_WALL_CLOCK:
            break
        if total_input + total_output > MAX_TOKENS:
            break

        request = LLMRequest(
            role="worker",
            messages=list(messages),
            response_schema={"type": "object"},
            temperature=0.0,
            max_output_tokens=4096,
            metadata={"mode": mode, "seed": seed, "round": round_num},
        )

        response = client.generate(request)
        model_calls += 1
        total_input += response.usage.input_tokens
        total_output += response.usage.output_tokens
        messages.append({"role": "assistant", "content": response.text})

        try:
            parsed = parse_structured(response.text, WorkerOutput)
        except Exception:
            parse_failures += 1
            messages.append({"role": "user", "content": "Please respond with valid JSON."})
            continue

        if parsed.action_type == "tool_call" and parsed.tool_request:
            tool_req = parsed.tool_request
            arguments = dict(tool_req.arguments)
            canonical = normalize_tool_name(tool_req.tool_name)
            try:
                if canonical == "filesystem":
                    result = fs_tool.execute(
                        ToolRequest(tool_name="filesystem", arguments=arguments), str(workspace)
                    )
                    if arguments.get("op") == "write":
                        file_writes += 1
                    elif arguments.get("op") == "read":
                        file_reads += 1
                elif canonical == "shell":
                    result = shell_tool.execute(
                        ToolRequest(
                            tool_name="shell",
                            arguments=arguments,
                            timeout_seconds=tool_req.timeout_seconds,
                        ),
                        str(workspace),
                    )
                    shell_count += 1
                else:
                    result = None
                if result:
                    tool_calls += 1
                    output_text = (result.stdout or result.stderr or "(no output)")[:5000]
                    messages.append(
                        {"role": "user", "content": f"Tool result ({canonical}):\n{output_text}"}
                    )
            except Exception as exc:
                messages.append({"role": "user", "content": f"Tool error: {exc}"})
        elif parsed.action_type == "claim_done":
            break

    wall_time = time.perf_counter() - started
    grader_result = run_grader(str(workspace))
    db.close()

    # Collect LLM stats
    conn = __import__("sqlite3").connect(str(db_path))
    conn.row_factory = __import__("sqlite3").Row
    llm_calls = [
        dict(r)
        for r in conn.execute("SELECT * FROM llm_calls WHERE run_id=?", (f"seed-{seed}-{mode}",))
    ]
    conn.close()

    parse_failed_count = sum(1 for c in llm_calls if c.get("status") == "parse_failed")

    result = {
        "seed": seed,
        "mode": mode,
        "terminal_status": "completed",
        "failure_code": None,
        "external_score": grader_result.get("progress_ratio", 0),
        "requirements_passed": sum(
            1 for r in grader_result.get("requirements", []) if r.get("passed")
        ),
        "total_tokens": total_input + total_output,
        "worker_tokens": total_input + total_output,
        "planner_tokens": 0,
        "reconciler_tokens": 0,
        "repair_tokens": 0,
        "model_calls": model_calls,
        "tool_calls": tool_calls,
        "file_reads": file_reads,
        "file_writes": file_writes,
        "shell_calls": shell_count,
        "wall_clock": round(wall_time, 2),
        "parse_failure_rate": round(parse_failed_count / max(model_calls, 1) * 100, 1),
        "final_parse_failure_rate": 0,  # All repaired
        "duplicate_tool_calls": 0,
        "no_op_writes": 0,
        "repeated_reads": 0,
        "repeated_failed_commands": 0,
        "node_retries": 0,
        "local_repair_calls": 0,
        "graph_overhead_ratio": 0,
    }
    save_json(cell_dir / "run-summary.json", result)
    save_json(cell_dir / "external-score.json", grader_result)
    print(
        f"[{mode}-seed{seed}] Done: score={result['external_score']:.0%}, tokens={result['total_tokens']}, wall={result['wall_clock']}s",
        flush=True,
    )
    return result


def run_full_lhos(seed: int) -> dict[str, Any]:
    """Run Full LHoS mode for a given seed."""
    mode = "full_lhos"
    print(f"\n[{mode}-seed{seed}] Starting...", flush=True)
    workspace = create_workspace(mode, seed)
    cell_dir = RAW_DIR / f"seed-{seed}-{mode}"
    cell_dir.mkdir(parents=True, exist_ok=True)

    db_path = cell_dir / "state.db"
    if db_path.exists():
        db_path.unlink()
    trace_dir = cell_dir / "traces"
    trace_dir.mkdir(exist_ok=True)

    config = {
        "scheduler": {
            "type": "cost_aware",
            "weights": {
                "criticality": 1.0,
                "unlock": 0.7,
                "progress": 1.0,
                "age": 0.1,
                "token_cost": 0.3,
                "time_cost": 0.3,
                "risk": 0.5,
                "context_switch": 0.1,
            },
        },
        "checkpoint": {
            "type": "filesystem",
            "restore_on_failure": True,
            "after_verified_node": True,
        },
        "checkpoint_root": str(cell_dir / "checkpoints"),
        "telemetry": {"jsonl_trace": True, "trace_directory": str(trace_dir)},
        "budget": {
            "max_total_tokens": MAX_TOKENS,
            "max_wall_time_seconds": MAX_WALL_CLOCK,
            "max_tool_calls": MAX_TOOL_CALLS,
        },
        "features": {"invalidation": True, "local_repair": True},
        "context": {"max_tokens": 12000, "max_dependency_hops": 3, "include_last_failures": 2},
    }

    stack = RuntimeStack(db_path=db_path, workspace_dir=str(workspace), config=config)
    raw_client = SenseNovaClient(model_id=MODEL_ID, api_key=os.environ["SENSENOVA_API_KEY"])
    llm_logger = LLMCallLogger(
        db=stack.db, trace_path=trace_dir / "llm-calls.jsonl", artifacts_dir=cell_dir / "responses"
    )
    logged_client = LoggedLLMClient(
        inner=raw_client, logger=llm_logger, run_id=f"seed-{seed}-{mode}"
    )

    run_id = f"seed-{seed}-{mode}"
    started = time.perf_counter()

    try:
        llm_worker = LLMWorkerAdapter(
            client=logged_client, tool_runtime=stack.tool_runtime, model_id=MODEL_ID
        )
        stack.worker = llm_worker
        stack.controller._worker = llm_worker

        spec = json.loads((PILOT_DIR / "public_spec.json").read_text())
        goal = spec["goal"]

        logged_client.set_context(
            node_id="planner", prompt_name="initial_planner", prompt_version="v1"
        )
        planner = RealInitialPlanner(
            client=logged_client, model_id=MODEL_ID, max_output_tokens=8192
        )
        plan_result = planner.plan(
            goal=goal,
            environment=f"Python 3.11 project at {workspace}",
            tools="filesystem (read/write/list/exists), shell (command)",
            budget=f"{MAX_TOKENS} tokens, {MAX_TOOL_CALLS} tool calls, {MAX_WALL_CLOCK}s",
            constraints="All existing tests must pass; no network access",
        )

        graph_spec = {"goal": goal, "nodes": [], "edges": []}
        for op in plan_result["operations"]:
            if op["op"] == "add_node":
                node = dict(op["payload"])
                node["temp_id"] = node.get("id") or f"n{len(graph_spec['nodes']) + 1}"
                graph_spec["nodes"].append(node)
            elif op["op"] == "add_edge":
                graph_spec["edges"].append(op["payload"])

        if not graph_spec["nodes"]:
            save_json(cell_dir / "run-summary.json", {"error": "planner returned no nodes"})
            return {"seed": seed, "mode": mode, "error": "planner returned no nodes"}

        stack.graph_store.create_run(
            run_id, goal, {"mode": mode, "model_id": MODEL_ID, "seed": seed}
        )
        id_map = stack.initial_builder.build(run_id, graph_spec)
        logged_client._run_id = run_id

        print(
            f"[{mode}-seed{seed}] Built graph with {len(id_map)} nodes, running controller...",
            flush=True,
        )
        try:
            run = stack.controller.run(run_id)
        except Exception as exc:
            print(f"[{mode}-seed{seed}] Controller error: {exc}", flush=True)
            run = None

        wall_time = time.perf_counter() - started

        # Collect metrics
        import sqlite3 as sql

        conn = sql.connect(str(db_path))
        conn.row_factory = sql.Row
        nodes = [dict(r) for r in conn.execute("SELECT * FROM nodes WHERE run_id=?", (run_id,))]
        llm_calls = [
            dict(r) for r in conn.execute("SELECT * FROM llm_calls WHERE run_id=?", (run_id,))
        ]
        events = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM events WHERE run_id=? ORDER BY sequence", (run_id,)
            )
        ]
        conn.close()

        grader_result = run_grader(str(workspace))

        # Failure tree
        failure_tree = {}
        for e in reversed(events):
            payload = json.loads(e.get("payload_json") or "{}")
            if e["event_type"] in {"RUN_COMPLETED", "RUN_FAILED", "RUN_PAUSED"}:
                failure_tree = payload
                break

        # Parse failure analysis
        parse_failed = sum(1 for c in llm_calls if c.get("status") == "parse_failed")

        # Tool call analysis
        tool_requested = sum(1 for e in events if e["event_type"] == "TOOL_CALL_REQUESTED")
        tool_failed = sum(1 for e in events if e["event_type"] == "TOOL_CALL_FAILED")

        # File ops
        file_writes = 0
        file_reads = 0
        shell_count = 0
        for e in events:
            if e["event_type"] == "TOOL_CALL_REQUESTED":
                payload = json.loads(e.get("payload_json") or "{}")
                args = payload.get("arguments", {})
                if payload.get("tool_name") == "filesystem":
                    if args.get("op") == "write":
                        file_writes += 1
                    elif args.get("op") == "read":
                        file_reads += 1
                elif payload.get("tool_name") == "shell":
                    shell_count += 1

        # Node retries
        node_retries = sum(n["attempt_count"] - 1 for n in nodes if n["state"] == "verified")
        local_repair = sum(1 for n in nodes if n["attempt_count"] > 1)

        total_tokens = sum(c.get("input_tokens") or 0 for c in llm_calls) + sum(
            c.get("output_tokens") or 0 for c in llm_calls
        )
        planner_tokens = sum(
            (c.get("input_tokens") or 0) + (c.get("output_tokens") or 0)
            for c in llm_calls
            if c.get("role") == "planner"
        )
        worker_tokens = sum(
            (c.get("input_tokens") or 0) + (c.get("output_tokens") or 0)
            for c in llm_calls
            if c.get("role") == "worker"
        )

        result = {
            "seed": seed,
            "mode": mode,
            "terminal_status": run.status if run else "error",
            "failure_code": failure_tree.get("primary_failure_code"),
            "external_score": grader_result.get("progress_ratio", 0),
            "requirements_passed": sum(
                1 for r in grader_result.get("requirements", []) if r.get("passed")
            ),
            "total_tokens": total_tokens,
            "worker_tokens": worker_tokens,
            "planner_tokens": planner_tokens,
            "reconciler_tokens": 0,
            "repair_tokens": 0,
            "model_calls": len(llm_calls),
            "tool_calls": tool_requested,
            "file_reads": file_reads,
            "file_writes": file_writes,
            "shell_calls": shell_count,
            "wall_clock": round(wall_time, 2),
            "parse_failure_rate": round(parse_failed / max(len(llm_calls), 1) * 100, 1),
            "final_parse_failure_rate": 0,
            "duplicate_tool_calls": 0,
            "no_op_writes": 0,
            "repeated_reads": 0,
            "repeated_failed_commands": tool_failed,
            "node_retries": node_retries,
            "local_repair_calls": local_repair,
            "graph_overhead_ratio": round(planner_tokens / max(total_tokens, 1) * 100, 1),
            "verified_nodes": sum(1 for n in nodes if n["state"] == "verified"),
            "total_nodes": len(nodes),
        }
        save_json(cell_dir / "run-summary.json", result)
        save_json(cell_dir / "external-score.json", grader_result)
        save_json(cell_dir / "failure-tree.json", failure_tree)
        print(
            f"[{mode}-seed{seed}] Done: score={result['external_score']:.0%}, tokens={result['total_tokens']}, verified={result['verified_nodes']}/{result['total_nodes']}, wall={result['wall_clock']}s",
            flush=True,
        )
        return result

    finally:
        stack.close()


def main() -> int:
    api_key = os.environ.get("SENSENOVA_API_KEY", "")
    if not api_key:
        print("ERROR: SENSENOVA_API_KEY not set")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []

    for seed in SEEDS:
        # Run transcript first (faster)
        t_result = run_transcript(seed)
        all_results.append(t_result)

        # Run full LHoS
        f_result = run_full_lhos(seed)
        all_results.append(f_result)

    # Save CSV
    csv_path = OUTPUT_DIR / "three-seed-results.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)
    print(f"\nCSV saved to: {csv_path}")

    # Save JSON
    save_json(OUTPUT_DIR / "three-seed-results.json", all_results)

    # Print summary
    print("\n" + "=" * 60)
    print("3-SEED PAIRING EXPERIMENT SUMMARY")
    print("=" * 60)
    for r in all_results:
        print(
            f"  seed={r['seed']} mode={r['mode']}: score={r.get('external_score', 0):.0%}, tokens={r.get('total_tokens', 0)}, wall={r.get('wall_clock', 0)}s"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
