#!/usr/bin/env python
"""Vertical Slice runner: Transcript vs Full LHoS with real LLM (spec Phase 2G-2I).

Runs the config_loader task in two modes:
1. Transcript: LLM-driven linear execution (no graph).
2. Full LHoS: LLM planner + graph runtime + LLM worker + verification gate.

Both modes use the same SenseNova model. The external grader scores both
independently. Results are saved to artifacts/real_llm_vertical_slice/.

Step 2 fix: all real LLM calls now go through ``LoggedLLMClient`` which
wraps the ``SenseNovaClient``. This ensures every call is written to the
``llm_calls`` table and JSONL trace.

Usage:
    SENSENOVA_API_KEY=sk-... python scripts/run_vertical_slice.py
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# Ensure src is on the path.
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
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "real_llm_vertical_slice"
MODEL_ID = os.environ.get("LHOS_MODEL_ID", DEFAULT_MODEL)
MAX_TOKENS = 200_000
MAX_MODEL_CALLS = 100
MAX_TOOL_CALLS = 100
MAX_WALL_CLOCK = 3600  # 60 minutes

# Enable diagnostic logging.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)


def create_workspace(mode: str) -> Path:
    """Create a fresh workspace from the initial_repo."""
    workspace = OUTPUT_DIR / mode / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    shutil.copytree(INITIAL_REPO, workspace)
    # Install the package in development mode.
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(workspace), "-q"],
        capture_output=True,
        timeout=60,
    )
    return workspace


def run_transcript_mode(client: LoggedLLMClient, workspace: Path) -> dict[str, Any]:
    """Run the task in transcript mode: LLM-driven linear execution.

    The LLM sees the goal, inspects files, writes code, and claims done.
    No graph, no verification gate, no context compiler.
    """
    print("[transcript] Starting transcript mode...", flush=True)
    started = time.perf_counter()

    # Read task spec.
    spec = json.loads((PILOT_DIR / "public_spec.json").read_text())
    goal = spec["goal"]

    # Load the worker prompt.
    from lhos.agents.prompt_manager import PromptManager

    pm = PromptManager()
    prompt_info = pm.load("node_worker", "v1")

    # Set context on the logged client.
    client.set_context(
        node_id="transcript",
        prompt_name=prompt_info.name,
        prompt_version=prompt_info.version,
        prompt_file_hash=prompt_info.file_hash,
    )

    # Simple conversation: system + user goal.
    messages = [
        {"role": "system", "content": prompt_info.content},
        {"role": "user", "content": f"Goal: {goal}\nWorkspace: {workspace}\n"},
    ]

    total_input = 0
    total_output = 0
    tool_calls = 0
    model_calls = 0
    all_artifacts = []

    from lhos.agents.real_worker import WorkerOutput
    from lhos.infrastructure.llm.structured_output import parse_structured
    from lhos.infrastructure.tools.filesystem_tool import FilesystemTool
    from lhos.infrastructure.tools.shell_tool import ShellTool
    from lhos.ports.tools import ToolRequest

    fs_tool = FilesystemTool()
    shell_tool = ShellTool()

    for round_num in range(30):  # max 30 rounds
        if model_calls >= MAX_MODEL_CALLS:
            print(f"[transcript] Max model calls reached ({MAX_MODEL_CALLS})", flush=True)
            break
        if time.perf_counter() - started > MAX_WALL_CLOCK:
            print("[transcript] Wall-clock budget exhausted", flush=True)
            break

        request = LLMRequest(
            role="worker",
            messages=list(messages),
            response_schema={"type": "object"},
            temperature=0.0,
            max_output_tokens=4096,
            metadata={
                "mode": "transcript",
                "round": round_num,
                "prompt_name": prompt_info.name,
                "prompt_version": prompt_info.version,
                "prompt_file_hash": prompt_info.file_hash,
                "node_id": "transcript",
            },
        )

        response = client.generate(request)
        model_calls += 1
        total_input += response.usage.input_tokens
        total_output += response.usage.output_tokens
        messages.append({"role": "assistant", "content": response.text})

        print(
            f"[transcript] Round {round_num}: tokens={response.usage.total_tokens} "
            f"parse_failures={response.parse_failure_count}",
            flush=True,
        )

        if total_input + total_output > MAX_TOKENS:
            print(f"[transcript] Token budget exhausted ({total_input + total_output})", flush=True)
            break

        try:
            parsed = parse_structured(response.text, WorkerOutput)
        except Exception:
            # If we can't parse, try to continue.
            messages.append({"role": "user", "content": "Please respond with valid JSON."})
            continue

        if parsed.action_type == "tool_call" and parsed.tool_request:
            tool_req = parsed.tool_request
            arguments = dict(tool_req.arguments)

            # Normalize tool name (Step 6).
            from lhos.infrastructure.tools.registry import normalize_tool_name

            canonical = normalize_tool_name(tool_req.tool_name)

            try:
                if canonical == "filesystem":
                    result = fs_tool.execute(
                        ToolRequest(tool_name="filesystem", arguments=arguments),
                        str(workspace),
                    )
                elif canonical == "shell":
                    result = shell_tool.execute(
                        ToolRequest(
                            tool_name="shell",
                            arguments=arguments,
                            timeout_seconds=tool_req.timeout_seconds,
                        ),
                        str(workspace),
                    )
                else:
                    result = None

                if result:
                    tool_calls += 1
                    output_text = result.stdout or result.stderr or "(no output)"
                    if len(output_text) > 5000:
                        output_text = output_text[:5000] + "...[truncated]"
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Tool result ({canonical}):\n{output_text}",
                        }
                    )
                else:
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Unknown tool: {tool_req.tool_name} (normalized: {canonical})",
                        }
                    )
            except Exception as exc:
                messages.append(
                    {
                        "role": "user",
                        "content": f"Tool error ({canonical}): {exc}",
                    }
                )

        elif parsed.action_type == "claim_done":
            print(f"[transcript] Worker claimed done: {parsed.summary[:100]}", flush=True)
            all_artifacts = parsed.produced_artifacts
            break

    wall_time = time.perf_counter() - started

    # Run external grader.
    print("[transcript] Running external grader...", flush=True)
    grader_result = run_grader(str(workspace))

    return {
        "mode": "transcript",
        "model_id": MODEL_ID,
        "model_calls": model_calls,
        "input_tokens": total_input,
        "output_tokens": total_output,
        "total_tokens": total_input + total_output,
        "tool_calls": tool_calls,
        "wall_time_seconds": round(wall_time, 2),
        "external_score": grader_result,
        "produced_artifacts": all_artifacts,
    }


def run_full_lhos_mode(client: LoggedLLMClient, workspace: Path) -> dict[str, Any]:
    """Run the task in Full LHoS mode: planner + graph + worker + verification."""
    print("[full_lhos] Starting Full LHoS mode...", flush=True)
    started = time.perf_counter()

    spec = json.loads((PILOT_DIR / "public_spec.json").read_text())
    goal = spec["goal"]

    # Set up the runtime stack.
    db_path = OUTPUT_DIR / "full_lhos" / "state.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    trace_dir = OUTPUT_DIR / "full_lhos" / "traces"
    artifacts_dir = OUTPUT_DIR / "full_lhos" / "model_responses"

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
        "checkpoint_root": str(OUTPUT_DIR / "full_lhos" / "checkpoints"),
        "telemetry": {
            "jsonl_trace": True,
            "trace_directory": str(trace_dir),
        },
        "budget": {
            "max_total_tokens": MAX_TOKENS,
            "max_wall_time_seconds": MAX_WALL_CLOCK,
            "max_tool_calls": MAX_TOOL_CALLS,
        },
        "features": {"invalidation": True, "local_repair": True},
        "context": {"max_tokens": 12000, "max_dependency_hops": 3, "include_last_failures": 2},
    }

    stack = RuntimeStack(
        db_path=db_path,
        workspace_dir=str(workspace),
        config=config,
    )

    # Step 2: update the LoggedLLMClient to use the stack's database.
    # The LLMCallLogger needs the stack's db for the llm_calls table.
    llm_logger = LLMCallLogger(
        db=stack.db,
        trace_path=trace_dir / "llm_calls.jsonl",
        artifacts_dir=artifacts_dir,
    )
    client._logger = llm_logger  # Re-bind logger to the stack's database.

    run_id = "vertical-slice-full-lhos"

    try:
        # Replace FakeWorker with LLMWorkerAdapter.
        llm_worker = LLMWorkerAdapter(
            client=client,
            tool_runtime=stack.tool_runtime,
            model_id=MODEL_ID,
        )
        stack.worker = llm_worker
        stack.controller._worker = llm_worker

        # Use the real planner to generate the initial graph.
        print("[full_lhos] Calling real planner...", flush=True)
        client.set_context(node_id="planner", prompt_name="initial_planner", prompt_version="v1")
        planner = RealInitialPlanner(
            client=client,
            model_id=MODEL_ID,
            max_output_tokens=8192,
        )

        plan_result = planner.plan(
            goal=goal,
            environment=f"Python 3.11 project at {workspace}",
            tools="filesystem (read/write/list/exists), shell (command)",
            budget=f"{MAX_TOKENS} tokens, {MAX_TOOL_CALLS} tool calls, {MAX_WALL_CLOCK}s",
            constraints="All existing tests must pass; no network access",
        )

        print(
            f"[full_lhos] Planner generated {len(plan_result['operations'])} operations",
            flush=True,
        )

        # Convert planner output to graph spec format.
        graph_spec = {"goal": goal, "nodes": [], "edges": []}
        for op in plan_result["operations"]:
            if op["op"] == "add_node":
                node = dict(op["payload"])
                node["temp_id"] = node.get("id") or f"n{len(graph_spec['nodes']) + 1}"
                graph_spec["nodes"].append(node)
            elif op["op"] == "add_edge":
                graph_spec["edges"].append(op["payload"])

        if not graph_spec["nodes"]:
            print("[full_lhos] Planner returned no nodes, aborting", flush=True)
            return {
                "mode": "full_lhos",
                "model_id": MODEL_ID,
                "error": "planner returned no nodes",
                "wall_time_seconds": round(time.perf_counter() - started, 2),
            }

        # Create run and build graph.
        stack.graph_store.create_run(
            run_id,
            goal,
            {
                "mode": "full_lhos",
                "model_id": MODEL_ID,
                "task_id": "config_loader",
            },
        )
        id_map = stack.initial_builder.build(run_id, graph_spec)
        print(f"[full_lhos] Built graph with {len(id_map)} nodes", flush=True)

        # Update client run_id for logging.
        client._run_id = run_id

        # Run the controller.
        print("[full_lhos] Running controller...", flush=True)
        try:
            run = stack.controller.run(run_id)
            print(f"[full_lhos] Controller finished with status: {run.status}", flush=True)
        except Exception as exc:
            print(f"[full_lhos] Controller error: {exc}", flush=True)
            import traceback

            traceback.print_exc()
            run = None

        wall_time = time.perf_counter() - started

        # Collect metrics from the run.
        nodes = stack.graph_store.list_nodes(run_id)
        executions = stack.graph_store.list_executions(run_id)

        # Run external grader.
        print("[full_lhos] Running external grader...", flush=True)
        grader_result = run_grader(str(workspace))

        # Collect LLM call data from the database.
        llm_calls: list[dict[str, Any]] = []
        try:
            llm_calls = stack.db.conn.execute(
                "SELECT * FROM llm_calls WHERE run_id = ? ORDER BY timestamp",
                (run_id,),
            ).fetchall()
            llm_calls = [dict(r) for r in llm_calls]
        except Exception:
            pass

        # Collect tool call events.
        tool_events: list[dict[str, Any]] = []
        try:
            events = stack.event_store.list_events(run_id)
            tool_events = [
                {
                    "event_type": e.event_type,
                    "tool_name": e.payload.get("tool_name"),
                    "node_id": e.payload.get("node_id"),
                }
                for e in events
                if e.event_type
                in {"TOOL_CALL_REQUESTED", "TOOL_CALL_COMPLETED", "TOOL_CALL_FAILED"}
            ]
        except Exception:
            pass

        # Collect failure tree from run status event.
        failure_tree: dict[str, Any] = {}
        try:
            events = stack.event_store.list_events(run_id)
            for e in reversed(events):
                if e.event_type in {"RUN_COMPLETED", "RUN_FAILED", "RUN_PAUSED"}:
                    failure_tree = e.payload
                    break
        except Exception:
            pass

        total_input = sum(e.input_tokens or 0 for e in executions)
        total_output = sum(e.output_tokens or 0 for e in executions)

        # Count file writes.
        file_writes = sum(1 for c in tool_events if c.get("tool_name") == "filesystem")
        shell_calls = sum(1 for c in tool_events if c.get("tool_name") == "shell")

        return {
            "mode": "full_lhos",
            "model_id": MODEL_ID,
            "run_status": run.status if run else "error",
            "model_calls": len(executions),
            "input_tokens": total_input,
            "output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "tool_calls": sum(e.tool_calls or 0 for e in executions),
            "wall_time_seconds": round(wall_time, 2),
            "node_count": len(nodes),
            "verified_nodes": sum(1 for n in nodes if n.state == "verified"),
            "failed_nodes": sum(1 for n in nodes if n.state == "failed"),
            "external_score": grader_result,
            "llm_call_count": len(llm_calls),
            "llm_calls_by_role": _count_by_role(llm_calls),
            "llm_calls_by_status": _count_by_status(llm_calls),
            "tool_events": tool_events,
            "file_write_count": file_writes,
            "shell_call_count": shell_calls,
            "failure_tree": failure_tree,
            "planner_summary": plan_result.get("planning_summary", ""),
        }
    finally:
        stack.close()


def _count_by_role(calls: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for c in calls:
        role = c.get("role", "unknown")
        counts[role] = counts.get(role, 0) + 1
    return counts


def _count_by_status(calls: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for c in calls:
        status = c.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def run_grader(workspace: str) -> dict[str, Any]:
    """Run the external grader on the workspace."""
    try:
        result = subprocess.run(
            [sys.executable, str(GRADER), workspace],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return {"error": "grader output not JSON", "stdout": result.stdout[:500]}
        return {"error": f"grader exited {result.returncode}", "stderr": result.stderr[:500]}
    except Exception as exc:
        return {"error": str(exc)}


def main():
    # Check API key.
    api_key = os.environ.get("SENSENOVA_API_KEY", "")
    if not api_key:
        print("ERROR: SENSENOVA_API_KEY environment variable not set")
        return 1

    # Create output directory.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 2: Composition root — create the raw client, then wrap it.
    raw_client = SenseNovaClient(model_id=MODEL_ID, api_key=api_key)

    # Create a temporary logger (will be re-bound to the stack's db for
    # Full LHoS mode; for Transcript mode it uses a separate db).
    transcript_db_path = OUTPUT_DIR / "transcript" / "state.db"
    transcript_db_path.parent.mkdir(parents=True, exist_ok=True)
    if transcript_db_path.exists():
        transcript_db_path.unlink()

    from lhos.infrastructure.db.connection import Database

    transcript_db = Database(transcript_db_path)
    transcript_logger = LLMCallLogger(
        db=transcript_db,
        trace_path=OUTPUT_DIR / "transcript" / "traces" / "llm_calls.jsonl",
        artifacts_dir=OUTPUT_DIR / "transcript" / "model_responses",
    )
    transcript_client = LoggedLLMClient(
        inner=raw_client,
        logger=transcript_logger,
        run_id="vertical-slice-transcript",
    )

    full_client = LoggedLLMClient(
        inner=raw_client,
        logger=transcript_logger,  # Will be re-bound in run_full_lhos_mode
        run_id="vertical-slice-full-lhos",
    )

    print(f"Using model: {MODEL_ID}", flush=True)
    print("All LLM calls go through LoggedLLMClient", flush=True)

    # Run transcript mode.
    transcript_workspace = create_workspace("transcript")
    transcript_result = run_transcript_mode(transcript_client, transcript_workspace)

    # Close transcript db.
    transcript_db.close()

    # Run full LHoS mode.
    full_workspace = create_workspace("longhorizonos")
    full_result = run_full_lhos_mode(full_client, full_workspace)

    # Generate paired comparison.
    comparison = {
        "task": "config_loader",
        "model_id": MODEL_ID,
        "transcript": transcript_result,
        "longhorizonos": full_result,
    }

    comparison_path = OUTPUT_DIR / "paired-comparison.json"
    comparison_path.write_text(json.dumps(comparison, indent=2, default=str))
    print(f"\nPaired comparison saved to: {comparison_path}")

    # Print summary.
    print("\n" + "=" * 60)
    print("VERTICAL SLICE SUMMARY")
    print("=" * 60)
    for mode_name, result in [("Transcript", transcript_result), ("Full LHoS", full_result)]:
        score = result.get("external_score", {})
        print(f"\n{mode_name}:")
        print(f"  Status: {result.get('run_status', 'completed')}")
        print(f"  Model calls: {result.get('model_calls', 0)}")
        print(f"  LLM calls logged: {result.get('llm_call_count', 'N/A')}")
        print(f"  Total tokens: {result.get('total_tokens', 0)}")
        print(f"  Tool calls: {result.get('tool_calls', 0)}")
        print(f"  File writes: {result.get('file_write_count', 'N/A')}")
        print(f"  Shell calls: {result.get('shell_call_count', 'N/A')}")
        print(f"  Wall time: {result.get('wall_time_seconds', 0)}s")
        print(f"  External score: {score.get('progress_ratio', 0):.1%}")
        print(
            f"  Requirements passed: {sum(1 for r in score.get('requirements', []) if r.get('passed'))}/{len(score.get('requirements', []))}"
        )
        if result.get("failure_tree"):
            ft = result["failure_tree"]
            print(f"  Failure code: {ft.get('primary_failure_code', 'N/A')}")
            print(f"  Termination reason: {ft.get('termination_reason', 'N/A')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
