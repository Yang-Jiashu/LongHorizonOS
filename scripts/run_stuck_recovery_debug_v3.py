#!/usr/bin/env python
"""Stuck-Recovery Debug v3: single Full LHoS run with full instrumentation.

Runs ONLY config_loader x Full LongHorizonOS (no Transcript first).
Uses the same seed, model, temperature, reasoning config, budget, and
PublicTaskSpec as Vertical Slice v2.

Output: artifacts/stuck_recovery_debug_v3/

Usage:
    SENSENOVA_API_KEY=sk-... python scripts/run_stuck_recovery_debug_v3.py
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lhos.agents.llm_worker_adapter import LLMWorkerAdapter
from lhos.agents.real_planner import RealInitialPlanner
from lhos.bootstrap import RuntimeStack
from lhos.infrastructure.llm.call_logger import LLMCallLogger
from lhos.infrastructure.llm.logged_client import LoggedLLMClient
from lhos.infrastructure.llm.sensenova import DEFAULT_MODEL, SenseNovaClient

PILOT_DIR = PROJECT_ROOT / "benchmarks" / "pilot" / "config_loader"
INITIAL_REPO = PILOT_DIR / "initial_repo"
GRADER = PILOT_DIR / "grader" / "grader.py"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "stuck_recovery_debug_v3"
MODEL_ID = os.environ.get("LHOS_MODEL_ID", DEFAULT_MODEL)
MAX_TOKENS = 200_000
MAX_MODEL_CALLS = 100
MAX_TOOL_CALLS = 100
MAX_WALL_CLOCK = 3600  # 60 minutes
SEED = 1  # Same as Vertical Slice v2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("stuck_recovery_v3")


def create_fresh_workspace() -> Path:
    """Create a fresh workspace from initial_repo."""
    workspace = OUTPUT_DIR / "workspace"
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


def snapshot_workspace(workspace: Path) -> dict[str, Any]:
    """Take a snapshot of workspace file listing and hashes."""
    files: list[dict[str, Any]] = []
    for p in sorted(workspace.rglob("*")):
        if p.is_file() and ".git" not in p.parts and "__pycache__" not in p.parts:
            rel = p.relative_to(workspace)
            import hashlib

            files.append(
                {
                    "path": str(rel),
                    "size": p.stat().st_size,
                    "sha256": hashlib.sha256(p.read_bytes()).hexdigest()[:16],
                }
            )
    return {"files": files, "count": len(files)}


def run_grader(workspace: str) -> dict[str, Any]:
    """Run the external grader on the workspace (separate process)."""
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


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def main() -> int:
    api_key = os.environ.get("SENSENOVA_API_KEY", "")
    if not api_key:
        print("ERROR: SENSENOVA_API_KEY environment variable not set")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save run-config.json
    run_config = {
        "task": "config_loader",
        "mode": "full_lhos",
        "model_id": MODEL_ID,
        "seed": SEED,
        "temperature": 0.0,
        "reasoning_effort": "none",
        "max_total_tokens": MAX_TOKENS,
        "max_model_calls": MAX_MODEL_CALLS,
        "max_tool_calls": MAX_TOOL_CALLS,
        "max_wall_clock_seconds": MAX_WALL_CLOCK,
        "retry_policy": "3 retries, exponential backoff",
        "structured_output_repair": "1 retry",
        "scheduler": "cost_aware",
        "checkpoint": "filesystem",
        "features": {"invalidation": True, "local_repair": True},
        "context": {"max_tokens": 12000, "max_dependency_hops": 3, "include_last_failures": 2},
        "initial_commit": "fixed_initial_commit",
        "public_spec": str(PILOT_DIR / "public_spec.json"),
        "grader": str(GRADER),
        "git_commit_before": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, text=True
        ).strip(),
    }
    save_json(OUTPUT_DIR / "run-config.json", run_config)

    # Save capability-manifest.json
    from lhos.infrastructure.tools.filesystem_tool import FILESYSTEM_METADATA, FilesystemTool
    from lhos.infrastructure.tools.registry import ToolRegistry
    from lhos.infrastructure.tools.shell_tool import SHELL_METADATA, ShellTool

    reg = ToolRegistry()
    reg.register(ShellTool(), SHELL_METADATA)
    reg.register(FilesystemTool(), FILESYSTEM_METADATA)
    capability_manifest = {
        "model_id": MODEL_ID,
        "tools": [
            {
                "name": reg.metadata(name).name,
                "side_effect_level": reg.metadata(name).side_effect_level,
                "retry_safe": reg.metadata(name).retry_safe,
                "default_timeout_seconds": reg.metadata(name).default_timeout_seconds,
                "supports_idempotency": reg.metadata(name).supports_idempotency,
            }
            for name in reg.names()
        ],
        "prompts": {
            "planner": "initial_planner_v1",
            "worker": "node_worker_v2",
            "reconciler": "semantic_reconciler_v1",
        },
        "verifiers": ["file_exists", "file_changed", "file_contains", "command", "artifact_exists"],
        "runtime_features": [
            "NodeExecutionBudget",
            "LocalRepairManager",
            "DuplicateWorkDetector",
            "StructuredVerificationFeedback",
            "ContextCompiler",
            "CostAwareScheduler",
            "FilesystemCheckpointManager",
        ],
    }
    save_json(OUTPUT_DIR / "capability-manifest.json", capability_manifest)

    # Create fresh workspace
    workspace = create_fresh_workspace()
    workspace_before = snapshot_workspace(workspace)
    save_json(OUTPUT_DIR / "workspace-before.json", workspace_before)

    # Set up the runtime stack
    db_path = OUTPUT_DIR / "state.db"
    if db_path.exists():
        db_path.unlink()

    trace_dir = OUTPUT_DIR / "traces"
    artifacts_dir = OUTPUT_DIR / "model_responses"
    trace_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

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
        "checkpoint_root": str(OUTPUT_DIR / "checkpoints"),
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

    # Set up LLM client
    raw_client = SenseNovaClient(model_id=MODEL_ID, api_key=api_key)
    llm_logger = LLMCallLogger(
        db=stack.db,
        trace_path=trace_dir / "llm-calls.jsonl",
        artifacts_dir=artifacts_dir,
    )
    logged_client = LoggedLLMClient(
        inner=raw_client,
        logger=llm_logger,
        run_id="stuck-recovery-v3",
    )

    run_id = "stuck-recovery-v3"
    started = time.perf_counter()

    try:
        # Replace FakeWorker with LLMWorkerAdapter
        llm_worker = LLMWorkerAdapter(
            client=logged_client,
            tool_runtime=stack.tool_runtime,
            model_id=MODEL_ID,
        )
        stack.worker = llm_worker
        stack.controller._worker = llm_worker

        # Read task spec
        spec = json.loads((PILOT_DIR / "public_spec.json").read_text())
        goal = spec["goal"]

        # Run the real planner
        print("[v3] Calling real planner...", flush=True)
        logged_client.set_context(
            node_id="planner", prompt_name="initial_planner", prompt_version="v1"
        )
        planner = RealInitialPlanner(
            client=logged_client,
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

        print(f"[v3] Planner generated {len(plan_result['operations'])} operations", flush=True)

        # Convert planner output to graph spec
        graph_spec = {"goal": goal, "nodes": [], "edges": []}
        for op in plan_result["operations"]:
            if op["op"] == "add_node":
                node = dict(op["payload"])
                node["temp_id"] = node.get("id") or f"n{len(graph_spec['nodes']) + 1}"
                graph_spec["nodes"].append(node)
            elif op["op"] == "add_edge":
                graph_spec["edges"].append(op["payload"])

        if not graph_spec["nodes"]:
            print("[v3] Planner returned no nodes, aborting", flush=True)
            save_json(OUTPUT_DIR / "run-summary.json", {"error": "planner returned no nodes"})
            return 1

        # Save graph-before.json (before controller run)
        save_json(OUTPUT_DIR / "graph-before.json", graph_spec)

        # Create run and build graph
        stack.graph_store.create_run(
            run_id,
            goal,
            {"mode": "full_lhos", "model_id": MODEL_ID, "task_id": "config_loader", "seed": SEED},
        )
        id_map = stack.initial_builder.build(run_id, graph_spec)
        print(f"[v3] Built graph with {len(id_map)} nodes", flush=True)

        # Update client run_id
        logged_client._run_id = run_id

        # Run the controller
        print("[v3] Running controller...", flush=True)
        try:
            run = stack.controller.run(run_id)
            print(f"[v3] Controller finished with status: {run.status}", flush=True)
        except Exception as exc:
            print(f"[v3] Controller error: {exc}", flush=True)
            import traceback

            traceback.print_exc()
            run = None

        wall_time = time.perf_counter() - started

        # Save graph-after.json
        try:
            nodes = stack.graph_store.list_nodes(run_id)
            graph_after = {
                "nodes": [
                    {
                        "id": n.id,
                        "title": n.title,
                        "state": n.state,
                        "attempt_count": n.attempt_count,
                        "max_attempts": n.max_attempts,
                        "verification_attempts": getattr(n, "verification_attempts", 0),
                        "parse_attempts": getattr(n, "parse_attempts", 0),
                        "tool_attempts": getattr(n, "tool_attempts", 0),
                    }
                    for n in nodes
                ],
                "edges": [
                    {"source": e.source, "target": e.target, "kind": e.kind}
                    for e in stack.graph_store.list_edges(run_id)
                ],
            }
            save_json(OUTPUT_DIR / "graph-after.json", graph_after)
        except Exception as exc:
            print(f"[v3] Error collecting graph-after: {exc}", flush=True)

        # Save events.jsonl
        try:
            events = stack.event_store.list_events(run_id)
            with open(OUTPUT_DIR / "events.jsonl", "w") as f:
                for e in events:
                    f.write(
                        json.dumps(
                            {
                                "event_type": e.event_type,
                                "timestamp": e.timestamp.isoformat()
                                if hasattr(e.timestamp, "isoformat")
                                else str(e.timestamp),
                                "actor_type": e.actor_type if hasattr(e, "actor_type") else None,
                                "payload": e.payload,
                            },
                            default=str,
                        )
                        + "\n"
                    )
        except Exception as exc:
            print(f"[v3] Error collecting events: {exc}", flush=True)

        # Save llm-calls.jsonl (copy from trace if exists, or from DB)
        try:
            llm_calls = stack.db.conn.execute(
                "SELECT * FROM llm_calls WHERE run_id = ? ORDER BY timestamp",
                (run_id,),
            ).fetchall()
            with open(OUTPUT_DIR / "llm-calls.jsonl", "w") as f:
                for row in llm_calls:
                    f.write(json.dumps(dict(row), default=str) + "\n")
        except Exception as exc:
            print(f"[v3] Error collecting llm-calls: {exc}", flush=True)

        # Save tool-calls.jsonl
        try:
            events = stack.event_store.list_events(run_id)
            with open(OUTPUT_DIR / "tool-calls.jsonl", "w") as f:
                for e in events:
                    if e.event_type in {
                        "TOOL_CALL_REQUESTED",
                        "TOOL_CALL_COMPLETED",
                        "TOOL_CALL_FAILED",
                    }:
                        f.write(
                            json.dumps(
                                {
                                    "event_type": e.event_type,
                                    "node_id": e.payload.get("node_id"),
                                    "tool_name": e.payload.get("tool_name"),
                                    "arguments": e.payload.get("arguments"),
                                    "result": e.payload.get("result"),
                                    "error": e.payload.get("error"),
                                },
                                default=str,
                            )
                            + "\n"
                        )
        except Exception as exc:
            print(f"[v3] Error collecting tool-calls: {exc}", flush=True)

        # Save worker-iterations.jsonl
        try:
            events = stack.event_store.list_events(run_id)
            with open(OUTPUT_DIR / "worker-iterations.jsonl", "w") as f:
                for e in events:
                    if e.event_type in {
                        "NODE_CLAIMED",
                        "NODE_LEASE_RELEASED",
                        "NODE_VERIFIED",
                        "NODE_FAILED",
                        "NODE_RETRY_SCHEDULED",
                        "WORKER_ITERATION",
                        "NODE_BUDGET_EXHAUSTED",
                        "LOCAL_REPAIR_TRIGGERED",
                    }:
                        f.write(
                            json.dumps(
                                {
                                    "event_type": e.event_type,
                                    "node_id": e.payload.get("node_id"),
                                    "payload": e.payload,
                                },
                                default=str,
                            )
                            + "\n"
                        )
        except Exception as exc:
            print(f"[v3] Error collecting worker-iterations: {exc}", flush=True)

        # Save workspace-after.json
        workspace_after = snapshot_workspace(workspace)
        save_json(OUTPUT_DIR / "workspace-after.json", workspace_after)

        # Save external-score.json
        print("[v3] Running external grader...", flush=True)
        grader_result = run_grader(str(workspace))
        save_json(OUTPUT_DIR / "external-score.json", grader_result)

        # Save failure-tree.json
        failure_tree: dict[str, Any] = {}
        try:
            events = stack.event_store.list_events(run_id)
            for e in reversed(events):
                if e.event_type in {"RUN_COMPLETED", "RUN_FAILED", "RUN_PAUSED"}:
                    failure_tree = e.payload
                    break
        except Exception:
            pass
        save_json(OUTPUT_DIR / "failure-tree.json", failure_tree)

        # Save run-summary.json
        try:
            executions = stack.graph_store.list_executions(run_id)
            total_input = sum(e.input_tokens or 0 for e in executions)
            total_output = sum(e.output_tokens or 0 for e in executions)

            llm_calls_list = []
            import contextlib

            with contextlib.suppress(Exception):
                llm_calls_list = [
                    dict(r)
                    for r in stack.db.conn.execute(
                        "SELECT * FROM llm_calls WHERE run_id = ? ORDER BY timestamp",
                        (run_id,),
                    ).fetchall()
                ]

            run_summary = {
                "run_id": run_id,
                "task": "config_loader",
                "mode": "full_lhos",
                "model_id": MODEL_ID,
                "seed": SEED,
                "run_status": run.status if run else "error",
                "wall_time_seconds": round(wall_time, 2),
                "node_count": len(nodes) if "nodes" in dir() else 0,
                "verified_nodes": sum(1 for n in nodes if n.state == "verified")
                if "nodes" in dir()
                else 0,
                "failed_nodes": sum(1 for n in nodes if n.state == "failed")
                if "nodes" in dir()
                else 0,
                "execution_count": len(executions),
                "total_input_tokens": total_input,
                "total_output_tokens": total_output,
                "total_tokens": total_input + total_output,
                "llm_call_count": len(llm_calls_list),
                "llm_calls_by_role": _count_by(llm_calls_list, "role"),
                "llm_calls_by_status": _count_by(llm_calls_list, "status"),
                "planner_summary": plan_result.get("planning_summary", ""),
                "external_score": grader_result,
                "failure_tree": failure_tree,
            }
            save_json(OUTPUT_DIR / "run-summary.json", run_summary)

            # Print summary
            print("\n" + "=" * 60)
            print("STUCK RECOVERY DEBUG v3 — SUMMARY")
            print("=" * 60)
            print(f"Status: {run_summary['run_status']}")
            print(f"Model calls (executions): {run_summary['execution_count']}")
            print(f"LLM calls logged: {run_summary['llm_call_count']}")
            print(f"Total tokens: {run_summary['total_tokens']}")
            print(f"Wall time: {run_summary['wall_time_seconds']}s")
            print(f"Nodes: {run_summary['node_count']}")
            print(f"Verified: {run_summary['verified_nodes']}")
            print(f"Failed: {run_summary['failed_nodes']}")
            score = grader_result.get("progress_ratio", 0)
            print(f"External score: {score:.1%}")
            if failure_tree:
                print(f"Failure code: {failure_tree.get('primary_failure_code', 'N/A')}")
                print(f"Termination: {failure_tree.get('termination_reason', 'N/A')}")
            print(f"\nArtifacts saved to: {OUTPUT_DIR}")
        except Exception as exc:
            print(f"[v3] Error building run summary: {exc}", flush=True)
            import traceback

            traceback.print_exc()

    finally:
        stack.close()

    return 0


def _count_by(items: list[dict], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        val = str(item.get(key, "unknown"))
        counts[val] = counts.get(val, 0) + 1
    return counts


if __name__ == "__main__":
    sys.exit(main())
