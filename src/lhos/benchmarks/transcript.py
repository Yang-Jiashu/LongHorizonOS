"""Transcript-only baseline (spec 25, mode ``transcript``).

No task graph, no event log, no reconciliation, no checkpoints: the agent
sees the goal plus the full accumulated transcript and executes subtasks in
topological order. Deliberate fidelity simplifications (documented in the
README):

- tokens are modeled: input = len(goal + transcript + spec) // 4, output =
  the script's ``output_tokens``;
- tool calls are the script's artifact writes, performed directly on the
  filesystem (no tool runtime, no idempotency log);
- verification reuses the real verifier registry (file_exists / command),
  so ``同一 verification`` holds;
- failure/retry semantics (``fail_times``, ``max_attempts``, per-attempt
  overrides) match the FakeWorker;
- environment events fire but have no graph effect — the transcript cannot
  invalidate completed work (that is the baseline weakness being measured);
- a simulated crash discards all verified progress and restarts the task
  from scratch (no persistence — the point of the baseline); each (node,
  crash point) fires exactly once, so restarts are bounded;
- waiting nodes are resolved immediately by the scripted environment.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import networkx as nx

from lhos.benchmarks.controlled.task_schema import ControlledTask
from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationSpec
from lhos.ports.verifier import VerificationContext
from lhos.verification.registry import build_default_registry


class TranscriptResult:
    def __init__(self) -> None:
        self.success = False
        self.executions: list[dict[str, Any]] = []
        self.final_states: dict[str, str] = {}  # temp_id -> verified | failed | blocked
        self.curve: list[dict[str, Any]] = []
        self.restarts = 0
        self.discarded_tokens = 0  # work discarded by crash restarts
        self.remaining_cost_at_failure = 0.0
        self.env_events_fired: list[dict[str, Any]] = []
        self.wall_time_seconds = 0.0
        self.simulated_time_seconds = 0.0
        self.verified_progress = 0.0
        self.progress_ratio = 0.0


def _topo_order(task: ControlledTask) -> list[str]:
    dag = nx.DiGraph()
    schedulable = {n["temp_id"] for n in task.spec.oracle_nodes if n.get("schedulable")}
    dag.add_nodes_from(schedulable)
    for e in task.spec.oracle_edges:
        if e.get("kind", "depends_on") != "depends_on":
            continue
        if e["source"] in schedulable and e["target"] in schedulable:
            # source depends on target: execution order target -> source.
            dag.add_edge(e["target"], e["source"])
    return list(nx.topological_sort(dag))


def run_transcript(task: ControlledTask, workspace_dir: str | Path) -> TranscriptResult:
    workspace = Path(workspace_dir)
    workspace.mkdir(parents=True, exist_ok=True)
    registry = build_default_registry()
    result = TranscriptResult()
    nodes = {n["temp_id"]: n for n in task.spec.oracle_nodes if n.get("schedulable")}
    total_weight = task.spec.total_progress_weight or 1.0

    crashed_once: set[tuple[str, str]] = set()
    transcript_lines: list[str] = []
    cum_tokens = 0
    cum_tool_calls = 0
    cum_seconds = 0.0
    verified_weight = 0.0
    token_mark = 0  # cum_tokens at the previous restart

    started = time.perf_counter()
    order = _topo_order(task)
    index = 0
    while index < len(order):
        temp_id = order[index]
        node = nodes[temp_id]
        script = dict(node.get("metadata", {}).get("script", {}))
        max_attempts = int(node.get("max_attempts", 3))
        est_seconds = float(node.get("estimated_time_ms") or 1000) / 1000.0
        attempts: dict[str, dict[str, Any]] = script.get("attempts", {})
        fail_times = int(script.get("fail_times", 0))
        node_done = False
        restarted = False

        def do_restart(temp: str, crash_point: str) -> None:
            nonlocal verified_weight, token_mark, index, restarted
            crashed_once.add((temp, crash_point))
            result.restarts += 1
            result.discarded_tokens += cum_tokens - token_mark
            token_mark = cum_tokens
            result.remaining_cost_at_failure = float(
                sum(
                    float(n.get("estimated_token_cost") or 1000)
                    for t, n in nodes.items()
                    if result.final_states.get(t) != "verified"
                )
            )
            transcript_lines.append(f"[crash at {temp} ({crash_point}); restart from scratch]")
            result.curve.append(
                {
                    "seconds": round(cum_seconds, 6),
                    "tokens": cum_tokens,
                    "tool_calls": cum_tool_calls,
                    "cost_usd": 0.0,
                    "verified_progress": 0.0,  # all progress discarded
                }
            )
            result.final_states.clear()
            verified_weight = 0.0
            restarted = True
            index = 0

        for attempt in range(1, max_attempts + 1):
            # --- simulated crashes: discard everything, restart from scratch
            crash_point = None
            if script.get("crash_on_attempt") == attempt:
                crash_point = "worker_execute"
            elif script.get("crash_before_execution") and attempt == 1:
                crash_point = "before_execution"
            elif script.get("crash_before_verification") and attempt == 1:
                crash_point = "before_verification"
            if crash_point and (temp_id, crash_point) not in crashed_once:
                do_restart(temp_id, crash_point)
                break

            merged = dict(script)
            if str(attempt) in attempts:
                merged.update(attempts[str(attempt)])
            status = merged.get("status")
            if status is None:
                status = "failed" if attempt <= fail_times else "claimed_done"

            context = (
                task.spec.goal + "\n" + "\n".join(transcript_lines) + "\n" + node.get("specification", "")
            )
            input_tokens = max(1, len(context) // 4)
            output_tokens = int(merged.get("output_tokens", 50))
            tool_calls = 0

            if status == "waiting":
                # The scripted environment resolves immediately (no graph).
                transcript_lines.append(
                    f"[{temp_id}] waited for an external event; environment resolved it"
                )
                continue

            if status != "failed":
                for artifact in merged.get("produced_artifacts", []):
                    if "path" in artifact and "content" in artifact:
                        (workspace / artifact["path"]).parent.mkdir(parents=True, exist_ok=True)
                        (workspace / artifact["path"]).write_text(artifact["content"], encoding="utf-8")
                        tool_calls += 1

            cum_tokens += input_tokens + output_tokens
            cum_tool_calls += tool_calls
            cum_seconds += est_seconds
            result.executions.append(
                {
                    "node_id": temp_id,
                    "attempt": attempt,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "tokens": input_tokens + output_tokens,
                    "tool_calls": tool_calls,
                    "seconds": est_seconds,
                    "status": status,
                }
            )

            crash_after = merged.get("crash_after_tool_calls")
            if crash_after and tool_calls >= int(crash_after):
                crash_point = "after_tool_before_event"
                if (temp_id, crash_point) not in crashed_once:
                    do_restart(temp_id, crash_point)
                    break

            for raw in merged.get("environment_events", []):
                if attempt == 1 or merged.get("environment_events_every_attempt"):
                    result.env_events_fired.append(dict(raw))
                    transcript_lines.append(
                        f"[environment event: {raw.get('type')}] (no graph to reconcile)"
                    )

            if status == "failed":
                transcript_lines.append(f"[{temp_id}] attempt {attempt} failed")
                continue

            # --- verification with the real registry
            verified = True
            raw_spec = merged.get("verification_request") or node.get("verification_spec")
            if raw_spec:
                spec = VerificationSpec.from_raw(raw_spec)
                vnode = GraphNode(
                    id=temp_id,
                    run_id="transcript",
                    kind="subtask",
                    title=node.get("title", temp_id),
                    specification=node.get("specification", ""),
                )
                context_obj = VerificationContext(
                    run_id="transcript",
                    workspace_dir=str(workspace),
                    worker_result={},
                    baseline_hashes={},
                )
                verified = registry.get(spec.verifier_type).verify(vnode, spec, context_obj).passed

            if verified:
                result.final_states[temp_id] = "verified"
                weight = float(node.get("progress_weight", 1.0))
                verified_weight += weight
                progress = verified_weight / total_weight
                result.curve.append(
                    {
                        "seconds": round(cum_seconds, 6),
                        "tokens": cum_tokens,
                        "tool_calls": cum_tool_calls,
                        "cost_usd": 0.0,
                        "verified_progress": round(progress, 6),
                    }
                )
                transcript_lines.append(f"[{temp_id}] verified: {node.get('title', temp_id)}")
                node_done = True
                break
            transcript_lines.append(f"[{temp_id}] verification failed")

        if restarted:
            continue
        if not node_done and result.final_states.get(temp_id) != "verified":
            # Attempts exhausted: permanent failure ends the transcript run
            # (there is no repair mechanism to route around it).
            result.final_states[temp_id] = "failed"
            for remaining in order[index + 1:]:
                result.final_states.setdefault(remaining, "blocked")
            break
        index += 1

    result.wall_time_seconds = round(time.perf_counter() - started, 6)
    result.simulated_time_seconds = round(cum_seconds, 6)
    result.verified_progress = round(verified_weight, 6)
    result.progress_ratio = round(verified_weight / total_weight, 6)
    result.success = bool(order) and all(
        result.final_states.get(t) == "verified" for t in order
    )
    return result
