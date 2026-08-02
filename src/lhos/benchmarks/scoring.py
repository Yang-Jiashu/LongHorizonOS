"""Per-run scoring (spec 23, 24): assemble one result row per (task, mode, seed).

Graph-mode rows are computed from the event log + execution records; the
transcript baseline row is computed from the synthesized TranscriptResult.
Both go through the same metric functions (benchmarks/metrics.py).

Reproducibility contract: every field except ``wall_time_seconds``,
``aupbc_time``, ``scheduler_time_seconds`` and ``checkpoint_time_seconds`` is
a deterministic function of (task, mode, seed).
"""

from __future__ import annotations

from typing import Any

from lhos.benchmarks import metrics
from lhos.benchmarks.controlled.task_schema import ControlledTask
from lhos.benchmarks.transcript import TranscriptResult
from lhos.domain.enums import NodeState
from lhos.domain.events import EventType

_STATE_SUFFIXES = {"verified", "stale", "invalidated"}


def _temp_id(real_id: str) -> str:
    """Node ids are ``{run_id}:{temp_id}``; run ids never contain ':'."""
    return real_id.split(":", 1)[1] if ":" in real_id else real_id


def _state_name(raw: str) -> str:
    return str(raw).split(".")[-1].lower()


# ---------------------------------------------------------------- graph runs
def progress_budget_curve(events: list[Any], weight_by_node: dict[str, float], total_weight: float) -> list[dict[str, Any]]:
    """One sample per verified-progress change (spec 24.3), x-axes from the
    BUDGET_UPDATED snapshots and event timestamps."""
    curve: list[dict[str, Any]] = []
    tokens = tool_calls = 0
    cost = 0.0
    verified: set[str] = set()
    t0 = events[0].created_at if events else None
    for e in events:
        if e.event_type == EventType.BUDGET_UPDATED:
            budget = e.payload.get("budget", {})
            tokens = int(budget.get("input_tokens", 0)) + int(budget.get("output_tokens", 0))
            tool_calls = int(budget.get("tool_calls", 0))
            cost = float(budget.get("cost_usd", 0.0))
            continue
        if e.event_type != EventType.NODE_STATE_CHANGED:
            continue
        node_id = e.payload.get("node_id")
        to_state = _state_name(e.payload.get("to_state", ""))
        before = len(verified)
        if to_state == "verified":
            verified.add(node_id)
        elif node_id in verified and to_state in _STATE_SUFFIXES:
            verified.discard(node_id)
        if len(verified) == before:
            continue
        progress = sum(weight_by_node.get(n, 0.0) for n in verified) / (total_weight or 1.0)
        seconds = (e.created_at - t0).total_seconds() if t0 else 0.0
        curve.append(
            {
                "seconds": round(seconds, 6),
                "tokens": tokens,
                "tool_calls": tool_calls,
                "cost_usd": cost,
                "verified_progress": round(progress, 6),
            }
        )
    return curve


def score_graph_run(
    stack: Any,
    run_id: str,
    task: ControlledTask,
    mode_name: str,
    wall_seconds: float,
    crashes: int,
) -> dict[str, Any]:
    events = stack.event_store.list_events(run_id)
    executions_raw = stack.graph_store.list_executions(run_id)
    nodes = stack.graph_store.list_nodes(run_id)
    run = stack.graph_store.get_run(run_id)

    schedulable = [n for n in nodes if n.schedulable]
    weight_by_node = {n.id: float(n.progress_weight or 1.0) for n in schedulable}
    total_weight = task.spec.total_progress_weight or 1.0

    executions = [
        {
            "node_id": ex.node_id,
            "attempt": int(ex.attempt_number),
            "input_tokens": int(ex.input_tokens or 0),
            "output_tokens": int(ex.output_tokens or 0),
            "tokens": int(ex.input_tokens or 0) + int(ex.output_tokens or 0),
            "tool_calls": int(ex.tool_calls or 0),
        }
        for ex in executions_raw
    ]
    input_tokens = sum(e["input_tokens"] for e in executions)
    output_tokens = sum(e["output_tokens"] for e in executions)
    tool_calls = sum(e["tool_calls"] for e in executions)

    # Simulated execution clock: FakeWorker runs instantly, so per-attempt
    # estimated times stand in for execution latency (documented).
    est_ms = {n.id: float(n.estimated_time_ms or 1000) for n in nodes}
    simulated_seconds = sum(est_ms.get(e["node_id"], 1000.0) / 1000.0 for e in executions)

    final_verified = {n.id for n in schedulable if n.state == NodeState.VERIFIED}
    failed_nodes = sum(1 for n in schedulable if n.state == NodeState.FAILED)
    invalidated_nodes = sum(1 for n in schedulable if n.state == NodeState.INVALIDATED)
    verified_progress = sum(weight_by_node[n] for n in final_verified)

    curve = progress_budget_curve(events, weight_by_node, total_weight)

    # Instrumentation snapshot from the terminal run event payload.
    scheduler_seconds = checkpoint_seconds = 0.0
    for e in reversed(events):
        if e.event_type in {EventType.RUN_COMPLETED, EventType.RUN_FAILED, EventType.RUN_PAUSED}:
            scheduler_seconds = float(e.payload.get("scheduler_time_seconds", 0.0))
            checkpoint_seconds = float(e.payload.get("checkpoint_time_seconds", 0.0))
            break

    # Replanning amplification: nodes actually re-planned/re-executed because
    # of invalidation (stale re-queues + invalidated replans, observed via
    # EXECUTION_STARTED retry_reason) vs the oracle true affected scope.
    from lhos.graph.invalidation import invalidation_metrics

    re_executed = 0
    replanned = 0
    maintenance_events = 0
    for report in invalidation_metrics(stack.event_store, run_id):
        re_executed += int(report.get("re_executed_count", 0))
        replanned += int(report.get("replanned_count", 0))
    for e in events:
        if e.event_type == EventType.INVALIDATION_PROPAGATED:
            maintenance_events += 1
        elif e.event_type in {EventType.ARTIFACT_UPDATED, EventType.CONSTRAINT_CHANGED, EventType.FACT_OBSERVED}:
            maintenance_events += 1
    oracle_affected = 0
    for e in events:
        if e.event_type not in {EventType.CONSTRAINT_CHANGED, EventType.ARTIFACT_UPDATED}:
            continue
        if e.event_type == EventType.ARTIFACT_UPDATED and e.payload.get("old_hash") is None:
            continue  # initial version recording, not a modification
        changed = e.payload.get("node_id")
        if not changed:
            continue
        oracle_affected += len(task.oracle.affected_by_event.get(_temp_id(changed), []))

    # Recovery overhead: re-executed attempts after a crash / remaining
    # estimated cost when the (first) crash fired. Crash markers: controller
    # CRASH_INJECTED events (runtime crash flags) and RUN_RESUMED (every
    # crash is followed by a resume, which also covers worker/tool crashes
    # that leave no CRASH_INJECTED marker).
    repeated_cost = 0.0
    remaining_cost = 0.0
    crash_markers = [
        e for e in events if e.event_type in {"CRASH_INJECTED", EventType.RUN_RESUMED}
    ]
    if crash_markers:
        crash_time = min(e.created_at for e in crash_markers)
        failed_before: set[str] = set()
        for ex in executions_raw:
            if ex.status == "failed" and ex.started_at <= crash_time:
                failed_before.add(ex.node_id)
        for ex in executions_raw:
            if (
                ex.node_id in failed_before
                and ex.started_at >= crash_time
                and int(ex.attempt_number) > 1
            ):
                repeated_cost += int(ex.input_tokens or 0) + int(ex.output_tokens or 0)
        crash_seq = min(e.sequence for e in crash_markers)
        verified_at_crash = set()
        for e in events:
            if e.sequence > crash_seq:
                break
            if e.event_type == EventType.NODE_STATE_CHANGED and _state_name(e.payload.get("to_state", "")) == "verified":
                verified_at_crash.add(e.payload.get("node_id"))
        remaining_cost = float(
            sum(
                float(n.estimated_token_cost or 1000)
                for n in schedulable
                if n.id not in verified_at_crash
            )
        )

    row = {
        "task_id": task.task_id,
        "preset": task.preset,
        "size": task.size,
        "seed": task.seed,
        "mode": mode_name,
        "success": run.status == "completed",
        "run_status": run.status,
        "verified_progress": round(verified_progress, 6),
        "progress_ratio": round(verified_progress / total_weight, 6),
        "failed_nodes": failed_nodes,
        "invalidated_nodes": invalidated_nodes,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "model_calls": len(executions),
        "tool_calls": tool_calls,
        "wall_time_seconds": round(wall_seconds, 6),
        "simulated_time_seconds": round(simulated_seconds, 6),
        "model_cost_usd": 0.0,
        "graph_maintenance_tokens": 0,
        "verification_tokens": 0,
        "graph_maintenance_events": maintenance_events,
        "scheduler_time_seconds": round(scheduler_seconds, 6),
        "checkpoint_time_seconds": round(checkpoint_seconds, 6),
        "aupbc_tokens": metrics.aupbc(curve, "tokens"),
        "aupbc_time": metrics.aupbc(curve, "seconds"),
        "aupbc_tool_calls": metrics.aupbc(curve, "tool_calls"),
        "useful_work_ratio": metrics.useful_work_ratio(executions, final_verified),
        "replanning_amplification": metrics.replanning_amplification(re_executed, oracle_affected),
        "invalidated_work_rate": metrics.invalidated_work_rate(executions),
        "recovery_overhead": metrics.recovery_overhead(repeated_cost, remaining_cost),
        "critical_path_stretch": metrics.critical_path_stretch(
            simulated_seconds, task.oracle.critical_path_seconds
        ),
        "oracle_critical_path_seconds": task.oracle.critical_path_seconds,
        "crashes": crashes,
        "restarts": 0,
        "replanned_nodes": replanned,
        "re_executed_nodes": re_executed,
        "oracle_affected_nodes": oracle_affected,
    }
    return row


# ------------------------------------------------------------ transcript run
def score_transcript_run(result: TranscriptResult, task: ControlledTask) -> dict[str, Any]:
    executions = result.executions
    input_tokens = sum(e["input_tokens"] for e in executions)
    output_tokens = sum(e["output_tokens"] for e in executions)
    total_tokens = input_tokens + output_tokens
    tool_calls = sum(e["tool_calls"] for e in executions)
    final_verified = {t for t, s in result.final_states.items() if s == "verified"}
    failed_nodes = sum(1 for s in result.final_states.values() if s == "failed")

    oracle_affected = 0
    for raw in result.env_events_fired:
        changed = raw.get("node_id")
        if changed:
            oracle_affected += len(task.oracle.affected_by_event.get(changed, []))

    # The transcript discards work on crash restarts; count that as
    # invalidated/superseded work on top of ordinary retries.
    base_rate = metrics.invalidated_work_rate(executions)
    discarded_rate = (result.discarded_tokens / total_tokens) if total_tokens else 0.0
    invalidated_rate = round(min(1.0, base_rate + discarded_rate), 6)

    return {
        "task_id": task.task_id,
        "preset": task.preset,
        "size": task.size,
        "seed": task.seed,
        "mode": "transcript",
        "success": result.success,
        "run_status": "completed" if result.success else "failed",
        "verified_progress": result.verified_progress,
        "progress_ratio": result.progress_ratio,
        "failed_nodes": failed_nodes,
        "invalidated_nodes": 0,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "model_calls": len(executions),
        "tool_calls": tool_calls,
        "wall_time_seconds": result.wall_time_seconds,
        "simulated_time_seconds": result.simulated_time_seconds,
        "model_cost_usd": 0.0,
        "graph_maintenance_tokens": 0,
        "verification_tokens": 0,
        "graph_maintenance_events": 0,
        "scheduler_time_seconds": 0.0,
        "checkpoint_time_seconds": 0.0,
        "aupbc_tokens": metrics.aupbc(result.curve, "tokens"),
        "aupbc_time": metrics.aupbc(result.curve, "seconds"),
        "aupbc_tool_calls": metrics.aupbc(result.curve, "tool_calls"),
        "useful_work_ratio": metrics.useful_work_ratio(executions, final_verified),
        "replanning_amplification": metrics.replanning_amplification(0, oracle_affected),
        "invalidated_work_rate": invalidated_rate,
        "recovery_overhead": metrics.recovery_overhead(
            float(result.discarded_tokens), result.remaining_cost_at_failure
        ),
        "critical_path_stretch": metrics.critical_path_stretch(
            result.simulated_time_seconds, task.oracle.critical_path_seconds
        ),
        "oracle_critical_path_seconds": task.oracle.critical_path_seconds,
        "crashes": 0,
        "restarts": result.restarts,
        "replanned_nodes": 0,
        "re_executed_nodes": 0,
        "oracle_affected_nodes": oracle_affected,
    }
