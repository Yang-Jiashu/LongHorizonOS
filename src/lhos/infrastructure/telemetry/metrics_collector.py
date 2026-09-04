"""Run metrics collector: feeds `lhos inspect` (spec sections 20, 24)."""

from __future__ import annotations

from typing import Any

from lhos.domain.enums import NodeKind, NodeState
from lhos.domain.events import EventType


class MetricsCollector:
    def __init__(self, graph_store, event_store):
        self._store = graph_store
        self._events = event_store

    def collect(self, run_id: str) -> dict[str, Any]:
        run = self._store.get_run(run_id)
        nodes = self._store.list_nodes(run_id)
        schedulable = [n for n in nodes if n.kind == NodeKind.SUBTASK and n.schedulable]
        total_weight = sum(n.progress_weight for n in schedulable) or 1.0
        verified_weight = sum(
            n.progress_weight for n in schedulable if n.state == NodeState.VERIFIED
        )
        executions = self._store.list_executions(run_id)
        input_tokens = sum(e.input_tokens for e in executions)
        output_tokens = sum(e.output_tokens for e in executions)
        tool_calls = self._events.count_events(run_id, EventType.TOOL_CALL_COMPLETED)
        wall_seconds = (run.updated_at - run.created_at).total_seconds()
        graph_events = sum(
            self._events.count_events(run_id, t)
            for t in (
                EventType.NODE_ADDED,
                EventType.NODE_UPDATED,
                EventType.NODE_STATE_CHANGED,
                EventType.EDGE_ADDED,
                EventType.EDGE_REMOVED,
                EventType.NODE_MARKED_STALE,
                EventType.NODE_INVALIDATED,
            )
        )
        return {
            "run_id": run_id,
            "goal": run.goal,
            "status": run.status,
            "verified_progress": round(verified_weight, 4),
            "total_progress": round(total_weight, 4),
            "progress_ratio": round(verified_weight / total_weight, 4),
            "ready_nodes": [n.title for n in schedulable if n.state == NodeState.READY],
            "running_nodes": [n.title for n in schedulable if n.state == NodeState.RUNNING],
            "waiting_nodes": [n.title for n in schedulable if n.state == NodeState.WAITING],
            "failed_nodes": [n.title for n in schedulable if n.state == NodeState.FAILED],
            "verified_nodes": [n.title for n in schedulable if n.state == NodeState.VERIFIED],
            "invalidated_nodes": [n.title for n in schedulable if n.state == NodeState.INVALIDATED],
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "model_calls": len(executions),
            "tool_calls": tool_calls,
            "wall_time_seconds": round(wall_seconds, 3),
            "graph_maintenance_events": graph_events,
            "total_events": self._events.count_events(run_id),
        }
