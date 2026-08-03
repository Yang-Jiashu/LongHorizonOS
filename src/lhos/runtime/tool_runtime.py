"""Tool runtime: persistence-first tool execution (spec section 13).

- Every call appends TOOL_CALL_REQUESTED before and TOOL_CALL_COMPLETED /
  TOOL_CALL_FAILED after (13.3).
- Side-effecting calls require an idempotency key (invariant 7); a completed
  call with the same key is replayed from the log instead of re-executed,
  which is what makes post-crash recovery safe (13.3, 16.3).
- MVP side-effect policy: only read_only / local_write tools run (13.2).

Idempotency key convention: the REQUESTED event carries the caller's key; the
terminal event carries ``<key>:completed`` or ``<key>:failed`` (the events
table enforces UNIQUE(run_id, idempotency_key)).
"""

from __future__ import annotations

from lhos.domain.enums import ALLOWED_SIDE_EFFECT_LEVELS, SIDE_EFFECT_READ_ONLY
from lhos.domain.errors import (
    IdempotencyKeyRequiredError,
    SimulatedCrashError,
    ToolExecutionError,
    ToolNotAllowedError,
)
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.ports.tools import ToolRequest, ToolResult


class ToolRuntime:
    def __init__(
        self,
        event_store,
        registry,
        workspace_dir: str,
        budget_manager=None,
    ):
        self._events = event_store
        self._registry = registry
        self._workspace_dir = workspace_dir
        self._budget = budget_manager

    def execute(self, run_id: str, node_id: str, request: ToolRequest) -> ToolResult:
        meta = self._registry.check_allowed(request.tool_name)
        if meta.side_effect_level not in ALLOWED_SIDE_EFFECT_LEVELS:
            raise ToolNotAllowedError(request.tool_name)

        # Idempotent replay (spec 13.3): any keyed call that already completed
        # returns the recorded result instead of re-executing. Side-effecting
        # tools additionally REQUIRE a key (invariant 7).
        key = request.idempotency_key or None
        if key:
            # Rollback generation (spec 13.3/16.3): a checkpoint restore rolls
            # the environment back, so completions recorded BEFORE the restore
            # no longer describe the world. Mixing the restore count
            # (event-sourced, deterministic) into the key makes post-restore
            # attempts re-execute while ordinary post-crash retries replay.
            generation = self._events.count_events(run_id, EventType.CHECKPOINT_RESTORED)
            if generation:
                key = f"{key}:gen{generation}"
            completed = self._events.find_by_idempotency(run_id, f"{key}:completed")
            if completed is not None:
                return ToolResult(**completed.payload["result"])
        elif meta.side_effect_level != SIDE_EFFECT_READ_ONLY:
            raise IdempotencyKeyRequiredError(
                f"side-effecting tool {request.tool_name!r} requires an idempotency key"
            )

        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.TOOL_CALL_REQUESTED,
                actor_type=ActorType.WORKER,
                actor_id=node_id,
                idempotency_key=key,
                payload={
                    "node_id": node_id,
                    "tool_name": request.tool_name,
                    "arguments": request.arguments,
                },
            )
        )
        tool = self._registry.get(request.tool_name)
        try:
            result = tool.execute(request, self._workspace_dir)
        except SimulatedCrashError:
            # Process death mid-tool (26.2): NO terminal event is written, so
            # recovery can detect the incomplete call via the idempotency key.
            raise
        except Exception as exc:
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.TOOL_CALL_FAILED,
                    actor_type=ActorType.WORKER,
                    actor_id=node_id,
                    idempotency_key=f"{key}:failed" if key else None,
                    payload={
                        "node_id": node_id,
                        "tool_name": request.tool_name,
                        "error": str(exc),
                    },
                )
            )
            if isinstance(exc, ToolExecutionError):
                raise
            raise ToolExecutionError(str(exc)) from exc

        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.TOOL_CALL_COMPLETED,
                actor_type=ActorType.WORKER,
                actor_id=node_id,
                idempotency_key=f"{key}:completed" if key else None,
                payload={
                    "node_id": node_id,
                    "tool_name": request.tool_name,
                    "result": result.model_dump(mode="json"),
                },
            )
        )
        if self._budget is not None:
            self._budget.record_tool_call(run_id)
        return result
