"""Crash recovery (spec section 16.3).

On restart:
- RUNNING nodes (worker died mid-execution) become FAILED so readiness can
  re-queue them; with ``restore_on_crash`` the environment is rolled back to
  the node's ``checkpoint_before`` and CHECKPOINT_RESTORED is recorded;
- CLAIMED_DONE nodes whose claim was never verified (crash before
  verification) become FAILED — unconfirmed nodes go back to FAILED/READY
  (spec 16.3);
- INVALIDATED nodes are re-planned to PENDING;
- dangling leases are released;
- tool calls REQUESTED but never COMPLETED are reported (the idempotency-key
  replay in ToolRuntime handles them on re-execution);
- verified nodes are never re-executed.
"""

from __future__ import annotations

from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType, EventType, RuntimeEvent


class RecoveryManager:
    def __init__(
        self,
        graph_store,  # noqa: ANN001 - SqliteGraphStore
        event_store,  # noqa: ANN001 - SqliteEventStore
        checkpoint_manager=None,  # noqa: ANN001 - optional CheckpointManager
        restore_on_crash: bool = False,
    ):
        self._store = graph_store
        self._events = event_store
        self._checkpoints = checkpoint_manager
        self._restore_on_crash = restore_on_crash

    def recover(self, run_id: str) -> dict[str, int | list[str]]:
        recovered_running: list[str] = []
        recovered_claimed: list[str] = []
        replanned_invalidated: list[str] = []
        released_leases: list[str] = []
        restored_checkpoints: list[str] = []

        for node in self._store.list_nodes(run_id):
            if node.state == NodeState.RUNNING:
                if self._restore_on_crash and self._checkpoints is not None:
                    restored = self._restore_node_checkpoint(run_id, node.id)
                    if restored:
                        restored_checkpoints.append(restored)
                # The attempt was already counted when EXECUTION_STARTED fired.
                self._store.set_state(
                    node.id,
                    NodeState.FAILED,
                    actor=ActorType.SYSTEM,
                    event_type=EventType.EXECUTION_FAILED,
                    payload_extra={"reason": "recovery: worker died mid-execution"},
                )
                recovered_running.append(node.id)
                if node.lease_owner is not None:
                    self._store.release_lease(node.id)
                    released_leases.append(node.id)
            elif node.state == NodeState.CLAIMED_DONE:
                # Crash between claim and verification: the claim was never
                # confirmed, so the node goes back (spec 16.3).
                self._store.set_state(
                    node.id,
                    NodeState.FAILED,
                    actor=ActorType.SYSTEM,
                    event_type=EventType.EXECUTION_FAILED,
                    payload_extra={"reason": "recovery: claim not verified before crash"},
                )
                recovered_claimed.append(node.id)
            elif node.state == NodeState.INVALIDATED:
                # Crash between INVALIDATED and its local replan.
                self._store.set_state(
                    node.id,
                    NodeState.PENDING,
                    actor=ActorType.SYSTEM,
                    payload_extra={"reason": "recovery: replan invalidated node"},
                )
                replanned_invalidated.append(node.id)
            elif node.lease_owner is not None:
                self._store.release_lease(node.id)
                released_leases.append(node.id)

        stale_executions = 0
        for record in self._store.list_executions(run_id):
            if record.status == "running":
                self._store.finish_execution(
                    record.id,
                    status="failed",
                    error={"reason": "recovery: execution interrupted"},
                )
                stale_executions += 1

        requested_keys: set[str] = set()
        terminal_keys: set[str] = set()
        for event in self._events.list_events(run_id):
            if event.event_type == EventType.TOOL_CALL_REQUESTED and event.idempotency_key:
                requested_keys.add(event.idempotency_key)
            elif event.event_type in {
                EventType.TOOL_CALL_COMPLETED,
                EventType.TOOL_CALL_FAILED,
            }:
                if event.idempotency_key:
                    terminal_keys.add(event.idempotency_key.rsplit(":", 1)[0])
        incomplete_tool_calls = len(requested_keys - terminal_keys)

        return {
            "recovered_running_nodes": len(recovered_running),
            "recovered_node_ids": recovered_running,
            "recovered_claimed_nodes": len(recovered_claimed),
            "recovered_claimed_node_ids": recovered_claimed,
            "replanned_invalidated_nodes": len(replanned_invalidated),
            "released_leases": len(released_leases),
            "restored_checkpoints": restored_checkpoints,
            "stale_executions_failed": stale_executions,
            "incomplete_tool_calls": incomplete_tool_calls,
        }

    def _restore_node_checkpoint(self, run_id: str, node_id: str) -> str | None:
        executions = self._store.list_executions(run_id, node_id)
        checkpoint_before = next(
            (e.checkpoint_before for e in reversed(executions) if e.checkpoint_before),
            None,
        )
        if checkpoint_before is None:
            return None
        self._checkpoints.restore(checkpoint_before)
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.CHECKPOINT_RESTORED,
                actor_type=ActorType.SYSTEM,
                payload={
                    "checkpoint_id": checkpoint_before,
                    "node_id": node_id,
                    "reason": "recovery: restore_on_crash",
                },
            )
        )
        return checkpoint_before
