"""Scripted benchmark environment (spec 22).

The environment is the external world the runtime cannot control: it fires
the task's scripted environment events (embedded in injector-node scripts by
the generator) and resolves nodes that are WAITING for an external event
(spec 22.3 scenario 12). Resolution is deterministic: every node currently
WAITING is resolved on the next check, exactly once per WAITING episode.
"""

from __future__ import annotations

from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType


class ScriptedEnvironment:
    def __init__(self, graph_store, run_id: str):
        self._store = graph_store
        self._run_id = run_id

    def resolve_waiting(self) -> int:
        """Move every WAITING node back to READY (external event arrived).

        Returns the number of nodes resolved, so the drive loop knows whether
        a paused run can continue.
        """
        graph = self._store.load_graph(self._run_id)
        resolved = 0
        for node in graph.nodes.values():
            if node.state != NodeState.WAITING:
                continue
            self._store.set_state(
                node.id,
                NodeState.READY,
                actor=ActorType.SYSTEM,
                payload_extra={"reason": "benchmark environment: external event arrived"},
            )
            resolved += 1
        return resolved
