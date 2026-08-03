"""Graph reconciler: deterministic rules first, LLM semantic hook last (spec 8.3).

Deterministic environment-event rules (spec 15):
- ARTIFACT_UPDATED: bump the artifact node's version/hash, then propagate.
  ``removed: true`` means the artifact is gone — a must-invalidate for its
  direct consumers (INVALIDATED, not merely STALE).
- CONSTRAINT_CHANGED: bump the constraint node; an explicit
  ``invalidates: [...]`` payload marks those VERIFIED nodes INVALIDATED
  (must-invalidate), otherwise consumers degrade to STALE.
- FACT_OBSERVED with ``invalidates_node_id`` propagates from that node.
"""

from __future__ import annotations

from typing import Protocol

from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.graph.invalidation import propagate_invalidation


class SemanticReconciler(Protocol):
    def reconcile(self, run_id: str, event: RuntimeEvent) -> bool: ...


class DeterministicReconciler:
    def __init__(
        self,
        graph_store,
        semantic: SemanticReconciler | None = None,
        deterministic_first: bool = True,
        local_repair: bool = True,
    ):
        self._store = graph_store
        self._semantic = semantic
        self._deterministic_first = deterministic_first
        # Benchmark ablation (spec 25): with local repair disabled, INVALIDATED
        # nodes are not replanned back to PENDING.
        self._local_repair = local_repair

    def project_new_events(self, run_id: str) -> int:
        """Projection is transactional (spec 5.3); nothing to reproject."""
        return 0

    def reconcile_event(self, run_id: str, event: RuntimeEvent) -> bool:
        """Apply a deterministic rule; fall back to the semantic hook.

        Returns True when the event was handled deterministically.
        """
        payload = event.payload
        if event.event_type == EventType.ARTIFACT_UPDATED:
            node_id = payload.get("node_id")
            if not node_id:
                return self._fallback(run_id, event)
            self._bump_artifact(node_id, payload)
            propagate_invalidation(
                self._store,
                run_id,
                node_id,
                invalidate_consumers=bool(payload.get("removed")),
                trigger={
                    "event_type": event.event_type,
                    "reason": payload.get("reason", "artifact updated"),
                },
                local_repair=self._local_repair,
            )
            return True

        if event.event_type == EventType.CONSTRAINT_CHANGED:
            node_id = payload.get("node_id")
            if not node_id:
                return self._fallback(run_id, event)
            self._bump_node(node_id)
            invalidates = payload.get("invalidates") or []
            propagate_invalidation(
                self._store,
                run_id,
                node_id,
                must_invalidate_ids=set(invalidates),
                trigger={
                    "event_type": event.event_type,
                    "reason": payload.get("reason", "constraint changed"),
                },
                local_repair=self._local_repair,
            )
            return True

        if event.event_type == EventType.FACT_OBSERVED:
            invalidates = payload.get("invalidates_node_id")
            if invalidates:
                propagate_invalidation(
                    self._store, run_id, invalidates, local_repair=self._local_repair
                )
                return True
        return self._fallback(run_id, event)

    # ---------------------------------------------------------------- helpers
    def _bump_artifact(self, node_id: str, payload: dict) -> None:
        node = self._store.get_node(node_id)
        if "new_hash" in payload:
            node.metadata["content_hash"] = payload["new_hash"]
        if payload.get("removed"):
            node.metadata["removed"] = True
        node.metadata["artifact_version"] = int(node.metadata.get("artifact_version", 0)) + 1
        self._store.update_node(node, actor=ActorType.RECONCILER)

    def _bump_node(self, node_id: str) -> None:
        node = self._store.get_node(node_id)
        self._store.update_node(node, actor=ActorType.RECONCILER)

    def _fallback(self, run_id: str, event: RuntimeEvent) -> bool:
        if self._semantic is not None:
            # Semantic reconciler is the last resort (spec 8.3).
            return self._semantic.reconcile(run_id, event)
        return False
