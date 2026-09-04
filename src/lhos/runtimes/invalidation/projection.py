"""D3 — projection (rebuildable, NOT authority) (§24).

The D3 projection is a DERIVED VIEW, not a source of truth.  It holds the
computed Evidence applicability, current invalidation causes, stale node
set, invalidation proofs, and the current Repair Frontier.  It can be
discarded and rebuilt deterministically from the historical source of
truth (VPG patch/event history + ArtifactVersion truth + operational fact
truth) 3 consecutive times producing byte-identical serialization (§24).

Architecture boundary (§39): D3 never claims a Task and never dispatches an
Agent.  This projection is purely observational.
"""

from __future__ import annotations

import hashlib
import json
import zlib
from collections.abc import Callable
from typing import Any

from .models import (
    D3Event,
    EvidenceApplicability,
    InvalidationCause,
    InvalidationProof,
    RepairFrontier,
)


def _normjson(obj: Any) -> str:
    """Deterministically serialize for byte-identity (sort_keys + stable)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


class D3Projection:
    """Immutable-by-convention derived view, rebuildable deterministically."""

    def __init__(
        self,
        graph_id: str,
        version: int,
        applicability: tuple[EvidenceApplicability, ...] = (),
        causes: tuple[InvalidationCause, ...] = (),
        stale_nodes: tuple[str, ...] = (),
        proofs: tuple[InvalidationProof, ...] = (),
        frontier: RepairFrontier | None = None,
        events: tuple[D3Event, ...] = (),
        reopened_goals: tuple[str, ...] = (),
    ) -> None:
        self.graph_id = graph_id
        self.version = version
        self._applicability = tuple(sorted(applicability, key=lambda a: a.evidence_id))
        self._causes = tuple(sorted(causes, key=lambda c: c.cause_id))
        self._stale_nodes = tuple(sorted(stale_nodes))
        self._proofs = tuple(sorted(proofs, key=lambda p: p.task_id))
        self._frontier = frontier
        self._events = tuple(sorted(events, key=lambda e: e.event_id))
        self._reopened_goals = tuple(sorted(reopened_goals))

    def to_dict(self) -> dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "version": self.version,
            "evidence_applicability": [self._dump(a) for a in self._applicability],
            "causes": [self._dump(c) for c in self._causes],
            "stale_nodes": list(self._stale_nodes),
            "proofs": [self._dump(p) for p in self._proofs],
            "repair_frontier": (_normjson(self._frontier) if self._frontier else None),
            "reopened_goals": list(self._reopened_goals),
            "events": [self._dump(e) for e in self._events],
        }

    def _dump(self, m: Any) -> str:
        return _normjson(m.model_dump() if hasattr(m, "model_dump") else m)

    def serialize(self) -> bytes:
        """Byte-deterministic serialization for replay-diff comparison."""
        return zlib.compress(_normjson(self.to_dict()).encode("utf-8"))

    def identity_hash(self) -> str:
        h = hashlib.sha256(self.serialize()).hexdigest()
        return h


def rebuild_from_history(
    graph_id: str,
    version: int,
    history_provider: Callable[[str, int], dict[str, Any]],
) -> D3Projection:
    """Rebuild a projection from the immutable source of truth.

    history_provider(version) returns a dict with keys:
      applicability, causes, stale_nodes, proofs, frontier, events,
      reopened_goals  — read from the D3 event/history store.
    """
    src = history_provider(graph_id, version)
    frontier = src.get("frontier")
    frontier_model = None
    if frontier is not None:
        from .models import RepairFrontier

        if not isinstance(frontier, RepairFrontier):
            frontier_model = RepairFrontier(**json.loads(frontier))
        else:
            frontier_model = frontier
    return D3Projection(
        graph_id=graph_id,
        version=version,
        applicability=tuple(src.get("applicability", ())),
        causes=tuple(src.get("causes", ())),
        stale_nodes=tuple(src.get("stale_nodes", ())),
        proofs=tuple(src.get("proofs", ())),
        frontier=frontier_model,
        events=tuple(src.get("events", ())),
        reopened_goals=tuple(src.get("reopened_goals", ())),
    )
