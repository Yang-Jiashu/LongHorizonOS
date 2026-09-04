"""D3 — runtime facade: deterministic invalidation + repair frontier.

This is the public entrypoint for D3.  It wires the pure engine to a
read-only view of the VPG via injected providers (the host supplies the
adapters) so D3 never reaches into the Agent OS kernel, services, or
storage internals, nor any D2 internals, directly.

Authority boundary invariants (§3, §39):
  - D3 NEVER writes to VPG task semantic state.
  - D3 NEVER claims a Task (no Kernel Lease).
  - D3 NEVER dispatches an Agent.
  - D3 NEVER mutates Artifact or Evidence history.

The runtime's `invalidate()` returns a full atomic `InvalidationResult`
that the HOST commits atomically (or discards).  `reconcile_patch()` is the
small helper that checks whether a just-seen VPG Patch bumped the version so
the host aborts a stale computation instead of silently merging (§19).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .engine import EngineInputs, build_invalidation_result, run_invalidation_engine
from .models import InvalidationResult


class InvalidGraphVersionRace(Exception):
    """Raised when an invalidation was computed on a stale graph version."""


class InvalidationRuntime:
    """Deterministic version-aware causal invalidation runtime (D3).

    It is stateless w.r.t. ownership — pure derivation from read adapters.
    """

    def __init__(
        self,
        *,
        current_version_of: Callable[[str], int] | None = None,
        task_nodes_of: Callable[[str], dict[str, Any]] | None = None,
        goal_nodes_of: Callable[[str], dict[str, Any]] | None = None,
        evidence_nodes_of: Callable[[str], dict[str, Any]] | None = None,
        edges_of: Callable[[str], list[Any]] | None = None,
        goal_direct_tasks_of: Callable[[str], dict[str, tuple[str, ...]]] | None = None,
        evidence_of_task: Callable[[str], dict[str, tuple[str, ...]]] | None = None,
        verify_binding: Callable[[str, int, str], bool] | None = None,
        action_valid: Callable[[str], bool] | None = None,
        event_valid: Callable[[str], bool] | None = None,
        has_active_claim: Callable[[str], bool] | None = None,
    ) -> None:
        self._cur = current_version_of
        self._tasks = task_nodes_of
        self._goals = goal_nodes_of
        self._evids = evidence_nodes_of
        self._edges = edges_of
        self._goal_deps = goal_direct_tasks_of
        self._ev_of = evidence_of_task
        self._verify_binding = verify_binding
        self._action_valid = action_valid
        self._event_valid = event_valid
        self._has_claim = has_active_claim

    def invalidate(self, graph_id: str, base_graph_version: int) -> InvalidationResult:
        """Compute the atomic invalidation + repair-frontier result.

        The caller MUST only commit if the graph is still at
        base_graph_version (see assert_version_is_current).  This method is
        PURE — it does not write anything.
        """
        tasks = {} if self._tasks is None else self._tasks(graph_id)
        goals = {} if self._goals is None else self._goals(graph_id)
        evids = {} if self._evids is None else self._evids(graph_id)
        edges = [] if self._edges is None else self._edges(graph_id)
        goal_deps = {} if self._goal_deps is None else self._goal_deps(graph_id)
        ev_of = {} if self._ev_of is None else self._ev_of(graph_id)

        inp = EngineInputs(
            graph_id=graph_id,
            current_version=base_graph_version,
            task_nodes=tasks,
            goal_nodes=goals,
            evidence_nodes=evids,
            edges=edges,
            verify_binding=self._verify_binding,
            action_valid=self._action_valid,
            event_valid=self._event_valid,
            has_active_claim=self._has_claim,
            goal_direct_tasks=goal_deps,
            evidence_of=ev_of,
        )
        er = run_invalidation_engine(inp)
        return build_invalidation_result(inp, er)

    def assert_version_is_current(self, graph_id: str, compute_version: int) -> None:
        """Raise if a VPG Patch bumped the version mid-computation (§19)."""
        if self._cur is None:
            return
        now = self._cur(graph_id)
        if now != compute_version:
            raise InvalidGraphVersionRace(
                f"invalidation computed on v{compute_version}, "
                f"graph now at v{now}; MUST recompute — no silent merge"
            )
