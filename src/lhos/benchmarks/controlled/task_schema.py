"""Controlled task schema (spec 22.1) plus the generator-side task wrapper.

``ControlledTaskSpec`` mirrors the spec exactly. ``ControlledTask`` adds the
generator metadata the benchmark harness needs: the oracle answer (true
critical path, true affected scope per environment event), the chosen control
variables, and the preset/size/seed identity.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ControlledTaskSpec(BaseModel):
    """Spec section 22.1."""

    task_id: str
    goal: str
    oracle_nodes: list[dict[str, Any]]
    oracle_edges: list[dict[str, Any]]
    tool_costs: dict[str, dict[str, float]]
    failure_injections: list[dict[str, Any]]
    environment_events: list[dict[str, Any]]
    total_progress_weight: float


class OracleInfo(BaseModel):
    """The generator's ground truth, hidden from non-oracle modes.

    - ``critical_path``: temp_ids of the time-longest DEPENDS_ON chain.
    - ``critical_path_seconds``: sum of estimated times on that chain.
    - ``affected_by_event``: changed-node temp_id -> true affected scope
      (must-invalidate victims + their transitive dependents), used as the
      denominator of Replanning Amplification (spec 24.3).
    - ``priorities``: temp_id -> oracle criticality in [0, 1]; only oracle
      modes may see it (as node ``priority``).
    """

    critical_path: list[str] = Field(default_factory=list)
    critical_path_seconds: float = 0.0
    affected_by_event: dict[str, list[str]] = Field(default_factory=dict)
    priorities: dict[str, float] = Field(default_factory=dict)


class ControlledTask(BaseModel):
    spec: ControlledTaskSpec
    oracle: OracleInfo
    preset: str
    size: str
    seed: int
    control_variables: dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------ accessors
    @property
    def task_id(self) -> str:
        return self.spec.task_id

    def graph_spec(self, use_oracle_priorities: bool = False) -> dict[str, Any]:
        """Section 8.1 JSON for the InitialGraphBuilder.

        Node ids stay temp_ids here; the runner rewrites environment-event
        payloads once the run_id (and therefore real node ids) is known.
        Non-oracle modes get ``priority = 0`` so the hint cannot leak.
        Returns a deep copy: callers may mutate it freely (id rewrites).
        """
        import copy

        nodes: list[dict[str, Any]] = []
        for raw in self.spec.oracle_nodes:
            node = copy.deepcopy(raw)
            node["priority"] = (
                self.oracle.priorities.get(node["temp_id"], 0.0) if use_oracle_priorities else 0.0
            )
            nodes.append(node)
        return {
            "goal": self.spec.goal,
            "nodes": nodes,
            "edges": copy.deepcopy(self.spec.oracle_edges),
        }
