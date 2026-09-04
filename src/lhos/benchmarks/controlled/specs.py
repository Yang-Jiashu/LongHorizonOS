"""Public/Hidden spec separation (audit Milestone 1D).

Explicitly separates the public task specification (visible to the Runtime,
Planner, Worker, Reconciler, Scheduler) from the hidden oracle (visible only
to the benchmark environment, external scorer, oracle baseline, and analysis
code).

This module provides:
- ``PublicTaskSpec``: the information a Runtime is allowed to see.
- ``HiddenOracleSpec``: the ground truth only oracle/scoring code may access.
- ``to_public_spec()``: extract the public view from a ``ControlledTask``.
- ``to_hidden_oracle()``: extract the hidden view (raises if called from a
  non-oracle context).

Architecture invariant: the ``lhos.runtime`` package must NOT import this
module's ``HiddenOracleSpec`` or ``to_hidden_oracle``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PublicTaskSpec(BaseModel):
    """The information a Runtime, Planner, Worker, Reconciler, or Scheduler
    is allowed to see.

    This is a strict subset of ``ControlledTaskSpec``: it contains the goal,
    the graph structure (nodes + edges), tool costs, and public constraints,
    but NOT oracle priorities, true affected sets, or critical path info.
    """

    task_id: str
    goal: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    tool_costs: dict[str, dict[str, float]]
    available_tools: list[str] = Field(default_factory=list)
    initial_observations: dict[str, Any] = Field(default_factory=dict)
    public_constraints: list[str] = Field(default_factory=list)
    total_progress_weight: float = 1.0


class HiddenOracleSpec(BaseModel):
    """The generator's ground truth, hidden from non-oracle modes.

    Only the benchmark environment, external scorer, oracle baseline, and
    analysis code may access this. The Runtime must NEVER read it.

    - ``oracle_graph``: the true dependency structure (same as public nodes
      but with true priorities).
    - ``optimal_schedule``: the optimal execution order (critical path).
    - ``failure_events``: the true failure injection plan.
    - ``grading_rules``: hidden test criteria for external grading.
    - ``true_costs``: the actual execution costs per node.
    - ``affected_sets``: the true affected-node sets per environment event.
    """

    critical_path: list[str] = Field(default_factory=list)
    critical_path_seconds: float = 0.0
    affected_by_event: dict[str, list[str]] = Field(default_factory=dict)
    priorities: dict[str, float] = Field(default_factory=dict)
    failure_events: list[dict[str, Any]] = Field(default_factory=list)
    grading_rules: dict[str, Any] = Field(default_factory=dict)
    true_costs: dict[str, dict[str, float]] = Field(default_factory=dict)


def to_public_spec(task: Any) -> PublicTaskSpec:
    """Extract the public view from a ControlledTask.

    Strips oracle priorities (sets them to 0.0) and removes any hidden
    information. The returned spec is safe to pass to the Runtime.
    """
    spec = task.spec
    nodes: list[dict[str, Any]] = []
    for raw in spec.oracle_nodes:
        node = dict(raw)
        node["priority"] = 0.0
        nodes.append(node)
    return PublicTaskSpec(
        task_id=spec.task_id,
        goal=spec.goal,
        nodes=nodes,
        edges=list(spec.oracle_edges),
        tool_costs=dict(spec.tool_costs),
        available_tools=list(spec.tool_costs.keys()),
        total_progress_weight=spec.total_progress_weight,
    )


def to_hidden_oracle(task: Any) -> HiddenOracleSpec:
    """Extract the hidden oracle from a ControlledTask.

    This function should ONLY be called from:
    - benchmark environment setup
    - external scorer
    - oracle baseline
    - analysis/scoring code

    Calling this from the Runtime is an architecture violation.
    """
    oracle = task.oracle
    return HiddenOracleSpec(
        critical_path=list(oracle.critical_path),
        critical_path_seconds=oracle.critical_path_seconds,
        affected_by_event=dict(oracle.affected_by_event),
        priorities=dict(oracle.priorities),
        failure_events=list(spec_failure_injections(task)),
        true_costs=dict(spec_tool_costs(task)),
    )


def spec_failure_injections(task: Any) -> list[dict[str, Any]]:
    return [dict(f) for f in task.spec.failure_injections]


def spec_tool_costs(task: Any) -> dict[str, dict[str, float]]:
    return dict(task.spec.tool_costs)
