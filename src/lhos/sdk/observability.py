"""LongHorizonOS E3 — observability read-models (READ-ONLY projection, not authority).

Provides GoalView / TaskView / EvidenceView / OwnershipView / InvalidationView /
RepairView + JSON (schema-versioned, deterministic) + deterministic semantic
`explain(...)`.  Everything is derived from the frozen Core's public APIs; a
CLI/observability surface never mutates semantic state (OBS-G1..G12).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskView:
    task_id: str
    lifecycle: str
    validity: str
    owner: str | None = None
    lease_active: bool | None = None
    artifact: str | None = None
    artifact_version: int | None = None
    supporting_evidence: str | None = None
    evidence_current_applicable: bool | None = None
    in_repair_frontier: bool = False
    blocked_by: list[dict[str, str]] = field(default_factory=list)
    why: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        d = {
            "task_id": self.task_id,
            "lifecycle": self.lifecycle,
            "validity": self.validity,
            "in_repair_frontier": self.in_repair_frontier,
        }
        if self.owner is not None:
            d["owner"] = self.owner
        if self.lease_active is not None:
            d["lease_active"] = self.lease_active
        if self.artifact is not None:
            d["artifact"] = self.artifact
        if self.artifact_version is not None:
            d["artifact_version"] = self.artifact_version
        if self.supporting_evidence is not None:
            d["supporting_evidence"] = self.supporting_evidence
            d["evidence_current_applicable"] = self.evidence_current_applicable
        if self.blocked_by:
            d["blocked_by"] = self.blocked_by
        if self.why:
            d["why"] = self.why
        return d


@dataclass
class StatusView:
    """Top-level status read model."""

    goal_id: str
    goal_state: str
    schema_version: str = "0.1"
    tasks: dict[str, Any] = field(default_factory=dict)  # task_id -> dict
    ready: list[str] = field(default_factory=list)
    verified: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)
    preserved_verified: list[str] = field(default_factory=list)
    owner_by_task: dict[str, str | None] = field(default_factory=dict)
    leases: dict[str, bool] = field(default_factory=dict)  # task -> lease_active
    artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    repair_frontier: list[str] = field(default_factory=list)
    blocked: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    version: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.goal_id,
            "goal": self.goal_id,
            "graph_version": self.version,
            "goal_state": self.goal_state,
            "tasks": self.tasks,
            "ready": self.ready,
            "verified": self.verified,
            "stale": self.stale,
            "unverified": self.unverified,
            "preserved_verified": self.preserved_verified,
            "owner_by_task": self.owner_by_task,
            "leases": self.leases,
            "artifacts": self.artifacts,
            "repair_frontier": self.repair_frontier,
            "blocked": self.blocked,
        }

    def render_ascii(self) -> str:
        lines = [
            f"LONGHORIZONOS STATUS v{self.schema_version}",
            f"Goal: {self.goal_id}  State: {self.goal_state}",
        ]
        for tid in sorted(self.tasks):
            tv = self.tasks[tid]
            mark = {"verified": "\u2713", "stale": "\u2717", "unverified": "?", "invalid": "!"}.get(
                tv["validity"], "?"
            )
            own = tv.get("owner") or "-"
            prev = " PRESERVED" if tid in self.preserved_verified else ""
            lines.append(f"  {mark} {tid:<16} {tv['validity'].upper():<12} owner={own}{prev}")
        if self.repair_frontier:
            lines.append("Repair Frontier: " + ", ".join(self.repair_frontier))
        else:
            lines.append("Repair Frontier: (empty)")
        return "\n".join(lines)
