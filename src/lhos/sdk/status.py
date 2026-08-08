"""LongHorizonOS Public SDK — StatusSnapshot (E1, read-only).

A public, read-only view of a graph's semantic/ownership state so a future E3
CLI/UX can render it without reading internal DBs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StatusSnapshot:
    goal_id: str
    version: int
    tasks: dict[str, str] = field(default_factory=dict)  # task_id -> validity
    lifecycle: dict[str, str] = field(default_factory=dict)  # task_id -> lifecycle
    ready: list[str] = field(default_factory=list)
    verified: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)
    goal_closed: bool = False
    owner_by_task: dict[str, str | None] = field(default_factory=dict)
    artifact_versions: dict[str, int] = field(default_factory=dict)  # art_id -> version
    repair_frontier: list[str] = field(default_factory=list)
    invalidation_causes: dict[str, str] = field(default_factory=dict)  # task -> reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "goal_id": self.goal_id,
            "graph_version": self.version,
            "tasks": self.tasks,
            "lifecycle": self.lifecycle,
            "ready": self.ready,
            "verified": self.verified,
            "stale": self.stale,
            "unverified": self.unverified,
            "goal_closed": self.goal_closed,
            "owner_by_task": self.owner_by_task,
            "artifact_versions": self.artifact_versions,
            "repair_frontier": self.repair_frontier,
            "invalidation_causes": self.invalidation_causes,
        }

    def render_ascii(self) -> str:
        """Human-readable table for demos / docs (not a final E3 CLI)."""
        lines = [
            f"GOAL {self.goal_id}  (v{self.version})  {'CLOSED' if self.goal_closed else 'OPEN'}"
        ]
        for tid in self.tasks:
            mark = {"verified": chr(0x2713), "stale": "x", "unverified": "?", "invalid": "!"}.get(
                self.tasks[tid], "?"
            )
            own = self.owner_by_task.get(tid) or "-"
            cause = self.invalidation_causes.get(tid, "")
            suffix = f"  (cause: {cause})" if cause else ""
            lines.append(f"  {mark} {tid:<14} {self.tasks[tid].upper():<12} owner={own}{suffix}")
        if self.repair_frontier:
            lines.append("Repair Frontier: " + ", ".join(self.repair_frontier))
        else:
            lines.append("Repair Frontier: (empty)")
        return "\n".join(lines)
