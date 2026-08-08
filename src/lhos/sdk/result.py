"""LongHorizonOS Public SDK — RunResult (E1).

A structured result of a run; never a bare boolean/printer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunResult:
    goal_id: str
    goal_state: str = "open"
    task_states: dict[str, str] = field(default_factory=dict)  # task_id -> validity
    ready: list[str] = field(default_factory=list)
    verified: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    owner_by_task: dict[str, str | None] = field(default_factory=dict)
    artifacts: dict[str, tuple[int, str]] = field(default_factory=dict)  # id -> (ver, hash)
    attempts: dict[str, str] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    frontier: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "goal_id": self.goal_id,
            "goal_state": self.goal_state,
            "task_states": self.task_states,
            "ready": self.ready,
            "verified": self.verified,
            "stale": self.stale,
            "owner_by_task": self.owner_by_task,
            "artifacts": {
                k: {"version": v[0], "content_hash": v[1]} for k, v in self.artifacts.items()
            },
            "attempts": self.attempts,
            "failures": self.failures,
            "frontier": self.frontier,
            "meta": self.meta,
        }

    def __repr__(self) -> str:
        return (
            "RunResult("
            + ", ".join(
                [
                    f"goal_state={self.goal_state}",
                    f"verified={self.verified}",
                    f"stale={self.stale}",
                    f"frontier={self.frontier}",
                ]
            )
            + ")"
        )


@dataclass
class RepairOutcome:
    """Result of a D3 invalidation pass (affected / preserved / frontier)."""

    affected: list[str] = field(default_factory=list)
    preserved: list[str] = field(default_factory=list)
    frontier: list[str] = field(default_factory=list)
    causes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "affected": self.affected,
            "preserved": self.preserved,
            "repair_frontier": self.frontier,
            "causes": self.causes,
        }

    def __repr__(self) -> str:
        return f"RepairOutcome(affected={self.affected}, frontier={self.frontier})"
