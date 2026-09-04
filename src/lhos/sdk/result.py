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
class OnlineExecutionLoopResult:
    """Bounded transcript returned by :meth:`AgentOS.execute_online_epochs`.

    Each item in ``epochs`` is the complete :class:`RunResult` returned by one
    explicit ``execute_online_epoch`` call.  The loop never owns a background
    task, Python stack, Claim, or Lease beyond the individual epoch; it is
    merely a sequential control-plane convenience with explicit bounds.
    """

    goal_id: str
    epochs: tuple[RunResult, ...] = ()
    final_result: RunResult | None = None
    stop_reason: str = "max_epochs"
    max_epochs: int = 0
    max_concurrency: int = 1
    max_dispatches_per_epoch: int | None = None
    max_parallelism: int | None = None
    resource_aware: bool = False
    stop_when_closed: bool = True

    @property
    def epoch_results(self) -> tuple[RunResult, ...]:
        """Alias emphasizing that every entry is a full epoch result."""

        return self.epochs

    @property
    def last_result(self) -> RunResult | None:
        """Alias for the final observed result."""

        return self.final_result

    @property
    def goal_closed(self) -> bool:
        """Whether the final observed Goal is semantically closed."""

        return bool(self.final_result and self.final_result.goal_state == "closed")

    @property
    def total_dispatches(self) -> int:
        """Count dispatches admitted across all bounded epochs."""

        total = 0
        for result in self.epochs:
            online = result.meta.get("online_epoch", {})
            dispatched = online.get("actual_dispatched_task_ids", ())
            if isinstance(dispatched, (tuple, list)):
                total += len(dispatched)
            else:
                total += int(result.meta.get("dispatched", 0) or 0)
        return total

    @property
    def complete(self) -> bool:
        """Whether the loop reached a verified closed Goal."""

        return self.goal_closed

    def as_dict(self) -> dict[str, Any]:
        return {
            "goal_id": self.goal_id,
            "epochs": [item.as_dict() for item in self.epochs],
            "final_result": (None if self.final_result is None else self.final_result.as_dict()),
            "stop_reason": self.stop_reason,
            "max_epochs": self.max_epochs,
            "max_concurrency": self.max_concurrency,
            "max_dispatches_per_epoch": self.max_dispatches_per_epoch,
            "max_parallelism": self.max_parallelism,
            "resource_aware": self.resource_aware,
            "stop_when_closed": self.stop_when_closed,
            "total_dispatches": self.total_dispatches,
            "goal_closed": self.goal_closed,
        }

    def __repr__(self) -> str:
        return (
            "OnlineExecutionLoopResult("
            f"goal_id={self.goal_id!r}, epochs={len(self.epochs)}, "
            f"stop_reason={self.stop_reason!r}, goal_closed={self.goal_closed}"
            ")"
        )


@dataclass
class RepairOutcome:
    """Result of a D3 invalidation pass (affected / preserved / frontier)."""

    affected: list[str] = field(default_factory=list)
    preserved: list[str] = field(default_factory=list)
    frontier: list[str] = field(default_factory=list)
    causes: list[str] = field(default_factory=list)
    # Structured cause records are additive to the legacy human-readable
    # ``causes`` strings so callers can audit exact ArtifactVersion transitions.
    cause_details: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "affected": self.affected,
            "preserved": self.preserved,
            "repair_frontier": self.frontier,
            "causes": self.causes,
            "cause_details": self.cause_details,
        }

    def __repr__(self) -> str:
        return f"RepairOutcome(affected={self.affected}, frontier={self.frontier})"
