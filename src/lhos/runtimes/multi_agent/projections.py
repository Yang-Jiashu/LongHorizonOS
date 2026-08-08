"""Scheduler projections — rebuildable materialized views (Section 29).

Five projections:
    agents_projection
    claims_projection
    attempts_projection
    agent_load_projection
    dispatch_projection

The Scheduler projection is explicitly NOT the ownership authority —
only a rebuildable view over authoritative VPG + Kernel + event history.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import (
    AgentDescriptor,
    ClaimState,
    ScheduledExecutionAttempt,
    TaskClaim,
)


@dataclass
class AgentLoad:
    agent_id: str
    active_claims: int = 0
    max_concurrency: int = 1
    last_claimed_at: str | None = None


@dataclass
class SchedulerProjection:
    """Mutable, rebuildable view; never the source of truth."""

    agents: dict[str, AgentDescriptor] = field(default_factory=dict)
    claims: dict[str, TaskClaim] = field(default_factory=dict)
    attempts: dict[str, ScheduledExecutionAttempt] = field(default_factory=dict)
    loads: dict[str, AgentLoad] = field(default_factory=dict)

    def rebuild(
        self,
        agents: list[AgentDescriptor],
        claims: list[TaskClaim],
        attempts: list[ScheduledExecutionAttempt],
    ) -> None:
        """Rebuild all projections from authoritative inputs (event replay
        / VPG / Kernel truth).  Deterministic: the same inputs always
        produce the same in-memory projection.
        """
        self.agents = {a.agent_id: a for a in agents}
        self.claims = {c.claim_id: c for c in claims}
        self.attempts = {a.attempt_id: a for a in attempts}
        self.loads = {}
        for agent in agents:
            active = [
                c
                for c in claims
                if c.agent_id == agent.agent_id and c.state == ClaimState.ACTIVE
            ]
            last_active = max(
                (c.activated_at for c in active if c.activated_at),
                default=None,
            )
            self.loads[agent.agent_id] = AgentLoad(
                agent_id=agent.agent_id,
                active_claims=len(active),
                max_concurrency=agent.max_concurrency,
                last_claimed_at=last_active.isoformat() if last_active else None,
            )


def active_claim_count_by_agent(claims: list[TaskClaim]) -> dict[str, int]:
    """Helper for eligibility: count ACTIVE claims per agent_id."""
    counts: dict[str, int] = {}
    for c in claims:
        if c.state == ClaimState.ACTIVE:
            counts[c.agent_id] = counts.get(c.agent_id, 0) + 1
    return counts


def active_claims_for_task(claims: list[TaskClaim], task_id: str) -> list[TaskClaim]:
    return [c for c in claims if c.task_id == task_id and c.state == ClaimState.ACTIVE]


def attempts_for_task(
    attempts: list[ScheduledExecutionAttempt], task_id: str
) -> list[ScheduledExecutionAttempt]:
    return [a for a in attempts if a.task_id == task_id]


def latest_attempt_for_task(
    attempts: list[ScheduledExecutionAttempt], task_id: str
) -> ScheduledExecutionAttempt | None:
    matching = [a for a in attempts if a.task_id == task_id]
    if not matching:
        return None
    return max(matching, key=lambda a: a.started_at)
