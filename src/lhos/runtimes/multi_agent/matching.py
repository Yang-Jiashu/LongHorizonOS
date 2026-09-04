"""Deterministic Matching (Section 13).

Policy ``deterministic_best_fit_v1``:
    score =
        + specialization_bonus   (preferred specs that the agent holds)
        + locality_bonus         (task reads already resident in this agent)
        - load_penalty           (current active claims)
        - cost_penalty           (cost_weight)

Final stable sort:
    1. score DESC
    2. current_load ASC
    3. cost_weight ASC
    4. agent_id ASC

All scoring is integer.  Preferred specializations and locality may only
boost an agent that is already HARD-Eligible.

Locality is the only term that depends on *agent-side* state rather than the
graph: it scores how much of the candidate task's declared read-set this
specific agent has already read in a durable ``AgentSnapshot``.  Because agent
context is path-dependent, this is exactly the signal a graph-derived readiness
proof cannot carry.  Only counts enter the audit trail — never resource
identities, which must not leak into a durable decision hash.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .models import AgentDescriptor, AgentMatchScore, MatchDecision

# Tuning knobs (all integer).  Kept tiny and documented.
SPECIALIZATION_BONUS = 10  # per preferred specialization held
LOCALITY_BONUS = 5  # per declared read already resident in this agent
LOCALITY_BONUS_MAX = 25  # cap: locality never outweighs explicit task affinity
PREFERRED_AGENT_BONUS = 20  # explicit task affinity, eligibility still wins
LOAD_PENALTY = 1  # per active claim the agent already holds
COST_PENALTY_DIVISOR = 50  # cost_weight // this


def match_deterministic_best_fit_v1(
    *,
    graph_id: str,
    graph_version: int,
    task_id: str,
    task_priority: int,
    eligible_agents: list[AgentDescriptor],
    active_claims_by_agent: dict[str, int],
    preferred_specializations: tuple[str, ...] = (),
    preferred_agent: str = "",
    task_read_keys: tuple[str, ...] = (),
    resident_read_keys_by_agent: Mapping[str, tuple[str, ...]] | None = None,
) -> MatchDecision:
    """Pick the best-fit eligible agent.  Deterministic across hash seeds,
    insertion orders, and DB row orders.
    """
    declared_reads = frozenset(key for key in task_read_keys if key)
    scored: list[AgentMatchScore] = []
    for agent in eligible_agents:
        reasons: list[str] = []
        score = 0

        if preferred_agent and agent.agent_id == preferred_agent:
            score += PREFERRED_AGENT_BONUS
            reasons.append(f"preferred agent (+{PREFERRED_AGENT_BONUS})")

        # Preferred specialization bonus — an agent that ALREADY satisfies a
        # *preferred* (not required) specialization gets a mild boost.
        held_preferred = sorted(set(preferred_specializations) & set(agent.specializations))
        if held_preferred:
            bonus = SPECIALIZATION_BONUS * len(held_preferred)
            score += bonus
            reasons.append(f"preferred specializations {held_preferred} (+{bonus})")

        # Context-residency locality — how many of this task's declared reads
        # this agent has already read.  Absent residency evidence contributes
        # nothing, so an unobserved agent is never penalised into last place.
        resident_count = 0
        if declared_reads and resident_read_keys_by_agent:
            resident = resident_read_keys_by_agent.get(agent.agent_id) or ()
            resident_count = len(declared_reads.intersection(resident))
        if resident_count:
            bonus = min(LOCALITY_BONUS * resident_count, LOCALITY_BONUS_MAX)
            score += bonus
            reasons.append(f"context residency {resident_count}/{len(declared_reads)} (+{bonus})")

        # Load penalty — busier agents are slightly less attractive to
        # spread work across the fleet.
        load = active_claims_by_agent.get(agent.agent_id, 0)
        if load:
            penalty = LOAD_PENALTY * load
            score -= penalty
            reasons.append(f"load {load} (-{penalty})")

        # Cost penalty — cheaper agents slightly preferred.
        if agent.cost_weight:
            penalty = agent.cost_weight // COST_PENALTY_DIVISOR
            if penalty:
                score -= penalty
                reasons.append(f"cost_weight={agent.cost_weight} (-{penalty})")

        # Tiny task_priority signal — higher-priority work gets a very small
        # bump so matching is not entirely priority-blind, but priority never
        # trumps hard eligibility.
        if task_priority:
            bonus = min(task_priority, 3)
            score += bonus
            reasons.append(f"task priority {task_priority} (+{bonus})")

        scored.append(
            AgentMatchScore(
                agent_id=agent.agent_id,
                score=score,
                reasons=tuple(reasons) if reasons else ("baseline 0",),
            )
        )

    scored_sorted = sorted(
        scored,
        key=lambda s: (
            -s.score,
            active_claims_by_agent.get(s.agent_id, 0),
            _cost_of(s.agent_id, eligible_agents),
            s.agent_id,
        ),
    )
    selected = scored_sorted[0].agent_id if scored_sorted else ""
    decision_hash = _hash_decision(
        graph_id,
        graph_version,
        task_id,
        tuple(s.model_dump() for s in scored_sorted),
    )
    return MatchDecision(
        graph_id=graph_id,
        graph_version=graph_version,
        task_id=task_id,
        selected_agent_id=selected,
        candidates=tuple(scored_sorted),
        decision_hash=decision_hash,
    )


def _cost_of(agent_id: str, pool: list[AgentDescriptor]) -> int:
    for a in pool:
        if a.agent_id == agent_id:
            return a.cost_weight
    return 0


def _hash_decision(
    graph_id: str,
    graph_version: int,
    task_id: str,
    candidates_dump: tuple[dict[str, Any], ...],
) -> str:
    """Deterministic content hash of the match decision."""
    payload = json.dumps(
        {
            "graph_id": graph_id,
            "graph_version": graph_version,
            "task_id": task_id,
            "candidates": candidates_dump,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
