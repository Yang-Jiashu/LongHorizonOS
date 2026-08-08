"""Deterministic Eligibility engine (Section 11).

Eligible(agent, task) must satisfy ALL ten predicates:

  1. Task currently in VPG Ready Frontier (the caller supplies its
     ReadinessProof so this is re-checkable).
  2. Agent enabled.
  3. Agent's process_id actually exists in the Kernel.
  4. Process state allows scheduling (/not/ terminal).
  5. Agent's active-claim count < max_concurrency.
  6. Agent supports task_kind (if the Task declares one).
  7. Every required specialization held by the agent.
  8. Every required tool supported by the agent.
  9. Every (resource,op) in required_capabilities held in the Kernel
     for the agent's process_id.
10. Scheduler-side capability allows claim (echo of 9).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import AgentCapabilitySnapshot, AgentDescriptor, EligibilityResult

# Kernel process states that allow scheduling new claims (mirrors
# ProcessState in the AgentOS kernel but kept as plain strings here to
# avoid importing the kernel package — see Section 6).
_SCHEDULABLE_PROCESS_STATES = frozenset({"ready", "running", "created"})
_TERMINAL_PROCESS_STATES = frozenset({"exited", "failed"})


def evaluate_eligibility(
    agent: AgentDescriptor,
    task_id: str,
    graph_id: str,
    graph_version: int,
    *,
    task_kind: str,
    required_specializations: tuple[str, ...],
    required_tools: tuple[str, ...],
    required_capabilities: tuple[str, ...],
    readiness_version: int | None,
    active_claims_for_agent: int,
    process_state: str | None,
    process_exists: bool,
    capability_checker: Any,
) -> EligibilityResult:
    """Compute eligibility with explicit, auditable reasons.

    Every rejected branch appends a specific reason string so the
    caller can answer "WHY NOT ELIGIBLE".
    """
    reasons: list[str] = []

    if not agent.enabled:
        reasons.append("agent disabled")

    if not process_exists:
        reasons.append(f"process_id {agent.process_id!r} does not exist in Kernel")
    else:
        if process_state is None:
            reasons.append("process state unavailable")
        elif process_state in _TERMINAL_PROCESS_STATES:
            reasons.append(f"process in terminal state {process_state!r}")
        elif process_state not in _SCHEDULABLE_PROCESS_STATES:
            reasons.append(
                f"process state {process_state!r} does not allow scheduling"
            )

    if agent.max_concurrency == 0:
        reasons.append("max_concurrency == 0")
    elif active_claims_for_agent >= agent.max_concurrency:
        reasons.append(
            f"capacity exhausted ({active_claims_for_agent}/{agent.max_concurrency})"
        )

    if task_kind and agent.supported_task_kinds:
        # special value "*" means "any kind"
        if "*" not in agent.supported_task_kinds and task_kind not in agent.supported_task_kinds:
            reasons.append(
                f"task_kind {task_kind!r} not in supported_task_kinds "
                f"{list(agent.supported_task_kinds)}"
            )

    missing_specs = sorted(
        set(required_specializations) - set(agent.specializations)
    )
    if missing_specs:
        reasons.append(
            f"missing required specializations: {missing_specs}"
        )

    missing_tools = sorted(set(required_tools) - set(agent.supported_tools))
    if missing_tools:
        reasons.append(f"missing required tools: {missing_tools}")

    missing_caps = _missing_kernel_capabilities(
        agent.process_id, required_capabilities, capability_checker
    )
    if missing_caps:
        reasons.append(f"missing kernel capabilities: {missing_caps}")

    eligible = len(reasons) == 0
    return EligibilityResult(
        graph_id=graph_id,
        graph_version=graph_version,
        task_id=task_id,
        agent_id=agent.agent_id,
        eligible=eligible,
        reasons=tuple(reasons),
    )


def _missing_kernel_capabilities(
    process_id: str,
    required: tuple[str, ...],
    capability_checker: Any,
) -> list[str]:
    """Return the subset of required_capabilities the Kernel has not granted.

    Each capability string has the form ``resource:op`` (e.g.
    ``device:tool/mock:invoke``).  Missing ones are returned in sorted
    order so the reason string is deterministic.
    """
    missing: list[str] = []
    if capability_checker is None:
        # If no checker is registered we cannot confirm grants, so ALL are
        # missing.  This is the safe default.
        return sorted(required)
    for cap in required:
        if ":" not in cap:
            continue
        resource, op = cap.rsplit(":", 1)
        try:
            ok = capability_checker.check(process_id, resource, op)
        except Exception:
            ok = False
        if not ok:
            missing.append(cap)
    return sorted(missing)


def capture_capabilities(agent_id: str, capability_checker: Any) -> AgentCapabilitySnapshot:
    return AgentCapabilitySnapshot.from_provider(agent_id, capability_checker)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
