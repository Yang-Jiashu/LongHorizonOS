"""Multi-Agent Scheduler runtime for LongHorizonOS (Phase D2).

Dependency direction (ENFORCED):
    MultiAgentScheduler
        -> VPG public API (lhos.runtimes.verified_progress.sdk)
        -> Agent OS public SDK process/lease/capability providers (INJECTED)
        -> Agent OS Kernel internals are NEVER imported by this package.

This runtime implements:
    - Agent registration with structured capabilities
    - Deterministic eligibility over the VPG Ready Frontier
    - Deterministic agent matching (integer scoring, stable tiebreak)
    - Kernel-Exclusive ResourceLease-backed TaskClaims
    - Per-agent concurrency limits (max_concurrency)
    - Crash-safe task reassignment via Projection + Event log + Reconciliation
"""

from .errors import (
    ConcurrencyViolation,
    D2Error,
    GraphVersionStale,
    KernelLeaseRequired,
    LeaseAcquisitionFailed,
    NoEligibleAgentError,
    SemanticNotReadyError,
    TaskAlreadyClaimed,
)
from .models import (
    AgentCapabilitySnapshot,
    AgentDescriptor,
    AgentMatchScore,
    ClaimState,
    EligibilityResult,
    MatchDecision,
    ScheduledExecutionAttempt,
    TERMINAL_CLAIM_STATES,
    TaskClaim,
    TaskRequirements,
)
from .registry import AgentRegistry
from .sdk import SchedulerSession, create_scheduler

__all__ = [
    "SchedulerSession",
    "create_scheduler",
    "AgentDescriptor",
    "AgentRegistry",
    "AgentCapabilitySnapshot",
    "TaskRequirements",
    "EligibilityResult",
    "AgentMatchScore",
    "MatchDecision",
    "TaskClaim",
    "ClaimState",
    "TERMINAL_CLAIM_STATES",
    "ScheduledExecutionAttempt",
    "D2Error",
    "SemanticNotReadyError",
    "NoEligibleAgentError",
    "TaskAlreadyClaimed",
    "LeaseAcquisitionFailed",
    "KernelLeaseRequired",
    "ConcurrencyViolation",
    "GraphVersionStale",
]
