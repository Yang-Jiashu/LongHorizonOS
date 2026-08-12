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

from .durable_state import SchedulerStateCorruption, SchedulerStateStore
from .errors import (
    ConcurrencyViolation,
    D2Error,
    GraphVersionStale,
    KernelLeaseRequired,
    LeaseAcquisitionFailed,
    LeaseReleaseFailed,
    NoEligibleAgentError,
    SemanticNotReadyError,
    TaskAlreadyClaimed,
)
from .models import (
    TERMINAL_CLAIM_STATES,
    AgentCapabilitySnapshot,
    AgentDescriptor,
    AgentMatchScore,
    AttemptState,
    ClaimState,
    EligibilityResult,
    MatchDecision,
    ResourceVector,
    ScheduledExecutionAttempt,
    TaskClaim,
    TaskRequirements,
)
from .registry import AgentRegistry
from .resources import AtomicResourceManager, ResourceReservation
from .sdk import SchedulerSession, create_scheduler
from .worker_pool import (
    AsyncWorkerPool,
    CapacityRequestTooLarge,
    DispatchRejected,
    WorkerJob,
    WorkerOutcome,
    WorkerPoolError,
    WorkerStatus,
)

__all__ = [
    "TERMINAL_CLAIM_STATES",
    "AgentCapabilitySnapshot",
    "AgentDescriptor",
    "AgentMatchScore",
    "AgentRegistry",
    "AsyncWorkerPool",
    "AtomicResourceManager",
    "AttemptState",
    "CapacityRequestTooLarge",
    "ClaimState",
    "ConcurrencyViolation",
    "D2Error",
    "DispatchRejected",
    "EligibilityResult",
    "GraphVersionStale",
    "KernelLeaseRequired",
    "LeaseAcquisitionFailed",
    "LeaseReleaseFailed",
    "MatchDecision",
    "NoEligibleAgentError",
    "ResourceReservation",
    "ResourceVector",
    "ScheduledExecutionAttempt",
    "SchedulerSession",
    "SchedulerStateCorruption",
    "SchedulerStateStore",
    "SemanticNotReadyError",
    "TaskAlreadyClaimed",
    "TaskClaim",
    "TaskRequirements",
    "WorkerJob",
    "WorkerOutcome",
    "WorkerPoolError",
    "WorkerStatus",
    "create_scheduler",
]
