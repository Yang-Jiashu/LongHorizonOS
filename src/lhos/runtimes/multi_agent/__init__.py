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
from .handoff import (
    HANDOFF_SCHEMA_VERSION,
    OwnershipHandoffIntent,
    OwnershipHandoffPhase,
    OwnershipHandoffResult,
    OwnershipHandoffStatus,
)
from .models import (
    TERMINAL_CLAIM_STATES,
    AgentCapabilitySnapshot,
    AgentDescriptor,
    AgentMatchScore,
    AgentSnapshot,
    AttemptState,
    ClaimHandoffResult,
    ClaimHandoffStatus,
    ClaimState,
    ComputationCost,
    ContextIdentity,
    EligibilityResult,
    MatchDecision,
    ResourceBinding,
    ResourceVector,
    ScheduledExecutionAttempt,
    TaskClaim,
    TaskRequirements,
)
from .registry import AgentRegistry
from .resources import AtomicResourceManager, ResourcePoolSnapshot, ResourceReservation
from .sdk import SchedulerSession, create_scheduler
from .worker_pool import (
    AsyncWorkerPool,
    CapacityRequestTooLarge,
    CooperativeCancellationToken,
    CooperativeInterrupt,
    DispatchRejected,
    HeartbeatFailed,
    InterruptDelivery,
    InterruptDeliveryStatus,
    InterruptTransition,
    InterruptTransitionPhase,
    UnobservedCooperativeInterrupt,
    WorkerJob,
    WorkerOutcome,
    WorkerPoolError,
    WorkerStatus,
)

__all__ = [
    "HANDOFF_SCHEMA_VERSION",
    "TERMINAL_CLAIM_STATES",
    "AgentCapabilitySnapshot",
    "AgentDescriptor",
    "AgentMatchScore",
    "AgentRegistry",
    "AgentSnapshot",
    "AsyncWorkerPool",
    "AtomicResourceManager",
    "AttemptState",
    "CapacityRequestTooLarge",
    "ClaimHandoffResult",
    "ClaimHandoffStatus",
    "ClaimState",
    "ComputationCost",
    "ConcurrencyViolation",
    "ContextIdentity",
    "CooperativeCancellationToken",
    "CooperativeInterrupt",
    "D2Error",
    "DispatchRejected",
    "EligibilityResult",
    "GraphVersionStale",
    "HeartbeatFailed",
    "InterruptDelivery",
    "InterruptDeliveryStatus",
    "InterruptTransition",
    "InterruptTransitionPhase",
    "KernelLeaseRequired",
    "LeaseAcquisitionFailed",
    "LeaseReleaseFailed",
    "MatchDecision",
    "NoEligibleAgentError",
    "OwnershipHandoffIntent",
    "OwnershipHandoffPhase",
    "OwnershipHandoffResult",
    "OwnershipHandoffStatus",
    "ResourceBinding",
    "ResourcePoolSnapshot",
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
    "UnobservedCooperativeInterrupt",
    "WorkerJob",
    "WorkerOutcome",
    "WorkerPoolError",
    "WorkerStatus",
    "create_scheduler",
]
