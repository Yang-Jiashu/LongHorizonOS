"""D2 Multi-Agent Scheduler domain errors."""


class D2Error(Exception):
    """Base class for all scheduler-domain errors."""


class SemanticNotReadyError(D2Error):
    """VPG does not currently surface this Task in its READY frontier."""

    def __init__(
        self,
        graph_id: str,
        task_id: str,
        graph_version: int | None = None,
    ) -> None:
        self.graph_id = graph_id
        self.task_id = task_id
        self.graph_version = graph_version
        ver = "" if graph_version is None else f" at v{graph_version}"
        super().__init__(
            f"Task {task_id!r} in graph {graph_id!r} is not currently in "
            f"the VPG READY frontier{ver}."
        )


class NoEligibleAgentError(D2Error):
    """READY Task exists but no Agent currently satisfies eligibility."""

    def __init__(
        self,
        graph_id: str,
        task_id: str,
        reasons: tuple[str, ...] = (),
    ) -> None:
        self.graph_id = graph_id
        self.task_id = task_id
        self.reasons = reasons
        body = "; ".join(reasons) if reasons else "no agent available"
        super().__init__(
            f"Task {task_id!r} in graph {graph_id!r} is READY but no eligible agent: {body}."
        )


class TaskAlreadyClaimed(D2Error):
    """A valid ACTIVE claim already exists for this task."""

    def __init__(self, task_id: str, claim_id: str) -> None:
        self.task_id = task_id
        self.claim_id = claim_id
        super().__init__(f"Task {task_id!r} already has an ACTIVE claim {claim_id!r}.")


class LeaseAcquisitionFailed(D2Error):
    """Kernel refused the exclusive lease required to linearize ownership."""

    def __init__(
        self,
        task_id: str,
        resource: str,
        reason: str = "",
    ) -> None:
        self.task_id = task_id
        self.resource = resource
        self.reason = reason
        tail = f" ({reason})" if reason else ""
        super().__init__(
            f"Could not acquire exclusive lease for task {task_id!r} on "
            f"resource {resource!r}{tail}."
        )


class LeaseReleaseFailed(D2Error):
    """Kernel lease release could not be confirmed."""

    def __init__(self, claim_id: str, lease_id: str, reason: str = "") -> None:
        self.claim_id = claim_id
        self.lease_id = lease_id
        self.reason = reason
        tail = f" ({reason})" if reason else ""
        super().__init__(
            f"Could not confirm release of Kernel lease {lease_id!r} for claim {claim_id!r}{tail}."
        )


class KernelLeaseRequired(D2Error):
    """ACTIVE claim record exists without a backing live Kernel ResourceLease."""

    def __init__(self, claim_id: str, task_id: str) -> None:
        self.claim_id = claim_id
        self.task_id = task_id
        super().__init__(
            f"Scheduler invariant broken: claim {claim_id!r} for task "
            f"{task_id!r} is ACTIVE but has no live Kernel lease."
        )


class ConcurrencyViolation(D2Error):
    """Agent would exceed max_concurrency if granted another claim."""

    def __init__(self, agent_id: str, active: int, maximum: int) -> None:
        self.agent_id = agent_id
        self.active = active
        self.maximum = maximum
        super().__init__(
            f"Agent {agent_id!r} already has {active} active claims (max_concurrency={maximum})."
        )


class GraphVersionStale(D2Error):
    """ReadinessProof used for matching came from a superseded VPG version."""

    def __init__(
        self,
        task_id: str,
        used_version: int,
        current_version: int,
    ) -> None:
        self.task_id = task_id
        self.used_version = used_version
        self.current_version = current_version
        super().__init__(
            f"Task {task_id!r} readiness proof at v{used_version} is stale "
            f"(current v{current_version}) — claim aborted."
        )
