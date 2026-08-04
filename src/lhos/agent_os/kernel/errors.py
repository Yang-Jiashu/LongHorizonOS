"""Kernel errors and exceptions."""

from __future__ import annotations


class KernelError(Exception):
    """Base kernel error."""


class IllegalStateTransition(KernelError):
    """Attempted an invalid process or action state transition."""

    def __init__(self, obj_id: str, old: str, new: str, reason: str = ""):
        self.obj_id = obj_id
        self.old = old
        self.new = new
        self.reason = reason
        super().__init__(f"Illegal transition {old}→{new} for {obj_id}: {reason}")


class CapabilityDenied(KernelError):
    """Capability check failed."""

    def __init__(self, pid: str, resource: str, operation: str):
        self.pid = pid
        self.resource = resource
        self.operation = operation
        super().__init__(f"Capability denied for pid={pid} resource={resource} op={operation}")


class LeaseAcquisitionFailed(KernelError):
    """Atomic lease acquisition could not satisfy all claims."""

    def __init__(self, pid: str, failed_resource: str):
        self.pid = pid
        self.failed_resource = failed_resource
        super().__init__(f"Lease acquisition failed for pid={pid} resource={failed_resource}")


class DuplicateEvent(KernelError):
    """An event with the same event_id was already journaled."""

    def __init__(self, event_id: str):
        self.event_id = event_id
        super().__init__(f"Duplicate event_id: {event_id}")


class ProgramStepError(KernelError):
    """User-space program step raised an exception."""

    def __init__(self, pid: str, cause: str):
        self.pid = pid
        self.cause = cause
        super().__init__(f"Program step failed for pid={pid}: {cause}")


class TerminalStateError(KernelError):
    """Attempted to transition from a terminal state."""

    def __init__(self, obj_id: str, state: str):
        self.obj_id = obj_id
        self.state = state
        super().__init__(f"{obj_id} is in terminal state {state}")


class WaitConditionMissing(KernelError):
    """Attempted to block without a wait condition."""

    def __init__(self, pid: str):
        self.pid = pid
        super().__init__(f"Cannot block pid={pid} without wait_condition")
