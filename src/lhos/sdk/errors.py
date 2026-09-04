"""LongHorizonOS Public SDK — developer-facing error taxonomy (E1).

Wraps Core exceptions so a user sees typed errors, never raw SQLite / private
service exceptions, while never swallowing the root cause (kept as __cause__).
"""

from __future__ import annotations

from typing import Any


class AgentOSError(Exception):
    """Base class for all public SDK errors."""

    def __init__(self, message: str, *, cause: Any = None) -> None:
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause


class ConfigurationError(AgentOSError):
    """Invalid SDK/OS/Agent configuration."""


class CapabilityError(AgentOSError):
    """A capability was required but not granted / denied."""


class VerificationError(AgentOSError):
    """A verifier/evidence-guardian failed to produce a valid Evidence."""


class SchedulingError(AgentOSError):
    """The scheduler could not assign/dispatch as requested."""


class ExecutionError(AgentOSError):
    """An executor/action failed to perform the task."""


class CoreInvariantError(AgentOSError):
    """Raised if the SDK detected a Core invariant violation (should not happen)."""
