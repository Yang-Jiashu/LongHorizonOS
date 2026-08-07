"""Protocols the Runtime uses to obtain external facts.

The Runtime NEVER imports agent_os Kernel internals.  It depends only on the
public Agent OS SDK surface, which the host passes in via these protocols.  This
enables clean in-memory unit tests AND clean architecture-audit enforcement.

Two protocols:

- ``ArtifactFactProvider``: reads artifact bytes / hashes / capability.
- ``KernelEventProvider``: reads committed Kernel Actions / Events.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from .models import ArtifactVersionBinding


# ── Action identity used by the Runtime ───────────────────────────────────────
class ActionTerminalState(Protocol):
    committed: str
    failed: str
    cancelled: str
    timed_out: str
    uncertain: str


class KernelActionInfo(Protocol):
    """Minimal view of a Kernel Action sufficient for Evidence validation."""

    action_id: str
    pid: str
    state: str
    result: dict[str, Any] | None
    artifact_refs: tuple[dict[str, Any], ...]


class KernelEventInfo(Protocol):
    """Minimal view of a Kernel Journal Event."""

    event_id: str
    pid: str
    event_type: str
    payload: dict[str, Any]


@runtime_checkable
class ArtifactFactProvider(Protocol):
    """Read-only access to committed ArtifactVersions.

    Implementations MUST go through the Agent OS SDK (ArtifactSDK).
    """

    def artifact_exists(self, pid: str, canonical_uri: str, version: int) -> bool:
        """Return True iff the committed ArtifactVersion is present & readable."""
        ...

    def read_hash(self, pid: str, canonical_uri: str, version: int) -> str | None:
        """Return the stored content_hash or None if not found."""
        ...

    def verify_binding(self, pid: str, binding: ArtifactVersionBinding) -> bool:
        """True iff binding.version is committed AND hash matches."""
        ...

    def can_read(self, pid: str, artifact_id: str, version: int) -> bool:
        """Capability check."""
        ...


@runtime_checkable
class KernelEventProvider(Protocol):
    """Read-only access to Kernel Actions and Events.

    Implementations MUST go through the Agent OS SDK/Journal, NOT by importing
    kernel internals.
    """

    def get_action(self, action_id: str) -> KernelActionInfo | None:
        """Return the ActionControlBlock-like value or None."""
        ...

    def has_event(self, event_id: str) -> bool:
        """Return True iff the journal contains this event."""
        ...

    def list_events_for_pid(self, pid: str) -> Sequence[KernelEventInfo]:
        """List events authored by pid."""
        ...
