"""Context VM error types.

CVM-specific failures. All inherit from ContextVMError so callers can
catch a single exception type if desired.
"""

from __future__ import annotations


class ContextVMError(Exception):
    """Base class for all Context VM errors."""


class ErrInvalidManifest(ContextVMError):
    """Manifest failed a structural or version-binding check."""


class ErrMissingVersionBinding(ContextVMError):
    """Ref does not pin an explicit ArtifactVersion."""


class ErrInvalidContentHash(ContextVMError):
    """Supplied content_hash does not match the authoritative ArtifactVersion."""


class ErrInvalidRange(ContextVMError):
    """start_byte/end_byte invalid or exceeds content."""


class ErrArtifactNotFound(ContextVMError):
    """Referenced ArtifactVersion does not exist."""


class ErrCapabilityDenied(ContextVMError):
    """Caller lacks capability for the operation."""


class ErrRequiredBudgetExceeded(ContextVMError):
    """Required pages alone exceed the supplied budget; load must fail explicitly."""


class ErrOptionalOmitted(ContextVMError):
    """An optional page would exceed budget and must be omitted (not an error)."""

    def __init__(self, ref_id: str, reason: str = "") -> None:
        super().__init__(f"optional page omitted: {ref_id} ({reason})")
        self.ref_id = ref_id
        self.reason = reason


class ErrHandleClosed(ContextVMError):
    """ContextHandle has been closed and can no longer be used."""


class ErrHandleNotOwned(ContextVMError):
    """A PID attempted to use a handle it does not own."""


class ErrPagePinned(ContextVMError):
    """Eviction target page is pinned; cannot be evicted."""


class ErrSnapshotCorrupt(ContextVMError):
    """Snapshot references an ArtifactVersion or content that is no longer valid."""


class ErrDuplicateRefId(ContextVMError):
    """Same ref_id appears more than once in a Manifest."""


class ErrInvalidEstimator(ContextVMError):
    """Token estimator returned an invalid (negative) count."""


class ErrIdempotentReplay(ContextVMError):
    """Idempotency key replay detected — serves as signal for inspect."""


class ErrInvalidPolicy(ContextVMError):
    """Policy ID is unknown or unsupported."""
