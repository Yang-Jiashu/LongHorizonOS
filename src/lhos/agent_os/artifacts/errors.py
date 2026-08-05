"""Artifact FS errors.

These errors are raised by the Artifact FS service layer and SDK.
They do not inherit from KernelError to keep the kernel clean of artifact concerns.
"""

from __future__ import annotations


class ArtifactError(Exception):
    """Base error for artifact operations."""


class ArtifactNotFound(ArtifactError):
    def __init__(self, uri: str):
        self.uri = uri
        super().__init__(f"Artifact not found: {uri}")


class ArtifactAlreadyExists(ArtifactError):
    def __init__(self, uri: str):
        self.uri = uri
        super().__init__(f"Artifact already exists: {uri}")


class VersionConflict(ArtifactError):
    """Optimistic concurrency conflict — expected_version does not match current."""

    def __init__(self, artifact_id: str, expected: int, actual: int):
        self.artifact_id = artifact_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Version conflict for {artifact_id}: expected {expected}, actual {actual}"
        )


class HandleNotFound(ArtifactError):
    def __init__(self, handle_id: str):
        self.handle_id = handle_id
        super().__init__(f"Handle not found: {handle_id}")


class HandleClosed(ArtifactError):
    def __init__(self, handle_id: str):
        self.handle_id = handle_id
        super().__init__(f"Handle already closed: {handle_id}")


class HandleNotOwned(ArtifactError):
    """A process attempted to use a handle owned by another process."""

    def __init__(self, handle_id: str, pid: str, owner_pid: str):
        self.handle_id = handle_id
        self.pid = pid
        self.owner_pid = owner_pid
        super().__init__(f"Handle {handle_id} owned by {owner_pid}, not {pid}")


class WrongHandleMode(ArtifactError):
    def __init__(self, handle_id: str, expected: str, actual: str):
        self.handle_id = handle_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"Handle {handle_id} mode is {actual}, expected {expected}")


class WriteLeaseHeld(ArtifactError):
    """Another process holds the exclusive write lease for this artifact."""

    def __init__(self, artifact_id: str, holder_pid: str):
        self.artifact_id = artifact_id
        self.holder_pid = holder_pid
        super().__init__(f"Artifact {artifact_id} write lease held by {holder_pid}")


class NamespaceNotFound(ArtifactError):
    def __init__(self, namespace_id: str):
        self.namespace_id = namespace_id
        super().__init__(f"Namespace not found: {namespace_id}")


class InvalidArtifactURI(ArtifactError):
    """URI is malformed or fails canonicalization."""

    def __init__(self, uri: str, reason: str = ""):
        self.uri = uri
        self.reason = reason
        super().__init__(f"Invalid artifact URI: {uri} ({reason})")


class PathTraversalRejected(InvalidArtifactURI):
    """URI contains path traversal sequence after canonicalization."""

    def __init__(self, uri: str):
        super().__init__(uri, "path traversal detected")


class SymlinkRejected(ArtifactError):
    """A symlink was found in the storage path."""

    def __init__(self, path: str, reason: str = ""):
        self.path = path
        self.reason = reason
        super().__init__(f"Symlink rejected: {path} ({reason})")


class QuotaExceeded(ArtifactError):
    def __init__(self, namespace_id: str, resource: str, limit: int, requested: int):
        self.namespace_id = namespace_id
        self.resource = resource
        self.limit = limit
        self.requested = requested
        super().__init__(
            f"Quota exceeded for {resource} in namespace {namespace_id}: "
            f"limit {limit}, requested {requested}"
        )


class TransactionConflict(ArtifactError):
    """Transaction entered conflicted state."""

    def __init__(self, transaction_id: str, reason: str = ""):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Transaction {transaction_id} conflicted: {reason}")


class TransactionAborted(ArtifactError):
    """Transaction was aborted."""

    def __init__(self, transaction_id: str, reason: str = ""):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Transaction {transaction_id} aborted: {reason}")


class TransactionUncertain(ArtifactError):
    """Transaction is in uncertain state after crash."""

    def __init__(self, transaction_id: str, reason: str = ""):
        self.transaction_id = transaction_id
        self.reason = reason
        super().__init__(f"Transaction {transaction_id} uncertain: {reason}")


class IdempotencyReplay(ArtifactError):
    """Idempotency key replay — original result is returned, not an error per se.

    This exception carries the original transaction so the caller can return
    the original result without re-executing.
    """

    def __init__(self, transaction_id: str, original_state: str):
        self.transaction_id = transaction_id
        self.original_state = original_state
        super().__init__(f"Idempotency replay: transaction {transaction_id} was {original_state}")
