"""Checkpoint manager port (spec section 16.1)."""

from typing import Protocol


class CheckpointManager(Protocol):
    def create(self, run_id: str, reason: str) -> str:
        """Create a checkpoint and return its id."""
        ...

    def restore(self, checkpoint_id: str) -> None:
        """Restore the environment to the checkpoint."""
        ...
