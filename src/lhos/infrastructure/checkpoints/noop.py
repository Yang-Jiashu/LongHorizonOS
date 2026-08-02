"""No-op checkpoint manager: the baseline (spec 16.2)."""

from uuid import uuid4


class NoopCheckpointManager:
    checkpoint_type = "noop"

    def create(self, run_id: str, reason: str) -> str:
        return f"noop-{uuid4().hex[:12]}"

    def record(self, run_id: str, reason: str) -> str:
        """Pre-execution snapshot point (nothing to record for noop)."""
        return f"noop-{uuid4().hex[:12]}"

    def restore(self, checkpoint_id: str) -> None:
        return None
