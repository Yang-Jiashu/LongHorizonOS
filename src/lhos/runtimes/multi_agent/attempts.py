"""Execution Attempt lifecycle (Section 22).

SUCCEEDED_OPERATIONALLY != VERIFIED_SEMANTICALLY.  The Scheduler can derive
the latter only by observing VPG task validity == VERIFIED.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import AttemptState, ScheduledExecutionAttempt


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AttemptManager:
    """Owns execution-attempt records for auditing and replay."""

    def __init__(self) -> None:
        self._attempts: dict[str, ScheduledExecutionAttempt] = {}

    def _book(self, attempt: ScheduledExecutionAttempt) -> ScheduledExecutionAttempt:
        self._attempts[attempt.attempt_id] = attempt
        return attempt

    def get(self, attempt_id: str) -> ScheduledExecutionAttempt | None:
        return self._attempts.get(attempt_id)

    def all_attempts(self) -> list[ScheduledExecutionAttempt]:
        return list(self._attempts.values())

    def attempts_for_task(self, task_id: str) -> list[ScheduledExecutionAttempt]:
        return [a for a in self._attempts.values() if a.task_id == task_id]

    def attempts_for_agent(self, agent_id: str) -> list[ScheduledExecutionAttempt]:
        return [a for a in self._attempts.values() if a.agent_id == agent_id]

    def latest_attempt_for_task(
        self, task_id: str
    ) -> ScheduledExecutionAttempt | None:
        ordered = sorted(
            (a for a in self._attempts.values() if a.task_id == task_id),
            key=lambda a: a.started_at,
        )
        return ordered[-1] if ordered else None

    # ── lifecycle ──────────────────────────────────────────────────────────
    def start_attempt(
        self,
        *,
        attempt_id: str,
        task_id: str,
        claim_id: str,
        agent_id: str,
        process_id: str,
        action_ids: tuple[str, ...] = (),
    ) -> ScheduledExecutionAttempt:
        attempt = ScheduledExecutionAttempt(
            attempt_id=attempt_id,
            task_id=task_id,
            claim_id=claim_id,
            agent_id=agent_id,
            process_id=process_id,
            action_ids=action_ids,
            state=AttemptState.DISPATCHED,
            started_at=_now(),
        )
        return self._book(attempt)

    def mark_running(self, attempt: ScheduledExecutionAttempt) -> None:
        attempt.state = AttemptState.RUNNING

    def mark_crashed(self, attempt: ScheduledExecutionAttempt, error: str = "") -> None:
        attempt.state = AttemptState.CRASHED
        attempt.ended_at = _now()
        attempt.error = error or "crashed"

    def mark_failed(self, attempt: ScheduledExecutionAttempt, error: str = "") -> None:
        attempt.state = AttemptState.FAILED
        attempt.ended_at = _now()
        attempt.error = error or "failed"

    def mark_operationally_succeeded(
        self, attempt: ScheduledExecutionAttempt
    ) -> None:
        attempt.state = AttemptState.SUCCEEDED_OPERATIONALLY
        attempt.ended_at = _now()

    def mark_semantically_verified(
        self, attempt: ScheduledExecutionAttempt
    ) -> None:
        """Promote an operationally-successful attempt to semantically
        verified after the VPG derives Task -> VERIFIED."""
        if attempt.state != AttemptState.SUCCEEDED_OPERATIONALLY:
            # Defensive: we still record the fact, but flag it so a later
            # auditor sees the sequence.
            attempt.error = (
                f"forced semantic verification from {attempt.state.value}"
            )
        attempt.state = AttemptState.VERIFIED_SEMANTICALLY
        attempt.ended_at = _now()

    def count_attempts_for_task(self, task_id: str) -> int:
        return sum(1 for a in self._attempts.values() if a.task_id == task_id)
