"""Execution Attempt lifecycle (Section 22).

SUCCEEDED_OPERATIONALLY != VERIFIED_SEMANTICALLY.  The Scheduler can derive
the latter only by observing VPG task validity == VERIFIED.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from .models import AgentSnapshot, AttemptState, ContextIdentity, ScheduledExecutionAttempt


def _now() -> datetime:
    return datetime.now(UTC)


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

    def latest_attempt_for_task(self, task_id: str) -> ScheduledExecutionAttempt | None:
        ordered = sorted(
            (a for a in self._attempts.values() if a.task_id == task_id),
            key=lambda a: a.started_at,
        )
        return ordered[-1] if ordered else None

    def latest_attempt_for_claim(self, claim_id: str) -> ScheduledExecutionAttempt | None:
        ordered = sorted(
            (a for a in self._attempts.values() if a.claim_id == claim_id),
            key=lambda a: a.started_at,
        )
        return ordered[-1] if ordered else None

    # ── lifecycle ──────────────────────────────────────────────────────────
    def start_attempt(
        self,
        *,
        attempt_id: str,
        graph_id: str = "",
        graph_version: int = 0,
        semantic_epoch: int = 0,
        task_id: str,
        claim_id: str,
        agent_id: str,
        process_id: str,
        attempt_number: int = 0,
        action_ids: tuple[str, ...] = (),
    ) -> ScheduledExecutionAttempt:
        attempt = ScheduledExecutionAttempt(
            attempt_id=attempt_id,
            graph_id=graph_id,
            graph_version=graph_version,
            semantic_epoch=semantic_epoch,
            task_id=task_id,
            claim_id=claim_id,
            agent_id=agent_id,
            process_id=process_id,
            attempt_number=attempt_number,
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

    def mark_preempted(
        self,
        attempt: ScheduledExecutionAttempt,
        error: str = "",
    ) -> None:
        """Terminalize an attempt whose exact claim was handed off."""

        attempt.state = AttemptState.PREEMPTED
        attempt.ended_at = _now()
        attempt.error = error or "preempted"

    def mark_stale_cognition(
        self,
        attempt: ScheduledExecutionAttempt,
        error: str = "",
    ) -> None:
        """Quarantine an Attempt whose bound inputs are no longer current."""

        attempt.state = AttemptState.STALE_COGNITION
        attempt.ended_at = _now()
        attempt.error = error or "stale_cognition"

    def mark_operationally_succeeded(self, attempt: ScheduledExecutionAttempt) -> None:
        attempt.state = AttemptState.SUCCEEDED_OPERATIONALLY
        attempt.ended_at = _now()

    def mark_semantically_verified(self, attempt: ScheduledExecutionAttempt) -> bool:
        """Promote an operationally-successful Attempt to semantic VERIFIED.

        This is a fail-closed lifecycle transition.  A direct caller cannot
        bypass the operational-success boundary by force-promoting a
        dispatched, running, failed, crashed, or stale attempt.  Replaying an
        already verified transition is idempotent and returns ``True``.
        """

        if attempt.state == AttemptState.VERIFIED_SEMANTICALLY:
            return True
        if attempt.state != AttemptState.SUCCEEDED_OPERATIONALLY:
            return False
        attempt.state = AttemptState.VERIFIED_SEMANTICALLY
        attempt.ended_at = _now()
        return True

    def bind_provenance_digest(
        self,
        attempt: ScheduledExecutionAttempt,
        digest: str,
    ) -> bool:
        """Seal one canonical provenance digest on an attempt.

        Binding is idempotent for the same digest and rejects replacement by a
        different digest.  The scheduler calls this while holding its
        lifecycle lock, so a late worker cannot race a newer attempt.
        """

        normalized = str(digest).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", normalized):
            raise ValueError("provenance digest must be a 64-character SHA-256 hex")
        existing = attempt.provenance_digest
        if existing is not None and existing != normalized:
            return False
        attempt.provenance_digest = normalized
        return True

    def bind_context_snapshot(
        self,
        attempt: ScheduledExecutionAttempt,
        *,
        snapshot_id: str,
        manifest_id: str,
        manifest_hash: str,
        working_set_hash: str,
        materialized_hash: str,
    ) -> bool:
        """Seal one Context VM identity on an attempt."""

        binding = {
            "context_snapshot_id": str(snapshot_id).strip(),
            "context_manifest_id": str(manifest_id).strip(),
            "context_manifest_hash": str(manifest_hash).strip().lower(),
            "context_working_set_hash": str(working_set_hash).strip().lower(),
            "context_materialized_hash": str(materialized_hash).strip().lower(),
        }
        if not all(binding.values()):
            raise ValueError("context snapshot identity fields must be non-empty")
        existing = {
            field: getattr(attempt, field)
            for field in binding
            if getattr(attempt, field) is not None
        }
        if existing and existing != {
            field: value for field, value in binding.items() if field in existing
        }:
            return False
        for field, value in binding.items():
            setattr(attempt, field, value)
        return True

    def bind_agent_snapshot(
        self,
        attempt: ScheduledExecutionAttempt,
        snapshot: AgentSnapshot,
    ) -> bool:
        """Bind the first immutable runtime/cognition snapshot to an Attempt.

        Repeating the exact snapshot is idempotent. Replacement must use
        :meth:`update_agent_snapshot`, which enforces monotonic observation
        rather than silently overwriting audit state.
        """

        if not self._snapshot_matches_attempt(attempt, snapshot):
            return False
        if snapshot.state != attempt.state:
            return False
        existing = attempt.agent_snapshot
        if existing is not None:
            return existing == snapshot
        attempt.agent_snapshot = snapshot
        return True

    def update_agent_snapshot(
        self,
        attempt: ScheduledExecutionAttempt,
        snapshot: AgentSnapshot,
        *,
        expected_fingerprint: str,
    ) -> bool:
        """Replace the current snapshot under a fail-closed CAS contract.

        Progress/cumulative cost/time may only advance. Read/write observations
        may only grow, so a later update cannot erase evidence that an input or
        effect was previously observed.
        """

        existing = attempt.agent_snapshot
        if existing is None:
            return False
        if existing == snapshot:
            return expected_fingerprint == existing.fingerprint()
        if not self._snapshot_matches_attempt(attempt, snapshot):
            return False
        if snapshot.state != attempt.state:
            return False
        if existing.fingerprint() != str(expected_fingerprint).strip().lower():
            return False
        if snapshot.captured_at <= existing.captured_at:
            return False
        if snapshot.progress < existing.progress:
            return False
        if not snapshot.cost.dominates(existing.cost):
            return False
        if not set(existing.read_set).issubset(snapshot.read_set):
            return False
        if not set(existing.write_set).issubset(snapshot.write_set):
            return False
        attempt.agent_snapshot = snapshot
        return True

    @staticmethod
    def _snapshot_matches_attempt(
        attempt: ScheduledExecutionAttempt,
        snapshot: AgentSnapshot,
    ) -> bool:
        attempt_identity = (
            attempt.agent_id,
            attempt.process_id,
            attempt.task_id,
            attempt.claim_id,
            attempt.attempt_id,
            attempt.graph_id,
            attempt.graph_version,
            attempt.semantic_epoch,
            attempt.started_at,
        )
        snapshot_identity = (
            snapshot.agent_id,
            snapshot.process_id,
            snapshot.task_id,
            snapshot.claim_id,
            snapshot.attempt_id,
            snapshot.graph_id,
            snapshot.graph_version,
            snapshot.semantic_epoch,
            snapshot.started_at,
        )
        if snapshot_identity != attempt_identity:
            return False
        try:
            sealed_context = ContextIdentity.from_attempt(attempt)
        except ValueError:
            return False
        return snapshot.context_identity == sealed_context

    def count_attempts_for_task(self, task_id: str) -> int:
        return sum(1 for a in self._attempts.values() if a.task_id == task_id)

    def count_attempts_for_epoch(
        self,
        graph_id: str,
        task_id: str,
        semantic_epoch: int,
    ) -> int:
        return sum(
            1
            for attempt in self._attempts.values()
            if attempt.graph_id == graph_id
            and attempt.task_id == task_id
            and attempt.semantic_epoch == semantic_epoch
        )
