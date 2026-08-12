"""Durable Scheduler state and event journal.

The multi-agent scheduler keeps a small, rebuildable projection in memory.
When a ``state_path`` is supplied, this module makes the projection durable:
each scheduler event and the corresponding claims/attempts snapshot are
committed in one SQLite transaction.  A restart verifies the append-only
event hash chain and the snapshot hash before exposing state to the caller.

This is intentionally Scheduler-owned storage.  Kernel leases/processes and
VPG graph history remain authoritative in their respective runtimes and are
still reconciled after restart.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .events import SchedulerEvent
from .models import MatchDecision, ScheduledExecutionAttempt, TaskClaim

_GENESIS_HASH = "0" * 64


class SchedulerStateCorruption(RuntimeError):
    """Raised when durable Scheduler state cannot be trusted."""


@dataclass
class SchedulerState:
    """Decoded Scheduler state loaded from a durable store."""

    claims: list[TaskClaim]
    attempts: list[ScheduledExecutionAttempt]
    match_log: list[MatchDecision]
    idempotent_keys: set[str]
    events: list[SchedulerEvent]


def _canonical_json(value: Any) -> str:
    """Encode JSON with one stable representation for hashing and storage."""
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _hash_event(previous_hash: str, event_json: str) -> str:
    return hashlib.sha256(f"{previous_hash}:{event_json}".encode()).hexdigest()


def _hash_state(state_json: str) -> str:
    return hashlib.sha256(state_json.encode("utf-8")).hexdigest()


class SchedulerStateStore:
    """SQLite-backed event journal plus latest projection snapshot.

    The store is deliberately independent from ``AgentKernel``'s storage
    wrapper so it can be used with an arbitrary VPG/Kernel provider setup.
    A file path may point at the same SQLite database as the Kernel; tables
    are namespaced with ``scheduler_``.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.db_path,
            isolation_level=None,
            check_same_thread=False,
            timeout=5.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS scheduler_events (
                    event_seq INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    event_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduler_snapshot (
                    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
                    state_json TEXT NOT NULL,
                    state_hash TEXT NOT NULL,
                    last_event_seq INTEGER NOT NULL,
                    last_event_hash TEXT NOT NULL
                );
                """
            )

    @property
    def path(self) -> str:
        return self.db_path

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def load(self) -> SchedulerState:
        """Verify and load the latest snapshot and complete event history.

        Any malformed row, gap, hash mismatch, or snapshot/event mismatch is
        fail-closed via ``SchedulerStateCorruption``.
        """
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT event_seq, event_id, event_json, event_hash "
                    "FROM scheduler_events ORDER BY event_seq"
                ).fetchall()
                events: list[SchedulerEvent] = []
                previous = _GENESIS_HASH
                expected_seq = 1
                for row in rows:
                    if int(row["event_seq"]) != expected_seq:
                        raise SchedulerStateCorruption(
                            f"scheduler event sequence gap at {row['event_seq']}"
                        )
                    event_json = str(row["event_json"])
                    event_hash = _hash_event(previous, event_json)
                    if event_hash != row["event_hash"]:
                        raise SchedulerStateCorruption(
                            f"scheduler event hash mismatch at sequence {expected_seq}"
                        )
                    try:
                        payload = json.loads(event_json)
                        event = SchedulerEvent.model_validate(payload)
                    except Exception as exc:
                        raise SchedulerStateCorruption(
                            f"invalid scheduler event at sequence {expected_seq}"
                        ) from exc
                    if event.event_id != row["event_id"]:
                        raise SchedulerStateCorruption(
                            f"scheduler event id mismatch at sequence {expected_seq}"
                        )
                    events.append(event)
                    previous = event_hash
                    expected_seq += 1

                snapshot = self._conn.execute(
                    "SELECT state_json, state_hash, last_event_seq, last_event_hash "
                    "FROM scheduler_snapshot WHERE snapshot_id = 1"
                ).fetchone()
                if snapshot is None:
                    if rows:
                        raise SchedulerStateCorruption(
                            "scheduler events exist but projection snapshot is missing"
                        )
                    return SchedulerState([], [], [], set(), [])

                state_json = str(snapshot["state_json"])
                if _hash_state(state_json) != snapshot["state_hash"]:
                    raise SchedulerStateCorruption("scheduler projection snapshot hash mismatch")
                last_seq = int(snapshot["last_event_seq"])
                last_hash = str(snapshot["last_event_hash"])
                if last_seq != len(rows) or last_hash != previous:
                    raise SchedulerStateCorruption(
                        "scheduler snapshot does not match the event journal tail"
                    )
                try:
                    state_payload = json.loads(state_json)
                    claims = [
                        TaskClaim.model_validate(item) for item in state_payload.get("claims", [])
                    ]
                    attempts = [
                        ScheduledExecutionAttempt.model_validate(item)
                        for item in state_payload.get("attempts", [])
                    ]
                    match_log = [
                        MatchDecision.model_validate(item)
                        for item in state_payload.get("match_log", [])
                    ]
                    idempotent_keys = {
                        str(item) for item in state_payload.get("idempotent_keys", [])
                    }
                except Exception as exc:
                    raise SchedulerStateCorruption(
                        "invalid scheduler projection snapshot payload"
                    ) from exc
                return SchedulerState(
                    claims=claims,
                    attempts=attempts,
                    match_log=match_log,
                    idempotent_keys=idempotent_keys,
                    events=events,
                )
            except SchedulerStateCorruption:
                raise
            except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
                raise SchedulerStateCorruption("unable to read scheduler durable state") from exc

    def append_event(
        self,
        event: SchedulerEvent,
        *,
        claims: Iterable[TaskClaim],
        attempts: Iterable[ScheduledExecutionAttempt],
        match_log: Iterable[MatchDecision],
        idempotent_keys: Iterable[str],
    ) -> None:
        """Atomically append ``event`` and publish the resulting projection."""
        self.append_events(
            (event,),
            claims=claims,
            attempts=attempts,
            match_log=match_log,
            idempotent_keys=idempotent_keys,
        )

    def append_events(
        self,
        events: Iterable[SchedulerEvent],
        *,
        claims: Iterable[TaskClaim],
        attempts: Iterable[ScheduledExecutionAttempt],
        match_log: Iterable[MatchDecision],
        idempotent_keys: Iterable[str],
    ) -> None:
        """Atomically append an ordered event batch and publish one snapshot."""
        event_batch = list(events)
        if not event_batch:
            self.persist_state(
                claims=claims,
                attempts=attempts,
                match_log=match_log,
                idempotent_keys=idempotent_keys,
            )
            return
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                tail = self._conn.execute(
                    "SELECT event_seq, event_hash FROM scheduler_events "
                    "ORDER BY event_seq DESC LIMIT 1"
                ).fetchone()
                previous_seq = int(tail["event_seq"]) if tail else 0
                previous_hash = str(tail["event_hash"]) if tail else _GENESIS_HASH
                event_seq = previous_seq
                event_hash = previous_hash
                for event in event_batch:
                    event_json = _canonical_json(event.model_dump(mode="json"))
                    existing = self._conn.execute(
                        "SELECT event_seq, event_json, event_hash FROM scheduler_events "
                        "WHERE event_id = ?",
                        (event.event_id,),
                    ).fetchone()
                    if existing is not None:
                        existing_seq = int(existing["event_seq"])
                        predecessor = (
                            _GENESIS_HASH
                            if existing_seq == 1
                            else str(
                                self._conn.execute(
                                    "SELECT event_hash FROM scheduler_events WHERE event_seq = ?",
                                    (existing_seq - 1,),
                                ).fetchone()["event_hash"]
                            )
                        )
                        if str(existing["event_json"]) != event_json or str(
                            existing["event_hash"]
                        ) != _hash_event(predecessor, event_json):
                            raise SchedulerStateCorruption(
                                f"duplicate scheduler event id {event.event_id!r} "
                                "has different payload"
                            )
                        continue
                    event_seq += 1
                    event_hash = _hash_event(event_hash, event_json)
                    self._conn.execute(
                        "INSERT INTO scheduler_events "
                        "(event_seq, event_id, event_json, event_hash, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            event_seq,
                            event.event_id,
                            event_json,
                            event_hash,
                            event.created_at.isoformat(),
                        ),
                    )
                self._write_snapshot(
                    state_json=self._state_json(
                        claims=claims,
                        attempts=attempts,
                        match_log=match_log,
                        idempotent_keys=idempotent_keys,
                    ),
                    last_event_seq=event_seq,
                    last_event_hash=event_hash,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def persist_state(
        self,
        *,
        claims: Iterable[TaskClaim],
        attempts: Iterable[ScheduledExecutionAttempt],
        match_log: Iterable[MatchDecision],
        idempotent_keys: Iterable[str],
    ) -> None:
        """Persist a projection change that has no new event (e.g. idempotency cleanup)."""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                tail = self._conn.execute(
                    "SELECT event_seq, event_hash FROM scheduler_events "
                    "ORDER BY event_seq DESC LIMIT 1"
                ).fetchone()
                self._write_snapshot(
                    state_json=self._state_json(
                        claims=claims,
                        attempts=attempts,
                        match_log=match_log,
                        idempotent_keys=idempotent_keys,
                    ),
                    last_event_seq=int(tail["event_seq"]) if tail else 0,
                    last_event_hash=str(tail["event_hash"]) if tail else _GENESIS_HASH,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def event_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM scheduler_events").fetchone()
            return int(row["n"]) if row else 0

    def _write_snapshot(
        self,
        *,
        state_json: str,
        last_event_seq: int,
        last_event_hash: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO scheduler_snapshot
                (snapshot_id, state_json, state_hash, last_event_seq, last_event_hash)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id) DO UPDATE SET
                state_json = excluded.state_json,
                state_hash = excluded.state_hash,
                last_event_seq = excluded.last_event_seq,
                last_event_hash = excluded.last_event_hash
            """,
            (state_json, _hash_state(state_json), last_event_seq, last_event_hash),
        )

    @staticmethod
    def _state_json(
        *,
        claims: Iterable[TaskClaim],
        attempts: Iterable[ScheduledExecutionAttempt],
        match_log: Iterable[MatchDecision],
        idempotent_keys: Iterable[str],
    ) -> str:
        payload = {
            "claims": [claim.model_dump(mode="json") for claim in claims],
            "attempts": [attempt.model_dump(mode="json") for attempt in attempts],
            "match_log": [decision.model_dump(mode="json") for decision in match_log],
            "idempotent_keys": sorted(set(str(key) for key in idempotent_keys)),
        }
        return _canonical_json(payload)


__all__ = ["SchedulerState", "SchedulerStateCorruption", "SchedulerStateStore"]
