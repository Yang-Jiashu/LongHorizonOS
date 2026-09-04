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
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .events import SchedulerEvent
from .models import ClaimState, MatchDecision, ScheduledExecutionAttempt, TaskClaim

_GENESIS_HASH = "0" * 64
# How many writes may verify only the newly appended run before one write
# re-verifies the whole history.  This bounds how long corruption introduced
# *outside* this API (a manual database edit, media failure) can be carried
# forward by later writes.  Amortized validation work per write is O(events/K),
# so K trades detection latency against cost; 64 keeps a ~64x reduction while
# holding the exposure window short, which matters because this is the
# Scheduler's fail-closed audit path.
SCRUB_EVERY_WRITES = 64
_NORMALIZED_PROJECTION_ENCODING = "normalized-v1"
# Small projections remain inline for backwards-compatible inspection and
# minimal table overhead. Once normalized, a store never switches back.
_NORMALIZED_PROJECTION_MIN_ITEMS = 64


class SchedulerStateCorruption(RuntimeError):
    """Raised when durable Scheduler state cannot be trusted."""


@dataclass(frozen=True, slots=True)
class _SnapshotToken:
    """One compare-and-swap generation of the durable Scheduler projection."""

    generation: int
    state_hash: str
    last_event_seq: int
    last_event_hash: str


class _UnboundSnapshot:
    """Marker for a Store that has not loaded or created durable state yet."""


_UNBOUND_SNAPSHOT = _UnboundSnapshot()


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


def _require_list(payload: dict[str, Any], field: str) -> list[Any]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise ValueError(f"scheduler projection field {field!r} must be a list")
    return value


def _require_count(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"scheduler projection field {field!r} must be a non-negative integer")
    return value


def _projection_hash(
    *,
    claims: Iterable[tuple[str, str]],
    attempts: Iterable[tuple[str, str]],
    match_log: Iterable[str],
    idempotent_keys: Iterable[str],
) -> str:
    """Hash one logical normalized projection without rebuilding its full JSON."""

    return _hash_state(
        _canonical_json(
            {
                "encoding": _NORMALIZED_PROJECTION_ENCODING,
                "claims": [list(item) for item in claims],
                "attempts": [list(item) for item in attempts],
                "match_log": list(match_log),
                "idempotent_keys": list(idempotent_keys),
            }
        )
    )


def _validate_projection_invariants(
    *,
    claims: list[TaskClaim],
    attempts: list[ScheduledExecutionAttempt],
) -> None:
    """Reject projection shapes the in-memory managers cannot represent safely."""

    claims_by_id: dict[str, TaskClaim] = {}
    active_by_task: dict[tuple[str, str], str] = {}
    for claim in claims:
        if claim.claim_id in claims_by_id:
            raise ValueError(f"duplicate durable claim id {claim.claim_id!r}")
        claims_by_id[claim.claim_id] = claim
        if claim.state == ClaimState.ACTIVE:
            task_key = (claim.graph_id, claim.task_id)
            previous = active_by_task.get(task_key)
            if previous is not None:
                raise ValueError(
                    "multiple ACTIVE durable claims for "
                    f"graph/task {task_key!r}: {previous!r}, {claim.claim_id!r}"
                )
            active_by_task[task_key] = claim.claim_id

    attempt_ids: set[str] = set()
    for attempt in attempts:
        if attempt.attempt_id in attempt_ids:
            raise ValueError(f"duplicate durable attempt id {attempt.attempt_id!r}")
        attempt_ids.add(attempt.attempt_id)

        referenced_claim = claims_by_id.get(attempt.claim_id)
        if referenced_claim is None:
            raise ValueError(
                f"durable attempt {attempt.attempt_id!r} references missing "
                f"claim {attempt.claim_id!r}"
            )
        attempt_identity = (
            attempt.graph_id,
            attempt.graph_version,
            attempt.task_id,
            attempt.agent_id,
            attempt.process_id,
            attempt.attempt_number,
        )
        claim_identity = (
            referenced_claim.graph_id,
            referenced_claim.graph_version,
            referenced_claim.task_id,
            referenced_claim.agent_id,
            referenced_claim.process_id,
            referenced_claim.attempt_number,
        )
        if attempt_identity != claim_identity:
            raise ValueError(
                f"durable attempt {attempt.attempt_id!r} identity disagrees "
                f"with claim {referenced_claim.claim_id!r}"
            )


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
        self._expected_snapshot_token: _SnapshotToken | _UnboundSnapshot | None = _UNBOUND_SNAPSHOT
        # Watermark of the event prefix this instance has already verified
        # row by row, so a write only has to verify what arrived since.  Full
        # verification still runs on ``load()``, on any write-token mismatch,
        # and every ``SCRUB_EVERY_WRITES`` writes.  See ``_validated_tail``.
        self._validated_seq = 0
        self._validated_hash = _GENESIS_HASH
        self._writes_since_scrub = 0

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
                    last_event_hash TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0)
                );
                CREATE TABLE IF NOT EXISTS scheduler_claims_projection (
                    claim_id TEXT PRIMARY KEY,
                    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduler_attempts_projection (
                    attempt_id TEXT PRIMARY KEY,
                    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduler_match_log_projection (
                    ordinal INTEGER PRIMARY KEY CHECK (ordinal >= 0),
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduler_idempotency_projection (
                    idempotency_key TEXT PRIMARY KEY
                );
                """
            )
            columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(scheduler_snapshot)")
            }
            if "generation" not in columns:
                # Existing inline-snapshot databases predate writer CAS. Their
                # current projection becomes generation zero and remains fully
                # readable; the first guarded write advances it to generation 1.
                self._conn.execute(
                    "ALTER TABLE scheduler_snapshot "
                    "ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
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
                # WAL readers otherwise get one snapshot per SELECT in
                # autocommit mode. A read transaction pins event tail,
                # manifest, and all normalized rows to one database version.
                self._conn.execute("BEGIN")
                state, token = self._load_in_transaction()
                self._conn.execute("COMMIT")
            except SchedulerStateCorruption:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                raise
            except (sqlite3.DatabaseError, ValueError, TypeError) as exc:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                raise SchedulerStateCorruption("unable to read scheduler durable state") from exc
            except BaseException:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                raise
            self._expected_snapshot_token = token
            return state

    def _load_in_transaction(
        self,
    ) -> tuple[SchedulerState, _SnapshotToken | None]:
        events, last_event_seq, previous = self._read_validated_events()

        snapshot = self._conn.execute(
            "SELECT state_json, state_hash, last_event_seq, last_event_hash, generation "
            "FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        if snapshot is None:
            if last_event_seq:
                raise SchedulerStateCorruption(
                    "scheduler events exist but projection snapshot is missing"
                )
            if self._normalized_projection_has_rows():
                raise SchedulerStateCorruption(
                    "scheduler normalized projection rows exist but snapshot is missing"
                )
            return SchedulerState([], [], [], set(), []), None

        state_json = str(snapshot["state_json"])
        state_hash = str(snapshot["state_hash"])
        if _hash_state(state_json) != state_hash:
            raise SchedulerStateCorruption("scheduler projection snapshot hash mismatch")
        last_seq = int(snapshot["last_event_seq"])
        last_hash = str(snapshot["last_event_hash"])
        generation = int(snapshot["generation"])
        if generation < 0:
            raise SchedulerStateCorruption("scheduler snapshot generation is invalid")
        if last_seq != last_event_seq or last_hash != previous:
            raise SchedulerStateCorruption(
                "scheduler snapshot does not match the event journal tail"
            )
        try:
            state_payload = json.loads(state_json)
            if not isinstance(state_payload, dict):
                raise ValueError("scheduler projection snapshot must be an object")
            encoding = state_payload.get("encoding")
            if encoding == _NORMALIZED_PROJECTION_ENCODING:
                claims, attempts, match_log, idempotent_keys = self._load_normalized_projection(
                    state_payload
                )
            elif encoding is None:
                # A store switches to normalized rows permanently.  If an
                # inline manifest is paired with any normalized rows, the
                # database is in a hybrid state (typically due to tampering
                # or an interrupted/manual migration).  Do not silently
                # ignore those rows and expose a partial projection.
                if self._normalized_projection_has_rows():
                    raise SchedulerStateCorruption(
                        "scheduler inline snapshot has residual normalized projection rows"
                    )
                claims = [
                    TaskClaim.model_validate(item)
                    for item in _require_list(state_payload, "claims")
                ]
                attempts = [
                    ScheduledExecutionAttempt.model_validate(item)
                    for item in _require_list(state_payload, "attempts")
                ]
                match_log = [
                    MatchDecision.model_validate(item)
                    for item in _require_list(state_payload, "match_log")
                ]
                raw_idempotent_keys = _require_list(
                    state_payload,
                    "idempotent_keys",
                )
                if not all(isinstance(item, str) for item in raw_idempotent_keys):
                    raise ValueError("scheduler idempotent keys must be strings")
                idempotent_keys = set(raw_idempotent_keys)
            else:
                raise ValueError(f"unknown scheduler projection encoding {encoding!r}")
            _validate_projection_invariants(claims=claims, attempts=attempts)
        except Exception as exc:
            if isinstance(exc, SchedulerStateCorruption):
                raise
            raise SchedulerStateCorruption("invalid scheduler projection snapshot payload") from exc
        state = SchedulerState(
            claims=claims,
            attempts=attempts,
            match_log=match_log,
            idempotent_keys=idempotent_keys,
            events=events,
        )
        token = _SnapshotToken(
            generation=generation,
            state_hash=state_hash,
            last_event_seq=last_seq,
            last_event_hash=last_hash,
        )
        return state, token

    def _read_validated_events(self) -> tuple[list[SchedulerEvent], int, str]:
        """Read and verify the complete append-only event history.

        This is the authoritative full check.  It runs on ``load()``, on
        ``scrub()``, whenever a write-token mismatch means another writer may
        have touched the database, and periodically from the write path.
        """

        events, last_seq, tail_hash = self._verify_events_from(
            after_seq=0, previous_hash=_GENESIS_HASH
        )
        self._validated_seq = last_seq
        self._validated_hash = tail_hash
        self._writes_since_scrub = 0
        return events, last_seq, tail_hash

    def _verify_events_from(
        self,
        *,
        after_seq: int,
        previous_hash: str,
    ) -> tuple[list[SchedulerEvent], int, str]:
        """Verify the contiguous run of events after ``after_seq``.

        Returns the decoded run, the last sequence seen (``after_seq`` when the
        run is empty) and the resulting tail hash.  The hash chain makes this
        resumable: given a verified prefix and its tail hash, verifying the rest
        proves the whole history.
        """

        rows = self._conn.execute(
            "SELECT event_seq, event_id, event_json, event_hash, created_at "
            "FROM scheduler_events WHERE event_seq > ? ORDER BY event_seq",
            (after_seq,),
        ).fetchall()
        events: list[SchedulerEvent] = []
        previous = previous_hash
        expected_seq = after_seq + 1
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
            if event.created_at.isoformat() != str(row["created_at"]):
                raise SchedulerStateCorruption(
                    f"scheduler event timestamp mismatch at sequence {expected_seq}"
                )
            events.append(event)
            previous = event_hash
            expected_seq += 1
        return events, expected_seq - 1, previous

    def _normalized_projection_has_rows(self) -> bool:
        for table in (
            "scheduler_claims_projection",
            "scheduler_attempts_projection",
            "scheduler_match_log_projection",
            "scheduler_idempotency_projection",
        ):
            if self._conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
                return True
        return False

    def _load_normalized_projection(
        self,
        manifest: dict[str, Any],
    ) -> tuple[
        list[TaskClaim],
        list[ScheduledExecutionAttempt],
        list[MatchDecision],
        set[str],
    ]:
        """Load and hash-verify a normalized projection referenced by a manifest."""

        expected_claims = _require_count(manifest, "claim_count")
        expected_attempts = _require_count(manifest, "attempt_count")
        expected_matches = _require_count(manifest, "match_log_count")
        expected_keys = _require_count(manifest, "idempotency_key_count")
        expected_hash = manifest.get("projection_hash")
        if not isinstance(expected_hash, str) or len(expected_hash) != 64:
            raise SchedulerStateCorruption(
                "scheduler normalized projection manifest has an invalid hash"
            )

        claims: list[TaskClaim] = []
        claim_refs: list[tuple[str, str]] = []
        claim_rows = self._conn.execute(
            "SELECT claim_id, ordinal, payload_json, payload_hash "
            "FROM scheduler_claims_projection ORDER BY ordinal"
        ).fetchall()
        if len(claim_rows) != expected_claims:
            raise SchedulerStateCorruption("scheduler normalized claim projection count mismatch")
        for expected_ordinal, row in enumerate(claim_rows):
            if int(row["ordinal"]) != expected_ordinal:
                raise SchedulerStateCorruption(
                    "scheduler normalized claim projection order is not contiguous"
                )
            claim_id = str(row["claim_id"])
            payload_json = str(row["payload_json"])
            payload_hash = str(row["payload_hash"])
            if _hash_state(payload_json) != payload_hash:
                raise SchedulerStateCorruption(
                    f"scheduler normalized claim payload hash mismatch for {claim_id!r}"
                )
            claim = TaskClaim.model_validate(json.loads(payload_json))
            if claim.claim_id != claim_id:
                raise SchedulerStateCorruption(
                    f"scheduler normalized claim identity mismatch for {claim_id!r}"
                )
            claims.append(claim)
            claim_refs.append((claim_id, payload_hash))

        attempts: list[ScheduledExecutionAttempt] = []
        attempt_refs: list[tuple[str, str]] = []
        attempt_rows = self._conn.execute(
            "SELECT attempt_id, ordinal, payload_json, payload_hash "
            "FROM scheduler_attempts_projection ORDER BY ordinal"
        ).fetchall()
        if len(attempt_rows) != expected_attempts:
            raise SchedulerStateCorruption("scheduler normalized attempt projection count mismatch")
        for expected_ordinal, row in enumerate(attempt_rows):
            if int(row["ordinal"]) != expected_ordinal:
                raise SchedulerStateCorruption(
                    "scheduler normalized attempt projection order is not contiguous"
                )
            attempt_id = str(row["attempt_id"])
            payload_json = str(row["payload_json"])
            payload_hash = str(row["payload_hash"])
            if _hash_state(payload_json) != payload_hash:
                raise SchedulerStateCorruption(
                    f"scheduler normalized attempt payload hash mismatch for {attempt_id!r}"
                )
            attempt = ScheduledExecutionAttempt.model_validate(json.loads(payload_json))
            if attempt.attempt_id != attempt_id:
                raise SchedulerStateCorruption(
                    f"scheduler normalized attempt identity mismatch for {attempt_id!r}"
                )
            attempts.append(attempt)
            attempt_refs.append((attempt_id, payload_hash))

        match_log: list[MatchDecision] = []
        match_refs: list[str] = []
        match_rows = self._conn.execute(
            "SELECT ordinal, payload_json, payload_hash "
            "FROM scheduler_match_log_projection ORDER BY ordinal"
        ).fetchall()
        if len(match_rows) != expected_matches:
            raise SchedulerStateCorruption("scheduler normalized match projection count mismatch")
        for expected_ordinal, row in enumerate(match_rows):
            if int(row["ordinal"]) != expected_ordinal:
                raise SchedulerStateCorruption(
                    "scheduler normalized match projection order is not contiguous"
                )
            payload_json = str(row["payload_json"])
            payload_hash = str(row["payload_hash"])
            if _hash_state(payload_json) != payload_hash:
                raise SchedulerStateCorruption("scheduler normalized match payload hash mismatch")
            match_log.append(MatchDecision.model_validate(json.loads(payload_json)))
            match_refs.append(payload_hash)

        key_rows = self._conn.execute(
            "SELECT idempotency_key FROM scheduler_idempotency_projection ORDER BY idempotency_key"
        ).fetchall()
        idempotent_keys = [str(row["idempotency_key"]) for row in key_rows]
        if len(idempotent_keys) != expected_keys:
            raise SchedulerStateCorruption(
                "scheduler normalized idempotency projection count mismatch"
            )

        actual_hash = _projection_hash(
            claims=claim_refs,
            attempts=attempt_refs,
            match_log=match_refs,
            idempotent_keys=idempotent_keys,
        )
        if actual_hash != expected_hash:
            raise SchedulerStateCorruption("scheduler normalized projection hash mismatch")
        return claims, attempts, match_log, set(idempotent_keys)

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
                previous_seq, previous_hash = self._event_tail()
                generation = self._assert_write_token(
                    last_event_seq=previous_seq,
                    last_event_hash=previous_hash,
                )
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
                new_token = self._write_projection(
                    claims=claims,
                    attempts=attempts,
                    match_log=match_log,
                    idempotent_keys=idempotent_keys,
                    last_event_seq=event_seq,
                    last_event_hash=event_hash,
                    generation=generation + 1,
                )
                self._conn.execute("COMMIT")
                self._expected_snapshot_token = new_token
            except Exception:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                # The prefix this instance thought it had verified may not be
                # the prefix that is stored -- another writer may have appended,
                # or a guard may have found tampering.  Re-verify from genesis.
                self._invalidate_validated_prefix()
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
                last_event_seq, last_event_hash = self._event_tail()
                generation = self._assert_write_token(
                    last_event_seq=last_event_seq,
                    last_event_hash=last_event_hash,
                )
                new_token = self._write_projection(
                    claims=claims,
                    attempts=attempts,
                    match_log=match_log,
                    idempotent_keys=idempotent_keys,
                    last_event_seq=last_event_seq,
                    last_event_hash=last_event_hash,
                    generation=generation + 1,
                )
                self._conn.execute("COMMIT")
                self._expected_snapshot_token = new_token
            except Exception:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                # The prefix this instance thought it had verified may not be
                # the prefix that is stored -- another writer may have appended,
                # or a guard may have found tampering.  Re-verify from genesis.
                self._invalidate_validated_prefix()
                raise

    def _event_tail(self) -> tuple[int, str]:
        """Verify up to the tail and return ``(last_seq, tail_hash)``.

        Verifying the entire history on every write made one write O(events) and
        a run of N writes O(N^2): measured 1.37ms per append at 100 events rising
        to 5.84ms at 800.  This verifies only the run appended since this
        instance last checked.

        The safety delta is deliberate and bounded.  A concurrent writer going
        through this API bumps the snapshot generation, so ``_assert_write_token``
        rejects this writer and the next ``load()`` re-verifies everything.  What
        is no longer caught *at the next write* is corruption introduced by
        something bypassing the API entirely -- a manual database edit or media
        failure -- which is now caught on the next ``load()`` or by ``scrub()``,
        including the periodic scrub below.
        """

        if self._writes_since_scrub >= SCRUB_EVERY_WRITES:
            _events, last_seq, tail_hash = self._read_validated_events()
            return last_seq, tail_hash
        _events, last_seq, tail_hash = self._verify_events_from(
            after_seq=self._validated_seq,
            previous_hash=self._validated_hash,
        )
        self._validated_seq = last_seq
        self._validated_hash = tail_hash
        self._writes_since_scrub += 1
        return last_seq, tail_hash

    def scrub(self) -> int:
        """Re-verify the whole event history now. Returns the events checked.

        Exposed because the write path only verifies newly appended events; this
        is how an operator forces the complete check without reloading state.
        """

        with self._lock:
            try:
                self._conn.execute("BEGIN")
                events, _last_seq, _tail = self._read_validated_events()
                self._conn.execute("COMMIT")
            except BaseException:
                with suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")
                raise
        return len(events)

    def _invalidate_validated_prefix(self) -> None:
        """Force the next verification to start from genesis.

        Called when another writer may have changed the database, because the
        prefix this instance believes it verified may no longer be the prefix
        that is stored.
        """

        self._validated_seq = 0
        self._validated_hash = _GENESIS_HASH
        self._writes_since_scrub = 0

    def _assert_write_token(
        self,
        *,
        last_event_seq: int,
        last_event_hash: str,
    ) -> int:
        """Reject a writer whose last loaded/committed snapshot is no longer current."""

        row = self._conn.execute(
            "SELECT state_json, state_hash, last_event_seq, last_event_hash, generation "
            "FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        if row is None:
            if (
                last_event_seq != 0
                or last_event_hash != _GENESIS_HASH
                or self._normalized_projection_has_rows()
            ):
                raise SchedulerStateCorruption(
                    "scheduler backing state exists without a projection snapshot"
                )
            current: _SnapshotToken | None = None
            current_generation = 0
        else:
            state_json = str(row["state_json"])
            state_hash = str(row["state_hash"])
            if _hash_state(state_json) != state_hash:
                raise SchedulerStateCorruption(
                    "scheduler projection snapshot hash mismatch before write"
                )
            # Validate the manifest and every referenced normalized row before
            # any event or projection mutation.  Without this check a same
            # process writer could accidentally "repair" a tampered row from
            # its in-memory state and bless corrupted durable data.
            self._validate_snapshot_projection_before_write(state_json)
            current_generation = int(row["generation"])
            if current_generation < 0:
                raise SchedulerStateCorruption(
                    "scheduler snapshot generation is invalid before write"
                )
            current = _SnapshotToken(
                generation=current_generation,
                state_hash=state_hash,
                last_event_seq=int(row["last_event_seq"]),
                last_event_hash=str(row["last_event_hash"]),
            )
            if (
                current.last_event_seq != last_event_seq
                or current.last_event_hash != last_event_hash
            ):
                raise SchedulerStateCorruption(
                    "scheduler snapshot does not match the event journal tail before write"
                )

        expected = self._expected_snapshot_token
        if isinstance(expected, _UnboundSnapshot):
            if current is not None:
                raise SchedulerStateCorruption(
                    "SchedulerStateStore must load existing state before writing"
                )
        elif current != expected:
            raise SchedulerStateCorruption(
                "stale scheduler writer lost the snapshot compare-and-swap"
            )
        return current_generation

    def _validate_snapshot_projection_before_write(self, state_json: str) -> None:
        """Fail closed if the current manifest/rows are not internally valid.

        This is deliberately performed inside the writer transaction before
        inserting events or synchronizing normalized rows.  It protects
        against silently overwriting externally tampered projection rows.
        """

        try:
            state_payload = json.loads(state_json)
            if not isinstance(state_payload, dict):
                raise ValueError("scheduler projection snapshot must be an object")
            encoding = state_payload.get("encoding")
            if encoding == _NORMALIZED_PROJECTION_ENCODING:
                claims, attempts, _match_log, _idempotent_keys = self._load_normalized_projection(
                    state_payload
                )
            elif encoding is None:
                if self._normalized_projection_has_rows():
                    raise SchedulerStateCorruption(
                        "scheduler inline snapshot has residual normalized projection rows"
                    )
                claims = [
                    TaskClaim.model_validate(item)
                    for item in _require_list(state_payload, "claims")
                ]
                attempts = [
                    ScheduledExecutionAttempt.model_validate(item)
                    for item in _require_list(state_payload, "attempts")
                ]
                # Decode every inline collection as the normal load path does.
                # Merely checking Claim/Attempt invariants would allow a
                # hash-rewritten but malformed match log or key set to be
                # silently replaced by the next writer.
                _ = [
                    MatchDecision.model_validate(item)
                    for item in _require_list(state_payload, "match_log")
                ]
                raw_idempotent_keys = _require_list(
                    state_payload,
                    "idempotent_keys",
                )
                if not all(isinstance(item, str) for item in raw_idempotent_keys):
                    raise ValueError("scheduler idempotent keys must be strings")
            else:
                raise ValueError(f"unknown scheduler projection encoding {encoding!r}")
            _validate_projection_invariants(claims=claims, attempts=attempts)
        except SchedulerStateCorruption:
            raise
        except Exception as exc:
            raise SchedulerStateCorruption(
                "invalid scheduler projection snapshot payload before write"
            ) from exc

    def event_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM scheduler_events").fetchone()
            return int(row["n"]) if row else 0

    def _write_projection(
        self,
        *,
        claims: Iterable[TaskClaim],
        attempts: Iterable[ScheduledExecutionAttempt],
        match_log: Iterable[MatchDecision],
        idempotent_keys: Iterable[str],
        last_event_seq: int,
        last_event_hash: str,
        generation: int,
    ) -> _SnapshotToken:
        """Publish inline state or an incrementally updated normalized projection."""

        claim_list = list(claims)
        attempt_list = list(attempts)
        match_list = list(match_log)
        key_list = sorted(set(str(key) for key in idempotent_keys))
        _validate_projection_invariants(claims=claim_list, attempts=attempt_list)

        item_count = len(claim_list) + len(attempt_list) + len(match_list) + len(key_list)
        normalized = (
            item_count >= _NORMALIZED_PROJECTION_MIN_ITEMS or self._current_snapshot_is_normalized()
        )
        if not normalized:
            return self._write_snapshot(
                state_json=self._state_json(
                    claims=claim_list,
                    attempts=attempt_list,
                    match_log=match_list,
                    idempotent_keys=key_list,
                ),
                last_event_seq=last_event_seq,
                last_event_hash=last_event_hash,
                generation=generation,
            )

        claim_rows = self._model_rows(claim_list, "claim_id")
        attempt_rows = self._model_rows(attempt_list, "attempt_id")
        match_rows = self._ordered_model_rows(match_list)
        self._sync_entity_rows(
            table="scheduler_claims_projection",
            id_column="claim_id",
            rows=claim_rows,
        )
        self._sync_entity_rows(
            table="scheduler_attempts_projection",
            id_column="attempt_id",
            rows=attempt_rows,
        )
        self._sync_ordered_rows(
            table="scheduler_match_log_projection",
            rows=match_rows,
        )
        self._sync_idempotency_keys(key_list)

        projection_hash = _projection_hash(
            claims=((row[0], row[3]) for row in claim_rows),
            attempts=((row[0], row[3]) for row in attempt_rows),
            match_log=(row[2] for row in match_rows),
            idempotent_keys=key_list,
        )
        manifest = _canonical_json(
            {
                "encoding": _NORMALIZED_PROJECTION_ENCODING,
                "projection_hash": projection_hash,
                "claim_count": len(claim_rows),
                "attempt_count": len(attempt_rows),
                "match_log_count": len(match_rows),
                "idempotency_key_count": len(key_list),
                # Legacy readers default missing collections to empty lists.
                # Non-list sentinels force those readers to fail closed rather
                # than silently treating a normalized projection as empty.
                "claims": _NORMALIZED_PROJECTION_ENCODING,
                "attempts": _NORMALIZED_PROJECTION_ENCODING,
                "match_log": _NORMALIZED_PROJECTION_ENCODING,
                "idempotent_keys": _NORMALIZED_PROJECTION_ENCODING,
            }
        )
        return self._write_snapshot(
            state_json=manifest,
            last_event_seq=last_event_seq,
            last_event_hash=last_event_hash,
            generation=generation,
        )

    def _current_snapshot_is_normalized(self) -> bool:
        row = self._conn.execute(
            "SELECT state_json FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        if row is None:
            return False
        try:
            payload = json.loads(str(row["state_json"]))
        except (TypeError, ValueError):
            return False
        return (
            isinstance(payload, dict) and payload.get("encoding") == _NORMALIZED_PROJECTION_ENCODING
        )

    @staticmethod
    def _model_rows(
        models: Iterable[Any],
        id_field: str,
    ) -> list[tuple[str, int, str, str]]:
        rows: list[tuple[str, int, str, str]] = []
        for ordinal, model in enumerate(models):
            identity = str(getattr(model, id_field))
            payload_json = _canonical_json(model.model_dump(mode="json"))
            rows.append((identity, ordinal, payload_json, _hash_state(payload_json)))
        return rows

    @staticmethod
    def _ordered_model_rows(
        models: Iterable[Any],
    ) -> list[tuple[int, str, str]]:
        rows: list[tuple[int, str, str]] = []
        for ordinal, model in enumerate(models):
            payload_json = _canonical_json(model.model_dump(mode="json"))
            rows.append((ordinal, payload_json, _hash_state(payload_json)))
        return rows

    def _sync_entity_rows(
        self,
        *,
        table: str,
        id_column: str,
        rows: list[tuple[str, int, str, str]],
    ) -> None:
        self._conn.executemany(
            f"""
            INSERT INTO {table} ({id_column}, ordinal, payload_json, payload_hash)
            VALUES (?, ?, ?, ?)
            ON CONFLICT({id_column}) DO UPDATE SET
                ordinal = excluded.ordinal,
                payload_json = excluded.payload_json,
                payload_hash = excluded.payload_hash
            WHERE {table}.ordinal != excluded.ordinal
               OR {table}.payload_json != excluded.payload_json
               OR {table}.payload_hash != excluded.payload_hash
            """,
            rows,
        )
        current_ids = {
            str(row[id_column])
            for row in self._conn.execute(f"SELECT {id_column} FROM {table}").fetchall()
        }
        desired_ids = {row[0] for row in rows}
        self._conn.executemany(
            f"DELETE FROM {table} WHERE {id_column} = ?",
            [(identity,) for identity in sorted(current_ids - desired_ids)],
        )

    def _sync_ordered_rows(
        self,
        *,
        table: str,
        rows: list[tuple[int, str, str]],
    ) -> None:
        self._conn.executemany(
            f"""
            INSERT INTO {table} (ordinal, payload_json, payload_hash)
            VALUES (?, ?, ?)
            ON CONFLICT(ordinal) DO UPDATE SET
                payload_json = excluded.payload_json,
                payload_hash = excluded.payload_hash
            WHERE {table}.payload_json != excluded.payload_json
               OR {table}.payload_hash != excluded.payload_hash
            """,
            rows,
        )
        self._conn.execute(
            f"DELETE FROM {table} WHERE ordinal >= ?",
            (len(rows),),
        )

    def _sync_idempotency_keys(self, keys: list[str]) -> None:
        existing = {
            str(row["idempotency_key"])
            for row in self._conn.execute(
                "SELECT idempotency_key FROM scheduler_idempotency_projection"
            ).fetchall()
        }
        desired = set(keys)
        self._conn.executemany(
            "INSERT OR IGNORE INTO scheduler_idempotency_projection (idempotency_key) VALUES (?)",
            [(key,) for key in sorted(desired - existing)],
        )
        self._conn.executemany(
            "DELETE FROM scheduler_idempotency_projection WHERE idempotency_key = ?",
            [(key,) for key in sorted(existing - desired)],
        )

    def _write_snapshot(
        self,
        *,
        state_json: str,
        last_event_seq: int,
        last_event_hash: str,
        generation: int,
    ) -> _SnapshotToken:
        state_hash = _hash_state(state_json)
        self._conn.execute(
            """
            INSERT INTO scheduler_snapshot
                (snapshot_id, state_json, state_hash, last_event_seq,
                 last_event_hash, generation)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id) DO UPDATE SET
                state_json = excluded.state_json,
                state_hash = excluded.state_hash,
                last_event_seq = excluded.last_event_seq,
                last_event_hash = excluded.last_event_hash,
                generation = excluded.generation
            """,
            (
                state_json,
                state_hash,
                last_event_seq,
                last_event_hash,
                generation,
            ),
        )
        return _SnapshotToken(
            generation=generation,
            state_hash=state_hash,
            last_event_seq=last_event_seq,
            last_event_hash=last_event_hash,
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
