"""Durable provenance event stores.

The v0.2 store is intentionally independent of AgentOS/VPG persistence.  It
provides an append-only, hash-chained trace that can later be consumed by a
VPG/D3 adapter.  Two implementations are included:

``InMemoryProvenanceStore``
    Fast deterministic test/runtime store.

``JSONLProvenanceStore``
    Single-host durable store.  One canonical JSON event is written per line;
    opening/replaying the file verifies sequence numbers, IDs and the hash
    chain and fails closed on corruption.

The stores do not assign semantic validity and do not infer dependencies.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterable, Iterator
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import Protocol, runtime_checkable

from .models import ProvenanceEvent, canonical_json

GENESIS_HASH = "0" * 64


class ProvenanceStoreCorruption(RuntimeError):
    """Raised when persisted provenance cannot be trusted."""


@runtime_checkable
class ProvenanceStore(Protocol):
    """Minimal recorder-facing store contract."""

    def append(self, event: ProvenanceEvent) -> ProvenanceEvent: ...

    def append_many(self, events: Iterable[ProvenanceEvent]) -> tuple[ProvenanceEvent, ...]: ...

    def list_events(
        self,
        *,
        graph_id: str | None = None,
        task_id: str | None = None,
        attempt_id: str | None = None,
        since_sequence: int = 0,
    ) -> tuple[ProvenanceEvent, ...]: ...

    def replay(self) -> tuple[ProvenanceEvent, ...]: ...


def _event_json(event: ProvenanceEvent) -> str:
    """Canonical persisted representation (one line, no insignificant space)."""

    return canonical_json(event.model_dump(mode="json"))


def _verify_run(
    events: Iterable[ProvenanceEvent],
    *,
    previous_hash: str,
    expected_sequence: int,
    known_event_ids: AbstractSet[str],
) -> tuple[str, int, set[str]]:
    """Verify one contiguous run and report the state it leaves behind.

    Returns ``(tail_hash, next_sequence, new_event_ids)``.  Nothing is mutated,
    so a caller can verify a staged run *before* committing it and leave the
    store untouched if the run is corrupt.

    Event-id uniqueness is the one rule that cannot be decided from the run
    alone, so ``known_event_ids`` carries the ids already committed.  Everything
    else -- sequence continuity, previous-hash linkage, recomputed event hash --
    is inductive: given a verified prefix, verifying the run extends the proof to
    the whole chain.  That is what makes incremental verification equivalent to
    re-verifying from genesis rather than weaker than it.
    """

    previous = previous_hash
    expected = expected_sequence
    fresh_ids: set[str] = set()
    for event in events:
        if event.sequence != expected:
            raise ProvenanceStoreCorruption(
                f"provenance sequence gap: expected {expected}, got {event.sequence}"
            )
        if not event.event_id:
            raise ProvenanceStoreCorruption(f"provenance event {expected} has empty event_id")
        if event.event_id in known_event_ids or event.event_id in fresh_ids:
            raise ProvenanceStoreCorruption(f"duplicate provenance event_id {event.event_id}")
        if event.previous_hash != previous:
            raise ProvenanceStoreCorruption(
                f"provenance previous hash mismatch at sequence {expected}"
            )
        expected_hash = event.compute_hash(previous)
        if event.event_hash != expected_hash:
            raise ProvenanceStoreCorruption(
                f"provenance event hash mismatch at sequence {expected}"
            )
        fresh_ids.add(event.event_id)
        previous = event.event_hash
        expected += 1
    return previous, expected, fresh_ids


def _verify_chain(events: Iterable[ProvenanceEvent]) -> tuple[ProvenanceEvent, ...]:
    """Verify and return events in sequence order."""

    result = tuple(events)
    _verify_run(
        result,
        previous_hash=GENESIS_HASH,
        expected_sequence=1,
        known_event_ids=frozenset(),
    )
    return result


def _idempotency_index(events: Iterable[ProvenanceEvent]) -> dict[str, ProvenanceEvent]:
    return {
        event.idempotency_key: event
        for event in events
        if event.idempotency_key is not None and event.idempotency_key != ""
    }


class InMemoryProvenanceStore:
    """Thread-safe append-only store useful for tests and ephemeral runs."""

    def __init__(self, events: Iterable[ProvenanceEvent] | None = None) -> None:
        self._lock = threading.RLock()
        self._events: list[ProvenanceEvent] = []
        self._closed = False
        # Maintained incrementally.  Rebuilding either of these per append made
        # a single append O(N) and a run of N appends O(N^2).
        self._by_key: dict[str, ProvenanceEvent] = {}
        self._event_ids: set[str] = set()
        if events:
            self.append_many(events)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("provenance store is closed")

    @property
    def path(self) -> None:
        return None

    def append(self, event: ProvenanceEvent) -> ProvenanceEvent:
        return self.append_many((event,))[0]

    def append_many(self, events: Iterable[ProvenanceEvent]) -> tuple[ProvenanceEvent, ...]:
        incoming = list(events)
        if not incoming:
            return ()
        with self._lock:
            self._ensure_open()
            # Preserve append order while coalescing idempotency-key retries.
            result: list[ProvenanceEvent] = []
            previous = self._events[-1].event_hash if self._events else GENESIS_HASH
            sequence = self._events[-1].sequence + 1 if self._events else 1
            staged: list[ProvenanceEvent] = []
            staged_keys: dict[str, ProvenanceEvent] = {}
            for raw in incoming:
                if raw.idempotency_key:
                    existing = self._by_key.get(raw.idempotency_key) or staged_keys.get(
                        raw.idempotency_key
                    )
                    if existing is not None:
                        result.append(existing)
                        continue
                bound = raw.bind_chain(sequence=sequence, previous_hash=previous)
                staged.append(bound)
                result.append(bound)
                sequence += 1
                previous = bound.event_hash
                if bound.idempotency_key:
                    staged_keys[bound.idempotency_key] = bound
            # Verify the staged run against the committed tail, then commit.
            # Verifying first means a corrupt run leaves the store unchanged
            # instead of raising with the bad events already appended.
            _tail, _next_sequence, fresh_ids = _verify_run(
                staged,
                previous_hash=self._events[-1].event_hash if self._events else GENESIS_HASH,
                expected_sequence=self._events[-1].sequence + 1 if self._events else 1,
                known_event_ids=self._event_ids,
            )
            self._events.extend(staged)
            self._event_ids |= fresh_ids
            self._by_key.update(staged_keys)
            return tuple(result)

    def list_events(
        self,
        *,
        graph_id: str | None = None,
        task_id: str | None = None,
        attempt_id: str | None = None,
        since_sequence: int = 0,
    ) -> tuple[ProvenanceEvent, ...]:
        with self._lock:
            self._ensure_open()
            return tuple(
                event
                for event in self._events
                if event.sequence > since_sequence
                and (graph_id is None or event.graph_id == graph_id)
                and (task_id is None or event.task_id == task_id)
                and (attempt_id is None or event.attempt_id == attempt_id)
            )

    def replay(self) -> tuple[ProvenanceEvent, ...]:
        with self._lock:
            self._ensure_open()
            return _verify_chain(self._events)

    def __iter__(self) -> Iterator[ProvenanceEvent]:
        return iter(self.list_events())

    def close(self) -> None:
        with self._lock:
            self._closed = True


class JSONLProvenanceStore:
    """Single-host durable JSONL provenance journal."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        if str(path) == ":memory:":
            # A JSONL path named ":memory:" is surprising and cannot be
            # durable; use a temporary in-memory implementation explicitly.
            raise ValueError("JSONLProvenanceStore requires a filesystem path")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._events = self._load_file()

    @property
    def path(self) -> str:
        return str(self._path)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("provenance store is closed")

    def _load_file(self) -> list[ProvenanceEvent]:
        if not self._path.exists():
            return []
        parsed: list[ProvenanceEvent] = []
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        parsed.append(ProvenanceEvent.model_validate(json.loads(line)))
                    except Exception as exc:
                        raise ProvenanceStoreCorruption(
                            f"invalid provenance JSON at line {line_no}"
                        ) from exc
        except OSError as exc:
            raise ProvenanceStoreCorruption(
                f"unable to read provenance journal: {self._path}"
            ) from exc
        _verify_chain(parsed)
        return parsed

    def append(self, event: ProvenanceEvent) -> ProvenanceEvent:
        return self.append_many((event,))[0]

    def append_many(self, events: Iterable[ProvenanceEvent]) -> tuple[ProvenanceEvent, ...]:
        incoming = list(events)
        if not incoming:
            return ()
        with self._lock:
            self._ensure_open()
            # Reload before writing so a second store instance sharing the
            # path cannot silently fork the chain.
            self._events = self._load_file()
            by_key = _idempotency_index(self._events)
            result: list[ProvenanceEvent] = []
            staged: list[ProvenanceEvent] = []
            previous = self._events[-1].event_hash if self._events else GENESIS_HASH
            sequence = self._events[-1].sequence + 1 if self._events else 1
            for raw in incoming:
                if raw.idempotency_key:
                    existing = by_key.get(raw.idempotency_key)
                    if existing is not None:
                        result.append(existing)
                        continue
                bound = raw.bind_chain(sequence=sequence, previous_hash=previous)
                staged.append(bound)
                result.append(bound)
                sequence += 1
                previous = bound.event_hash
                if bound.idempotency_key:
                    by_key[bound.idempotency_key] = bound

            if staged:
                try:
                    with self._path.open("a", encoding="utf-8", newline="\n") as handle:
                        for event in staged:
                            handle.write(_event_json(event) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                except OSError as exc:
                    raise ProvenanceStoreCorruption(
                        f"unable to append provenance journal: {self._path}"
                    ) from exc
                self._events.extend(staged)
                # Deliberately re-verifies the whole chain rather than just the
                # appended run.  This store reloads and re-verifies the file on
                # every append anyway (to stop a second instance forking the
                # chain), so it is already O(N) per append and an incremental
                # check here would only save a constant factor -- not worth index
                # arithmetic in a fail-closed audit path.
                _verify_chain(self._events)
            return tuple(result)

    def list_events(
        self,
        *,
        graph_id: str | None = None,
        task_id: str | None = None,
        attempt_id: str | None = None,
        since_sequence: int = 0,
    ) -> tuple[ProvenanceEvent, ...]:
        with self._lock:
            self._ensure_open()
            return tuple(
                event
                for event in self._events
                if event.sequence > since_sequence
                and (graph_id is None or event.graph_id == graph_id)
                and (task_id is None or event.task_id == task_id)
                and (attempt_id is None or event.attempt_id == attempt_id)
            )

    def replay(self) -> tuple[ProvenanceEvent, ...]:
        with self._lock:
            self._ensure_open()
            # Re-read from disk to make replay a genuine durability check.
            self._events = self._load_file()
            return tuple(self._events)

    def __iter__(self) -> Iterator[ProvenanceEvent]:
        return iter(self.list_events())

    def close(self) -> None:
        with self._lock:
            self._closed = True


# Friendly aliases for callers that prefer a generic naming scheme.
MemoryProvenanceStore = InMemoryProvenanceStore
JsonlProvenanceStore = JSONLProvenanceStore


__all__ = [
    "GENESIS_HASH",
    "InMemoryProvenanceStore",
    "JSONLProvenanceStore",
    "JsonlProvenanceStore",
    "MemoryProvenanceStore",
    "ProvenanceStore",
    "ProvenanceStoreCorruption",
]
