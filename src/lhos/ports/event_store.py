"""Event store port (spec section 5)."""

from typing import Protocol

from lhos.domain.events import RuntimeEvent


class EventStore(Protocol):
    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        """Append an event, assigning the next per-run sequence number.

        Idempotency: when ``event.idempotency_key`` is set and an event with the
        same (run_id, key) exists, the existing event is returned unchanged.
        """
        ...

    def list_events(self, run_id: str, since_sequence: int = 0) -> list[RuntimeEvent]: ...

    def find_by_idempotency(self, run_id: str, idempotency_key: str) -> RuntimeEvent | None: ...

    def next_sequence(self, run_id: str) -> int: ...
