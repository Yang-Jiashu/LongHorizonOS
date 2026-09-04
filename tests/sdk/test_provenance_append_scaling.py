"""Provenance append is O(1) per event, and a rejected run leaves no trace.

``InMemoryProvenanceStore.append_many`` used to rebuild the idempotency index
from the whole history and re-verify the entire hash chain on *every* append, so
one append was O(N) and a run of N appends was O(N^2). Measured before the fix:
11.6 ms per append at 2000 events and still climbing. Both passes are now
incremental.

Incremental verification is equivalent, not weaker: sequence continuity,
previous-hash linkage and the recomputed event hash are inductive over a verified
prefix. Event-id uniqueness is the one whole-chain rule, so the committed id set
is carried explicitly rather than recomputed.

The fix also moved verification *before* the commit. Previously the staged events
were appended to ``self._events`` and only then verified, so a corrupt run raised
while leaving the bad events in the store.
"""

from __future__ import annotations

import itertools

import pytest

from lhos.provenance.models import ProvenanceEvent
from lhos.provenance.store import (
    GENESIS_HASH,
    InMemoryProvenanceStore,
    ProvenanceStoreCorruption,
    _verify_chain,
)


def _event(index: int, *, event_id: str | None = None, key: str | None = None) -> ProvenanceEvent:
    return ProvenanceEvent(
        event_id=event_id if event_id is not None else f"e{index:05d}",
        graph_id="g",
        task_id="t",
        attempt_id="a",
        op="read",
        resource_uri=f"artifact://x{index}",
        idempotency_key=key if key is not None else f"k{index}",
    )


def test_appended_chain_still_verifies_from_genesis() -> None:
    """The incremental path must produce a chain the full verifier accepts."""

    store = InMemoryProvenanceStore()
    for index in range(50):
        store.append(_event(index))

    events = store.replay()
    assert len(events) == 50
    # The authoritative whole-chain check, unchanged.
    _verify_chain(events)
    assert events[0].previous_hash == GENESIS_HASH
    assert [event.sequence for event in events] == list(range(1, 51))
    for earlier, later in itertools.pairwise(events):
        assert later.previous_hash == earlier.event_hash


def test_idempotency_key_still_coalesces_across_separate_appends() -> None:
    """The index is now kept incrementally; retries must still collapse."""

    store = InMemoryProvenanceStore()
    first = store.append(_event(1, key="same"))
    second = store.append(_event(2, key="same"))

    assert second.event_id == first.event_id
    assert second.sequence == first.sequence
    assert len(store.replay()) == 1


def test_idempotency_key_coalesces_within_one_batch() -> None:
    """Two events sharing a key inside a single append_many collapse to one."""

    store = InMemoryProvenanceStore()
    returned = store.append_many([_event(1, key="dup"), _event(2, key="dup"), _event(3)])

    assert len(returned) == 3
    assert returned[0].event_id == returned[1].event_id
    assert len(store.replay()) == 2


def test_duplicate_event_id_is_rejected_against_committed_history() -> None:
    """Uniqueness is a whole-chain rule and must survive the incremental path."""

    store = InMemoryProvenanceStore()
    store.append(_event(1, event_id="shared", key="k1"))

    with pytest.raises(ProvenanceStoreCorruption, match="duplicate provenance event_id"):
        store.append(_event(2, event_id="shared", key="k2"))


def test_duplicate_event_id_inside_one_batch_is_rejected() -> None:
    store = InMemoryProvenanceStore()

    with pytest.raises(ProvenanceStoreCorruption, match="duplicate provenance event_id"):
        store.append_many(
            [_event(1, event_id="same", key="a"), _event(2, event_id="same", key="b")]
        )


def test_a_rejected_batch_leaves_the_store_untouched() -> None:
    """Verification happens before the commit, so a bad run cannot pollute.

    The old order appended first and verified second, which meant a corrupt run
    raised with its events already visible in ``replay()``.
    """

    store = InMemoryProvenanceStore()
    store.append(_event(0))
    before = store.replay()

    with pytest.raises(ProvenanceStoreCorruption):
        store.append_many([_event(1), _event(2, event_id="e00001", key="other")])

    assert store.replay() == before
    # And the store must still be usable, with no sequence gap left behind.
    following = store.append(_event(9))
    assert following.sequence == 2
    _verify_chain(store.replay())


def test_seeded_construction_matches_incremental_appends() -> None:
    """``__init__(events=...)`` routes through the same incremental path.

    The same event objects are fed to both stores: ``ProvenanceEvent`` stamps a
    creation time, so rebuilding the list would differ for reasons that have
    nothing to do with the append path.
    """

    events = [_event(index) for index in range(10)]
    seeded = InMemoryProvenanceStore(events)
    appended = InMemoryProvenanceStore()
    for event in events:
        appended.append(event)

    assert [event.event_hash for event in seeded.replay()] == [
        event.event_hash for event in appended.replay()
    ]
    assert [event.sequence for event in seeded.replay()] == list(range(1, 11))


def test_per_append_cost_does_not_grow_with_history_length() -> None:
    """The O(N^2) shape must not come back.

    Timing is noisy, so this gates only the asymptotic shape with a very loose
    bound: quadrupling the history may not quadruple the per-append cost. The
    old implementation grew ~7.6x across this range.
    """

    import time

    def per_append_us(count: int) -> float:
        store = InMemoryProvenanceStore()
        events = [_event(index) for index in range(count)]
        started = time.perf_counter()
        for event in events:
            store.append(event)
        return (time.perf_counter() - started) / count * 1e6

    small = per_append_us(200)
    large = per_append_us(800)
    assert large < small * 2.5, f"per-append cost grew {large / small:.1f}x from 200 to 800 events"
