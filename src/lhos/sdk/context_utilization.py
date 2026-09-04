"""Context utilization: how much materialized context was actually read.

The Context VM decides which declared pages fit a token budget and materializes
them.  Nothing measured whether the agent then *used* them, so there was no way
to answer either of the two questions that matter: what did we pay for and
waste, and how much smaller could the budget have been.

This module answers the first one, from data the snapshot already carries --
``PageBinding`` records ``byte_start``/``byte_end``, so per-page size needs no new
persistence.

The circularity that makes this measure worthless if ignored
------------------------------------------------------------
Materialized pages are *auto-recorded* into the provenance read-set with
``source="context_vm"``.  Comparing page bindings against a read-set that was
populated from those same bindings makes "every page was read" true by
construction.  So a read observation only counts here if it came from somewhere
other than the Context VM itself -- an executor's own access.  :func:`executor_read_keys`
enforces that filter, and passing unfiltered reads is treated as unmeasurable
rather than as a perfect score.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from .runtime_state import UnavailableField

CONTEXT_UTILIZATION_SCHEMA_VERSION: Final[str] = "context-utilization.v1"

# The provenance source the Context VM stamps on bindings it records itself.
CONTEXT_VM_SOURCE: Final[str] = "context_vm"


@dataclass(frozen=True)
class ContextUtilization:
    """Materialized-versus-read accounting for one attempt's context.

    ``observable`` is ``False`` when no executor-sourced read was available: the
    honest answer is then "unmeasured", never "nothing was wasted".  A fabricated
    zero here would make a budget look perfectly sized when it was simply never
    checked.
    """

    schema_version: str
    observable: bool
    materialized_pages: int
    read_pages: int | None
    unread_pages: int | None
    materialized_bytes: int
    unread_bytes: int | None
    unread_page_ids: tuple[str, ...]
    omitted_ref_ids: tuple[str, ...]
    unavailable: tuple[UnavailableField, ...] = ()

    @property
    def wasted_fraction_basis_points(self) -> int | None:
        """Wasted share of materialized bytes, in basis points."""

        if self.unread_bytes is None or self.materialized_bytes <= 0:
            return None
        return self.unread_bytes * 10_000 // self.materialized_bytes


def executor_read_keys(read_set: Iterable[Any]) -> frozenset[str]:
    """Identity keys an *executor* read, excluding Context-VM self-records.

    Both the raw ``resource_uri`` and the bare ``artifact_id`` are collected,
    because declarations and provenance disagree on which form they use and a
    page must be matchable under either.
    """

    keys: set[str] = set()
    for binding in read_set or ():
        if str(getattr(binding, "source", "") or "").strip() == CONTEXT_VM_SOURCE:
            continue
        uri = str(getattr(binding, "resource_uri", "") or "").strip()
        if uri:
            keys.add(uri)
        artifact_id = str(getattr(binding, "artifact_id", "") or "").strip()
        if artifact_id:
            keys.add(artifact_id)
    return frozenset(keys)


def measure_context_utilization(
    page_bindings: Iterable[Any],
    executor_reads: frozenset[str] | Iterable[str] | None,
    *,
    omitted_ref_ids: Iterable[str] = (),
) -> ContextUtilization:
    """Compare materialized pages against what the executor actually read.

    ``executor_reads`` must already exclude Context-VM self-records -- use
    :func:`executor_read_keys`.  ``None`` means no executor read observation
    exists for this attempt, which is reported unobservable.
    """

    pages = list(page_bindings or ())
    materialized_bytes = 0
    for page in pages:
        start = int(getattr(page, "byte_start", 0) or 0)
        end = int(getattr(page, "byte_end", 0) or 0)
        materialized_bytes += max(0, end - start)
    omitted = tuple(sorted({str(item).strip() for item in omitted_ref_ids if str(item).strip()}))

    if executor_reads is None:
        return ContextUtilization(
            schema_version=CONTEXT_UTILIZATION_SCHEMA_VERSION,
            observable=False,
            materialized_pages=len(pages),
            read_pages=None,
            unread_pages=None,
            materialized_bytes=materialized_bytes,
            unread_bytes=None,
            unread_page_ids=(),
            omitted_ref_ids=omitted,
            unavailable=(
                UnavailableField(
                    name="executor_reads",
                    reason=(
                        "no executor-sourced read observation for this attempt; "
                        "Context-VM self-records cannot measure their own utilization"
                    ),
                ),
            ),
        )

    keys = frozenset(executor_reads)
    unread_ids: list[str] = []
    unread_bytes = 0
    read_pages = 0
    for page in pages:
        uri = str(getattr(page, "canonical_uri", "") or "").strip()
        artifact_id = str(getattr(page, "artifact_id", "") or "").strip()
        if (uri and uri in keys) or (artifact_id and artifact_id in keys):
            read_pages += 1
            continue
        page_id = str(getattr(page, "page_id", "") or "").strip()
        if page_id:
            unread_ids.append(page_id)
        start = int(getattr(page, "byte_start", 0) or 0)
        end = int(getattr(page, "byte_end", 0) or 0)
        unread_bytes += max(0, end - start)

    return ContextUtilization(
        schema_version=CONTEXT_UTILIZATION_SCHEMA_VERSION,
        observable=True,
        materialized_pages=len(pages),
        read_pages=read_pages,
        unread_pages=len(pages) - read_pages,
        materialized_bytes=materialized_bytes,
        unread_bytes=unread_bytes,
        unread_page_ids=tuple(sorted(unread_ids)),
        omitted_ref_ids=omitted,
    )


__all__ = [
    "CONTEXT_UTILIZATION_SCHEMA_VERSION",
    "CONTEXT_VM_SOURCE",
    "ContextUtilization",
    "executor_read_keys",
    "measure_context_utilization",
]
