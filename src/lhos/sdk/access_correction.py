"""Fold observed undeclared reads back into the declared access sets.

The conflict graph decides which tasks may run together, and it is built purely
from *declared* read/write sets.  A read that happened but was never declared is
therefore the dangerous kind of error: two genuinely conflicting tasks look
independent, get co-scheduled, and one of them silently works from state the
other is changing.

Undeclared reads are now observable (see :mod:`lhos.sdk.undeclared_reads`), but
the loop was open: the runtime could *detect* an undeclared read after the fact
and still make the same wrong batching decision on the next epoch.  This closes
it -- an observation from a completed attempt corrects the access set used for
subsequent conflict derivation.

Two properties are deliberate:

*Corrections only ever widen.*  A declared read is never removed just because a
particular attempt did not exercise it; a task may legitimately read an input
only on some paths, and dropping it would manufacture a false "independent".
Only additions are applied.  ``declared_but_unread`` is reported for the budget
and context work, never used to shrink a conflict-relevant read set.

*An unobserved attempt corrects nothing.*  If observation did not happen, the
access set is returned unchanged and flagged, rather than being treated as
confirmation that the declaration was complete.  Absence of evidence is not
evidence of a correct declaration.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Final

from .conflict_graph import TaskAccessSet
from .runtime_state import UnavailableField

ACCESS_CORRECTION_SCHEMA_VERSION: Final[str] = "access-correction.v1"


@dataclass(frozen=True)
class AccessCorrection:
    """One task's declared access set, corrected by observation."""

    schema_version: str
    task_id: str
    observed: bool
    corrected: TaskAccessSet
    added_reads: tuple[str, ...]
    declared_but_unread: tuple[str, ...]
    unavailable: tuple[UnavailableField, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.added_reads)


def correct_access_set(
    access_set: TaskAccessSet,
    report: Any | None,
) -> AccessCorrection:
    """Widen one declared access set with reads that were actually observed.

    ``report`` is an :class:`~lhos.sdk.undeclared_reads.UndeclaredReadReport`, or
    ``None`` when the attempt produced no observation.
    """

    if report is None or not bool(getattr(report, "observed", False)):
        return AccessCorrection(
            schema_version=ACCESS_CORRECTION_SCHEMA_VERSION,
            task_id=access_set.task_id,
            observed=False,
            corrected=access_set,
            added_reads=(),
            declared_but_unread=(),
            unavailable=(
                UnavailableField(
                    name="observed_reads",
                    reason=(
                        "no read observation for this attempt; an unobserved "
                        "declaration is not a confirmed-complete declaration"
                    ),
                ),
            ),
        )

    undeclared = tuple(
        sorted(
            {
                str(item).strip()
                for item in (getattr(report, "undeclared_reads", ()) or ())
                if str(item).strip()
            }
        )
    )
    unread = tuple(
        sorted(
            {
                str(item).strip()
                for item in (getattr(report, "declared_but_unread", ()) or ())
                if str(item).strip()
            }
        )
    )
    added = tuple(item for item in undeclared if item not in set(access_set.read_set))
    if not added:
        corrected = access_set
    else:
        corrected = access_set.model_copy(
            update={
                "read_set": tuple(sorted({*access_set.read_set, *added})),
                # A task whose reads had to be corrected is now *known* at least
                # to the extent observed; it is not promoted to known if it was
                # never known, because one observation does not prove coverage.
                "known": access_set.known,
            }
        )
    return AccessCorrection(
        schema_version=ACCESS_CORRECTION_SCHEMA_VERSION,
        task_id=access_set.task_id,
        observed=True,
        corrected=corrected,
        added_reads=added,
        declared_but_unread=unread,
    )


def correct_access_sets(
    access_sets: Iterable[TaskAccessSet],
    reports: Mapping[str, Any],
) -> tuple[tuple[TaskAccessSet, ...], tuple[AccessCorrection, ...]]:
    """Correct a whole collection, preserving order.

    Returns the corrected sets ready to hand to ``ConflictGraph.from_access_sets``
    plus the per-task audit of what changed and why.
    """

    corrected: list[TaskAccessSet] = []
    audits: list[AccessCorrection] = []
    for access_set in access_sets:
        correction = correct_access_set(access_set, reports.get(access_set.task_id))
        corrected.append(correction.corrected)
        audits.append(correction)
    return tuple(corrected), tuple(audits)


__all__ = [
    "ACCESS_CORRECTION_SCHEMA_VERSION",
    "AccessCorrection",
    "correct_access_set",
    "correct_access_sets",
]
