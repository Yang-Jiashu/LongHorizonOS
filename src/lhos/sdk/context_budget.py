"""Recommend a smaller Context manifest from what was actually read.

The Context VM materializes what a manifest declares, within a declared
``token_budget``.  Nothing ever revisited that declaration, so a manifest that
over-declares kept paying for pages the agent never looked at, on every attempt,
forever.  This proposes a smaller one from observation.

It is advisory by design: it returns a recommendation plus the reason for each
ref, and never mutates a manifest.  A caller decides whether to adopt it, because
shrinking context trades cost against the risk of removing something a future
path would have needed.

Three safety rules, in descending importance:

*Never drop a ref that any observed attempt read.*  One attempt not touching a
ref is not evidence the ref is dead -- a task may read an input only on some
paths.  Only refs unread across **every** observation are candidates.

*Never drop ``required=True``.*  The manifest author declared it load-bearing;
budget pressure already fails closed rather than silently omitting it, and this
must not become a back door around that.

*Zero observations recommend nothing.*  With no history the manifest is returned
unchanged and flagged unavailable, rather than being read as "nothing was used".
Absence of evidence is not evidence of waste.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from .runtime_state import UnavailableField

CONTEXT_BUDGET_SCHEMA_VERSION: Final[str] = "context-budget-recommendation.v1"


@dataclass(frozen=True)
class ContextBudgetRecommendation:
    """A proposed smaller manifest, with per-ref justification."""

    schema_version: str
    manifest_id: str
    observed_attempts: int
    drop_ref_ids: tuple[str, ...]
    keep_ref_ids: tuple[str, ...]
    required_ref_ids: tuple[str, ...]
    declared_token_budget: int
    recommended_token_budget: int | None
    reasons: tuple[str, ...]
    unavailable: tuple[UnavailableField, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.drop_ref_ids) or (
            self.recommended_token_budget is not None
            and self.recommended_token_budget != self.declared_token_budget
        )


def recommend_context_budget(
    manifest: Any,
    read_ref_ids_per_attempt: Iterable[Iterable[str]],
    *,
    observed_peak_tokens: Iterable[int] | None = None,
) -> ContextBudgetRecommendation:
    """Propose dropping never-read refs and lowering the budget to observed peak.

    ``read_ref_ids_per_attempt`` is one collection of ref ids per observed
    attempt -- the refs that attempt actually read.  The caller maps page-level
    observations onto refs, because only the caller holds both the manifest and
    the attempt's snapshot.

    ``observed_peak_tokens`` are the per-attempt token totals actually
    materialized.  Omitted means the budget cannot be justified downward, which
    is reported rather than guessed.
    """

    refs = tuple(getattr(manifest, "refs", ()) or ())
    declared_budget = int(getattr(manifest, "token_budget", 0) or 0)
    manifest_id = str(getattr(manifest, "manifest_id", "") or "")
    required_ids = tuple(
        sorted(
            str(getattr(ref, "ref_id", "") or "")
            for ref in refs
            if bool(getattr(ref, "required", False))
        )
    )

    observations = [
        {str(item).strip() for item in (attempt or ()) if str(item).strip()}
        for attempt in read_ref_ids_per_attempt
    ]
    all_ref_ids = tuple(sorted(str(getattr(ref, "ref_id", "") or "") for ref in refs))

    if not observations:
        return ContextBudgetRecommendation(
            schema_version=CONTEXT_BUDGET_SCHEMA_VERSION,
            manifest_id=manifest_id,
            observed_attempts=0,
            drop_ref_ids=(),
            keep_ref_ids=all_ref_ids,
            required_ref_ids=required_ids,
            declared_token_budget=declared_budget,
            recommended_token_budget=None,
            reasons=("no observed attempt; nothing can be justified",),
            unavailable=(
                UnavailableField(
                    name="read_ref_ids_per_attempt",
                    reason="no attempt observation exists for this manifest",
                ),
            ),
        )

    ever_read: set[str] = set()
    for attempt in observations:
        ever_read |= attempt

    drops: list[str] = []
    reasons: list[str] = []
    for ref in refs:
        ref_id = str(getattr(ref, "ref_id", "") or "")
        if not ref_id:
            continue
        if bool(getattr(ref, "required", False)):
            reasons.append(f"{ref_id}: kept (required)")
            continue
        if ref_id in ever_read:
            reasons.append(f"{ref_id}: kept (read by at least one attempt)")
            continue
        drops.append(ref_id)
        reasons.append(f"{ref_id}: drop candidate (unread across {len(observations)} attempts)")

    peaks = [int(value) for value in (observed_peak_tokens or ()) if int(value) >= 0]
    if peaks:
        recommended = min(declared_budget, max(peaks)) if declared_budget else max(peaks)
        reasons.append(f"token_budget: {declared_budget} -> {recommended} (observed peak)")
        unavailable: tuple[UnavailableField, ...] = ()
    else:
        recommended = None
        unavailable = (
            UnavailableField(
                name="observed_peak_tokens",
                reason="no materialized token totals supplied; budget cannot be justified downward",
            ),
        )

    dropped = tuple(sorted(drops))
    return ContextBudgetRecommendation(
        schema_version=CONTEXT_BUDGET_SCHEMA_VERSION,
        manifest_id=manifest_id,
        observed_attempts=len(observations),
        drop_ref_ids=dropped,
        keep_ref_ids=tuple(sorted(set(all_ref_ids) - set(dropped))),
        required_ref_ids=required_ids,
        declared_token_budget=declared_budget,
        recommended_token_budget=recommended,
        reasons=tuple(reasons),
        unavailable=unavailable,
    )


__all__ = [
    "CONTEXT_BUDGET_SCHEMA_VERSION",
    "ContextBudgetRecommendation",
    "recommend_context_budget",
]
