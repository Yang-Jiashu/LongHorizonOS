"""Deterministic page-selection policy.

Implements `priority_stable_v1` (spec Section 9) tie-breaking order:

 1. required=True first
 2. priority descending
 3. canonical_uri ascending  (stable lexical)
 4. version ascending
 5. byte_start ascending
 6. page_id ascending

The default Manifest-level refs ordering is not part of the selection tie-break;
we sort refs deterministically for the same 6-level order before pagination so
that the OMITTED/SELECTED results are independent of the caller's ref order.
"""

from __future__ import annotations

from dataclasses import dataclass

from lhos.agent_os.context.errors import ErrInvalidPolicy, ErrRequiredBudgetExceeded
from lhos.agent_os.context.models import ContentRef, ContextManifest, ContextPage


@dataclass(frozen=True)
class RefPages:
    """A ref paired with its deterministically-materialized pages."""

    ref: ContentRef
    pages: tuple[ContextPage, ...]

    @property
    def token_cost(self) -> int:
        return sum(p.estimated_tokens for p in self.pages)

    @property
    def byte_cost(self) -> int:
        return sum(p.size_bytes for p in self.pages)


def _ref_sort_key(ref: ContentRef) -> tuple:
    """Deterministic ref ordering for stable selection."""
    return (
        0 if ref.required else 1,
        -ref.priority,
        ref.canonical_uri,
        ref.version,
        ref.start_byte if ref.start_byte is not None else 0,
        ref.ref_id,
    )


def sort_refs_deterministic(refs: tuple[ContentRef, ...]) -> list[ContentRef]:
    """Refs are sorted deterministically so caller ordering doesn't matter."""
    return sorted(refs, key=_ref_sort_key)


def select_pages_v1(
    *,
    manifest: ContextManifest,
    ref_pages: list[RefPages],
) -> tuple[list[ContextPage], list[str], int, int]:
    """Deterministic selection under `priority_stable_v1`.

    Returns (selected_pages, omitted_ref_ids, tokens_used, bytes_used).

    A ref is either fully loaded or fully omitted; no partial-page truncation.
    """
    if manifest.policy_id != "priority_stable_v1":
        raise ErrInvalidPolicy(f"unsupported context policy: {manifest.policy_id}")

    selected: list[ContextPage] = []
    omitted_ref_ids: list[str] = []
    tokens_used = 0
    bytes_used = 0

    # Required-first pass: budget must fit all required refs.
    required_cost_tokens = 0
    required_cost_bytes = 0
    for rp in ref_pages:
        if rp.ref.required:
            required_cost_tokens += rp.token_cost
            required_cost_bytes += rp.byte_cost

    if required_cost_tokens > manifest.token_budget:
        raise ErrRequiredBudgetExceeded(
            f"required pages ({required_cost_tokens} tokens) exceed "
            f"budget ({manifest.token_budget} tokens)"
        )
    if manifest.byte_budget is not None and required_cost_bytes > manifest.byte_budget:
        raise ErrRequiredBudgetExceeded(
            f"required pages ({required_cost_bytes} bytes) exceed "
            f"budget ({manifest.byte_budget} bytes)"
        )

    for rp in ref_pages:
        if rp.ref.required:
            selected.extend(rp.pages)
            tokens_used += rp.token_cost
            bytes_used += rp.byte_cost
            continue

        tentative_tokens = tokens_used + rp.token_cost
        tentative_bytes = bytes_used + rp.byte_cost if manifest.byte_budget is not None else 0
        exceeds_tokens = tentative_tokens > manifest.token_budget
        exceeds_bytes = manifest.byte_budget is not None and tentative_bytes > manifest.byte_budget
        if exceeds_tokens or exceeds_bytes:
            omitted_ref_ids.append(rp.ref.ref_id)
            continue
        selected.extend(rp.pages)
        tokens_used += rp.token_cost
        bytes_used += rp.byte_cost

    return (selected, omitted_ref_ids, tokens_used, bytes_used)


def manifest_hash_for(manifest: ContextManifest) -> str:
    return manifest.manifest_hash()
