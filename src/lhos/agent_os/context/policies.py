"""Deterministic page-selection policies.

Two policies live here. Both are LLM-free, fully deterministic (no wall-clock,
no RNG), independent of caller ref ordering, and fail closed on required pages.

``priority_stable_v1`` (spec Section 9) — a *stable sort*, not a replacement
policy. Tie-breaking order:

 1. required=True first
 2. priority descending
 3. canonical_uri ascending  (stable lexical)
 4. version ascending
 5. byte_start ascending
 6. page_id ascending

None of those terms reflect whether a page will actually be *used*; it orders
by identity, not utility. It is referenced by ``policy_id`` in durable
snapshots and MUST keep behaving identically — a regression would corrupt the
meaning of already-persisted snapshots — so it is left untouched below.

``recoverability_residual_lifecycle_v1`` — a utility-aware eviction ordering
that adopts two published, deterministic ideas rather than reinventing them:

 * Recoverability (CWL, "Beyond Compaction: Structured Context Eviction for
   Long-Horizon Agents", arXiv 2606.11213): "drops the oldest-and-most-
   recoverable content according to the dependency graph". Adopted here as:
   at equal priority, a page whose content is already durably persisted /
   re-derivable (``ContentRef.recoverable``) is the cheapest to omit, so it is
   omitted before a non-recoverable page.
 * Residual utility / lifecycle-aware eviction (TokenPilot, "Cache-Efficient
   Context Management for LLM Agents", arXiv 2606.17016): offload "only when
   task relevance expires". Adopted here as: refs whose ``ref_id`` is in the
   task's declared read-set are still needed and are kept before refs whose
   relevance has expired (not in the read-set). The declared ``ContentRef`` set
   is the only signal used — no semantic analysis.

TokenPilot also observes that mutating the sequence breaks prompt-prefix
continuity and invalidates the KV cache, so *where* you evict matters, not only
*what*. We do not implement a cache; we only *report* prefix stability via
``analyze_prefix_stability`` so callers can see whether a re-selection changed
the previously-selected prefix (cache-invalidating) or only appended/truncated
the tail (cache-preserving).

Keep-order tie-break for the lifecycle policy:

 1. required=True first (fail closed if required alone exceeds budget)
 2. priority descending
 3. residual utility: refs in the read-set kept before expired refs
 4. recoverability: non-recoverable kept before recoverable
 5. canonical_uri ascending
 6. version ascending
 7. byte_start ascending
 8. ref_id ascending
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from lhos.agent_os.context.errors import ErrInvalidPolicy, ErrRequiredBudgetExceeded
from lhos.agent_os.context.models import ContentRef, ContextManifest, ContextPage

PRIORITY_STABLE_POLICY_ID = "priority_stable_v1"
LIFECYCLE_POLICY_ID = "recoverability_residual_lifecycle_v1"


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


# ── recoverability_residual_lifecycle_v1 ──────────────────────────────────────
#
# Adopts recoverability from CWL (arXiv 2606.11213) and residual-utility /
# lifecycle-aware eviction from TokenPilot (arXiv 2606.17016). See module
# docstring for the mapping onto this codebase's models.


def read_set_from_metadata(manifest: ContextManifest) -> frozenset[str] | None:
    """Extract a declared read-set (set of ``ref_id``) from manifest metadata.

    Deterministic given the manifest. Returns ``None`` (meaning "every ref is
    still relevant") when no ``read_set`` key is present. Any non-string members
    are ignored so a malformed hint degrades to a smaller read-set rather than
    raising. The value is expected under ``manifest.metadata["read_set"]``.
    """
    raw = manifest.metadata.get("read_set")
    if raw is None:
        return None
    if isinstance(raw, str):
        # A bare string is a single ref_id, not an iterable of characters.
        return frozenset({raw})
    if isinstance(raw, Iterable):
        return frozenset(m for m in raw if isinstance(m, str))
    return None


def _lifecycle_keep_sort_key(ref: ContentRef, read_set: frozenset[str] | None) -> tuple:
    """Keep-order key: lower sorts earlier, i.e. is kept first / omitted last.

    Residual utility precedes recoverability: an expired-relevance ref is even
    cheaper to drop than a recoverable-but-still-needed one. Both rank strictly
    below ``priority`` so equal-priority ties are what the two signals break —
    matching CWL's "at equal priority, drop the most recoverable" framing.
    """
    in_read_set = read_set is None or ref.ref_id in read_set
    return (
        0 if ref.required else 1,
        -ref.priority,
        0 if in_read_set else 1,
        0 if not ref.recoverable else 1,
        ref.canonical_uri,
        ref.version,
        ref.start_byte if ref.start_byte is not None else 0,
        ref.ref_id,
    )


def sort_ref_pages_lifecycle(
    ref_pages: Iterable[RefPages],
    read_set: frozenset[str] | None,
) -> list[RefPages]:
    """Deterministic keep-order sort of RefPages; caller ordering is irrelevant.

    ``ref_id`` is the terminal tie-breaker, so the result is a total order.
    """
    return sorted(ref_pages, key=lambda rp: _lifecycle_keep_sort_key(rp.ref, read_set))


@dataclass(frozen=True)
class PrefixStability:
    """Whether a re-selection preserved the previously-selected prompt prefix.

    ``prefix_stable`` is False only when the shared prefix diverged mid-sequence
    (``kind == "prefix_changed"``) — the case that invalidates a KV cache
    (TokenPilot, arXiv 2606.17016). Pure tail append/truncate preserve the
    cached prefix and are reported stable.
    """

    prefix_stable: bool
    kind: str  # "initial" | "identical" | "append" | "truncate" | "prefix_changed"
    first_divergence_index: int | None  # None unless kind == "prefix_changed"


def analyze_prefix_stability(
    previous_page_order: Sequence[str] | None,
    new_page_order: Sequence[str],
) -> PrefixStability:
    """Compare a new selected-page order against the previous one.

    ``None`` previous order means there is no prior prompt to invalidate, so the
    result is stable (``kind="initial"``).
    """
    if previous_page_order is None:
        return PrefixStability(prefix_stable=True, kind="initial", first_divergence_index=None)

    common = 0
    for prev_id, new_id in zip(previous_page_order, new_page_order, strict=False):
        if prev_id != new_id:
            break
        common += 1

    prev_len = len(previous_page_order)
    new_len = len(new_page_order)
    if common == prev_len and common == new_len:
        return PrefixStability(prefix_stable=True, kind="identical", first_divergence_index=None)
    if common == prev_len:
        # previous order is a strict prefix of the new one → append only.
        return PrefixStability(prefix_stable=True, kind="append", first_divergence_index=None)
    if common == new_len:
        # new order is a strict prefix of the previous one → tail truncation.
        return PrefixStability(prefix_stable=True, kind="truncate", first_divergence_index=None)
    return PrefixStability(
        prefix_stable=False, kind="prefix_changed", first_divergence_index=common
    )


@dataclass(frozen=True)
class LifecycleSelection:
    """Result of ``select_pages_lifecycle_v1`` including prefix-stability report."""

    selected_pages: tuple[ContextPage, ...]
    omitted_ref_ids: tuple[str, ...]
    tokens_used: int
    bytes_used: int
    prefix: PrefixStability

    def as_v1_tuple(self) -> tuple[list[ContextPage], list[str], int, int]:
        """Adapt to the legacy 4-tuple shape consumed by ``ContextService``."""
        return (
            list(self.selected_pages),
            list(self.omitted_ref_ids),
            self.tokens_used,
            self.bytes_used,
        )


def select_pages_lifecycle_v1(
    *,
    manifest: ContextManifest,
    ref_pages: Iterable[RefPages],
    read_set: Iterable[str] | None = None,
    previous_page_order: Sequence[str] | None = None,
) -> LifecycleSelection:
    """Deterministic selection under ``recoverability_residual_lifecycle_v1``.

    A ref is either fully loaded or fully omitted; no partial-page truncation
    (same granularity as ``priority_stable_v1``). Required refs fail closed with
    ``ErrRequiredBudgetExceeded``. Ordering is computed internally, so the result
    is independent of the caller's ``ref_pages`` ordering.

    ``read_set`` — the task's declared still-needed ``ref_id`` set (residual
    utility). ``None`` means every ref is still relevant.
    ``previous_page_order`` — the previously selected ``page_id`` order, used
    only to report prefix stability; ``None`` on first selection.
    """
    if manifest.policy_id != LIFECYCLE_POLICY_ID:
        raise ErrInvalidPolicy(f"unsupported context policy: {manifest.policy_id}")

    rs = frozenset(read_set) if read_set is not None else None
    ordered = sort_ref_pages_lifecycle(ref_pages, rs)

    # Required-first pass: budget must fit ALL required refs or fail closed.
    required_cost_tokens = sum(rp.token_cost for rp in ordered if rp.ref.required)
    required_cost_bytes = sum(rp.byte_cost for rp in ordered if rp.ref.required)
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

    selected: list[ContextPage] = []
    omitted_ref_ids: list[str] = []
    tokens_used = 0
    bytes_used = 0

    for rp in ordered:
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

    new_order = tuple(p.page_id for p in selected)
    prefix = analyze_prefix_stability(previous_page_order, new_order)
    return LifecycleSelection(
        selected_pages=tuple(selected),
        omitted_ref_ids=tuple(omitted_ref_ids),
        tokens_used=tokens_used,
        bytes_used=bytes_used,
        prefix=prefix,
    )


def select_pages(
    *,
    manifest: ContextManifest,
    ref_pages: list[RefPages],
    read_set: Iterable[str] | None = None,
    previous_page_order: Sequence[str] | None = None,
) -> tuple[list[ContextPage], list[str], int, int]:
    """Dispatch to the policy named by ``manifest.policy_id``.

    Returns the legacy 4-tuple ``(selected, omitted_ref_ids, tokens, bytes)`` so
    it is a drop-in for the historical ``select_pages_v1`` call site. The
    ``priority_stable_v1`` branch delegates verbatim and ignores the lifecycle-
    only arguments, preserving its snapshot-critical behaviour exactly.
    """
    if manifest.policy_id == PRIORITY_STABLE_POLICY_ID:
        return select_pages_v1(manifest=manifest, ref_pages=ref_pages)
    if manifest.policy_id == LIFECYCLE_POLICY_ID:
        return select_pages_lifecycle_v1(
            manifest=manifest,
            ref_pages=ref_pages,
            read_set=read_set,
            previous_page_order=previous_page_order,
        ).as_v1_tuple()
    raise ErrInvalidPolicy(f"unsupported context policy: {manifest.policy_id}")


__all__ = [
    "LIFECYCLE_POLICY_ID",
    "PRIORITY_STABLE_POLICY_ID",
    "LifecycleSelection",
    "PrefixStability",
    "RefPages",
    "analyze_prefix_stability",
    "manifest_hash_for",
    "read_set_from_metadata",
    "select_pages",
    "select_pages_lifecycle_v1",
    "select_pages_v1",
    "sort_ref_pages_lifecycle",
    "sort_refs_deterministic",
]
