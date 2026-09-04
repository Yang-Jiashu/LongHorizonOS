"""Deterministic pager.

Splits a fixed ArtifactVersion's (possibly range-trimmed) bytes into a stable
sequence of ContextPage objects.

Invariants per spec Section 8:
 - contiguous
 - non-overlapping
 - no-gaps
 - stable ordering
 - stable page_id
"""

from __future__ import annotations

import math
from typing import Protocol

from lhos.agent_os.context.errors import ErrInvalidRange
from lhos.agent_os.context.estimator import TokenEstimator
from lhos.agent_os.context.models import (
    ContentRef,
    ContextPage,
    _content_hash_for,
    _deterministic_hash,
)


class VersionContentProvider(Protocol):
    """Supply the exact committed bytes for one ArtifactVersion."""

    def read_version(
        self,
        *,
        artifact_id: str,
        version: int,
        canonical_uri: str,
    ) -> bytes: ...

    def read_version_size(self, *, artifact_id: str, version: int) -> int: ...


def _stable_page_id(
    *,
    artifact_id: str,
    version: int,
    content_hash: str,
    byte_start: int,
    byte_end: int,
    page_size: int,
    page_index: int,
) -> str:
    """Stable identity for a page. Not random (spec requirement)."""
    blob = "\x00".join(
        [
            artifact_id,
            str(version),
            content_hash,
            str(byte_start),
            str(byte_end),
            str(page_size),
            str(page_index),
        ]
    )
    return _deterministic_hash([blob])


def compute_pages_for_ref(
    *,
    ref: ContentRef,
    content_supplier: VersionContentProvider,
    estimator: TokenEstimator,
    page_size: int,
) -> list[ContextPage]:
    """Deterministically split one ref into pages off its committed bytes."""
    full_bytes = content_supplier.read_version(
        artifact_id=ref.artifact_id,
        version=ref.version,
        canonical_uri=ref.canonical_uri,
    )
    start = ref.start_byte or 0
    end = ref.end_byte if ref.end_byte is not None else len(full_bytes)

    if not (0 <= start <= end <= len(full_bytes)):
        raise ErrInvalidRange(
            f"ref {ref.ref_id}: range [{start}, {end}) out of bounds "
            f"(content length {len(full_bytes)})"
        )

    payload = full_bytes[start:end]
    if len(payload) == 0:
        # Zero-length ref is structurally unusual; produce one empty page.
        page_id = _stable_page_id(
            artifact_id=ref.artifact_id,
            version=ref.version,
            content_hash=ref.content_hash,
            byte_start=start,
            byte_end=end,
            page_size=page_size,
            page_index=0,
        )
        return [
            ContextPage(
                page_id=page_id,
                canonical_uri=ref.canonical_uri,
                artifact_id=ref.artifact_id,
                version=ref.version,
                content_hash=ref.content_hash,
                byte_start=start,
                byte_end=end,
                page_hash=_content_hash_for(payload),
                estimated_tokens=max(
                    estimator.estimate(
                        content=payload,
                        media_type=ref.media_type,
                        encoding=ref.encoding,
                    ),
                    0,
                ),
                size_bytes=0,
                required=ref.required,
                priority=ref.priority,
            )
        ]

    num_pages = math.ceil(len(payload) / page_size)
    pages: list[ContextPage] = []
    for i in range(num_pages):
        s = i * page_size
        e = min((i + 1) * page_size, len(payload))
        chunk = payload[s:e]
        absolute_start = start + s
        absolute_end = start + e
        page_id = _stable_page_id(
            artifact_id=ref.artifact_id,
            version=ref.version,
            content_hash=ref.content_hash,
            byte_start=absolute_start,
            byte_end=absolute_end,
            page_size=page_size,
            page_index=i,
        )
        estimated = estimator.estimate(
            content=chunk,
            media_type=ref.media_type,
            encoding=ref.encoding,
        )
        pages.append(
            ContextPage(
                page_id=page_id,
                canonical_uri=ref.canonical_uri,
                artifact_id=ref.artifact_id,
                version=ref.version,
                content_hash=ref.content_hash,
                byte_start=absolute_start,
                byte_end=absolute_end,
                page_hash=_content_hash_for(chunk),
                estimated_tokens=max(estimated, 0),
                size_bytes=len(chunk),
                required=ref.required,
                priority=ref.priority,
            )
        )
    return pages
