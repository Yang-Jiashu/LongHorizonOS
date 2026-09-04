"""Token estimator protocol and deterministic default implementation.

Context VM decouples token estimation from selection policy: the estimator
produces a deterministic integer cost; the policy decides whether fitting is
possible.

A third-party tokenizer may replace this impl, but its version must be pinned
and recorded in the snapshot.
"""

from __future__ import annotations

import math
from typing import Protocol, runtime_checkable

from lhos.agent_os.context.errors import ErrInvalidEstimator


@runtime_checkable
class TokenEstimator(Protocol):
    """Pure deterministic interface for estimating how many tokens some bytes
    are worth to the upstream model.

    Constraints:
    - No network / model API calls.
    - Identical inputs always yield identical output.
    """

    @property
    def estimator_id(self) -> str: ...

    def estimate(
        self,
        *,
        content: bytes,
        media_type: str,
        encoding: str,
    ) -> int: ...


class DeterministicByteTokenEstimator:
    """Default token estimate: ceil(decoded_character_count / 4).

    For binary media types (not decodable as the declared encoding) we fall
    back to ceil(byte_length / 4) — the conservative upper bound.
    """

    @property
    def estimator_id(self) -> str:
        return "byte_x4_utf8_v1"

    def estimate(
        self,
        *,
        content: bytes,
        media_type: str,
        encoding: str,
    ) -> int:
        text = self._decode(content, media_type, encoding)
        # When the declared media_type is binary or fails to decode, fall
        # back to byte_length (the conservative upper bound per spec).
        if text is None:
            return math.ceil(len(content) / 4.0)
        count = max(len(text), 1)
        return math.ceil(count / 4.0)

    @staticmethod
    def _decode(content: bytes, media_type: str, encoding: str) -> str | None:
        if not content:
            return ""
        if media_type.startswith(("image/", "audio/", "video/", "application/octet-stream")):
            return None
        try:
            return content.decode(encoding or "utf-8", errors="strict")
        except (UnicodeDecodeError, LookupError):
            return None


def validate_estimate(value: int) -> int:
    """Ensure a returned estimate is non-negative; raise otherwise."""
    if value < 0:
        raise ErrInvalidEstimator(f"token estimator returned negative value: {value}")
    return value
