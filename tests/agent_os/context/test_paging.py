"""Tests for deterministic pager."""

from __future__ import annotations

import math
from typing import Any

import pytest

from lhos.agent_os.context.errors import ErrInvalidRange
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, _content_hash_for
from lhos.agent_os.context.pager import (
    _stable_page_id,
    compute_pages_for_ref,
)


class _FakeSupplier:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def read_version(self, **_: Any) -> bytes:
        return self._content

    def read_version_size(self, **_: Any) -> int:
        return len(self._content)


def _ref(start=None, end=None) -> ContentRef:
    content = b"A" * 1000
    return ContentRef(
        ref_id="r1",
        canonical_uri="artifact://ns-p1/a.md",
        artifact_id="aid",
        version=1,
        content_hash=_content_hash_for(content),
        media_type="text/plain",
        start_byte=start,
        end_byte=end,
    )


class TestStablePageId:
    def test_deterministic(self):
        a = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=0,
            byte_end=10,
            page_size=64,
            page_index=0,
        )
        b = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=0,
            byte_end=10,
            page_size=64,
            page_index=0,
        )
        assert a == b

    def test_differs_by_position(self):
        first = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=0,
            byte_end=10,
            page_size=64,
            page_index=0,
        )
        second = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=10,
            byte_end=20,
            page_size=64,
            page_index=1,
        )
        assert first != second


class TestComputePagesForRef:
    def setup_method(self):
        self.estimator = DeterministicByteTokenEstimator()

    def test_contiguous_non_overlapping(self):
        ref = _ref()
        pages = compute_pages_for_ref(
            ref=ref,
            content_supplier=_FakeSupplier(b"A" * 1000),
            estimator=self.estimator,
            page_size=100,
        )
        assert len(pages) == 10
        for i in range(len(pages) - 1):
            assert pages[i].byte_end == pages[i + 1].byte_start

    def test_no_gaps(self):
        ref = _ref()
        pages = compute_pages_for_ref(
            ref=ref,
            content_supplier=_FakeSupplier(b"A" * 1000),
            estimator=self.estimator,
            page_size=100,
        )
        assert pages[0].byte_start == 0
        assert pages[-1].byte_end == 1000

    def test_page_count_matches_ceil(self):
        for length in (1, 4, 5, 100, 101, 999, 1000, 1001):
            content = b"x" * length
            ref = ContentRef(
                ref_id="r1",
                canonical_uri="artifact://ns/a",
                artifact_id="aid",
                version=1,
                content_hash=_content_hash_for(content),
                media_type="text/plain",
            )
            pages = compute_pages_for_ref(
                ref=ref,
                content_supplier=_FakeSupplier(content),
                estimator=self.estimator,
                page_size=64,
            )
            expected = max(1, math.ceil(length / 64))
            assert len(pages) == expected, f"length={length} expected {expected} got {len(pages)}"

    def test_invalid_range_raises(self):
        ref = _ref(start=50, end=2000)
        with pytest.raises(ErrInvalidRange):
            compute_pages_for_ref(
                ref=ref,
                content_supplier=_FakeSupplier(b"A" * 1000),
                estimator=self.estimator,
                page_size=64,
            )

    def test_page_hash_matches_content_chunk(self):
        ref = _ref()
        pages = compute_pages_for_ref(
            ref=ref,
            content_supplier=_FakeSupplier(b"A" * 1000),
            estimator=self.estimator,
            page_size=250,
        )
        # page 0 covers bytes 0-250
        expected_chunk = (b"A" * 1000)[0:250]
        assert pages[0].page_hash == _content_hash_for(expected_chunk)

    def test_stable_page_ids_across_invocations(self):
        ref = _ref()
        supplier = _FakeSupplier(b"Z" * 500)
        a = compute_pages_for_ref(
            ref=ref, content_supplier=supplier, estimator=self.estimator, page_size=100
        )
        b = compute_pages_for_ref(
            ref=ref, content_supplier=supplier, estimator=self.estimator, page_size=100
        )
        ids_a = [p.page_id for p in a]
        ids_b = [p.page_id for p in b]
        assert ids_a == ids_b

    def test_negative_range_rejected(self):
        ref = ContentRef(
            ref_id="r",
            canonical_uri="artifact://ns/a",
            artifact_id="aid",
            version=1,
            content_hash="h" * 64,
            media_type="text/plain",
            start_byte=-1,
            end_byte=10,
        )
        with pytest.raises(ErrInvalidRange):
            compute_pages_for_ref(
                ref=ref,
                content_supplier=_FakeSupplier(b"A" * 100),
                estimator=self.estimator,
                page_size=64,
            )

    def test_size_bytes_matches_range(self):
        ref = _ref()
        pages = compute_pages_for_ref(
            ref=ref,
            content_supplier=_FakeSupplier(b"A" * 1000),
            estimator=self.estimator,
            page_size=250,
        )
        for p in pages:
            assert p.size_bytes == p.byte_end - p.byte_start
