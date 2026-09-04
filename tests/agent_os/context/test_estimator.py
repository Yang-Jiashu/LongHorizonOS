"""Tests for DeterministicByteTokenEstimator + protocol."""

from __future__ import annotations

import pytest

from lhos.agent_os.context.errors import ErrInvalidEstimator
from lhos.agent_os.context.estimator import (
    DeterministicByteTokenEstimator,
    TokenEstimator,
    validate_estimate,
)


class TestDeterministicByteTokenEstimator:
    def setup_method(self):
        self.est = DeterministicByteTokenEstimator()

    def test_estimator_id(self):
        assert self.est.estimator_id == "byte_x4_utf8_v1"

    def test_satisfies_protocol(self):
        assert isinstance(self.est, TokenEstimator)

    def test_empty_content_returns_one(self):
        assert self.est.estimate(content=b"", media_type="text/plain", encoding="utf-8") == 1

    def test_four_chars_returns_one(self):
        # 4 chars / 4 = 1
        assert self.est.estimate(content=b"abcd", media_type="text/plain", encoding="utf-8") == 1

    def test_five_chars_returns_two(self):
        # 5 chars / 4 = 1.25 → ceil = 2
        assert self.est.estimate(content=b"abcde", media_type="text/plain", encoding="utf-8") == 2

    def test_unicode_chars_counted_not_bytes(self):
        # one 4-byte UTF-8 codepoint counts as 1 character → 1 token
        content = "é".encode()  # 2 bytes, 1 char
        assert self.est.estimate(content=content, media_type="text/plain", encoding="utf-8") == 1

    def test_image_media_undecodable_returns_byte_x4(self):
        # image/unknown decode → None → fallback ceil(byte_length/4)
        content = b"\x00" * 100
        assert self.est.estimate(content=content, media_type="image/png", encoding="utf-8") == 25

    def test_binary_octet_stream_undecodable_returns_byte_x4(self):
        content = b"\xff" * 100
        assert (
            self.est.estimate(
                content=content, media_type="application/octet-stream", encoding="utf-8"
            )
            == 25
        )

    def test_strict_decode_failure_returns_byte_x4(self):
        # utf-8 declared but content is invalid utf-8 → fallback ceil(byte_length/4)
        content = b"\xff\xfe\xfd" * 20  # 60 bytes
        assert self.est.estimate(content=content, media_type="text/plain", encoding="utf-8") == 15

    def test_100_chars_yields_25_tokens(self):
        content = b"a" * 100
        assert self.est.estimate(content=content, media_type="text/plain", encoding="utf-8") == 25

    def test_never_returns_zero_for_non_empty(self):
        for n in (1, 2, 3, 4, 5, 10, 100):
            result = self.est.estimate(content=b"x" * n, media_type="text/plain", encoding="utf-8")
            assert result >= 1


class TestValidateEstimate:
    def test_accepts_positive(self):
        assert validate_estimate(5) == 5

    def test_accepts_zero(self):
        assert validate_estimate(0) == 0

    def test_rejects_negative(self):
        with pytest.raises(ErrInvalidEstimator):
            validate_estimate(-1)
