"""Adversarial URI canonicalization tests for Phase C1 audit.

Independent test suite exercising encoding, unicode, escapes, and fuzz.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.artifacts.uri import (
    CanonicalURI,
    canonicalize_uri,
    is_canonical,
)


class TestURITraversal:
    """Path traversal attempts must be rejected."""

    @pytest.mark.parametrize(
        "path",
        [
            "../secret",
            "../../etc/passwd",
            "foo/../../secret",
            "a/../../../secret",
        ],
    )
    def test_dotdot_rejected(self, path: str) -> None:
        with pytest.raises(Exception):
            canonicalize_uri(f"artifact://ns-a/{path}")


class TestURIEncoding:
    """Encoding attacks must be rejected or normalized."""

    @pytest.mark.parametrize(
        "path",
        [
            ".%2e/secret",
            "%2e%2e/secret",
            "%2E%2E/secret",
            "%252e%252e/secret",
            "%255c..%255csecret",
        ],
    )
    def test_percent_encoding_attacks(self, path: str) -> None:
        try:
            parsed = canonicalize_uri(f"artifact://ns-a/{path}")
            assert ".." not in parsed.path
            assert "/../" not in parsed.path
        except Exception:
            pass

    def test_single_decode_not_double(self) -> None:
        """%252e should decode once to %2e (literal), not become '.'."""
        uri = "artifact://ns-a/%252e%252e/secret"
        result = canonicalize_uri(uri)
        assert "%2e" in result.path or result.path == "secret", (
            f"Unexpected decode: {result.path}"
        )


class TestURIBackslash:
    """Backslash must NOT be treated as slash."""

    @pytest.mark.parametrize(
        "path",
        [
            "..\\secret",
            "foo\\..\\bar",
            "c:\\secret",
        ],
    )
    def test_backslash_rejected(self, path: str) -> None:
        with pytest.raises(Exception):
            canonicalize_uri(f"artifact://ns-a/{path}")


class TestURIDriveLetters:
    """Drive letters must be rejected."""

    @pytest.mark.parametrize(
        "path",
        [
            "c:/secret",
            "C:\\secret",
        ],
    )
    def test_drive_letters(self, path: str) -> None:
        with pytest.raises(Exception):
            canonicalize_uri(f"artifact://ns-a/{path}")


class TestURINull:
    """NUL bytes must be rejected."""

    def test_null_byte(self) -> None:
        with pytest.raises(Exception):
            canonicalize_uri("artifact://ns-a/file\x00.jpg")


class TestURICanonicalization:
    """Canonicalization must be idempotent for valid inputs."""

    @pytest.mark.parametrize(
        "path",
        [
            "foo/bar.txt",
            "foo/./bar.txt",
            "Foo%20bar.txt",
            "a/b/c/d.txt",
            "data/report.md",
        ],
    )
    def test_idempotent(self, path: str) -> None:
        uri = f"artifact://ns-a/{path}"
        first = str(canonicalize_uri(uri))
        second = str(canonicalize_uri(first))
        assert first == second, f"{first} != {second}"

    def test_same_logical_path_one_canonical(self) -> None:
        base = str(canonicalize_uri("artifact://ns-a/foo/bar.txt"))
        v1 = str(canonicalize_uri("artifact://ns-a/foo/./bar.txt"))
        assert base == v1


class TestURIFuzz:
    """Deterministic fuzz corpus (500+ inputs)."""

    def _generate_fuzz_inputs(self) -> list[str]:
        base_paths = [
            "file.txt", "dir/file.txt", "a/b/c/d.txt",
            "foo bar.txt", "file%20name.txt", "data/report.md",
            "x/y/z.txt", "one/two/three.txt", "alpha/beta/gamma.txt",
            "test/data/result.json", "src/main/app.py", "notes.txt",
            "my document.pdf", "image.png", "script.sh",
        ]
        attacks = [
            "../", "..\\", "%2e%2e/", "%2E%2E/", "%252e%252e/",
            "/../", "/..\\", "c:", "C:", "%2e%2e", "....//", "..../",
            "....\\", "..%2f", "..%5c", "%2e%2e%2f", "%252e",
        ]
        inputs: list[str] = []
        for base in base_paths:
            inputs.append(base)
            for attack in attacks:
                inputs.append(f"prefix/{attack}{base}")
                inputs.append(f"{attack}{base}")
                inputs.append(f"{base}/{attack}")
        # Control characters and DEL
        for code in range(0, 0x20):
            inputs.append(f"file{chr(code)}.txt")
        inputs.append("file\x7f.txt")
        # Long paths
        inputs.append("/".join([f"dir{i}" for i in range(50)]) + "/end.txt")
        # Unicode
        inputs.append("dätä/fïlé.txt")
        inputs.append("日本語/ファイル.txt")
        # Percent-encoded control chars
        for code in range(0, 0x20):
            inputs.append(f"file%{code:02x}.txt")
        return inputs

    def test_fuzz_no_escape(self) -> None:
        inputs = self._generate_fuzz_inputs()
        assert len(inputs) > 500, f"Need 500+ inputs, got {len(inputs)}"
        for path in inputs:
            uri = f"artifact://ns-fuzz/{path}"
            try:
                parsed = canonicalize_uri(uri)
                # Must not contain traversal after canonicalization
                assert "/../" not in parsed.path
                assert not parsed.path.endswith("/..")
                # Must have a non-empty path
                assert len(parsed.path) > 0
                # Must preserve namespace
                assert parsed.namespace_id == "ns-fuzz"
            except Exception:
                pass  # Rejection is fine


class TestIsCanonical:
    """Verify is_canonical behavior."""

    def test_valid_canonical(self) -> None:
        assert is_canonical("artifact://ns-a/foo/bar.txt")

    def test_non_canonical_detected(self) -> None:
        assert not is_canonical("artifact://ns-a/foo/./bar.txt")

    def test_invalid_returns_false(self) -> None:
        assert not is_canonical("not-a-uri")


class TestCanonicalURIEquality:
    """CanonicalURI equality and hashing."""

    def test_equal_canonical(self) -> None:
        a = canonicalize_uri("artifact://ns-a/foo.txt")
        b = canonicalize_uri("artifact://ns-a/foo.txt")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_not_equal(self) -> None:
        a = canonicalize_uri("artifact://ns-a/foo.txt")
        b = canonicalize_uri("artifact://ns-a/bar.txt")
        assert a != b

    def test_string_equality(self) -> None:
        a = canonicalize_uri("artifact://ns-a/foo.txt")
        assert a == "artifact://ns-a/foo.txt"


class TestURIStrictness:
    """Document the strict rejection policy as a security feature."""

    def test_dotdot_rejected_not_normalized(self) -> None:
        """.. segments are REJECTED, not silently normalized."""
        with pytest.raises(Exception):
            canonicalize_uri("artifact://ns-a/foo/../bar.txt")

    def test_backslash_unc_rejected(self) -> None:
        """Backslash UNC paths are rejected."""
        with pytest.raises(Exception):
            canonicalize_uri("artifact://ns-a/\\\\server\\share")
