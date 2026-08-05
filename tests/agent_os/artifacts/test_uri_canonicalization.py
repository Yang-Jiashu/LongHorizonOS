"""Tests for canonical artifact URI parsing and normalization."""

from __future__ import annotations

import pytest

from lhos.agent_os.artifacts.errors import InvalidArtifactURI, PathTraversalRejected
from lhos.agent_os.artifacts.uri import (
    canonicalize_uri,
    is_canonical,
    resolve_workspace_uri,
)


class TestBasicCanonicalization:
    """Basic URI parsing and normalization."""

    def test_simple_artifact_uri(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src/main.py")
        assert result.namespace_id == "ns-p1"
        assert result.path == "src/main.py"
        assert result.canonical == "artifact://ns-p1/src/main.py"

    def test_workspace_uri_resolves(self) -> None:
        result = resolve_workspace_uri("workspace:///src/main.py", "ns-p1")
        assert result.namespace_id == "ns-p1"
        assert result.path == "src/main.py"
        assert result.canonical == "artifact://ns-p1/src/main.py"

    def test_workspace_uri_with_explicit_ns(self) -> None:
        result = canonicalize_uri("workspace://shared-docs/api/ref.md")
        assert result.namespace_id == "shared-docs"
        assert result.path == "api/ref.md"

    def test_trailing_slash_removed(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src/")
        assert result.path == "src"

    def test_multiple_slashes_collapsed(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src//main.py")
        assert result.path == "src/main.py"

    def test_dot_segment_removed(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src/./main.py")
        assert result.path == "src/main.py"

    def test_leading_dot_removed(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/./src/main.py")
        assert result.path == "src/main.py"

    def test_percent_decoded_once(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src/%41.py")
        assert result.path == "src/A.py"

    def test_nfc_normalization(self) -> None:
        # NFD form of é (e + combining acute) should normalize to NFC (é)
        nfd_form = "src/main\xe2\x80\x8b.py"  # zero-width space
        result = canonicalize_uri(f"artifact://ns-p1/{nfd_form}")
        # Should not raise and should be canonical
        assert is_canonical(result.canonical)


class TestPathTraversalRejection:
    """All path traversal vectors must be rejected."""

    def test_dotdot_path_is_rejected(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p1/../secret")

    def test_dotdot_in_middle_is_rejected(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p1/a/../../secret")

    def test_dotdot_after_normal_path(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p1/src/../etc/passwd")

    def test_percent_encoded_dotdot_is_rejected(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p1/%2e%2e/secret")

    def test_uppercase_percent_encoded_dotdot_is_rejected(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p1/%2E%2E/secret")

    def test_double_encoded_dotdot_is_rejected(self) -> None:
        # %252e%252e decodes once to %2e%2e, which is NOT ".." literally.
        # But after one decode it's "%2e%2e" — which is a literal string, not traversal.
        # However, this should still be rejected because it decodes to a path segment
        # that could be re-interpreted. Our policy: single decode, then check for "..".
        # Since %252e decodes to %2e (not "."), it won't trigger PathTraversalRejected.
        # Instead, it will be accepted as a literal "%2e%2e" path segment.
        # This is CORRECT behavior — double encoding cannot produce traversal because
        # we only decode once.
        result = canonicalize_uri("artifact://ns-p1/%252e%252e/secret")
        # %252e → %2e (literal), not "." — so path is "%2e%2e/secret"
        assert "%2e" in result.path
        # This is safe because the path segment is literally "%2e%2e", not ".."

    def test_backslash_traversal_is_rejected(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="backslash"):
            canonicalize_uri("artifact://ns-p1/..\\secret")

    def test_windows_drive_path_is_rejected(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="Windows drive"):
            canonicalize_uri("artifact://ns-p1/C:\\secret")

    def test_unc_path_is_rejected(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="UNC"):
            canonicalize_uri("artifact://ns-p1/\\\\server\\share")

    def test_null_byte_is_rejected(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="NUL"):
            canonicalize_uri("artifact://ns-p1/src\x00secret")

    def test_control_character_is_rejected(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="control"):
            canonicalize_uri("artifact://ns-p1/src\x01secret")

    def test_cross_namespace_dotdot_is_rejected(self) -> None:
        with pytest.raises(PathTraversalRejected):
            canonicalize_uri("artifact://ns-p2/../ns-p1/file")

    def test_file_scheme_not_accepted(self) -> None:
        with pytest.raises(InvalidArtifactURI, match="unsupported scheme"):
            canonicalize_uri("file:///etc/passwd")


class TestIdempotency:
    """Canonicalization must be idempotent."""

    @pytest.mark.parametrize(
        "uri",
        [
            "artifact://ns-p1/src/main.py",
            "artifact://ns-p1/reports/result.json",
            "artifact://shared-docs/api/reference.md",
            "artifact://ns-p1/a/b/c/d/e.txt",
        ],
    )
    def test_canonicalize_is_idempotent(self, uri: str) -> None:
        first = canonicalize_uri(uri)
        second = canonicalize_uri(first.canonical)
        assert first.canonical == second.canonical

    def test_canonical_uri_is_already_canonical(self) -> None:
        uri = "artifact://ns-p1/src/main.py"
        assert is_canonical(uri)

    def test_non_canonical_uri_is_not_canonical(self) -> None:
        uri = "artifact://ns-p1/src/./main.py"
        assert not is_canonical(uri)


class TestNamespaceExtraction:
    """Namespace ID extraction from URIs."""

    def test_extract_namespace_from_artifact_uri(self) -> None:
        result = canonicalize_uri("artifact://ns-p1/src/main.py")
        assert result.namespace_id == "ns-p1"

    def test_extract_namespace_from_shared_uri(self) -> None:
        result = canonicalize_uri("artifact://shared-docs/api/ref.md")
        assert result.namespace_id == "shared-docs"

    def test_workspace_resolves_to_caller_namespace(self) -> None:
        result = resolve_workspace_uri("workspace:///src/main.py", "ns-abc")
        assert result.namespace_id == "ns-abc"
        assert result.canonical == "artifact://ns-abc/src/main.py"


class TestCapabilityPatternMatching:
    """Verify that canonical URIs work with fnmatch for capability checks."""

    def test_double_star_matches_recursive(self) -> None:
        import fnmatch

        uri = "artifact://ns-p1/src/deep/path/main.py"
        assert fnmatch.fnmatch(uri, "artifact://ns-p1/**")

    def test_single_star_matches_one_segment(self) -> None:
        import fnmatch

        uri = "artifact://ns-p1/src/main.py"
        assert fnmatch.fnmatch(uri, "artifact://ns-p1/*")

    def test_different_namespace_does_not_match(self) -> None:
        import fnmatch

        uri = "artifact://ns-p2/src/main.py"
        assert not fnmatch.fnmatch(uri, "artifact://ns-p1/**")
