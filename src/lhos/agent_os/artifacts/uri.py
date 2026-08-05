"""Canonical Artifact URI parser and normalizer.

Transforms agent-facing URIs into a single canonical form:
    artifact://<namespace-id>/<path>

Normalization order (per spec):
1. Parse URI
2. Validate scheme
3. Percent-decode once
4. Unicode NFC normalization
5. Unify path separators to /
6. Eliminate "." segments
7. Check ".." segments → reject
8. Check NUL and control characters → reject
9. Check Windows drive/UNC paths → reject
10. Parse Namespace
11. Apply Mount (deferred to service layer)
12. Capability check (deferred to service layer)
13. Convert to backend-relative path (deferred to storage driver)

Canonicalization must be idempotent: canonicalize(canonicalize(uri)) == canonicalize(uri).
"""

from __future__ import annotations

import re
import unicodedata
from urllib.parse import unquote

from lhos.agent_os.artifacts.errors import InvalidArtifactURI, PathTraversalRejected

# Allowed schemes
ARTIFACT_SCHEME = "artifact"
WORKSPACE_SCHEME = "workspace"

# Characters that must not appear in canonical URIs
_NUL_PATTERN = re.compile(r"\x00")
_CONTROL_PATTERN = re.compile(r"[\x01-\x1f\x7f]")

# Windows drive letter pattern (e.g., C:\)
_WIN_DRIVE_PATTERN = re.compile(r"^[a-zA-Z]:[\\/]")


class CanonicalURI:
    """A parsed, normalized artifact URI."""

    __slots__ = ("canonical", "namespace_id", "path")

    def __init__(self, namespace_id: str, path: str) -> None:
        self.namespace_id = namespace_id
        self.path = path
        self.canonical = f"artifact://{namespace_id}/{path}"

    def __str__(self) -> str:
        return self.canonical

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CanonicalURI):
            return self.canonical == other.canonical
        if isinstance(other, str):
            return self.canonical == other
        return False

    def __hash__(self) -> int:
        return hash(self.canonical)

    def __repr__(self) -> str:
        return f"CanonicalURI({self.canonical!r})"


def canonicalize_uri(uri: str) -> CanonicalURI:
    """Parse and normalize an artifact URI.

    Accepts:
    - artifact://<namespace-id>/<path>
    - workspace:///<path>  (resolves to artifact://ns-<caller-pid>/<path> —
      namespace resolution is done by the service layer which knows the caller PID)

    Raises:
        InvalidArtifactURI: if the URI is malformed.
        PathTraversalRejected: if path traversal is detected.

    Returns:
        CanonicalURI with normalized namespace_id and path.
    """
    if not uri or not isinstance(uri, str):
        raise InvalidArtifactURI(str(uri), "empty or non-string")

    # Step 1: Parse URI — split into scheme and rest
    scheme_sep = "://"
    idx = uri.find(scheme_sep)
    if idx < 0:
        raise InvalidArtifactURI(uri, "no scheme separator '://'")

    scheme = uri[:idx].lower()
    rest = uri[idx + 3 :]  # after ://

    # Step 2: Validate scheme
    if scheme not in (ARTIFACT_SCHEME, WORKSPACE_SCHEME):
        raise InvalidArtifactURI(uri, f"unsupported scheme: {scheme}")

    if scheme == WORKSPACE_SCHEME:
        # workspace:///path → triple-slash means no explicit namespace
        # workspace://<ns-id>/path → explicit namespace
        if rest.startswith("/"):
            # Triple-slash form: workspace:///path → no namespace
            namespace_id = ""
            raw_path = rest.lstrip("/")
        else:
            # workspace://<ns-id>/path → first segment is namespace
            parts = rest.split("/", 1)
            if len(parts) == 2 and parts[0]:
                namespace_id = parts[0]
                raw_path = parts[1]
            else:
                namespace_id = ""
                raw_path = rest
    else:
        # artifact://<namespace-id>/<path>
        parts = rest.split("/", 1)
        if len(parts) < 2 or not parts[0]:
            raise InvalidArtifactURI(uri, "missing namespace_id")
        namespace_id = parts[0]
        raw_path = parts[1]

    if not namespace_id and scheme == ARTIFACT_SCHEME:
        raise InvalidArtifactURI(uri, "missing namespace_id for artifact scheme")

    # Step 3: Percent-decode once (only once — double-encoded stays decoded once)
    decoded_path = unquote(raw_path)

    # Step 4: Unicode NFC normalization
    nfc_path = unicodedata.normalize("NFC", decoded_path)

    # Step 8 (early): Check NUL and control characters
    if _NUL_PATTERN.search(nfc_path):
        raise InvalidArtifactURI(uri, "NUL byte in path")
    if _CONTROL_PATTERN.search(nfc_path):
        raise InvalidArtifactURI(uri, "control character in path")

    # Step 9 (early): Check Windows drive / UNC paths
    if _WIN_DRIVE_PATTERN.match(nfc_path):
        raise InvalidArtifactURI(uri, "Windows drive path detected")
    if nfc_path.startswith("\\\\") or nfc_path.startswith("//"):
        # UNC path — but only if it looks like //server/share, not just //
        # We already stripped the scheme, so // at start means UNC
        raise InvalidArtifactURI(uri, "UNC path detected")

    # Reject backslash as path separator (Phase C1 security policy)
    if "\\" in nfc_path:
        raise InvalidArtifactURI(uri, "backslash in path — use forward slash")

    # Step 5: Unify path separators (already enforced no backslash above)
    unified = nfc_path

    # Step 6: Eliminate "." segments
    segments: list[str] = []
    for seg in unified.split("/"):
        if seg == "." or seg == "":
            continue
        segments.append(seg)

    # Step 7: Check ".." segments
    for seg in unified.split("/"):
        if seg == "..":
            raise PathTraversalRejected(uri)

    normalized_path = "/".join(segments)

    if not normalized_path:
        raise InvalidArtifactURI(uri, "empty path after normalization")

    return CanonicalURI(namespace_id=namespace_id, path=normalized_path)


def is_canonical(uri: str) -> bool:
    """Check if a URI is already in canonical form."""
    try:
        result = canonicalize_uri(uri)
        return result.canonical == uri
    except InvalidArtifactURI:
        return False


def resolve_workspace_uri(uri: str, caller_namespace_id: str) -> CanonicalURI:
    """Resolve a workspace:/// URI to a canonical artifact:// URI.

    Called by the service layer which knows the caller's namespace.
    """
    if uri.startswith(f"{WORKSPACE_SCHEME}://"):
        canonical = canonicalize_uri(uri)
        if not canonical.namespace_id:
            canonical.namespace_id = caller_namespace_id
            canonical.canonical = f"artifact://{caller_namespace_id}/{canonical.path}"
        return canonical
    return canonicalize_uri(uri)
