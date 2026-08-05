# Canonical URI — Security Specification

> Component: Artifact FS (L3)
> Status: Implemented
> Date: 2026-08-05

## 1. Goal

Ensure that every artifact URI resolves to exactly one canonical form,
preventing namespace escape via encoding tricks, path traversal, or
symlink following.

## 2. Threat Model

| Threat | Vector |
|--------|--------|
| Path traversal | `../../etc/passwd` |
| Double encoding | `%252e%252e/etc` |
| Unicode normalization | `％２ｅ` full-width chars |
| Backslash conversion | `..\..\windows\system32` |
| Symlink escape | Symlink in tree pointing outside root |
| Drive letter | `c:/windows/system32` |
| UNC path | `//host/share/file` |
| Null byte injection | `/artifact.txt%00.jpg` |
| Case normalization | `C:\Windows` vs `c:\windows` |

## 3. Canonicalization Rules

### 3.1 Parse

Split scheme (`artifact://`), authority (namespace), path.

### 3.2 Decode

Percent-decode exactly once:
- `%2F` → `/` (then normalized as segment separator)
- `%00` → rejected (null byte)
- Overlong UTF-8 → rejected
- Surrogate pairs → rejected

### 3.3 Unicode

Apply NFC normalization. Reject non-character code points.

### 3.4 Path Normal

- Collapse `/+` → `/`
- Remove `./` segments
- Reject any `..` segment → raise PathTraversalError
- Reject empty segments (implied `.`)
- Strip trailing `/` (treat as directory reference, but only files stored)

### 3.5 Reject

- Backslash (`\`): not converted to `/`
- Drive letter (`c:`): forbidden
- UNC authority (`//host`): forbidden
- Control char < 0x20: forbidden
- DEL (0x7F): forbidden

## 4. Storage Driver Symlink Defense

```
def _secure_path(self, relative: str) -> Path:
    """Resolve relative path, refusing escapes."""
    root = self._root.resolve()
    target = (root / relative).resolve()
    if root != target and root not in target.parents:
        raise PathTraversalError(f"{relative} escapes root")
    return target
```

The driver `write()` and `read()` methods must:
1. Verify resolved path is under root
2. Verify no symlink exists on any path component (use `O_NOFOLLOW` where available)
3. Verify parent directory is not a symlink

## 5. Verification Tests

| Test | Expected |
|------|----------|
| `artifact://ns-p1/../../etc/passwd` | PathTraversalError |
| `artifact://ns-p1/%2e%2e/%2e%2e/etc` | PathTraversalError |
| `artifact://ns-p1/foo\..\..\bar` | InvalidURLError |
| `artifact://ns-p1/%00.jpg` | InvalidURLError |
| `C:/windows` | InvalidURLError |
| `//host/share` | InvalidURLError |
| `artifact://ns-p1/file%20name.txt` | Normalized, OK |
| `artifact://ns-p1/` (trailing slash) | Tolerated or normalized |

## 6. Implementation

`src/lhos/agent_os/artifacts/uri.py`

Key methods:
- `canonicalize(uri: str) -> str`
- `parse(uri: str) -> ParsedArtifactUri`
- `is_within(namespace_id: str, uri: str) -> bool`
- `normalize_path(path: str) -> str`
