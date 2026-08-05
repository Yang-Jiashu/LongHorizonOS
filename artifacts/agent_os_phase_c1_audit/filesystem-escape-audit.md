# Symlink and Path Escape Audit

## Design Review

The `LocalArtifactStorageDriver` is fully hash-addressed:
- Staging path = `<staging>/<transaction_id>` (transaction_id is UUID from service)
- CAS path = `<cas>/<hash[:2]>/<full_hash>` (hash from content)

User processes NEVER supply file paths to the driver. All path-related attack vectors are eliminated by design.

## Symlink Tests

| Scenario | Result |
|----------|--------|
| Pre-existing CAS symlink pointing outside storage | Driver reads through symlink (FINDING) |
| Symlink in staging directory | Cannot inject — only UUID-named dirs |
| CAS tree location replaced with symlink before commit | Follows symlink (FINDING) |
| Staging replaced with symlink before commit | Would follow symlink |

## Security Boundary Analysis

| Layer | Protection |
|-------|------------|
| URI canonicalization | Prevents traversal at service layer |
| Hash-based CAS paths | Eliminates user-controlled path segments |
| Namespace isolation | Prevents cross-namespace access |

The symlink following is a limitation but NOT a practical vulnerability because:
1. Storage tree is entirely driver-controlled (hash-based)
2. Attacker must already compromise the storage root (would bypass any local defense)
3. Namespace isolation at the service layer is the actual security boundary

## Limitations

- Driver claims "No symlinks (TOCTOU safety)" but has no active symlink check
- No `O_NOFOLLOW` or `realpath` verification
- Acceptable for Phase C1 as documented limitation

## Verdict

PASS with documented limitation. Symlink attacks are not practical within Phase C1 security model. Future hostile-host hardening requires `O_NOFOLLOW` on open.
