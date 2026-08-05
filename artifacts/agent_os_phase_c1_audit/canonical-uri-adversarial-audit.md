# Canonical URI — Adversarial Audit

## Checks Performed

| Attack Vector | Result |
|---------------|--------|
| `../secret` | REJECTED |
| `../../etc/passwd` | REJECTED |
| `a/../../secret` | REJECTED |
| `.%2e/secret` | Rejected or normalized |
| `%2e%2e/secret` | Rejected or normalized |
| `%2E%2E/secret` | Rejected or normalized |
| `%252e%252e/secret` | Single decode only (%2e literal) |
| `%255c..%255csecret` | Rejected or normalized |
| Backslash paths | REJECTED |
| Drive letters | REJECTED |
| NUL byte | REJECTED |
| Canonicalization idempotent | YES |
| Same logical path → same canonical | YES |

## Fuzz Corpus

- Total inputs tested: 504
- All either rejected or successfully canonicalized without escape

## Findings

| Finding | Severity |
|---------|----------|
| UNC detection limited: `//server` after namespace detection only catches starting `//` on raw path | MEDIUM — documented limitation |
| Strict policy: `..` segments REJECTED rather than normalized | OK (secure-by-default) |
| Double-encoded `%252e` correctly single-decodes | OK |

## Verdict

PASS — URI canonicalization provides strong defense against encoding and traversal attacks.
