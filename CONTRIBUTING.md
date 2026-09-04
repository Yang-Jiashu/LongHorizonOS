# Contributing to LongHorizonOS

Thank you for your interest.  LongHorizonOS is a **state-centric operating
substrate** for long-running and multi-agent systems.  Its core identity is the
**Verified Progress Graph (VPG)** — the persistent semantic state substrate — with
D1+D3 implementing incremental semantic reconciliation.  Please keep that identity
intact when you contribute.

## Development setup

```bash
# Python >= 3.11
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Commands

```bash
python -m pytest tests/ -q                # run the full suite
python -m pytest tests/sdk tests/cli tests/demo tests/benchmarks -q
python -m ruff check src/lhos tests/      # lint
python -m ruff format --check src/lhos     # formatting
python -m mypy src/lhos                   # typecheck
```

See `docs/QUICKSTART.md` and `docs/architecture/LONGHORIZONOS-CORE-V1.md`.

## Architecture boundaries

- **Core V1 semantics are frozen** (`longhorizonos-core-v1`).  Do not change
  Kernel authority, ArtifactVersion identity, Evidence exact-version binding,
  READY/VERIFIED/STALE derivation, D2 matching/ownership separation, D3
  invalidation, or Repair Frontier minimality.
- **The Verified Progress Graph is the semantic authority**, not a workflow DAG.
  Never make the VPG a "workflow visualization" or route semantic truth through a
  queue.
- **SDK / CLI / integrations / demo / benchmark are product surfaces**.  They may
  evolve, but they must not become semantic authorities and must not bypass
  Evidence or Kernel-Lease ownership.
- The **legacy plane** (`graph/`, `runtime/`, `agents/`, `domain/`, `ports/`,
  `infrastructure/`, `verification/`, `benchmarks/`) is OUT-OF-CORE-V1 and is kept
  import-disjoint from the audited plane.

## Session rules

- **One authority per fact.**  A patch must not introduce a second place that
  decides VERIFIED / READY / STALE / Goal closure / ownership / Repair Frontier.
- **Evidence is exact-version immutable history.**  Never mutate a historical
  Evidence node or let old Evidence validate a newer ArtifactVersion.
- **Ownership is the Kernel ResourceLease.**  A Scheduler claim row is a
  projection, not an authority.
- **CLI / observability are read-only.**  Do not add a query that mutates
  GraphVersion.

## How to propose SDK / tool changes

Open an issue or PR.  Scope should stay thin (a composable adapter over the
public Core API) and preserve the VPG-identity tests in `tests/sdk/integrations/`,
`tests/cli/`, `tests/demo/`, `tests/benchmarks/`.

## How to report a Core / semantic defect

If you believe a VERIFIED/STALE/Evidence/ownership/Repair-Frontier result is
incorrect, include a **reproduction** and the relevant **semantic state** (goal,
task validity, evidence + artifact version, ownership, Repair Frontier).  Label
`core-semantics` / `correctness`.

## PR expectations

- Keep the change minimal and explain **why**.
- Run the **Core regression** suites (Agent OS, D1, D2, D3, VPG Guardian) plus the
  SDK/CLI/demo/bench areas you touch.
- Report `Core semantic delta` and **new semantic authorities introduced** (must
  be 0).
- Do **not** introduce a second semantic authority.
- CI must be green.  **Do not** add CI retries / skip / allow-failure to hide a
  correctness failure.
