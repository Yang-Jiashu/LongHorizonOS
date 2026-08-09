# LongHorizonOS — Release Checklist

Pre-flight checklist for cutting a release. Run this top to bottom; each step
should pass cleanly before the next. A release is **not** cut until the LICENSE
blocker below is resolved.

## Gate 1 — Code health

- [ ] Full test suite green: `make test` (or `python -m pytest tests/ -q`) — 0 failures.
- [ ] `ruff check .` clean.
- [ ] `ruff format --check .` clean.
- [ ] `mypy` clean on `src/`.
- [ ] Witness an independent full-suite stress pass (repeat runs, order-independent).

## Gate 2 — Packaging

- [ ] `python -m build` produces `dist/lhos-0.1.0-*.whl` and `dist/lhos-0.1.0-*.tar.gz`.
- [ ] Wheel installs in a **fresh** virtualenv (`pip install dist/*.whl`).
- [ ] Installed wheel imports and its console script runs **outside the repo**:
      `lhos demo recovery-repair`.
- [ ] Package content audit: wheel contains `lhos/` packages + metadata, no stray
      test/tmp/secret files.
- [ ] Dependency audit: runtime deps are exactly `pydantic`, `pyyaml`, `networkx`;
      no heavyweight or unused dependency introduced.
- [ ] Version string is consistent across `pyproject.toml` and the release notes.

## Gate 3 — Hygiene & security

- [ ] No secrets in tracked files (API keys, private keys, `ghp_*`, `sk-*`, …).
- [ ] `CHANGELOG.md` reflects this release.
- [ ] `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, and issue/PR
      templates are present and current.
- [ ] CI workflow (`.github/workflows/ci.yml`) is mergeable and non-broken.

## Gate 4 — Docs

- [ ] `README.md` (install + quickstart + "See the difference") is accurate and
      its code snippets run.
- [ ] `docs/QUICKSTART.md`, `docs/CONCEPTS.md`, `docs/architecture/README.md`,
      `docs/releases/v0.1.0.md`, and `docs/RELEASE-CHECKLIST.md` are present.
- [ ] Every file path and symbol referenced in docs actually exists.

## Gate 5 — Legal

- [ ] **LICENSE present.** `artifacts/oss_productization_e6/LICENSE-BLOCKER.md`
      documents that a LICENSE file is absent. This is a project-owner decision;
      the current status allows source-available distribution only, not a public
      release. Do not guess a license.

## Gate 6 — Ship

- [ ] Version bumped in `pyproject.toml`.
- [ ] Release tag created (e.g. `v0.1.0`) and annotated with the release notes.
- [ ] Release artifact (wheel) uploaded and `pip install` rehearsed from it.
- [ ] Launch-ready status recorded (see `artifacts/oss_productization_e6/`).
