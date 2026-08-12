# LongHorizonOS — Release Checklist

Pre-flight checklist for cutting a release. Run this top to bottom; each step
should pass cleanly before the next.

## v0.1.0 local validation snapshot — August 12, 2026

This repository is ready for a **GitHub experimental research-alpha release**,
not a production-readiness claim.

- [x] Focused correctness/public-claims gates pass.
- [x] Ruff, formatter, Mypy, and Compileall gates pass.
- [x] Wheel and sdist build successfully; `twine check` passes.
- [x] The final wheel installs in a fresh virtualenv outside the repository.
- [x] Installed `lhos --help` and `lhos demo recovery-repair --json` pass.
- [x] README links, checked-in benchmark artifacts, license, and release notes
      are present.
- [ ] Complete non-slow repository suite proven green in one run. The August 12
      Windows run did not finish within 600 seconds and exposed one isolated
      SIGKILL-recovery flake; do not claim this gate passed.
- [ ] VPG commit-latency audit below the aspirational p99 target. Durable
      history growth is fixed, but full-projection commit work remains
      superlinear.
- [ ] Git commit, annotated `v0.1.0` tag, GitHub push, and release publication.
      This extracted workspace has no `.git` directory, so these are manual
      release-owner steps.

For exact measurements and limitations, use
[`docs/releases/v0.1.0.md`](releases/v0.1.0.md) as the release authority.

## Gate 1 — Code health

- [ ] Regular test gate green: `make test` — 0 failures. This remains an
      aspirational full-suite gate for v0.1.0; see the validation snapshot
      above rather than marking it complete.
- [ ] `python -m ruff check src/lhos tests/` clean.
- [ ] `python -m ruff format --check src/lhos examples/ tests/` clean.
- [ ] `python -m mypy src/lhos` clean.
- [ ] Slow VPG audits pass via `make test-stress` or the scheduled
      `.github/workflows/vpg-stress.yml` workflow.

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

- [ ] Root `LICENSE` is the complete Apache License 2.0 text.
- [ ] Root `NOTICE` identifies the project copyright holder.
- [ ] Wheel and sdist both include `LICENSE` and `NOTICE`.
- [ ] Project metadata declares `Apache-2.0` and the project author.

## Gate 6 — Ship

- [ ] Version bumped in `pyproject.toml`.
- [ ] Release tag created (e.g. `v0.1.0`) and annotated with the release notes.
- [ ] Release artifacts (wheel and sdist) uploaded.
- [ ] `pip install` rehearsed from the built wheel in a fresh environment.
- [ ] Launch-ready status recorded in the release notes.
