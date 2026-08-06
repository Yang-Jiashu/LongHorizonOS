"""Architecture / import-boundary tests for the VPG runtime.

D1 VerifiedProgress is a LOW-LEVEL runtime.  It must NOT import anything
from the higher-level ``lhos.agent_os`` package family (kernel, services,
drivers, storage).  Conversely the agent_os layer must not reach down into
``lhos.runtimes.verified_progress``.

These tests use static source analysis (``import`` line scanning), not runtime
imports, so they fail fast if a circular dependency is introduced.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # up to longhorizonos/
SRC = PROJECT_ROOT / "src"
VP = SRC / "lhos" / "runtimes" / "verified_progress"
AGENT_OS = SRC / "lhos" / "agent_os"


# Forbidden import patterns that VPG must NOT contain.
FORBIDDEN_VPG_IMPORTS = [
    re.compile(
        r"^\s*(?:from\s+lhos\.agent_os|import\s+lhos\.agent_os)\b"
    ),
]

# Forbidden import patterns that agent_os must NOT contain.
FORBIDDEN_AGENT_OS_IMPORTS = [
    re.compile(
        r"^\s*(?:from\s+lhos\.runtimes\.verified_progress"
        r"|import\s+lhos\.runtimes\.verified_progress)\b"
    ),
]


def _py_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*.py") if p.is_file())


def _grep(path: Path, pattern: re.Pattern) -> list[str]:
    """Return lines in path matching pattern."""
    matches: list[str] = []
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return matches
    for line in text.splitlines():
        if pattern.search(line):
            matches.append(line.strip())
    return matches


class TestVpgDoesNotImportAgentOs:
    """No file under ``lhos.runtimes.verified_progress`` may import from
    ``lhos.agent_os``."""

    def test_no_vpg_module_imports_agent_os(self):
        offenders: list[tuple[Path, list[str]]] = []
        for p in _py_files(VP):
            for pat in FORBIDDEN_VPG_IMPORTS:
                hits = _grep(p, pat)
                if hits:
                    offenders.append((p, hits))
        assert not offenders, (
            "VPG runtime must NOT import lhos.agent_os.*:\n"
            + "\n".join(f"  {p.relative_to(PROJECT_ROOT)}: {h}" for p, hs in offenders for h in hs)
        )

    def test_vpg_no_kernel_import(self):
        # Strict: scan for direct import of kernel internals.  The word
        # "Kernel" alone may appear in docstrings and protocol names — that
        # is acceptable.  Here we only forbid literal import statements of
        # lhos.agent_os.kernel or lhos.agent_os.services.
        pat = re.compile(
            r"^\s*(?:from|import)\s+lhos\.agent_os\.(?:kernel|services|drivers|storage)\b",
            re.MULTILINE,
        )
        offenders: list[tuple[str, str]] = []
        for p in _py_files(VP):
            text = p.read_text(encoding="utf-8")
            for line in text.splitlines():
                if pat.search(line):
                    offenders.append((str(p.relative_to(PROJECT_ROOT)), line.strip()))
        assert not offenders, "VPG must not import kernel internals:\n" + "\n".join(
            f"  {p}: {l}" for p, l in offenders
        )


class TestAgentOsDoesNotImportVpg:
    """Files under ``lhos.agent_os.*`` must not import from
    ``lhos.runtimes.verified_progress``."""

    def test_agent_os_package_exists(self):
        # Skip if agent_os has not been written yet.  The test still passes
        # because the boundary is only enforced when both sides exist.
        if not AGENT_OS.exists():
            pytest.skip("lhos.agent_os package does not exist yet")

    def test_no_agent_os_module_imports_vpg(self):
        if not AGENT_OS.exists():
            return  # nothing to enforce
        offenders: list[tuple[Path, list[str]]] = []
        for p in _py_files(AGENT_OS):
            for pat in FORBIDDEN_AGENT_OS_IMPORTS:
                hits = _grep(p, pat)
                if hits:
                    offenders.append((p, hits))
        assert not offenders, (
            "agent_os.* must NOT import lhos.runtimes.verified_progress:\n"
            + "\n".join(f"  {p.relative_to(PROJECT_ROOT)}: {h}" for p, hs in offenders for h in hs)
        )


class TestVpgIsSelfContainedLayer:
    """Structural sanity: VPG must define its own pure graph semantics
    using only stdlib + pydantic + its own package."""

    def test_vpg_does_not_import_lhos_kernels(self):
        if not VP.exists():
            pytest.skip("VPG package missing")
        pat = re.compile(r"^\s*(?:from|import)\s+lhos\.(?!runtimes\.verified_progress)", re.MULTILINE)
        offenders: list[str] = []
        for p in _py_files(VP):
            text = p.read_text(encoding="utf-8")
            for line in text.splitlines():
                if pat.search(line) and not line.strip().startswith("#"):
                    offenders.append(f"  {p.name}: {line.strip()}")
        assert not offenders, (
            "VPG runtime must not reach out to other lhos subpackages:\n"
            + "\n".join(offenders)
        )
