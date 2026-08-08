"""LongHorizonOS E1 — SDK ↔ Core architecture boundary.

Enforces the E1 rule: the SDK imports only Core public APIs + standard lib; it
must not import tests, the legacy plane, or another runtime's internals, and
Core must not import the SDK.
"""

from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent  # repo root
SRC = ROOT / "src" / "lhos"
SDK = SRC / "sdk"


def _files(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        p for p in root.rglob("*.py") if p.name != "__init__.py" and "__pycache__" not in str(p)
    ]


def test_sdk_never_imports_tests():
    for p in _files(SDK):
        src = p.read_text()
        assert "tests" not in src, f"{p} must not import tests"


def test_sdk_never_imports_legacy_plane():
    legacy = (
        "lhos.cli",
        "lhos.graph",
        "lhos.runtime",
        "lhos.agents",
        "lhos.domain",
        "lhos.ports",
        "lhos.infrastructure",
        "lhos.benchmarks",
        "lhos.verification",
    )
    for p in _files(SDK):
        src = p.read_text()
        for banned in legacy:
            assert banned not in src, f"{p} must not import {banned!r}"


def test_sdk_imports_only_public_core_api():
    """SDK may import only agent_os.sdk / agent_os.kernel models / runtimes SDKs
    and its own modules; never runtimes internals or another runtime directly."""
    allowed_prefixes = (
        "from lhos.sdk",
        "from lhos.agent_os",
        "from lhos.runtimes.verified_progress",
        "from lhos.runtimes.multi_agent",
        "from lhos.runtimes.invalidation",
    )
    for p in _files(SDK):
        src = p.read_text()
        for line in src.splitlines():
            ls = line.strip()
            if ls.startswith("from lhos") or ls.startswith("import lhos"):
                ok = any(ls.startswith(a) for a in allowed_prefixes)
                assert ok, f"{p}: {ls!r} not via public Core API"


def test_core_never_imports_sdk():
    imports_sdk = []
    for p in (ROOT / "src" / "lhos").rglob("*.py"):
        if (
            "__pycache__" in str(p)
            or p.name == "__init__.py"
            or str(p).endswith("/sdk/__init__.py")
        ):
            continue
        if "lhos.sdk" in p.read_text():
            imports_sdk.append(str(p))
    assert imports_sdk == [], f"Core must never import the SDK: {imports_sdk}"
