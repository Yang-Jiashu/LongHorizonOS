"""D3 architecture boundary (§39): layering enforced by imports + primitives.

Guards:
- D3 source must not IMPORT any Kernel/agent_os/D2 internal module.
- D3 source must not CALL any Claim (try_acquire_lease) or Dispatch
  (mark_dispatched / create_scheduler) primitive.
- D3 source must not reference KernelLeaseProvider.

Docstring prose that merely *names* the architecture concepts (Kernel,
ArtifactFS, ContextVM) to explain the boundary is allowed — what matters is
that D3 never imports or calls them.  This matches the §39 "does not import /
does not own" language rather than a naive substring ban.
"""

from __future__ import annotations

import pathlib
import re


def _find_repo_root(start: pathlib.Path) -> pathlib.Path:
    for cand in [start, *start.parents]:
        if (cand / "pyproject.toml").exists() and (cand / "src" / "lhos").exists():
            return cand
    raise RuntimeError("could not locate repo root (pyproject.toml + src/lhos)")


REPO = _find_repo_root(pathlib.Path(__file__).resolve())
SRC = REPO / "src" / "lhos"
D3_SRC = SRC / "runtimes" / "invalidation"


def _files(root: pathlib.Path) -> list[pathlib.Path]:
    return [p for p in root.rglob("*.py") if p.name != "__init__.py"]


_IMPORT_RE = re.compile(
    r"^\s*(?:from|import)\s+("
    r"lhos\.agent_os.*|lhos\.runtimes\.multi_agent.*"
    r")",
    re.MULTILINE,
)


def test_d3_never_imports_kernel_or_d2():
    for p in _files(D3_SRC):
        src = p.read_text(encoding="utf-8")
        for m in _IMPORT_RE.finditer(src):
            raise AssertionError(f"{p} must not import kernel/D2 internal: {m.group(1)!r}")
        # stripped-line presence as exact module path is equally disallowed
        for banned in (
            "import lhos.agent_os",
            "from lhos.agent_os",
            "import lhos.runtimes.multi_agent",
            "from lhos.runtimes.multi_agent",
        ):
            assert banned not in src, f"{p} must not import {banned!r}"


def test_d3_never_calls_claim_or_dispatch_primitive():
    banned_call_sites = (
        "try_acquire_lease",
        "mark_dispatched",
        "create_scheduler",
        "KernelLeaseProvider",
    )
    for p in _files(D3_SRC):
        src = p.read_text(encoding="utf-8")
        for b in banned_call_sites:
            assert b not in src, f"{p} must not reference claim/dispatch primitive {b!r}"


def test_d2_does_not_depend_on_d3():
    d2_files = _files(SRC / "runtimes" / "multi_agent")
    for p in d2_files:
        src = p.read_text(encoding="utf-8")
        assert "invalidation" not in src, f"{p} must not import D3"
