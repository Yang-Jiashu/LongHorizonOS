"""Architecture tests — verify dependency isolation.

agent_os must NOT import any old runtime modules.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).parent.parent.parent / "src" / "lhos" / "agent_os"

FORBIDDEN_KERNEL_IMPORTS = [
    "lhos.graph",
    "lhos.runtime",
    "lhos.agents",
    "lhos.benchmarks",
    "verified_progress",
    "lhos.planner",
    "lhos.worker",
    "lhos.reconciler",
]

FORBIDDEN_DRIVER_IMPORTS = [
    "lhos.runtime",
    "lhos.benchmarks",
    "lhos.graph",
    "lhos.agents",
]


def _extract_imports(file_path: Path) -> list[str]:
    """Extract all import module paths from a Python file."""
    try:
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def _get_python_files(directory: Path) -> list[Path]:
    return list(directory.rglob("*.py"))


class TestKernelNoOldImports:
    """agent_os/kernel must not import old runtime modules."""

    @pytest.mark.parametrize("forbidden", FORBIDDEN_KERNEL_IMPORTS)
    def test_kernel_no_forbidden_imports(self, forbidden: str) -> None:
        kernel_dir = SRC_ROOT / "kernel"
        for py_file in _get_python_files(kernel_dir):
            imports = _extract_imports(py_file)
            for imp in imports:
                assert not imp.startswith(forbidden), f"{py_file} imports forbidden module: {imp}"


class TestDriversNoOldImports:
    """agent_os/drivers must not import old runtime modules."""

    @pytest.mark.parametrize("forbidden", FORBIDDEN_DRIVER_IMPORTS)
    def test_drivers_no_forbidden_imports(self, forbidden: str) -> None:
        drivers_dir = SRC_ROOT / "drivers"
        for py_file in _get_python_files(drivers_dir):
            imports = _extract_imports(py_file)
            for imp in imports:
                assert not imp.startswith(forbidden), f"{py_file} imports forbidden module: {imp}"


class TestAgentOSNoOldImports:
    """All of agent_os must not import old runtime modules."""

    @pytest.mark.parametrize("forbidden", FORBIDDEN_KERNEL_IMPORTS)
    def test_all_agent_os_no_forbidden_imports(self, forbidden: str) -> None:
        for py_file in _get_python_files(SRC_ROOT):
            imports = _extract_imports(py_file)
            for imp in imports:
                assert not imp.startswith(forbidden), f"{py_file} imports forbidden module: {imp}"


class TestOldCodeNoAgentOS:
    """Old code must not import agent_os (Phase B isolation)."""

    def test_old_code_no_agent_os_imports(self) -> None:
        old_src = SRC_ROOT.parent / "runtime"
        if not old_src.exists():
            old_src = SRC_ROOT.parent / "graph"
        if not old_src.exists():
            pytest.skip("No old runtime/graph directory found")

        for py_file in _get_python_files(old_src):
            imports = _extract_imports(py_file)
            for imp in imports:
                assert not imp.startswith("lhos.agent_os"), (
                    f"Old code {py_file} imports agent_os: {imp}"
                )
