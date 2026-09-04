"""Architecture tests (Step 2).

Verifies that:
- Real agent roles (RealInitialPlanner, RealNodeWorker, RealSemanticReconciler)
  accept an injected LLMClient and do NOT construct SenseNovaClient internally.
- SenseNovaClient is only imported in the composition root and infrastructure.
- The agents/ and runtime/ directories do not import SenseNovaClient.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Directories where SenseNovaClient must NOT be imported.
FORBIDDEN_DIRS = [
    "src/lhos/agents",
    "src/lhos/runtime",
    "src/lhos/graph",
]

# Files where SenseNovaClient MAY be imported.
ALLOWED_LOCATIONS = [
    "src/lhos/infrastructure/llm/sensenova.py",
    "src/lhos/infrastructure/llm/logged_client.py",
    "scripts/run_vertical_slice.py",
    "tests/",
]


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _find_python_files(directory: str) -> list[Path]:
    """Find all Python files in a directory."""
    root = PROJECT_ROOT / directory
    if not root.exists():
        return []
    return list(root.rglob("*.py"))


def _imports_sensenova(file_path: Path) -> bool:
    """Check if a Python file imports SenseNovaClient."""
    try:
        content = file_path.read_text(encoding="utf-8")
        tree = ast.parse(content)
    except Exception:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "sensenova" in node.module.lower():
                return True
            for alias in node.names:
                if "SenseNova" in alias.name:
                    return True
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if "sensenova" in alias.name.lower():
                    return True
    return False


def _constructs_sensenova(file_path: Path) -> bool:
    """Check if a Python file constructs SenseNovaClient(...)."""
    try:
        content = file_path.read_text(encoding="utf-8")
        tree = ast.parse(content)
    except Exception:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and "SenseNova" in func.id:
                return True
            if isinstance(func, ast.Attribute) and "SenseNova" in func.attr:
                return True
    return False


class TestAgentsDoNotConstructProviderClients:
    """test_agents_do_not_construct_provider_clients"""

    @pytest.mark.parametrize("forbidden_dir", FORBIDDEN_DIRS)
    def test_no_sensenova_import_in_forbidden_dirs(self, forbidden_dir):
        """No file in agents/ or runtime/ imports SenseNovaClient."""
        files = _find_python_files(forbidden_dir)
        violations = [f for f in files if _imports_sensenova(f)]
        assert not violations, (
            f"SenseNovaClient is imported in forbidden directory {forbidden_dir}: "
            f"{[str(f.relative_to(PROJECT_ROOT)) for f in violations]}"
        )

    @pytest.mark.parametrize("forbidden_dir", FORBIDDEN_DIRS)
    def test_no_sensenova_construction_in_forbidden_dirs(self, forbidden_dir):
        """No file in agents/ or runtime/ constructs SenseNovaClient(...)."""
        files = _find_python_files(forbidden_dir)
        violations = [f for f in files if _constructs_sensenova(f)]
        assert not violations, (
            f"SenseNovaClient is constructed in forbidden directory {forbidden_dir}: "
            f"{[str(f.relative_to(PROJECT_ROOT)) for f in violations]}"
        )


class TestAllRealRolesUseInjectedLLMClient:
    """test_all_real_roles_use_injected_llm_client"""

    def test_planner_accepts_client(self):
        """RealInitialPlanner.__init__ accepts a client parameter."""
        import inspect

        from lhos.agents.real_planner import RealInitialPlanner

        sig = inspect.signature(RealInitialPlanner.__init__)
        assert "client" in sig.parameters

    def test_worker_accepts_client(self):
        """RealNodeWorker.__init__ accepts a client parameter."""
        import inspect

        from lhos.agents.real_worker import RealNodeWorker

        sig = inspect.signature(RealNodeWorker.__init__)
        assert "client" in sig.parameters

    def test_reconciler_accepts_client(self):
        """RealSemanticReconciler.__init__ accepts a client parameter."""
        import inspect

        from lhos.agents.real_reconciler import RealSemanticReconciler

        sig = inspect.signature(RealSemanticReconciler.__init__)
        assert "client" in sig.parameters

    def test_worker_adapter_accepts_client(self):
        """LLMWorkerAdapter.__init__ accepts a client parameter."""
        import inspect

        from lhos.agents.llm_worker_adapter import LLMWorkerAdapter

        sig = inspect.signature(LLMWorkerAdapter.__init__)
        assert "client" in sig.parameters
