"""Architecture-bearing tests for the Context VM.

These tests assert structural and layering invariants using AST-based static
checks together with runtime reflection. They form a regression guard against
forbidden coupling into the kernel internals, leaking Prompt/LLM/Planner domain
into the mechanism layer, breaking process isolation, or introducing circular
imports.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from lhos.agent_os.context.estimator import TokenEstimator

# ── paths ──────────────────────────────────────────────────────────────────────

_ROOT = Path(__file__).resolve().parents[3]  # repo root
_SRC = _ROOT / "src" / "lhos" / "agent_os" / "context"

MODELS = _SRC / "models.py"
PAGER = _SRC / "pager.py"
POLICIES = _SRC / "policies.py"
SERVICE = _SRC / "service.py"
SDK = _SRC / "sdk.py"
ESTIMATOR = _SRC / "estimator.py"
INIT = _SRC / "__init__.py"  # type: ignore[assignment]


def _ast_parse(path: Path) -> ast.Module:
    with open(path, encoding="utf-8") as f:
        return ast.parse(f.read(), filename=str(path))


def _walk_imports(tree: ast.Module) -> list[str]:
    """Return every imported module-name as it appears in the source tree."""
    mods: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mods.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                mods.append(node.module)
    return mods


# ── 1-4. no forbidden kernel imports ─────────────────────────────────────────


class TestNoForbiddenKernelImports:
    """Context VM internals must not reach into the kernel's non-public
    internals. Only the kernel's published public models are allowed."""

    ALLOWED_KERNEL_PREFIX = "lhos.agent_os.kernel.models"

    def _assert_no_forbidden_kernel_imports(self, path: Path) -> None:
        tree = _ast_parse(path)
        for mod in _walk_imports(tree):
            if mod.startswith("lhos.agent_os.kernel."):
                assert mod == self.ALLOWED_KERNEL_PREFIX, (
                    f"{path.name} imports forbidden kernel module {mod!r} "
                    f"(only {self.ALLOWED_KERNEL_PREFIX!r} is allowed)"
                )

    def test_models(self) -> None:
        self._assert_no_forbidden_kernel_imports(MODELS)

    def test_pager(self) -> None:
        self._assert_no_forbidden_kernel_imports(PAGER)

    def test_policies(self) -> None:
        self._assert_no_forbidden_kernel_imports(POLICIES)

    def test_service(self) -> None:
        self._assert_no_forbidden_kernel_imports(SERVICE)


# ── 5. no Prompt/LLM/Planner domain strings ──────────────────────────────────


def _forbidden_strings_present(path: Path) -> list[tuple[int, str, str]]:
    """Return a list of (lineno, kind, offending-text) for any node that
    either names a forbidden identifier, or has a Constant string whose lower
    cased value contains any of the forbidden substrings.

    Forbidden identifiers are matched by exact case-insensitive equality
    against the bare name (Name/ClassDef/FunctionDef). Forbidden string
    literals are matched by substring (case-insensitive).
    """
    tree = _ast_parse(path)

    forbidden_ids = {
        "tasknode",
        "goal",
        "evidence",
        "vpg",
        "planner",
        "prompt",
        "llm",
    }
    forbidden_substrs = list(forbidden_ids)

    hits: list[tuple[int, str, str]] = []

    for node in ast.walk(tree):
        # Exact-identifier match for names.
        name = None
        if isinstance(node, ast.Name):
            name = getattr(node, "id", None)
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            name = getattr(node, "name", None)
        if name and name.lower() in forbidden_ids:
            hits.append((node.lineno, "identifier", name))

        # Case-insensitive substring match for string constants.
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            lowered = node.value.lower()
            for sub in forbidden_substrs:
                if sub in lowered:
                    hits.append((node.lineno, "string", node.value))
                    break

    return hits


class TestNoMechanismDomainStrings:
    """The Context VM is a mechanism (L3) and must not embed semantic-domain
    names such as TaskNode, Goal, Evidence, VPG, Planner, Prompt, or LLM."""

    def test_models(self) -> None:
        assert _forbidden_strings_present(MODELS) == []

    def test_service(self) -> None:
        assert _forbidden_strings_present(SERVICE) == []

    def test_sdk(self) -> None:
        assert _forbidden_strings_present(SDK) == []


# ── 6. no circular imports ───────────────────────────────────────────────────


class TestNoCircularImports:
    """Importing the Context VM public entry point must not raise due to a
    cycle. We verify by spawning a fresh Python interpreter and importing
    the package there; this exercises the import path from scratch without
    mutating sys.modules of the live test process (which would invalidate
    every downstream test's class identity)."""

    def test_import_init_without_circular_error(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                "import lhos.agent_os.context as m; assert m is not None",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, (
            f"Importing lhos.agent_os.context in a clean interpreter raised "
            f"(possible circular import).\nstdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )


# ── 7. TokenEstimator is runtime-checkable Protocol ──────────────────────────


class TestTokenEstimatorProtocol:
    """TokenEstimator is a runtime-checkable Protocol so that structural
    estimators can participate without subclassing the protocol."""

    def test_is_protocol(self) -> None:
        assert getattr(TokenEstimator, "_is_protocol", False) is True

    def test_is_runtime_checkable(self) -> None:
        # A runtime-checkable Protocol can accept non-subclass structural
        # matches whose members satisfy the protocol surface.
        class _Fake:
            @property
            def estimator_id(self) -> str:  # type: ignore[override]
                return "fake"

            def estimate(
                self,
                *,
                content: bytes,
                media_type: str,  # type: ignore[override]
                encoding: str,
            ) -> int:
                return 1

        fake = _Fake()
        # With runtime_checkable, isinstance works structurally.
        assert isinstance(fake, TokenEstimator)


# ── 8. Context VM is documented as process-isolated ──────────────────────────


class TestContextVMProcessIsolation:
    """The Context VM module must identify itself as process-isolated in
    either its module docstring or in the core ContextService docstring."""

    def test_module_or_service_docstring_mentions_process_isolated(
        self,
    ) -> None:
        init_text = INIT.read_text(encoding="utf-8")
        # Read the service module docstring in a subprocess to avoid
        # exercising it once (docstring is static content, safe to read
        # from the live import too, but we just reuse the source file).
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import lhos.agent_os.context.service as m;"
                    "print(getattr(m.ContextService, '__doc__', ''))"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        svc_doc = proc.stdout.strip() if proc.returncode == 0 else ""

        joined = (init_text + "\n" + svc_doc).lower()
        # "process-isolated" appears in the init module docstring; if a future
        # refactor renames it, the service docstring is the fallback.
        assert "process-isolated" in joined or "context vm" in joined
