"""Architecture compliance — the Scheduler MUST NOT import Kernel / VPG
internals (Section 6 dependency boundary)."""

from __future__ import annotations

import importlib
import os
import pkgutil


def _d2_src() -> str:
    here = os.path.dirname(__file__)
    return os.path.abspath(
        os.path.join(
            here,
            os.pardir,
            os.pardir,
            os.pardir,
            os.pardir,
            "src",
            "lhos",
            "runtimes",
            "multi_agent",
        )
    )


def test_scheduler_source_file_forbidden_imports():
    """Static guard: grep the D2 source tree for any direct Kernel import."""
    src = _d2_src()
    offenders = []
    forbidden_prefixes = (
        "from lhos.agent_os.",
        "import lhos.agent_os.",
    )
    for root, _dirs, files in os.walk(src):
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    s = line.strip()
                    if s.startswith("#"):
                        continue
                    if any(
                        s.startswith(p) or ("lhos.agent_os." in s and "import" in s)
                        for p in forbidden_prefixes
                    ):
                        offenders.append(f"{path}: {s}")
    assert not offenders, "Forbidden Kernel/VPG imports in D2:\n" + "\n".join(offenders)


def test_scheduler_package_runtime_no_kernel_modules():
    """Walking the lhos.runtimes.multi_agent package, no imported module
    should resolve to lhos.agent_os internals."""
    import lhos.agent_os.kernel
    import lhos.agent_os.services
    import lhos.agent_os.storage  # noqa: F401
    import lhos.runtimes.multi_agent as d2_pkg

    forbidden = (
        "lhos.agent_os.kernel",
        "lhos.agent_os.services",
        "lhos.agent_os.storage",
    )
    violations: list[str] = []
    for importer, modname, ispkg in pkgutil.walk_packages(
        d2_pkg.__path__,
        prefix="lhos.runtimes.multi_agent.",
    ):
        mod = importlib.import_module(modname)
        for name, obj in vars(mod).items():
            if isinstance(obj, type):
                mod_of = getattr(obj, "__module__", "") or ""
                if any(mod_of.startswith(f) for f in forbidden):
                    violations.append(f"{modname} defines/imports {name} from {mod_of}")
    assert not violations, "D2 source module imports Kernel/VPG internals:\n" + "\n".join(
        violations
    )
