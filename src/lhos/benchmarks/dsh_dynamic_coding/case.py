"""Static workload definition for the real DeepSeek Harness coding pilot."""

from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TaskSpec:
    task_id: str
    dependencies: tuple[str, ...]
    target_files: tuple[str, ...]
    public_test: str
    artifact_id: str
    inputs: tuple[str, ...]


TASK_SPECS: dict[str, TaskSpec] = {
    "pricing_core": TaskSpec(
        task_id="pricing_core",
        dependencies=(),
        target_files=("src/pricing_service/core.py",),
        public_test="tests_public/test_core.py",
        artifact_id="src/pricing_service/core.py",
        inputs=("requirements/pricing_contract.json", "tests_public/test_core.py"),
    ),
    "audit": TaskSpec(
        task_id="audit",
        dependencies=(),
        target_files=("src/pricing_service/audit.py",),
        public_test="tests_public/test_audit.py",
        artifact_id="src/pricing_service/audit.py",
        inputs=("requirements/audit_contract.json", "tests_public/test_audit.py"),
    ),
    "pricing_api": TaskSpec(
        task_id="pricing_api",
        dependencies=("pricing_core",),
        target_files=("src/pricing_service/api.py",),
        public_test="tests_public/test_api.py",
        artifact_id="src/pricing_service/api.py",
        inputs=(
            "requirements/pricing_contract.json",
            "tests_public/test_api.py",
            "src/pricing_service/core.py",
        ),
    ),
    "integration": TaskSpec(
        task_id="integration",
        dependencies=("pricing_api", "audit"),
        target_files=("src/pricing_service/service.py",),
        public_test="tests_public/test_integration.py",
        artifact_id="src/pricing_service/service.py",
        inputs=(
            "tests_public/test_integration.py",
            "src/pricing_service/api.py",
            "src/pricing_service/audit.py",
        ),
    ),
}

TASK_ORDER: tuple[str, ...] = ("pricing_core", "audit", "pricing_api", "integration")
AFFECTED_V2: tuple[str, ...] = ("pricing_core", "pricing_api", "integration")
PRESERVED_V2: tuple[str, ...] = ("audit",)


def benchmark_root() -> Path:
    return Path(__file__).resolve().parents[4] / "benchmarks" / "real_dsh_dynamic_coding"


def create_workspace(destination: Path) -> None:
    shutil.copytree(
        benchmark_root() / "template_repo",
        destination,
        ignore=shutil.ignore_patterns(
            ".pytest_cache",
            "__pycache__",
            "*.pyc",
            "*.pyo",
        ),
    )


def apply_v2_mutation(workspace: Path) -> None:
    source = benchmark_root() / "mutation_v2"
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        target = workspace / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def protected_files(workspace: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            (
                workspace / "requirements" / "pricing_contract.json",
                workspace / "requirements" / "audit_contract.json",
                *(workspace / "tests_public").glob("test_*.py"),
            ),
            key=lambda path: str(path),
        )
    )


def content_hashes(paths: tuple[Path, ...]) -> dict[str, str]:
    return {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def artifact_content(workspace: Path, task_id: str) -> str:
    spec = TASK_SPECS[task_id]
    parts: list[str] = []
    for relative in spec.target_files:
        path = workspace / relative
        parts.append(f"## {relative}\n{path.read_text(encoding='utf-8')}")
    return "\n\n".join(parts)


def task_prompt(task_id: str, version: int) -> str:
    spec = TASK_SPECS[task_id]
    targets = ", ".join(f"`{item}`" for item in spec.target_files)
    common = (
        "Work only in the current repository. Do not use web search, subagents, "
        "workflows, or external services. Inspect the requirement and test files "
        "listed below, edit only the target implementation file(s), and run the "
        f"targeted test with `python -m pytest -q {spec.public_test}`. "
        "Do not edit tests, requirement JSON, pyproject.toml, or files owned by "
        "other tasks. Finish only after the targeted test passes."
    )
    if task_id == "pricing_core":
        task = (
            f"Implement pricing contract version {version} in {targets}. Read "
            "`requirements/pricing_contract.json` and `tests_public/test_core.py`. "
            "Use Decimal arithmetic and implement every validation and rounding rule."
        )
    elif task_id == "pricing_api":
        task = (
            f"Implement pricing API contract version {version} in {targets}. Read "
            "`requirements/pricing_contract.json`, `tests_public/test_api.py`, and "
            "the current core implementation. Preserve the exact public wire shape."
        )
    elif task_id == "audit":
        version_note = (
            "The audit contract is unchanged by pricing contract version 2; verify "
            "the existing implementation and change it only if the audit test fails."
            if version == 2
            else "Implement the independent audit contract."
        )
        task = (
            f"{version_note} Target {targets}. Read "
            "`requirements/audit_contract.json` and `tests_public/test_audit.py`."
        )
    elif task_id == "integration":
        task = (
            f"Implement integration behavior for pricing contract version {version} "
            f"in {targets}. Read `tests_public/test_integration.py` plus the current "
            "pricing API and audit implementations. The audit event must use the "
            "final quoted total."
        )
    else:
        raise KeyError(task_id)
    return f"# Task: {task_id}\n\n{task}\n\n{common}\n"
