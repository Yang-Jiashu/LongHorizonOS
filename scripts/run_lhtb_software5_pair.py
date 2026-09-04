"""Prepare and run paired LHTB tasks with the custom DSH Harbor agent.

The two arms use the same local-pilot task, Docker image, DSH build, StepFun
model, timeout, verifier, and Harbor continuation path:

* both arms use Harbor's ``same_conversation`` mode with binary feedback and
  the same pinned-checkout verifier-isolation behavior;
* ``dsh_fresh`` starts a fresh DSH home/session after each rejection;
* ``lhos_resume`` resumes the exact persisted DSH session (or applies its
  bounded semantic-context policy).

The run is a deterministic, within-task paired contrast under a fixed custom
DSH/Harbor task harness. It estimates declared-arm outcome differences only;
it is neither a randomized causal estimate nor an official leaderboard
result. The intended treatment is durable LHOS context reuse and its
controller policy, not a different Harbor/verifier feedback protocol. The
baseline reconstructs the original task instruction when it starts a fresh
continuation session.

The result only labels the LHOS arm as context reuse when durable resume
evidence passes the explicit gate in :func:`_resume_gate`.

With no task-selection flags the historical five software-engineering tasks
remain the default. ``prepare --all`` discovers every ``task.toml`` below
``tasks_root`` (46 for LHTB), while ``--task-names`` selects an explicit
ordered subset. ``--official-leaderboard-contract`` is an explicit shared-
setting audit mode: it selects all 46 tasks, uses the public uniform 5400-second
budget, disables controller slices, and emits ``delete: true`` configs. It does
not validate the complete modified-Harbor protocol, and its score remains
non-official because this runner uses a custom paired DSH adapter and (normally)
the local-pilot network-enabled task tree.
"""

# ruff: noqa: RUF001

from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import hashlib
import json
import math
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import time
import tomllib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.subprocess_agent import subprocess_task_executor

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_LABEL = "stepfun/step-3.7-flash"
REASONING_EFFORT = "medium"
AGENT_IMPORT_PATH = "scripts.lhtb_dsh_harbor_agent:LHTBDeepSeekHarnessAgent"
DEFAULT_AGENT_TIMEOUT_SECONDS = 300
DEFAULT_WORKER_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_TIME_SLICE_SECONDS = 75
DEFAULT_VERIFIED_REWARD_THRESHOLD = 0.95
DEFAULT_FULL_REWARD_THRESHOLD = 1.0
OFFICIAL_LEADERBOARD_TASK_COUNT = 46
OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS = 5_400
OFFICIAL_LEADERBOARD_N_ATTEMPTS = 1
OFFICIAL_LEADERBOARD_TIMEOUT_MULTIPLIER = 1.0
OFFICIAL_LEADERBOARD_SOLVED_THRESHOLD = 0.95
OFFICIAL_LEADERBOARD_PARSER_NAME = "json"
OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD = 8_000
OFFICIAL_LEADERBOARD_RECORD_TERMINAL_SESSION = True
OFFICIAL_LEADERBOARD_REFERENCE_HARNESS = "terminus-2"
OFFICIAL_LEADERBOARD_PLATFORM = "linux/amd64"
OFFICIAL_HARDENED_CONTINUATION_TASK_COUNT = 30
OFFICIAL_HARDENED_INTERIM_PASS_THRESHOLD = 1.0
OFFICIAL_LHTB_SOURCE_COMMIT = "84d7ba5ee34fae6c11f0d7cb8ed5faa73a9ece54"
OFFICIAL_LEADERBOARD_CONTRACT_SCHEMA_V1 = "lhtb-official-leaderboard-contract.v1"
HARBOR_PREBUILT_PULL_POLICY_PATH = (
    "harbor/src/harbor/environments/docker/docker-compose-prebuilt.yaml"
)
DEFAULT_SEMANTIC_CONTEXT_CONFIG = {
    "semantic_context_control": True,
    "semantic_context_min_phases_before_restart": 2,
    "semantic_context_cache_tokens_per_call_threshold": 16_000,
    "semantic_context_cache_growth_ratio_threshold": 1.75,
    "semantic_context_max_consecutive_max_tokens": 2,
    "semantic_context_cumulative_cache_read_tokens_threshold": 96_000,
    "semantic_context_cumulative_cache_requires_max_tokens": 2,
    "semantic_context_no_progress_phases": 2,
    "semantic_context_no_progress_event_ratio_threshold": 0.10,
    "semantic_context_quality_stall_phases": 3,
    "semantic_context_quality_stall_error_ratio": 0.5,
    "semantic_context_quality_stall_min_test_calls": 1,
    "semantic_context_quality_stall_write_regression_phases": 2,
    "semantic_context_cooldown_phases": 2,
    "semantic_context_max_restarts": 6,
    "semantic_context_restart_unproductive_restarts": 2,
    "semantic_context_restart_payoff_window_phases": 3,
    "semantic_context_restart_payoff_rebloat_ratio": 1.0,
    "semantic_context_restart_payoff_failures": 2,
    "semantic_handoff_max_items": 12,
    "semantic_handoff_max_chars": 2_048,
    # 1 = legacy SemanticContextPolicy (unchanged default); 2 = isolated
    # restart-as-investment policy (optimization_v2.py).
    "semantic_policy_version": int(os.environ.get("LHOS_SEMANTIC_POLICY_VERSION", "1")),
    # v2-only knobs (silently dropped when policy version 1 is active; see
    # optimization_v2.py for semantics).  The safety-relevant ones take env
    # overrides so experiment batches can bisect without code edits.
    # Convergence halt defaults OFF (replay cannot distinguish a brain-dead
    # session from a long solver call at the observation level); stop-loss
    # halt defaults ON (8M cache burn with zero progress is unambiguous).
    "semantic_v2_convergence_halt_enabled": os.environ.get("LHOS_V2_CONVERGENCE_HALT", "0") == "1",
    "semantic_v2_stop_loss_halt_enabled": os.environ.get("LHOS_V2_STOP_LOSS_HALT", "1") == "1",
    "semantic_v2_stop_loss_cache_tokens": int(
        os.environ.get("LHOS_V2_STOP_LOSS_CACHE_TOKENS", "8000000")
    ),
    "semantic_v2_convergence_window_phases": int(
        os.environ.get("LHOS_V2_CONVERGENCE_WINDOW_PHASES", "4")
    ),
    "semantic_v2_max_converging_deferrals": int(
        os.environ.get("LHOS_V2_MAX_CONVERGING_DEFERRALS", "3")
    ),
    "semantic_v2_adaptive_slice_enabled": os.environ.get("LHOS_V2_ADAPTIVE_SLICE", "1") == "1",
}
ARMS = ("dsh_fresh", "lhos_resume")
ARM_MODES = ("both", "fresh", "lhos")
ARM_MODE_TO_ARMS = {
    "both": ARMS,
    "fresh": ("dsh_fresh",),
    "lhos": ("lhos_resume",),
}
_SESSION_ID = re.compile(r"session-[A-Za-z0-9-]+\Z")
MANIFEST_SCHEMA_V1 = "lhos-lhtb-dsh-software5-pair.v1"
MANIFEST_SCHEMA_V2 = "lhos-lhtb-dsh-pair.v2"
# V3 marks manifests produced after the controlled-pair protocol was made
# mandatory. V1/V2 remain readable for historical pilots such as R4.
MANIFEST_SCHEMA_V3 = "lhos-lhtb-dsh-controlled-pair.v3"
CONTROLLED_PAIR_SCHEMA_V1 = "lhos-lhtb-controlled-pair.v1"
PREBUILD_SCHEMA_V1 = "lhos-lhtb-dsh-software5-prebuild.v1"
PREBUILD_SCHEMA_V2 = "lhos-lhtb-dsh-prebuild.v2"
PAIR_ADMISSION_SCHEMA_V1 = "lhos-lhtb-pair-admission.v1"
RUN_PROGRESS_SCHEMA_V1 = "lhos-lhtb-run-progress.v1"
TERMINAL_ARM_STATUSES = frozenset({"completed", "failed"})
WINDOWS_CONTROL_C_EXIT = 0xC000013A
MAX_INFRASTRUCTURE_ARM_RETRIES = 1
MAX_CENSORSHIP_RETRIES = 3
# stepfun API connection-level abort (exit 134, SIGABRT before any agent
# event is produced). Probabilistic under concurrency; a clean resample
# usually connects. Retry more aggressively than censorship.
MAX_CONNECTION_RETRIES = 4
NBODY_TASK_NAME = "nbody-accel-iterative"
NBODY_VERIFIER_SEED_ENV = "LHTB_NBODY_VERIFIER_SEED"
GENERALS_TASK_NAME = "generals-bot-arena"
GENERALS_VERIFIER_SEED_ENV = "PYTHONHASHSEED"
PAIRED_VERIFIER_SEED_ENVS = {
    NBODY_TASK_NAME: NBODY_VERIFIER_SEED_ENV,
    GENERALS_TASK_NAME: GENERALS_VERIFIER_SEED_ENV,
}
# ---------------------------------------------------------------------------
# Proactive resource governance ("active stop-loss") for the LHOS arm.
#
# The LHOS arm can, on genuinely hard tasks, keep spending provider tokens
# inside a single continuation with no verified progress (observed up to 120M
# token units on super-mario with reward 0). An OS that merely reacts to
# changes cannot bound this; a proactive controller watches live progress and
# forcibly stops the run. Deterministic triggers (validated against all 46
# real runs so no high-reward task is ever killed):
#
#   1. token_budget_exceeded  - hard per-task token ceiling.
#   2. semantic_control_inert - semantic context control is claimed (flag on)
#                               but zero semantic decisions were ever made past
#                               a large spend: the controller never engaged,
#                               stop the bleed instead of letting it run out.
#   3. no_progress            - no new events and no new tokens over a window
#                               (stalled / looping computation).
#
# A fourth rule (invalid-topology retry loops) was implemented, validated and
# then REMOVED: real data proved grammar-fuzz (reward 0.899) and poc-exploit
# (reward 0.892) -- two of LHOS's best results -- also resume with invalid
# topology. Without a runtime reward signal no token/topology threshold can
# separate them from the wasteful runs, so the safe choice is not to guess.
# The true fix for single-continuation blowups is to enable time_slice (see
# ACTIVE-STOPLOSS-20260831.md), which gives every long session periodic
# checkpoints instead of relying on an outer kill.
# ---------------------------------------------------------------------------
LHOS_ACTIVE_GOVERNANCE_ENABLED = True
# Runtime knobs can be overridden per-run via environment variables, so a
# pilot can validate the stop-loss path quickly (e.g. LHOS_TOKEN_BUDGET_UNITS=3000000)
# and a production run can tune budgets without editing code.
LHOS_TOKEN_BUDGET_UNITS = int(
    os.environ.get("LHOS_TOKEN_BUDGET_UNITS", "80000000")
)
# Semantic-control-inert threshold. Validated against all 46 real runs:
# - super-mario (120.9M units, reward 0, dec=0) MUST be caught here -> 60M.
# - apex-law (58.5M, reward 0.79, dec=0) is a NORMAL completed run and must be
#   spared -> threshold above 58.5M.
# - apex-investment (51.9M, reward 0.028) is left alone (safe > aggressive).
LHOS_CONTROL_INERT_TOKEN_UNITS = int(
    os.environ.get("LHOS_CONTROL_INERT_TOKEN_UNITS", "60000000")
)
LHOS_NO_PROGRESS_WINDOW_SECONDS = float(
    os.environ.get("LHOS_NO_PROGRESS_WINDOW_SECONDS", "180")
)
LHOS_NO_PROGRESS_POLL_SECONDS = float(
    os.environ.get("LHOS_NO_PROGRESS_POLL_SECONDS", "20")
)
# Progressive-quality stall stop-loss (漏洞3, runtime sense). Uses the
# zero-cost heartbeat quality probe (test_calls / error_calls /
# distinct_writes) to stop an agent that keeps running tests/builds but they
# keep failing while the distinct artifact set stops growing. Off by default
# (0) so it never fires without an explicit tuned run; ablation enables it.
# token floor sits between the inert rule (60M) and nothing so microscopy
# (55M) / scientific-figure (46M) / modflow6 (33M) become reachable while
# high-reward grammar-fuzz (47M) is spared because its writes keep growing.
LHOS_QUALITY_STALL_TOKEN_UNITS = int(
    os.environ.get("LHOS_QUALITY_STALL_TOKEN_UNITS", "0")
)
LHOS_QUALITY_STALL_SAMPLES = int(
    os.environ.get("LHOS_QUALITY_STALL_SAMPLES", "4")
)
LHOS_QUALITY_STALL_MAX_ERROR_RATIO = float(
    os.environ.get("LHOS_QUALITY_STALL_MAX_ERROR_RATIO", "0.5")
)
TASK_WORKDIR_OVERRIDES = {
    "tabular-data-feature-covshift": "/workspace",
}
TASK_TOML_COMPATIBILITY_TRANSFORMS = {
    "langchain-version-migration": (
        'command = "rm -rf /app/outputs /tests && mkdir -p /app/outputs"',
        'command = "rm -rf /app/outputs && mkdir -p /app/outputs"',
    ),
    NBODY_TASK_NAME: (
        'command = "rm -rf /app/workspace /app/output /app/harness '
        '/opt/nbody-verifier /tests && mkdir -p /app/workspace /app/output"',
        'command = "rm -rf /app/workspace /app/output /app/harness '
        '/opt/nbody-verifier && mkdir -p /app/workspace /app/output"',
    ),
}
STOCHASTIC_VERIFIER_COHORT = frozenset(
    {
        NBODY_TASK_NAME,
        GENERALS_TASK_NAME,
    }
)
CONTINUATION_BOUNDARY_MODES = (
    "natural",
    "forced_time_slice",
    "one_shot",
)
_PROVIDER_CENSORSHIP = re.compile(
    r"(?i)(?:"
    r"censorship[\s_-]*blocked|"
    r"content[\s_-]*policy|"
    r"moderation[\s_-]*blocked|"
    r"content[\s_-]*filter|"
    r"(?:http|status(?:[\s_-]*code)?)\s*[:=]?\s*451\b"
    r")"
)


@dataclass(frozen=True, slots=True)
class SelectedTask:
    name: str
    priority: int
    rationale: str


class InfrastructureInterruptedError(RuntimeError):
    """A controller/worker interruption that may be retried once from clean state."""

    def __init__(
        self,
        message: str,
        *,
        reason: str,
        worker: dict[str, Any] | None = None,
        launcher_exit_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.worker = dict(worker or {})
        self.launcher_exit_code = launcher_exit_code


SELECTED_TASKS = (
    SelectedTask(
        "great-expectations-audit",
        1,
        "Small Python data-pipeline task with a 3600s task-declared budget.",
    ),
    SelectedTask(
        "alp-paper-reproduction",
        2,
        "Small Python implementation task with a 3600s task-declared budget.",
    ),
    SelectedTask(
        "foldseek-paper-reproduction",
        3,
        "Pure Python task with moderate fixture data and a 3600s task-declared budget.",
    ),
    SelectedTask(
        "unison-paper-reproduction",
        4,
        "Very small Docker context and no heavy native toolchain.",
    ),
    SelectedTask(
        "langchain-version-migration",
        5,
        "Representative dependency migration with existing local Docker cache.",
    ),
)

_DEFAULT_TASK_BY_NAME = {task.name: task for task in SELECTED_TASKS}


def _task_directories(tasks_root: Path) -> dict[str, Path]:
    if not tasks_root.is_dir():
        raise RuntimeError(f"tasks root does not exist: {tasks_root}")
    discovered = {
        path.name: path
        for path in sorted(tasks_root.iterdir(), key=lambda candidate: candidate.name)
        if path.is_dir() and (path / "task.toml").is_file()
    }
    if not discovered:
        raise RuntimeError(f"no task.toml files found under {tasks_root}")
    return discovered


def _normalize_task_names(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or ():
        for item in str(raw).split(","):
            name = item.strip()
            if not name or name in seen:
                continue
            if Path(name).name != name or name in {".", ".."}:
                raise RuntimeError(f"invalid task name: {name!r}")
            seen.add(name)
            normalized.append(name)
    return normalized


def _select_tasks(
    tasks_root: Path,
    *,
    all_tasks: bool = False,
    task_names: list[str] | tuple[str, ...] | None = None,
) -> tuple[tuple[SelectedTask, ...], dict[str, Any]]:
    discovered = _task_directories(tasks_root)
    explicit = _normalize_task_names(task_names)
    if all_tasks and explicit:
        raise RuntimeError("--all and --task-names are mutually exclusive")

    if all_tasks:
        names = sorted(discovered)
        mode = "all"
        if len(names) != 46:
            raise RuntimeError(
                f"--all expects the 46-task LHTB suite, found {len(names)} under {tasks_root}"
            )
    elif explicit:
        names = explicit
        mode = "task_names"
    else:
        names = [task.name for task in SELECTED_TASKS]
        mode = "default_software5"

    missing = [name for name in names if name not in discovered]
    if missing:
        raise RuntimeError(f"selected tasks are missing from tasks_root: {missing}")

    selected: list[SelectedTask] = []
    for priority, name in enumerate(names, start=1):
        historical = _DEFAULT_TASK_BY_NAME.get(name)
        if mode == "default_software5" and historical is not None:
            selected.append(historical)
        else:
            selected.append(
                SelectedTask(
                    name=name,
                    priority=priority,
                    rationale=(
                        "Selected from tasks_root by --all."
                        if mode == "all"
                        else "Selected explicitly by --task-names."
                    ),
                )
            )
    return tuple(selected), {
        "mode": mode,
        "discovered_task_count": len(discovered),
        "discovered_task_names": sorted(discovered),
        "requested_task_count": len(selected),
        "requested_task_names": [task.name for task in selected],
    }


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


ARM_RECORD_PARSER_ERROR = "missing_or_invalid_arm_record"


def _load_arm_record(path: Path) -> tuple[dict[str, Any], str | None]:
    """Load one persisted arm record while preserving why it was unusable."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}, ARM_RECORD_PARSER_ERROR
    if not isinstance(value, dict) or not value:
        return {}, ARM_RECORD_PARSER_ERROR
    return value, None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_hash(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        relative = item.relative_to(path).as_posix().encode("utf-8")
        content = item.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _canonical_payload_bytes(value: bytes) -> bytes:
    """Normalize checkout-only text EOL differences for provenance checks."""

    if b"\x00" in value:
        return value
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return value
    return text.replace("\r\n", "\n").encode("utf-8")


def _git_text(root: Path, *args: str) -> str:
    completed = _run(["git", "-C", str(root), *args])
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


def _git_blob(root: Path, object_name: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(root), "show", object_name],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.decode(errors="replace").strip())
    return completed.stdout


def _git_worktree_provenance(root: Path) -> dict[str, Any]:
    """Record dirty paths without treating an unrelated checkout edit as payload."""

    completed = _run(
        [
            "git",
            "-C",
            str(root),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ]
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    status = completed.stdout.rstrip("\r\n")
    entries: list[dict[str, str]] = []
    for line in status.splitlines():
        if len(line) < 4:
            continue
        entries.append({"status": line[:2], "path": line[3:]})
    paths = [entry["path"] for entry in entries]
    known_runner_mutations = [
        path for path in paths if path == HARBOR_PREBUILT_PULL_POLICY_PATH
    ]
    return {
        "schema_version": "lhtb-git-worktree-provenance.v1",
        "dirty": bool(entries),
        "entry_count": len(entries),
        "paths": paths,
        "entries": entries,
        "known_runner_mutation_paths": known_runner_mutations,
        "harbor_prebuilt_pull_policy_patch_present": bool(known_runner_mutations),
    }


def _verify_official_task_payload(
    *,
    lhtb_root: Path,
    tasks_root: Path,
    task_name: str,
    declared_compatibility_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prefix = f"tasks/{task_name}/"
    tracked = _git_text(
        lhtb_root,
        "ls-tree",
        "-r",
        "--name-only",
        "HEAD",
        "--",
        f"tasks/{task_name}",
    ).splitlines()
    if not tracked:
        raise RuntimeError(f"{task_name}: not found in LHTB Git HEAD")
    changed_payload: list[str] = []
    missing_payload: list[str] = []
    for repo_path in tracked:
        relative = repo_path.removeprefix(prefix)
        if relative == "task.toml":
            continue
        local = tasks_root / task_name / relative
        if not local.is_file():
            missing_payload.append(relative)
            continue
        if _canonical_payload_bytes(local.read_bytes()) != _canonical_payload_bytes(
            _git_blob(lhtb_root, f"HEAD:{repo_path}")
        ):
            changed_payload.append(relative)
    if missing_payload or changed_payload:
        raise RuntimeError(
            f"{task_name}: local-pilot payload differs from Git HEAD; "
            f"missing={missing_payload}, changed={changed_payload}"
        )

    official_bytes = _git_blob(
        lhtb_root,
        f"HEAD:tasks/{task_name}/task.toml",
    )
    local_path = tasks_root / task_name / "task.toml"
    local_bytes = local_path.read_bytes()
    canonical_local = _canonical_payload_bytes(local_bytes)
    canonical_official = _canonical_payload_bytes(official_bytes)
    task_toml_override = canonical_local != canonical_official
    compatibility_override = False
    if task_toml_override:
        local_text = canonical_local.decode("utf-8")
        official_text = canonical_official.decode("utf-8")
        normalized = re.sub(
            r"(?m)^allow_internet = true$",
            "allow_internet = false",
            local_text,
        )
        if normalized != official_text:
            transform = TASK_TOML_COMPATIBILITY_TRANSFORMS.get(task_name)
            override = declared_compatibility_override or {}
            expected_official_sha = hashlib.sha256(official_bytes).hexdigest()
            expected_local_sha = hashlib.sha256(local_bytes).hexdigest()
            override_valid = (
                transform is not None
                and override.get("kind")
                == "preserve_separate_verifier_tests_after_healthcheck"
                and override.get("official_task_toml_sha256")
                == expected_official_sha
                and override.get("local_task_toml_sha256") == expected_local_sha
            )
            expected = official_text
            if transform is not None:
                old, new = transform
                if expected.count(old) != 1:
                    raise RuntimeError(
                        f"{task_name}: expected compatibility transform source is missing"
                    )
                expected = expected.replace(old, new, 1)
            if not override_valid or normalized != expected:
                raise RuntimeError(
                    f"{task_name}: task.toml differs from Git HEAD beyond declared overrides"
                )
            compatibility_override = True
    return {
        "official_payload_matches": not compatibility_override,
        "network_override": task_toml_override,
        "compatibility_override": compatibility_override,
        "official_task_toml_sha256": hashlib.sha256(official_bytes).hexdigest(),
        "local_task_toml_sha256": hashlib.sha256(local_bytes).hexdigest(),
    }


def _harbor_metadata(harbor_project: Path) -> dict[str, str]:
    version = _run(
        ["uv", "run", "--project", str(harbor_project), "harbor", "--version"],
        timeout=120,
    )
    if version.returncode != 0:
        raise RuntimeError(version.stderr.strip() or version.stdout.strip())
    return {
        "project": str(harbor_project),
        "version": version.stdout.strip(),
        "commit": _git_text(harbor_project, "rev-parse", "HEAD"),
    }


def _task_metadata(
    lhtb_root: Path,
    tasks_root: Path,
    selected: SelectedTask,
    local_pilot_manifest: dict[str, Any],
) -> dict[str, Any]:
    root = tasks_root / selected.name
    config_path = root / "task.toml"
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    metadata = config.get("metadata", {})
    environment = config.get("environment", {})
    agent = config.get("agent", {})
    verifier = config.get("verifier", {})
    verifier_environment = verifier.get("environment", {})
    if environment.get("allow_internet") is not True:
        raise RuntimeError(
            f"{selected.name}: container-local DSH requires local-pilot allow_internet=true"
        )
    expected_hash = local_pilot_manifest.get("task_toml_sha256", {}).get(selected.name)
    observed_hash = _sha256_file(config_path)
    if expected_hash and expected_hash != observed_hash:
        raise RuntimeError(f"{selected.name}: task.toml no longer matches local-pilot manifest")
    provenance = _verify_official_task_payload(
        lhtb_root=lhtb_root,
        tasks_root=tasks_root,
        task_name=selected.name,
        declared_compatibility_override=(
            local_pilot_manifest.get("task_toml_compatibility_overrides", {}).get(
                selected.name
            )
        ),
    )
    dockerfile = root / "environment" / "Dockerfile"
    if not dockerfile.is_file():
        raise RuntimeError(f"{selected.name}: missing environment/Dockerfile")
    modified = set(local_pilot_manifest.get("modified_allow_internet_tasks", ()))
    declared_override = selected.name in modified
    docker_image = str(environment.get("docker_image", "") or "").strip()
    if not docker_image:
        raise RuntimeError(f"{selected.name}: [environment].docker_image is required")
    verifier_docker_image = str(
        verifier_environment.get("docker_image", "") or docker_image
    ).strip()
    required_images = tuple(dict.fromkeys((docker_image, verifier_docker_image)))
    stochastic_verifier = selected.name in STOCHASTIC_VERIFIER_COHORT
    return {
        "name": selected.name,
        "priority": selected.priority,
        "selection_rationale": selected.rationale,
        "task_name": config.get("task", {}).get("name"),
        "description": config.get("task", {}).get("description"),
        "category": metadata.get("category"),
        "difficulty": metadata.get("difficulty"),
        "expert_time_estimate_min": metadata.get("expert_time_estimate_min"),
        "official_agent_timeout_seconds": agent.get("timeout_sec"),
        "continue_until_timeout": bool(agent.get("continue_until_timeout", False)),
        "official_build_timeout_seconds": environment.get("build_timeout_sec"),
        "official_verifier_timeout_seconds": verifier.get("timeout_sec"),
        "verifier_environment_mode": verifier.get("environment_mode", "same"),
        "verifier_cpus": verifier_environment.get("cpus"),
        "verifier_memory_mb": verifier_environment.get("memory_mb"),
        "verifier_storage_mb": verifier_environment.get("storage_mb"),
        "stochastic_verifier": stochastic_verifier,
        "stochastic_pair_controlled": not stochastic_verifier,
        "stochastic_control_mode": (
            "pending_paired_control"
            if selected.name in PAIRED_VERIFIER_SEED_ENVS
            else "uncontrolled_stochastic_cohort"
            if stochastic_verifier
            else "not_declared_stochastic"
        ),
        "resource_metadata_source": "prepare_task_toml",
        "docker_image": docker_image,
        "verifier_docker_image": verifier_docker_image,
        "required_docker_images": list(required_images),
        "allow_internet": environment.get("allow_internet"),
        "official_payload_matches": provenance["official_payload_matches"],
        "local_pilot_network_override": provenance["network_override"],
        "local_pilot_compatibility_override": provenance["compatibility_override"],
        "local_pilot_manifest_declared_override": declared_override,
        "local_pilot_manifest_override_mismatch": (
            declared_override != provenance["network_override"]
        ),
        "cpus": environment.get("cpus"),
        "memory_mb": environment.get("memory_mb"),
        "storage_mb": environment.get("storage_mb"),
        "task_toml_sha256": observed_hash,
        "official_task_toml_sha256": provenance["official_task_toml_sha256"],
        "task_content_sha256": _directory_hash(root),
    }


def _validate_runtime(
    *,
    runtime_root: Path,
    patch_path: Path,
    agent_module: Path,
) -> dict[str, Any]:
    node = runtime_root / "node-v24.19.0-linux-x64" / "bin" / "node"
    dsh = (
        runtime_root / "dsh-0.1.0-rc.8" / "node_modules" / "@deepseek-ai" / "dsh" / "lib" / "bin.js"
    )
    required = (runtime_root, node, dsh, patch_path, agent_module)
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"missing DSH runtime inputs: {missing}")
    patch_text = patch_path.read_text(encoding="utf-8")
    for expected in (
        "model: step-3.7-flash",
        "reasoning: medium",
        "apiKeyEnv: STEPFUN_API_KEY",
        "https://api.stepfun.com/step_plan/v1",
    ):
        if expected not in patch_text:
            raise RuntimeError(f"StepFun patch is missing {expected!r}")
    return {
        "host_root": str(runtime_root),
        "container_root": "/opt/lhtb-runtime",
        "node_host_path": str(node),
        "node_container_path": ("/opt/lhtb-runtime/node-v24.19.0-linux-x64/bin/node"),
        "node_sha256": _sha256_file(node),
        "dsh_host_path": str(dsh),
        "dsh_container_path": (
            "/opt/lhtb-runtime/dsh-0.1.0-rc.8/node_modules/@deepseek-ai/dsh/lib/bin.js"
        ),
        "dsh_entry_sha256": _sha256_file(dsh),
        "dsh_version": "0.1.0-rc.8",
        "patch_host_path": str(patch_path),
        "patch_container_path": ("/opt/lhtb-patches/stepfun-3.7-pi-ai.cordis.patch.yml"),
        "patch_sha256": _sha256_file(patch_path),
        "agent_import_path": AGENT_IMPORT_PATH,
        "agent_module": str(agent_module),
        "agent_module_sha256": _sha256_file(agent_module),
    }


def _validate_agent_import(harbor_project: Path) -> None:
    env = dict(os.environ)
    python_path = os.pathsep.join((str(REPO_ROOT), str(REPO_ROOT / "src")))
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = python_path if not inherited else python_path + os.pathsep + inherited
    command = [
        "uv",
        "run",
        "--project",
        str(harbor_project),
        "python",
        "-c",
        (
            "from scripts.lhtb_dsh_harbor_agent import "
            "LHTBDeepSeekHarnessAgent as A; print(A.name())"
        ),
    ]
    completed = _run(command, cwd=REPO_ROOT, env=env, timeout=120)
    if completed.returncode != 0 or completed.stdout.strip() != "lhos-deepseek-harness":
        raise RuntimeError(
            "custom Harbor agent import failed: "
            + (completed.stderr.strip() or completed.stdout.strip())
        )


def _agent_kwargs(
    arm: str,
    runtime: dict[str, Any],
    *,
    time_slice_seconds: int | float | None,
    workdir: str = "/app",
    official_protocol_fields: bool = False,
) -> dict[str, Any]:
    kwargs = {
        "arm": "baseline" if arm == "dsh_fresh" else "lhos",
        # Both the runtime and provider patch mounts live below /opt.
        "bundle_root": "/opt",
        "node_path": runtime["node_container_path"],
        "dsh_path": runtime["dsh_container_path"],
        "patch_path": runtime["patch_container_path"],
        "credential_env": "STEPFUN_API_KEY",
        "profile": "headless",
        "workdir": workdir,
        "expected_dsh_version": runtime["dsh_version"],
        "permission_mode": "workspace-write",
        "tools_mode": "native",
        "require_container_internet": True,
        # Both arms deliberately enter Harbor's same-conversation path. The
        # custom agent implements the control arm as a fresh DSH session and
        # the LHOS arm as durable-session reuse; this keeps verifier feedback
        # and isolation constant across the pair.
        "controlled_pair_mode": True,
        "time_slice_seconds": time_slice_seconds,
        **DEFAULT_SEMANTIC_CONTEXT_CONFIG,
    }
    if official_protocol_fields:
        # Keep the Harbor-facing parser/summarizer contract visible in the
        # generated YAML even though the custom DSH adapter does not use the
        # stock Terminus-2 implementation. The manifest records that harness
        # deviation explicitly; these fields are not a reference-harness claim.
        kwargs.update(
            {
                "parser_name": OFFICIAL_LEADERBOARD_PARSER_NAME,
                "enable_summarize": True,
                "proactive_summarization_threshold": (
                    OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD
                ),
                "record_terminal_session": True,
            }
        )
    return kwargs


def _config_payload(
    *,
    arm: str,
    job_name: str,
    tasks_root: Path,
    runtime_root: Path,
    patch_path: Path,
    task_name: str,
    agent_timeout_seconds: int | float,
    time_slice_seconds: int | float | None,
    runtime: dict[str, Any],
    verifier_env: dict[str, str] | None = None,
    environment_delete: bool = False,
    official_protocol_fields: bool = False,
) -> dict[str, Any]:
    payload = {
        "job_name": job_name,
        "jobs_dir": "./jobs",
        "n_attempts": 1,
        "n_concurrent_trials": 1,
        "timeout_multiplier": 1.0,
        "environment": {
            "type": "docker",
            "force_build": False,
            # Historical paired pilots retain the prebuilt image between arms.
            # The opt-in leaderboard contract overrides this to the official
            # delete=true lifecycle.
            "delete": environment_delete,
            "mounts": [
                {
                    "type": "bind",
                    "source": runtime_root.as_posix(),
                    "target": runtime["container_root"],
                    "read_only": True,
                    "bind": {"create_host_path": False},
                },
                {
                    "type": "bind",
                    "source": patch_path.parent.as_posix(),
                    "target": "/opt/lhtb-patches",
                    "read_only": True,
                    "bind": {"create_host_path": False},
                },
            ],
        },
        "agents": [
            {
                "import_path": AGENT_IMPORT_PATH,
                "model_name": MODEL_LABEL,
                "override_timeout_sec": agent_timeout_seconds,
                # AgentFactory resolves this template in the Harbor process.
                # The adapter forwards the credential only to each DSH exec.
                "env": {"STEPFUN_API_KEY": "${STEPFUN_API_KEY}"},
                "kwargs": _agent_kwargs(
                    arm,
                    runtime,
                    time_slice_seconds=time_slice_seconds,
                    workdir=TASK_WORKDIR_OVERRIDES.get(task_name, "/app"),
                    official_protocol_fields=official_protocol_fields,
                ),
            }
        ],
        "datasets": [
            {
                "path": tasks_root.as_posix(),
                "task_names": [task_name],
            }
        ],
    }
    if verifier_env:
        payload["verifier"] = {"env": dict(verifier_env)}
    return payload


def _parity_hash(payload: dict[str, Any]) -> str:
    normalized = json.loads(json.dumps(payload))
    normalized.pop("job_name", None)
    normalized["agents"][0]["kwargs"].pop("arm", None)
    return hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _write_configs(
    output: Path,
    *,
    selected_tasks: tuple[SelectedTask, ...],
    tasks_root: Path,
    runtime_root: Path,
    patch_path: Path,
    agent_timeout_seconds: int | float,
    time_slice_seconds: int | float | None,
    runtime: dict[str, Any],
    job_prefix: str,
    task_timeout_overrides: dict[str, int | float] | None = None,
    task_time_slice_overrides: dict[str, int | float | None] | None = None,
    paired_verifier_seeds: dict[str, str] | None = None,
    environment_delete: bool = False,
    official_protocol_fields: bool = False,
) -> dict[str, dict[str, Any]]:
    root = output / "configs"
    root.mkdir(parents=True, exist_ok=False)
    configs: dict[str, dict[str, Any]] = {}
    for selected in selected_tasks:
        record: dict[str, Any] = {}
        parity: set[str] = set()
        verifier_env: dict[str, str] | None = None
        if selected.name in PAIRED_VERIFIER_SEED_ENVS:
            verifier_seed = str((paired_verifier_seeds or {}).get(selected.name, ""))
            seed_source = "provided_map"
            if not verifier_seed:
                verifier_seed = str(
                    secrets.randbits(64 if selected.name == NBODY_TASK_NAME else 32)
                )
                seed_source = "random_per_prepare"
            verifier_seed_env = PAIRED_VERIFIER_SEED_ENVS[selected.name]
            verifier_env = {verifier_seed_env: verifier_seed}
            record.update(
                {
                    "paired_verifier_seeded": True,
                    "paired_verifier_seed_source": seed_source,
                    "paired_verifier_seed_env": verifier_seed_env,
                    "paired_verifier_seed_sha256": hashlib.sha256(
                        verifier_seed.encode("utf-8")
                    ).hexdigest(),
                }
            )
        for arm in ARMS:
            job_name = f"{job_prefix}-{selected.name}-{arm.replace('_', '-')}"
            payload = _config_payload(
                arm=arm,
                job_name=job_name,
                tasks_root=tasks_root,
                runtime_root=runtime_root,
                patch_path=patch_path,
                task_name=selected.name,
                agent_timeout_seconds=(
                    task_timeout_overrides.get(selected.name, agent_timeout_seconds)
                    if task_timeout_overrides
                    else agent_timeout_seconds
                ),
                time_slice_seconds=(
                    task_time_slice_overrides[selected.name]
                    if task_time_slice_overrides is not None
                    else time_slice_seconds
                ),
                runtime=runtime,
                verifier_env=verifier_env,
                environment_delete=environment_delete,
                official_protocol_fields=official_protocol_fields,
            )
            path = root / f"{selected.name}.{arm}.yaml"
            path.write_text(
                yaml.safe_dump(payload, sort_keys=False),
                encoding="utf-8",
            )
            record[arm] = str(path)
            record[f"{arm}_job_name"] = job_name
            parity.add(_parity_hash(payload))
        if len(parity) != 1:
            raise RuntimeError(f"{selected.name}: paired configs are not equivalent")
        record["parity_sha256"] = parity.pop()
        configs[selected.name] = record
    return configs


def _validate_official_leaderboard_contract(
    *,
    task_names: list[str] | tuple[str, ...],
    configs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Fail closed unless every generated Harbor config matches the public contract."""

    names = [str(name) for name in task_names]
    errors: list[str] = []
    if len(names) != OFFICIAL_LEADERBOARD_TASK_COUNT:
        errors.append(
            f"task_count={len(names)} (expected {OFFICIAL_LEADERBOARD_TASK_COUNT})"
        )
    if len(set(names)) != len(names):
        errors.append("task_names are not unique")
    if set(configs) != set(names):
        errors.append("config task set does not match selected task set")

    for task_name in names:
        record = configs.get(task_name)
        if not isinstance(record, dict):
            errors.append(f"{task_name}: missing config record")
            continue
        for arm in ARMS:
            config_path = Path(str(record.get(arm, "")))
            if not config_path.is_file():
                errors.append(f"{task_name}/{arm}: config file is missing")
                continue
            payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                errors.append(f"{task_name}/{arm}: config is not a mapping")
                continue
            if payload.get("n_attempts") != OFFICIAL_LEADERBOARD_N_ATTEMPTS:
                errors.append(f"{task_name}/{arm}: n_attempts must be 1")
            if payload.get("timeout_multiplier") != OFFICIAL_LEADERBOARD_TIMEOUT_MULTIPLIER:
                errors.append(f"{task_name}/{arm}: timeout_multiplier must be 1.0")
            environment = payload.get("environment") or {}
            if environment.get("type") != "docker":
                errors.append(f"{task_name}/{arm}: environment.type must be docker")
            if not isinstance(environment.get("force_build"), bool):
                errors.append(
                    f"{task_name}/{arm}: environment.force_build must be explicit"
                )
            if environment.get("delete") is not True:
                errors.append(f"{task_name}/{arm}: environment.delete must be true")
            agents = payload.get("agents") or []
            if len(agents) != 1 or not isinstance(agents[0], dict):
                errors.append(f"{task_name}/{arm}: exactly one agent is required")
            else:
                agent = agents[0]
                if agent.get("override_timeout_sec") != OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS:
                    errors.append(f"{task_name}/{arm}: override_timeout_sec must be 5400")
                kwargs = agent.get("kwargs") or {}
                if kwargs.get("time_slice_seconds") is not None:
                    errors.append(f"{task_name}/{arm}: time_slice_seconds must be null")
                if kwargs.get("parser_name") != OFFICIAL_LEADERBOARD_PARSER_NAME:
                    errors.append(
                        f"{task_name}/{arm}: parser_name must be {OFFICIAL_LEADERBOARD_PARSER_NAME!r}"
                    )
                if kwargs.get("enable_summarize") is not True:
                    errors.append(f"{task_name}/{arm}: enable_summarize must be true")
                if kwargs.get("proactive_summarization_threshold") != OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD:
                    errors.append(
                        f"{task_name}/{arm}: proactive_summarization_threshold must be "
                        f"{OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD}"
                    )
                if kwargs.get("record_terminal_session") is not OFFICIAL_LEADERBOARD_RECORD_TERMINAL_SESSION:
                    errors.append(
                        f"{task_name}/{arm}: record_terminal_session must be true"
                    )
            datasets = payload.get("datasets") or []
            configured_task_names = (
                datasets[0].get("task_names")
                if len(datasets) == 1 and isinstance(datasets[0], dict)
                else None
            )
            if configured_task_names != [task_name]:
                errors.append(
                    f"{task_name}/{arm}: dataset must select exactly the expected task"
                )

    if errors:
        preview = "; ".join(errors[:12])
        if len(errors) > 12:
            preview += f"; ... ({len(errors) - 12} more)"
        raise RuntimeError(f"official leaderboard contract validation failed: {preview}")
    return {
        "schema_version": OFFICIAL_LEADERBOARD_CONTRACT_SCHEMA_V1,
        "enabled": True,
        "config_validated": True,
        "alignment_scope": "shared_static_yaml_and_posthoc_metrics",
        "leaderboard_comparable": False,
        "official_protocol_complete": False,
        "task_count": OFFICIAL_LEADERBOARD_TASK_COUNT,
        "n_attempts": OFFICIAL_LEADERBOARD_N_ATTEMPTS,
        "uniform_agent_timeout_seconds": OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS,
        "timeout_multiplier": OFFICIAL_LEADERBOARD_TIMEOUT_MULTIPLIER,
        "artificial_time_slice_seconds": None,
        "environment_type": "docker",
        "environment_delete": True,
        "force_build_policy": "explicit_per_config; official leaderboard is model-specific",
        "reference_harness": OFFICIAL_LEADERBOARD_REFERENCE_HARNESS,
        "configured_harness": AGENT_IMPORT_PATH,
        "reference_harness_match": False,
        "reference_harbor_distribution": (
            "LHTB-bundled modified Harbor, not stock upstream Harbor"
        ),
        "reference_continue_until_timeout_task_count": (
            OFFICIAL_HARDENED_CONTINUATION_TASK_COUNT
        ),
        "reference_interim_full_pass_reward": OFFICIAL_HARDENED_INTERIM_PASS_THRESHOLD,
        "reference_current_hardening": [
            "binary verifier rejection",
            "agent process freeze during interim verification",
            "verifier artifact isolation before resume",
        ],
        "configured_harbor_semantics_validated": False,
        "published_leaderboard_generation": (
            "historical snapshot predating binary-feedback default and verifier isolation"
        ),
        "configured_model_yaml_match": False,
        "parser_name": OFFICIAL_LEADERBOARD_PARSER_NAME,
        "enable_summarize": True,
        "proactive_summarization_threshold": OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD,
        "record_terminal_session": OFFICIAL_LEADERBOARD_RECORD_TERMINAL_SESSION,
        "parser_fields_effective": False,
        "dataset_layout": "one_task_per_paired_config",
        "dataset_layout_matches_reference": False,
        "platform_required": OFFICIAL_LEADERBOARD_PLATFORM,
        "platform_env_observed": os.environ.get("DOCKER_DEFAULT_PLATFORM"),
        "primary_metric": "mean_reward_over_46_tasks",
        "solved_metric": "reward_gte_0.95",
        "solved_reward_threshold": OFFICIAL_LEADERBOARD_SOLVED_THRESHOLD,
        "official_score": False,
        "official_protocol": True,
        "official_score_reason": (
            "Only shared static YAML fields and post-hoc metric semantics are checked. "
            "The custom paired agent, local-pilot task tree, and unvalidated modified-Harbor "
            "semantics make this result non-comparable to the official leaderboard."
        ),
        "protocol_deviations": [
            "custom paired DSH harness instead of the Terminus-2 reference harness",
            (
                "complete parity with the LHTB-modified Harbor "
                "continue-until-timeout loop is not validated"
            ),
            (
                "the controlled pair now forces both arms through the same "
                "same-conversation/binary Harbor path, but full parity with the "
                "official Terminus-2 harness is not claimed"
            ),
            (
                "the published leaderboard snapshot predates current binary-feedback "
                "and verifier-isolation hardening and must be reported as a separate "
                "protocol generation"
            ),
            (
                "parser/summarizer fields are recorded for parity but are not "
                "executed by the custom DSH adapter"
            ),
            (
                "the configured StepFun/custom-DSH inference settings do not match "
                "a model-specific official leaderboard YAML"
            ),
            "local-pilot network overrides may be present in the task tree",
            "per-task paired dataset configs instead of one reference config selecting all 46 tasks",
            "force_build policy is controlled by this custom runner rather than the model leaderboard YAML",
            (
                "reference model force_build/concurrency values are audited only; "
                "the paired runner's prebuild and task-pair scheduling lifecycle "
                "is not the official Harbor lifecycle"
            ),
            "paired arms use n_concurrent_trials=1 rather than the reference model config's throughput setting",
        ],
    }


def _validate_official_model_yaml_reference(
    *,
    lhtb_root: Path,
    model_yaml: Path,
    expected_task_names: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Validate and summarize one tracked, sanitized leaderboard model YAML."""

    root = lhtb_root.resolve()
    path = model_yaml.resolve()
    try:
        relative_path = path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(
            "official model YAML must be inside the pinned LHTB checkout"
        ) from exc
    if (
        len(relative_path.parts) != 3
        or relative_path.parts[:2] != ("configs", "leaderboard")
        or relative_path.suffix.lower() not in {".yaml", ".yml"}
    ):
        raise RuntimeError(
            "official model YAML must be a configs/leaderboard/*.yaml file"
        )
    if not path.is_file():
        raise RuntimeError(f"official model YAML is missing: {path}")

    git_path = relative_path.as_posix()
    official_bytes = _git_blob(root, f"HEAD:{git_path}")
    local_bytes = path.read_bytes()
    if _canonical_payload_bytes(local_bytes) != _canonical_payload_bytes(official_bytes):
        raise RuntimeError(
            f"official model YAML differs from Git HEAD: {git_path}"
        )
    try:
        payload = yaml.safe_load(official_bytes.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise RuntimeError(f"official model YAML cannot be parsed: {git_path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("official model YAML must contain a mapping")

    errors: list[str] = []
    if payload.get("n_attempts") != OFFICIAL_LEADERBOARD_N_ATTEMPTS:
        errors.append("n_attempts must be 1")
    n_concurrent_trials = payload.get("n_concurrent_trials")
    if (
        isinstance(n_concurrent_trials, bool)
        or not isinstance(n_concurrent_trials, int)
        or n_concurrent_trials < 1
    ):
        errors.append("n_concurrent_trials must be a positive integer")
    if payload.get("timeout_multiplier") != OFFICIAL_LEADERBOARD_TIMEOUT_MULTIPLIER:
        errors.append("timeout_multiplier must be 1.0")

    environment = payload.get("environment") or {}
    if not isinstance(environment, dict):
        errors.append("environment must be a mapping")
        environment = {}
    if environment.get("type") != "docker":
        errors.append("environment.type must be docker")
    force_build = environment.get("force_build")
    if not isinstance(force_build, bool):
        errors.append("environment.force_build must be explicit")
    if environment.get("delete") is not True:
        errors.append("environment.delete must be true")

    agents = payload.get("agents") or []
    agent: dict[str, Any] = {}
    if len(agents) != 1 or not isinstance(agents[0], dict):
        errors.append("exactly one agent is required")
    else:
        agent = agents[0]
    agent_name = agent.get("name")
    if agent_name != OFFICIAL_LEADERBOARD_REFERENCE_HARNESS:
        errors.append(
            f"agent.name must be {OFFICIAL_LEADERBOARD_REFERENCE_HARNESS!r}"
        )
    model_name = agent.get("model_name")
    if not isinstance(model_name, str) or not model_name.strip():
        errors.append("agent.model_name must be a non-empty string")
    if agent.get("override_timeout_sec") != OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS:
        errors.append("agent.override_timeout_sec must be 5400")
    kwargs = agent.get("kwargs") or {}
    if not isinstance(kwargs, dict):
        errors.append("agent.kwargs must be a mapping")
        kwargs = {}
    if kwargs.get("parser_name") != OFFICIAL_LEADERBOARD_PARSER_NAME:
        errors.append(f"agent.kwargs.parser_name must be {OFFICIAL_LEADERBOARD_PARSER_NAME!r}")
    if kwargs.get("enable_summarize") is not True:
        errors.append("agent.kwargs.enable_summarize must be true")
    if (
        kwargs.get("proactive_summarization_threshold")
        != OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD
    ):
        errors.append(
            "agent.kwargs.proactive_summarization_threshold must be "
            f"{OFFICIAL_LEADERBOARD_SUMMARIZE_THRESHOLD}"
        )
    if kwargs.get("record_terminal_session") is not OFFICIAL_LEADERBOARD_RECORD_TERMINAL_SESSION:
        errors.append("agent.kwargs.record_terminal_session must be true")

    datasets = payload.get("datasets") or []
    dataset: dict[str, Any] = {}
    if len(datasets) != 1 or not isinstance(datasets[0], dict):
        errors.append("exactly one dataset is required")
    else:
        dataset = datasets[0]
    dataset_path = dataset.get("path")
    if dataset_path != "./tasks":
        errors.append("datasets[0].path must be './tasks'")
    reference_task_names = dataset.get("task_names")
    expected_names = [str(name) for name in expected_task_names]
    if len(expected_names) != OFFICIAL_LEADERBOARD_TASK_COUNT or len(
        set(expected_names)
    ) != OFFICIAL_LEADERBOARD_TASK_COUNT:
        errors.append("expected task set must contain 46 unique names")
    if reference_task_names != expected_names:
        errors.append("datasets[0].task_names must exactly match the ordered 46-task suite")

    if errors:
        raise RuntimeError(
            "official model YAML validation failed: " + "; ".join(errors)
        )
    reference_names = [str(name) for name in reference_task_names]
    names_sha256 = hashlib.sha256(
        json.dumps(reference_names, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": "lhtb-official-model-yaml-reference.v1",
        "validated": True,
        "repository_relative_path": git_path,
        "git_object": f"HEAD:{git_path}",
        "git_blob_sha256": hashlib.sha256(official_bytes).hexdigest(),
        "agent_name": agent_name,
        "model_name": model_name.strip(),
        "n_concurrent_trials": n_concurrent_trials,
        "force_build": force_build,
        "record_terminal_session": kwargs.get("record_terminal_session"),
        "datasets_path": dataset_path,
        "task_count": len(reference_names),
        "task_names": reference_names,
        "task_names_sha256": names_sha256,
    }


def _validate_official_manifest_contract(manifest: dict[str, Any]) -> None:
    """Validate the manifest-wide settings that YAML checks cannot see."""

    contract = manifest.get("official_leaderboard_contract")
    if not isinstance(contract, dict) or contract.get("enabled") is not True:
        return
    errors: list[str] = []
    if contract.get("alignment_scope") != "shared_static_yaml_and_posthoc_metrics":
        errors.append(
            "contract alignment_scope must be shared_static_yaml_and_posthoc_metrics"
        )
    if contract.get("leaderboard_comparable") is not False:
        errors.append("contract leaderboard_comparable must be false")
    if contract.get("official_protocol_complete") is not False:
        errors.append("contract official_protocol_complete must be false")
    source_worktree = contract.get("source_worktree")
    if not isinstance(source_worktree, dict):
        errors.append("contract source_worktree must be a mapping")
    else:
        source_dirty = source_worktree.get("dirty")
        source_paths = source_worktree.get("paths")
        if not isinstance(source_dirty, bool):
            errors.append("contract source_worktree dirty must be boolean")
        if not isinstance(source_paths, list) or not all(
            isinstance(path, str) for path in source_paths
        ):
            errors.append("contract source_worktree paths must be a string list")
        if contract.get("source_worktree_dirty") is not source_dirty:
            errors.append("contract source_worktree_dirty does not match provenance")
        if contract.get("source_worktree_dirty_paths") != source_paths:
            errors.append("contract source_worktree_dirty_paths do not match provenance")
    model_reference = contract.get("official_model_yaml_reference")
    model_reference_validated = contract.get(
        "official_model_yaml_reference_validated", False
    )
    if model_reference is None:
        if model_reference_validated is not False:
            errors.append(
                "contract official_model_yaml_reference_validated must be false "
                "when no reference is recorded"
            )
    elif not isinstance(model_reference, dict):
        errors.append("contract official_model_yaml_reference must be a mapping")
    else:
        if model_reference_validated is not True:
            errors.append(
                "contract official_model_yaml_reference_validated must be true"
            )
        if model_reference.get("validated") is not True:
            errors.append("official model YAML reference validated must be true")
        if model_reference.get("agent_name") != OFFICIAL_LEADERBOARD_REFERENCE_HARNESS:
            errors.append("official model YAML reference agent_name must be terminus-2")
        if model_reference.get("task_count") != OFFICIAL_LEADERBOARD_TASK_COUNT:
            errors.append("official model YAML reference task_count must be 46")
        if model_reference.get("datasets_path") != "./tasks":
            errors.append("official model YAML reference datasets_path must be ./tasks")
        if model_reference.get("record_terminal_session") is not True:
            errors.append(
                "official model YAML reference record_terminal_session must be true"
            )
        if not isinstance(model_reference.get("force_build"), bool):
            errors.append("official model YAML reference force_build must be explicit")
        reference_concurrency = model_reference.get("n_concurrent_trials")
        if (
            isinstance(reference_concurrency, bool)
            or not isinstance(reference_concurrency, int)
            or reference_concurrency < 1
        ):
            errors.append(
                "official model YAML reference n_concurrent_trials must be positive"
            )
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != OFFICIAL_LEADERBOARD_TASK_COUNT:
        errors.append("manifest task_count must be 46")
    if manifest.get("agent_timeout_seconds") != OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS:
        errors.append("manifest agent_timeout_seconds must be 5400")
    if manifest.get("agent_timeout_mode") != "official_uniform_5400":
        errors.append("manifest agent_timeout_mode must be official_uniform_5400")
    if manifest.get("time_slice_seconds") is not None:
        errors.append("manifest time_slice_seconds must be null")
    if manifest.get("time_slice_mode") != "disabled":
        errors.append("manifest time_slice_mode must be disabled")
    if manifest.get("n_attempts") != OFFICIAL_LEADERBOARD_N_ATTEMPTS:
        errors.append("manifest n_attempts must be 1")
    if manifest.get("timeout_multiplier") != OFFICIAL_LEADERBOARD_TIMEOUT_MULTIPLIER:
        errors.append("manifest timeout_multiplier must be 1.0")
    if manifest.get("environment_delete") is not True:
        errors.append("manifest environment_delete must be true")
    if errors:
        raise RuntimeError(
            "official leaderboard manifest validation failed: " + "; ".join(errors)
        )


def _validate_controlled_pair_manifest(manifest: dict[str, Any]) -> None:
    """Fail closed when a new controlled pair loses its common harness path.

    V1/V2 manifests without a controlled block are historical outputs and are
    intentionally left readable. V3 is the opt-in schema emitted by ``prepare``
    and cannot silently downgrade to an uncontrolled run.
    """

    design = manifest.get("controlled_pair_experiment")
    schema = manifest.get("schema_version")
    strict_schema = schema == MANIFEST_SCHEMA_V3
    if not isinstance(design, dict) or design.get("enabled") is not True:
        if strict_schema:
            raise RuntimeError(
                "controlled pair manifest validation failed: V3 requires "
                "controlled_pair_experiment.enabled=true"
            )
        return

    errors: list[str] = []
    if design.get("schema_version") != CONTROLLED_PAIR_SCHEMA_V1:
        errors.append("controlled pair schema_version is invalid")
    if design.get("harness_constant") is not True:
        errors.append("controlled pair harness_constant must be true")
    if design.get("harbor_continue_mode") != "same_conversation":
        errors.append("controlled pair Harbor mode must be same_conversation")
    if design.get("verifier_feedback_mode") != "binary":
        errors.append("controlled pair verifier feedback must be binary")
    if design.get("control_arm") != "dsh_fresh":
        errors.append("controlled pair control arm must be dsh_fresh")
    if design.get("treatment_arm") != "lhos_resume":
        errors.append("controlled pair treatment arm must be lhos_resume")
    if design.get("execution_layout") not in {
        "same_batch_interleaved",
        "separate_arm_batches",
        None,
    }:
        errors.append("controlled pair execution_layout is invalid")
    observed_layout = design.get("observed_execution_layout")
    if observed_layout not in {
        "not_recorded",
        "same_batch_interleaved",
        "separate_arm_batches",
        None,
    }:
        errors.append("controlled pair observed_execution_layout is invalid")
    if design.get("validity") not in {"partial", "strict"}:
        errors.append("controlled pair validity must be partial or strict")
    if design.get("verifier_artifact_isolation_guaranteed") is not False:
        errors.append(
            "controlled pair verifier_artifact_isolation_guaranteed must be false"
        )
    if strict_schema:
        for field in ("agent", "model", "lhtb_source_commit"):
            if not manifest.get(field):
                errors.append(f"controlled pair manifest {field} pin is missing")
        if not isinstance(manifest.get("runtime"), dict) or not manifest["runtime"]:
            errors.append("controlled pair runtime pins are missing")

    tasks_raw = manifest.get("tasks")
    task_names: list[str] = []
    if not isinstance(tasks_raw, list):
        errors.append("controlled pair tasks must be a list")
    else:
        task_names = [str(task.get("name", "")) for task in tasks_raw if isinstance(task, dict)]
        if len(task_names) != len(tasks_raw) or any(not name for name in task_names):
            errors.append("controlled pair task records must have names")
        if len(set(task_names)) != len(task_names):
            errors.append("controlled pair task names must be unique")

    configs = manifest.get("configs")
    if not isinstance(configs, dict):
        errors.append("controlled pair configs must be a mapping")
        configs = {}
    elif task_names and set(configs) != set(task_names):
        errors.append("controlled pair task/config name sets do not match")

    for task_name, record in configs.items():
        if not isinstance(record, dict):
            errors.append(f"{task_name}: controlled pair config record is invalid")
            continue
        declared_parity = str(record.get("parity_sha256", "") or "")
        arm_parities: list[str] = []
        for arm in ARMS:
            path = Path(str(record.get(arm, "")))
            if not path.is_file():
                errors.append(f"{task_name}/{arm}: controlled pair config is missing")
                continue
            try:
                payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                errors.append(
                    f"{task_name}/{arm}: controlled pair config is unreadable:"
                    f"{type(exc).__name__}"
                )
                continue
            if not isinstance(payload, dict):
                errors.append(f"{task_name}/{arm}: controlled pair config is not a mapping")
                continue
            agents = payload.get("agents") or []
            if len(agents) != 1 or not isinstance(agents[0], dict):
                errors.append(f"{task_name}/{arm}: exactly one custom agent is required")
                continue
            try:
                actual_parity = _parity_hash(payload)
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                errors.append(
                    f"{task_name}/{arm}: config parity cannot be computed:"
                    f"{type(exc).__name__}"
                )
                continue
            arm_parities.append(actual_parity)
            if not declared_parity or actual_parity != declared_parity:
                errors.append(f"{task_name}/{arm}: config parity hash is stale")
            agent = agents[0]
            if strict_schema and agent.get("import_path") != AGENT_IMPORT_PATH:
                errors.append(f"{task_name}/{arm}: custom agent import path is invalid")
            kwargs = agent.get("kwargs")
            if not isinstance(kwargs, dict) or kwargs.get("controlled_pair_mode") is not True:
                errors.append(f"{task_name}/{arm}: controlled_pair_mode must be true")
            expected_arm = "baseline" if arm == "dsh_fresh" else "lhos"
            if not isinstance(kwargs, dict) or kwargs.get("arm") != expected_arm:
                errors.append(f"{task_name}/{arm}: custom agent arm binding is invalid")
        if arm_parities and len(set(arm_parities)) != 1:
            errors.append(f"{task_name}: fresh/LHOS config parity differs")

    if errors:
        preview = "; ".join(errors[:12])
        if len(errors) > 12:
            preview += f"; ... ({len(errors) - 12} more)"
        raise RuntimeError(f"controlled pair manifest validation failed: {preview}")


def _controlled_pair_enabled(manifest: dict[str, Any]) -> bool:
    design = manifest.get("controlled_pair_experiment")
    return (
        manifest.get("schema_version") == MANIFEST_SCHEMA_V3
        and isinstance(design, dict)
        and design.get("enabled") is True
    )


def _ensure_harbor_pull_policy_never(harbor_project: Path) -> dict[str, Any]:
    """Patch only the known bundled prebuilt compose shape, atomically."""

    path = (
        harbor_project
        / "src"
        / "harbor"
        / "environments"
        / "docker"
        / "docker-compose-prebuilt.yaml"
    )
    if not path.is_file():
        raise RuntimeError(f"Harbor prebuilt compose file is missing: {path}")
    before = path.read_text(encoding="utf-8")
    data = yaml.safe_load(before)
    main = data.get("services", {}).get("main", {}) if isinstance(data, dict) else {}
    if main.get("image") != "${PREBUILT_IMAGE_NAME}":
        raise RuntimeError("refusing to patch an unrecognized Harbor prebuilt compose file")
    policy = main.get("pull_policy")
    if policy == "never":
        return {
            "path": str(path),
            "changed": False,
            "pull_policy": "never",
            "sha256_before": hashlib.sha256(before.encode()).hexdigest(),
            "sha256_after": hashlib.sha256(before.encode()).hexdigest(),
        }
    if policy is not None:
        raise RuntimeError(
            f"refusing to replace Harbor pull_policy={policy!r}; expected absent or 'never'"
        )

    marker = "    image: ${PREBUILT_IMAGE_NAME}\n"
    if before.count(marker) != 1 or "pull_policy:" in before:
        raise RuntimeError(
            "refusing to patch Harbor prebuilt compose: exact image marker not found"
        )
    after = before.replace(
        marker,
        marker + "    pull_policy: never\n",
        1,
    )
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(after, encoding="utf-8")
    os.replace(temporary, path)
    verified = yaml.safe_load(path.read_text(encoding="utf-8"))
    if verified["services"]["main"].get("pull_policy") != "never":
        raise RuntimeError("Harbor pull_policy patch did not persist")
    return {
        "path": str(path),
        "changed": True,
        "pull_policy": "never",
        "sha256_before": hashlib.sha256(before.encode()).hexdigest(),
        "sha256_after": hashlib.sha256(after.encode()).hexdigest(),
    }


def _image_inventory(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    images: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_name = str(task["name"])
        for image in task.get("required_docker_images", (task["docker_image"],)):
            normalized = str(image).strip()
            if not normalized:
                continue
            entry = images.setdefault(
                normalized,
                {
                    "image": normalized,
                    "task_names": [],
                    "image_id": None,
                    "available": False,
                },
            )
            if task_name not in entry["task_names"]:
                entry["task_names"].append(task_name)
    for entry in images.values():
        image_id = _docker_image_id(str(entry["image"]))
        entry["image_id"] = image_id
        entry["available"] = bool(image_id)
        entry["task_names"].sort()

    records = sorted(images.values(), key=lambda item: str(item["image"]))
    missing = [record for record in records if not record["available"]]
    available = [record for record in records if record["available"]]
    missing_tasks = sorted({task_name for record in missing for task_name in record["task_names"]})
    available_tasks = sorted(
        task["name"]
        for task in tasks
        if all(
            images[str(image)]["available"]
            for image in task.get("required_docker_images", (task["docker_image"],))
        )
    )
    return {
        "schema_version": "lhos-lhtb-local-image-inventory.v1",
        "image_count": len(records),
        "available_image_count": len(available),
        "missing_image_count": len(missing),
        "available_task_count": len(available_tasks),
        "missing_task_count": len(missing_tasks),
        "available_task_names": available_tasks,
        "missing_task_names": missing_tasks,
        "available_images": available,
        "missing_images": missing,
        "images": records,
    }


def _resource_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return int(number) if number.is_integer() else number


def _task_pair_resource_requirement(task: dict[str, Any]) -> dict[str, Any]:
    """Return the conservative peak Docker resources for one task pair."""

    unknown_fields: list[str] = []
    main_cpus = _resource_number(task.get("cpus"))
    main_memory_mb = _resource_number(task.get("memory_mb"))
    if main_cpus is None:
        unknown_fields.append("environment.cpus")
    if main_memory_mb is None:
        unknown_fields.append("environment.memory_mb")

    has_verifier_mode = "verifier_environment_mode" in task
    raw_mode = task.get("verifier_environment_mode")
    mode = str(raw_mode or "unknown").strip().lower()
    verifier_cpus: int | float | None = None
    verifier_memory_mb: int | float | None = None
    verifier_included = mode == "separate"
    if verifier_included:
        verifier_cpus = _resource_number(task.get("verifier_cpus"))
        verifier_memory_mb = _resource_number(task.get("verifier_memory_mb"))
        if verifier_cpus is None:
            unknown_fields.append("verifier.environment.cpus")
        if verifier_memory_mb is None:
            unknown_fields.append("verifier.environment.memory_mb")
    elif mode != "same" or not has_verifier_mode:
        unknown_fields.append("verifier.environment_mode")

    known = not unknown_fields
    cpus: int | float | None = None
    memory_mb: int | float | None = None
    if known and main_cpus is not None and main_memory_mb is not None:
        cpus = main_cpus
        memory_mb = main_memory_mb
        if verifier_included:
            assert verifier_cpus is not None
            assert verifier_memory_mb is not None
            cpus += verifier_cpus
            memory_mb += verifier_memory_mb

    if not known:
        reason = "unknown_resources_fail_closed"
    elif verifier_included:
        reason = "main_plus_separate_verifier_peak"
    else:
        reason = "main_environment_peak"
    return {
        "known": known,
        "cpus": cpus,
        "memory_mb": memory_mb,
        "reason": reason,
        "unknown_fields": unknown_fields,
        "main_environment": {
            "cpus": main_cpus,
            "memory_mb": main_memory_mb,
        },
        "verifier": {
            "environment_mode": mode,
            "included_in_peak": verifier_included,
            "cpus": verifier_cpus,
            "memory_mb": verifier_memory_mb,
        },
    }


def _hydrate_manifest_task_resources(manifest: dict[str, Any]) -> None:
    """Backfill resource metadata in manifests prepared by older launcher versions."""

    tasks_root_value = manifest.get("tasks_root")
    tasks = manifest.get("tasks")
    if not tasks_root_value or not isinstance(tasks, list):
        return
    tasks_root = Path(str(tasks_root_value))
    for task in tasks:
        if not isinstance(task, dict):
            continue
        required = {
            "cpus",
            "memory_mb",
            "verifier_environment_mode",
            "verifier_cpus",
            "verifier_memory_mb",
        }
        if required.issubset(task):
            continue
        name = str(task.get("name", ""))
        config_path = tasks_root / name / "task.toml"
        expected_hash = str(task.get("task_toml_sha256", "") or "")
        if not config_path.is_file():
            task["resource_metadata_error"] = "task_toml_missing"
            continue
        if not expected_hash:
            task["resource_metadata_error"] = "task_toml_hash_missing"
            continue
        if expected_hash and _sha256_file(config_path) != expected_hash:
            task["resource_metadata_error"] = "task_toml_hash_mismatch"
            continue
        try:
            config = tomllib.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, tomllib.TOMLDecodeError):
            task["resource_metadata_error"] = "task_toml_invalid"
            continue
        environment = config.get("environment", {})
        verifier = config.get("verifier", {})
        verifier_environment = verifier.get("environment", {})
        task.setdefault("cpus", environment.get("cpus"))
        task.setdefault("memory_mb", environment.get("memory_mb"))
        task.setdefault(
            "verifier_environment_mode",
            verifier.get("environment_mode", "same"),
        )
        task.setdefault("verifier_cpus", verifier_environment.get("cpus"))
        task.setdefault("verifier_memory_mb", verifier_environment.get("memory_mb"))
        task["resource_metadata_source"] = "run_task_toml_backfill"


def _docker_pair_capacity(
    *,
    cpus_override: int | float | None = None,
    memory_mb_override: int | float | None = None,
) -> dict[str, Any]:
    """Resolve pair capacity from Docker, using explicit overrides when supplied."""

    cpus = _resource_number(cpus_override)
    memory_mb = _resource_number(memory_mb_override)
    if cpus_override is not None and cpus is None:
        raise RuntimeError("pair CPU capacity override must be positive")
    if memory_mb_override is not None and memory_mb is None:
        raise RuntimeError("pair memory capacity override must be positive")

    docker_info: dict[str, Any] = {}
    if cpus is None or memory_mb is None:
        completed = _run(
            ["docker", "info", "--format", "{{json .}}"],
            timeout=30,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "cannot derive pair capacity from Docker: "
                + (completed.stderr.strip() or completed.stdout.strip())
            )
        try:
            loaded = json.loads(completed.stdout)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("Docker returned invalid capacity metadata") from exc
        if not isinstance(loaded, dict):
            raise RuntimeError("Docker returned invalid capacity metadata")
        docker_info = loaded

    if cpus is None:
        cpus = _resource_number(docker_info.get("NCPU"))
        if cpus is None:
            raise RuntimeError("Docker capacity is missing a positive NCPU")
    if memory_mb is None:
        memory_bytes = _resource_number(docker_info.get("MemTotal"))
        if memory_bytes is None:
            raise RuntimeError("Docker capacity is missing a positive MemTotal")
        memory_mb = int(float(memory_bytes) // (1024 * 1024))
        if memory_mb < 1:
            raise RuntimeError("Docker memory capacity is below 1 MiB")

    return {
        "cpus": cpus,
        "memory_mb": memory_mb,
        "cpus_source": "override" if cpus_override is not None else "docker_info.NCPU",
        "memory_mb_source": (
            "override" if memory_mb_override is not None else "docker_info.MemTotal"
        ),
        "docker_engine_id": docker_info.get("ID"),
        "docker_engine_name": docker_info.get("Name"),
    }


def _task_admission_sort_key(task: dict[str, Any]) -> tuple[int, str]:
    try:
        priority = int(task.get("priority", sys.maxsize))
    except (TypeError, ValueError):
        priority = sys.maxsize
    return priority, str(task.get("name", ""))


def _continuation_boundary_mode(task: dict[str, Any]) -> str:
    if not bool(task.get("continue_until_timeout")):
        return "one_shot"
    if task.get("configured_time_slice_seconds") is not None:
        return "forced_time_slice"
    return "natural"


def _resource_aware_pair_plan(
    tasks: list[dict[str, Any]],
    *,
    capacity: dict[str, Any],
    max_concurrency: int,
) -> dict[str, Any]:
    """Build deterministic first-fit waves without over-admitting Docker resources."""

    if max_concurrency < 1:
        raise RuntimeError("max concurrency must be at least one")
    capacity_cpus = _resource_number(capacity.get("cpus"))
    capacity_memory_mb = _resource_number(capacity.get("memory_mb"))
    if capacity_cpus is None or capacity_memory_mb is None:
        raise RuntimeError("resource-aware pair capacity must declare positive CPU and memory")

    waves: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for task in sorted(tasks, key=_task_admission_sort_key):
        name = str(task["name"])
        requirement = _task_pair_resource_requirement(task)
        cpus = requirement["cpus"]
        memory_mb = requirement["memory_mb"]
        exceeds: list[str] = []
        if requirement["known"]:
            assert cpus is not None
            assert memory_mb is not None
            if float(cpus) > float(capacity_cpus):
                exceeds.append("cpu_capacity")
            if float(memory_mb) > float(capacity_memory_mb):
                exceeds.append("memory_capacity")

        if exceeds:
            decisions.append(
                {
                    "task_name": name,
                    "priority": task.get("priority"),
                    "wave": None,
                    "admitted": False,
                    "exclusive": False,
                    "reason": "requirement_exceeds_capacity",
                    "capacity_exceeded": exceeds,
                    "detail": (
                        f"{name}: pair resource requirement exceeds configured "
                        f"capacity ({', '.join(exceeds)})"
                    ),
                    "rejected_waves": [],
                    "resource_requirement": requirement,
                }
            )
            continue

        if not requirement["known"]:
            wave = {
                "wave": len(waves) + 1,
                "task_names": [name],
                "pair_count": 1,
                "cpus": cpus,
                "memory_mb": memory_mb,
                "exclusive": True,
                "within_capacity": False,
            }
            waves.append(wave)
            decisions.append(
                {
                    "task_name": name,
                    "priority": task.get("priority"),
                    "wave": wave["wave"],
                    "admitted": True,
                    "exclusive": True,
                    "reason": "exclusive_unknown_resources",
                    "capacity_exceeded": [],
                    "rejected_waves": [],
                    "resource_requirement": requirement,
                }
            )
            continue

        rejected_waves: list[dict[str, Any]] = []
        selected_wave: dict[str, Any] | None = None
        for wave in waves:
            rejection_reasons: list[str] = []
            if wave["exclusive"]:
                rejection_reasons.append("exclusive_wave")
            if int(wave["pair_count"]) >= max_concurrency:
                rejection_reasons.append("max_concurrency")
            if float(wave["cpus"] or 0) + float(cpus) > float(capacity_cpus):
                rejection_reasons.append("cpu_capacity")
            if (
                float(wave["memory_mb"] or 0) + float(memory_mb)
                > float(capacity_memory_mb)
            ):
                rejection_reasons.append("memory_capacity")
            if rejection_reasons:
                rejected_waves.append(
                    {
                        "wave": wave["wave"],
                        "reasons": rejection_reasons,
                    }
                )
                continue
            selected_wave = wave
            break

        if selected_wave is None:
            selected_wave = {
                "wave": len(waves) + 1,
                "task_names": [],
                "pair_count": 0,
                "cpus": 0,
                "memory_mb": 0,
                "exclusive": False,
                "within_capacity": True,
            }
            waves.append(selected_wave)
        selected_wave["task_names"].append(name)
        selected_wave["pair_count"] += 1
        selected_wave["cpus"] += cpus
        selected_wave["memory_mb"] += memory_mb
        decisions.append(
            {
                "task_name": name,
                "priority": task.get("priority"),
                "wave": selected_wave["wave"],
                "admitted": True,
                "exclusive": False,
                "reason": (
                    "shared_wave_first_fit"
                    if selected_wave["pair_count"] > 1
                    else "new_shared_wave"
                ),
                "capacity_exceeded": [],
                "rejected_waves": rejected_waves,
                "resource_requirement": requirement,
            }
        )

    return {
        "schema_version": PAIR_ADMISSION_SCHEMA_V1,
        "enabled": True,
        "policy": "deterministic_first_fit_waves",
        "fail_closed_unknown_resources": True,
        "fail_closed_capacity_exceeded": True,
        "separate_verifier_peak_policy": "sum_main_and_verifier",
        "capacity": dict(capacity),
        "max_concurrency": max_concurrency,
        "task_count": len(tasks),
        "wave_count": len(waves),
        "rejected_task_count": sum(
            not bool(decision["admitted"]) for decision in decisions
        ),
        "rejected_task_names": [
            decision["task_name"]
            for decision in decisions
            if not decision["admitted"]
        ],
        "waves": waves,
        "decisions": decisions,
    }


def _legacy_pair_plan(
    tasks: list[dict[str, Any]],
    *,
    max_concurrency: int,
) -> dict[str, Any]:
    return {
        "schema_version": PAIR_ADMISSION_SCHEMA_V1,
        "enabled": False,
        "policy": "legacy_thread_pool",
        "fail_closed_unknown_resources": False,
        "capacity": None,
        "max_concurrency": max_concurrency,
        "task_count": len(tasks),
        "wave_count": 0,
        "waves": [],
        "decisions": [
            {
                "task_name": str(task["name"]),
                "priority": task.get("priority"),
                "wave": None,
                "admitted": True,
                "exclusive": False,
                "reason": "resource_aware_pairs_disabled",
            }
            for task in sorted(tasks, key=_task_admission_sort_key)
        ],
    }


def _configured_worker_timeout_seconds(
    *,
    configured_agent_timeout_seconds: int | float | None,
    verifier_timeout_seconds: int | float | None,
    base_worker_timeout_seconds: int | float,
) -> float:
    return max(
        float(base_worker_timeout_seconds),
        float(configured_agent_timeout_seconds or 0)
        + float(verifier_timeout_seconds or 0)
        + 300.0,
    )


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output.resolve()
    if output.exists():
        raise SystemExit(f"output already exists: {output}")
    official_leaderboard_contract = bool(
        getattr(args, "official_leaderboard_contract", False)
    )
    official_model_yaml = getattr(args, "official_model_yaml", None)
    if official_model_yaml is not None and not official_leaderboard_contract:
        raise SystemExit(
            "--official-model-yaml requires --official-leaderboard-contract"
        )
    if official_leaderboard_contract and getattr(args, "task_names", None):
        raise SystemExit(
            "--official-leaderboard-contract requires the complete discovered suite; "
            "do not combine it with --task-names"
        )
    if official_leaderboard_contract and bool(
        getattr(args, "local_images_only", False)
    ):
        raise SystemExit(
            "--official-leaderboard-contract cannot exclude tasks with --local-images-only"
        )
    if official_leaderboard_contract and bool(
        getattr(args, "use_official_agent_timeouts", False)
    ):
        raise SystemExit(
            "--official-leaderboard-contract requires the uniform 5400s budget; "
            "task-declared timeouts are incompatible"
        )
    if official_leaderboard_contract and bool(
        getattr(args, "time_to_verified", False)
    ):
        raise SystemExit(
            "--official-leaderboard-contract cannot be combined with --time-to-verified, "
            "which selects task-declared timeouts"
        )
    if official_leaderboard_contract and getattr(args, "time_slice_seconds", None) is not None:
        raise SystemExit(
            "--official-leaderboard-contract forbids an artificial --time-slice-seconds"
        )
    resource_aware_pairs = bool(getattr(args, "resource_aware_pairs", False))
    max_concurrency = int(args.max_concurrency)
    if max_concurrency < 1:
        raise SystemExit("--max-concurrency must be at least 1")
    if not resource_aware_pairs and max_concurrency > DEFAULT_MAX_CONCURRENCY:
        raise SystemExit(
            "--max-concurrency above 2 requires --resource-aware-pairs"
        )
    pair_capacity_cpus = getattr(args, "pair_capacity_cpus", None)
    pair_capacity_memory_mb = getattr(args, "pair_capacity_memory_mb", None)
    if not resource_aware_pairs and (
        pair_capacity_cpus is not None or pair_capacity_memory_mb is not None
    ):
        raise SystemExit(
            "pair capacity overrides require --resource-aware-pairs"
        )
    lhtb_root = args.lhtb_root.resolve()
    harbor_project = lhtb_root / "harbor"
    lhtb_source_commit = _git_text(lhtb_root, "rev-parse", "HEAD")
    if official_leaderboard_contract and lhtb_source_commit != OFFICIAL_LHTB_SOURCE_COMMIT:
        raise SystemExit(
            "--official-leaderboard-contract requires the pinned official LHTB "
            f"commit {OFFICIAL_LHTB_SOURCE_COMMIT}; found {lhtb_source_commit}"
        )
    platform = os.environ.get("DOCKER_DEFAULT_PLATFORM")
    if official_leaderboard_contract and platform not in (None, OFFICIAL_LEADERBOARD_PLATFORM):
        raise SystemExit(
            "--official-leaderboard-contract requires "
            f"DOCKER_DEFAULT_PLATFORM={OFFICIAL_LEADERBOARD_PLATFORM}; found {platform}"
        )
    output.mkdir(parents=True)
    tasks_root = args.tasks_root.resolve()
    runtime_root = args.runtime_root.resolve()
    patch_path = args.patch.resolve()
    selected_tasks, selection = _select_tasks(
        tasks_root,
        all_tasks=(
            bool(getattr(args, "all", False)) or official_leaderboard_contract
        ),
        task_names=getattr(args, "task_names", None),
    )
    local_manifest_path = tasks_root.parent / "manifest.json"
    local_manifest = _load_json(local_manifest_path) if local_manifest_path.is_file() else {}
    if local_manifest:
        if local_manifest.get("schema_version") != "lhos-lhtb-local-pilot.v1":
            raise RuntimeError(f"invalid local-pilot manifest: {local_manifest_path}")
        if Path(str(local_manifest.get("destination", ""))).resolve() != tasks_root:
            raise RuntimeError("local-pilot manifest destination does not match tasks root")

    agent_module = REPO_ROOT / "scripts" / "lhtb_dsh_harbor_agent.py"
    runtime = _validate_runtime(
        runtime_root=runtime_root,
        patch_path=patch_path,
        agent_module=agent_module,
    )
    _validate_agent_import(harbor_project)
    pull_policy = _ensure_harbor_pull_policy_never(harbor_project)
    source_worktree = (
        _git_worktree_provenance(lhtb_root)
        if official_leaderboard_contract
        else None
    )
    requested_tasks = [
        _task_metadata(lhtb_root, tasks_root, selected, local_manifest)
        for selected in selected_tasks
    ]
    inventory = _image_inventory(requested_tasks)
    image_by_name = {str(item["image"]): item for item in inventory["images"]}
    for task in requested_tasks:
        required = task["required_docker_images"]
        task["local_image_ids"] = {
            str(image): image_by_name[str(image)]["image_id"] for image in required
        }
        task["all_required_images_local"] = all(
            image_by_name[str(image)]["available"] for image in required
        )

    configured_names = (
        set(inventory["available_task_names"])
        if bool(getattr(args, "local_images_only", False))
        else {task.name for task in selected_tasks}
    )
    configured_selected = tuple(task for task in selected_tasks if task.name in configured_names)
    tasks = [task for task in requested_tasks if str(task["name"]) in configured_names]
    if not configured_selected:
        _write_json(output / "image-inventory.json", inventory)
        _write_json(
            output / "missing-local-images.json",
            {
                "schema_version": inventory["schema_version"],
                "missing_image_count": inventory["missing_image_count"],
                "missing_task_count": inventory["missing_task_count"],
                "missing_task_names": inventory["missing_task_names"],
                "missing_images": inventory["missing_images"],
            },
        )
        raise RuntimeError("no selected tasks have all required Docker images locally")

    selection.update(
        {
            "local_images_only": bool(getattr(args, "local_images_only", False)),
            "configured_task_count": len(configured_selected),
            "configured_task_names": [task.name for task in configured_selected],
            "excluded_missing_image_task_names": sorted(
                set(selection["requested_task_names"]) - {task.name for task in configured_selected}
            ),
        }
    )
    _write_json(output / "image-inventory.json", inventory)
    _write_json(
        output / "missing-local-images.json",
        {
            "schema_version": inventory["schema_version"],
            "missing_image_count": inventory["missing_image_count"],
            "missing_task_count": inventory["missing_task_count"],
            "missing_task_names": inventory["missing_task_names"],
            "missing_images": inventory["missing_images"],
        },
    )
    historical_default = selection["mode"] == "default_software5" and not bool(
        getattr(args, "local_images_only", False)
    )
    time_to_verified_mode = bool(getattr(args, "time_to_verified", False))
    use_official_timeouts = bool(
        getattr(args, "use_official_agent_timeouts", False)
    ) or time_to_verified_mode
    configured_agent_timeout = (
        OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS
        if official_leaderboard_contract
        else None
        if use_official_timeouts
        else int(getattr(args, "agent_timeout_seconds", DEFAULT_AGENT_TIMEOUT_SECONDS))
    )
    task_timeout_overrides: dict[str, int | float] = {}
    if use_official_timeouts:
        for task in tasks:
            official_timeout = task.get("official_agent_timeout_seconds")
            if official_timeout is None:
                raise RuntimeError(
                    f"{task['name']}: official agent timeout is required "
                    "when --use-official-agent-timeouts is enabled"
                )
            task_timeout_overrides[str(task["name"])] = float(official_timeout)
        if not task_timeout_overrides:
            raise RuntimeError("no task-specific official agent timeouts are available")
    raw_time_slice = getattr(args, "time_slice_seconds", None)
    no_time_slice = bool(getattr(args, "no_time_slice", False))
    if raw_time_slice is not None and float(raw_time_slice) <= 0:
        raise RuntimeError("--time-slice-seconds must be positive")
    effective_time_slice = (
        None
        if official_leaderboard_contract
        or no_time_slice
        or (time_to_verified_mode and raw_time_slice is None)
        else DEFAULT_TIME_SLICE_SECONDS
        if raw_time_slice is None
        else raw_time_slice
    )
    task_time_slice_overrides: dict[str, int | float | None] = {}
    sliced_task_names: list[str] = []
    full_budget_one_shot_task_names: list[str] = []
    for task in tasks:
        name = str(task["name"])
        continue_until_timeout = bool(task.get("continue_until_timeout"))
        configured_slice = (
            effective_time_slice
            if effective_time_slice is not None and continue_until_timeout
            else None
        )
        task_time_slice_overrides[name] = configured_slice
        task["configured_time_slice_seconds"] = configured_slice
        if configured_slice is not None:
            task["time_slice_policy"] = "sliced_continuation"
            sliced_task_names.append(name)
        elif effective_time_slice is not None:
            task["time_slice_policy"] = "full_budget_one_shot"
            full_budget_one_shot_task_names.append(name)
        else:
            task["time_slice_policy"] = "unsliced"
        task["continuation_boundary_mode"] = _continuation_boundary_mode(task)
    time_slice_mode = (
        "disabled"
        if effective_time_slice is None
        else "mixed_by_continue_until_timeout"
        if full_budget_one_shot_task_names
        else "uniform_sliced"
    )
    configs = _write_configs(
        output,
        selected_tasks=configured_selected,
        tasks_root=tasks_root,
        runtime_root=runtime_root,
        patch_path=patch_path,
        agent_timeout_seconds=(
            configured_agent_timeout
            if configured_agent_timeout is not None
            else max(task_timeout_overrides.values())
        ),
        time_slice_seconds=effective_time_slice,
        runtime=runtime,
        job_prefix="lhtb-dsh-sw5" if historical_default else "lhtb-dsh",
        task_timeout_overrides=task_timeout_overrides or None,
        task_time_slice_overrides=task_time_slice_overrides,
        paired_verifier_seeds=getattr(args, "paired_verifier_seeds", None),
        environment_delete=official_leaderboard_contract,
        official_protocol_fields=official_leaderboard_contract,
    )
    official_contract = (
        _validate_official_leaderboard_contract(
            task_names=[task.name for task in configured_selected],
            configs=configs,
        )
        if official_leaderboard_contract
        else None
    )
    if official_contract is not None:
        assert source_worktree is not None
        model_yaml_reference = None
        if official_model_yaml is not None:
            model_yaml_path = Path(official_model_yaml)
            if not model_yaml_path.is_absolute():
                model_yaml_path = lhtb_root / model_yaml_path
            model_yaml_reference = _validate_official_model_yaml_reference(
                lhtb_root=lhtb_root,
                model_yaml=model_yaml_path,
                expected_task_names=[task.name for task in configured_selected],
            )
        official_tasks_root = tasks_root.resolve() == (lhtb_root / "tasks").resolve()
        network_override_names = sorted(
            str(task["name"])
            for task in tasks
            if bool(task.get("local_pilot_network_override"))
        )
        official_payload_exact = bool(
            official_tasks_root
            and not network_override_names
            and all(bool(task.get("official_payload_matches")) for task in tasks)
        )
        official_contract.update(
            {
                "official_tasks_root": official_tasks_root,
                "official_payload_exact": official_payload_exact,
                "network_override_task_names": network_override_names,
                "network_override_task_count": len(network_override_names),
                "docker_platform_required": OFFICIAL_LEADERBOARD_PLATFORM,
                "docker_platform_observed": platform,
                "reference_source_commit": OFFICIAL_LHTB_SOURCE_COMMIT,
                "source_commit_observed": lhtb_source_commit,
                "source_commit_match": lhtb_source_commit == OFFICIAL_LHTB_SOURCE_COMMIT,
                "official_model_yaml_reference_provided": (
                    model_yaml_reference is not None
                ),
                "official_model_yaml_reference_validated": (
                    model_yaml_reference is not None
                ),
                "official_model_yaml_reference": model_yaml_reference,
                "source_worktree_dirty": bool(source_worktree["dirty"]),
                "source_worktree_dirty_paths": list(source_worktree["paths"]),
                "source_worktree": source_worktree,
            }
        )
        if source_worktree["dirty"]:
            official_contract["official_score_reason"] += (
                " The pinned source checkout was dirty at preparation time; see "
                "source_worktree and protocol_deviations."
            )
            official_contract["protocol_deviations"] = [
                *official_contract["protocol_deviations"],
                (
                    "the pinned LHTB source worktree was dirty at preparation time; "
                    "all paths are recorded in source_worktree_dirty_paths"
                ),
            ]
        if source_worktree["harbor_prebuilt_pull_policy_patch_present"]:
            official_contract["protocol_deviations"] = [
                *official_contract["protocol_deviations"],
                (
                    "the runner changed harbor/src/harbor/environments/docker/"
                    "docker-compose-prebuilt.yaml to pull_policy=never; this is "
                    "recorded as a dirty source mutation"
                ),
            ]
    base_worker_timeout = float(
        getattr(args, "worker_timeout_seconds", DEFAULT_WORKER_TIMEOUT_SECONDS)
    )
    for task in tasks:
        task_config = configs[str(task["name"])]
        if task_config.get("paired_verifier_seeded"):
            task["stochastic_pair_controlled"] = True
            task["stochastic_control_mode"] = "paired_verifier_seed"
            task["paired_verifier_seed_env"] = task_config[
                "paired_verifier_seed_env"
            ]
            task["paired_verifier_seed_sha256"] = task_config[
                "paired_verifier_seed_sha256"
            ]
        configured_task_agent_timeout = (
            task_timeout_overrides.get(str(task["name"]), configured_agent_timeout)
        )
        task["configured_agent_timeout_seconds"] = configured_task_agent_timeout
        task["time_to_verified_supported"] = bool(task.get("continue_until_timeout"))
        task["worker_timeout_seconds"] = _configured_worker_timeout_seconds(
            configured_agent_timeout_seconds=configured_task_agent_timeout,
            verifier_timeout_seconds=task.get(
                "official_verifier_timeout_seconds"
            ),
            base_worker_timeout_seconds=base_worker_timeout,
        )
    pair_admission = (
        _resource_aware_pair_plan(
            tasks,
            capacity=_docker_pair_capacity(
                cpus_override=pair_capacity_cpus,
                memory_mb_override=pair_capacity_memory_mb,
            ),
            max_concurrency=max_concurrency,
        )
        if resource_aware_pairs
        else _legacy_pair_plan(tasks, max_concurrency=max_concurrency)
    )
    timeout_tier_counts: dict[str, int] = {}
    for task in tasks:
        configured_timeout = task.get("configured_agent_timeout_seconds")
        try:
            timeout_value = float(configured_timeout)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            continue
        timeout_label = str(int(timeout_value)) if timeout_value.is_integer() else str(timeout_value)
        timeout_tier_counts[timeout_label] = timeout_tier_counts.get(timeout_label, 0) + 1
    budget_condition = {
        "schema_version": "lhos-lhtb-budget-condition.v1",
        "source": (
            "official_task_toml_agent_timeout"
            if use_official_timeouts
            else "fixed_configured_agent_timeout"
        ),
        "configured_agent_timeout_seconds": configured_agent_timeout,
        "task_timeout_tier_counts": dict(
            sorted(timeout_tier_counts.items(), key=lambda item: float(item[0]))
        ),
        "same_budget_for_both_arms": True,
        "leaderboard_reference_budget_seconds": OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS,
        "official_leaderboard_score_claim": False,
    }
    budget_sweep_metadata = getattr(args, "budget_sweep_metadata", None)
    manifest = {
        # Every newly prepared run is a controlled-pair manifest. Historical
        # V1/V2 manifests remain loadable, but their missing controlled block
        # is treated as legacy rather than silently upgraded.
        "schema_version": MANIFEST_SCHEMA_V3,
        "benchmark": (
            "LHTB software-engineering five-case DSH pair"
            if historical_default
            else f"LHTB {len(tasks)}-task DSH pair"
        ),
        "official_score": False,
        "claim_status": "custom_harness_controlled_experiment",
        "ranking_eligible": False,
        "estimand": (
            "declared-arm outcome contrast for the full LHOS policy bundle "
            "under a fixed custom DSH Harbor harness"
        ),
        "budget_condition": budget_condition,
        "official_protocol": official_leaderboard_contract,
        "controlled_pair_experiment": {
            "schema_version": CONTROLLED_PAIR_SCHEMA_V1,
            "enabled": True,
            "harness_constant": True,
            "validity": "partial",
            "validity_scope": "fixed task/image/model/config/Harbor feedback path",
            "harbor_continue_mode": "same_conversation",
            "verifier_feedback_mode": "binary",
            "verifier_isolation_required": True,
            "verifier_isolation_verified": False,
            "control_arm": "dsh_fresh",
            "treatment_arm": "lhos_resume",
            "pairing_unit": "task",
            "planned_execution_layout": "same_batch_interleaved",
            "observed_execution_layout": "not_recorded",
            "execution_layout": "same_batch_interleaved",
            "assignment_rule": "priority_parity_AB_BA",
            "randomization_status": "deterministic_balanced_not_randomized",
            "outer_orchestration": {
                "control": "direct_worker_subprocess",
                "treatment": "AgentOS_plus_worker",
                "constant": False,
            },
            "continuation_driver": {
                "control": "fresh_recomputation",
                "treatment": "durable_resume_plus_semantic_policy",
                "constant": False,
            },
            "runtime_env_delta": {
                "common": {
                    "HB_CONTINUE_MODE": "same_conversation",
                    "HB_VERIFIER_FEEDBACK_MODE": "binary",
                },
                "agent_arm_delta": {
                    "baseline": "fresh DSH session per rejection",
                    "treatment": "durable DSH resume plus LHOS semantic policy",
                },
            },
            "treatment_variable": (
                "durable DSH conversation reuse plus LHOS semantic context policy "
                "and the LHOS outer controller"
            ),
            "control_continuation": (
                "fresh DSH home with original-instruction reconstruction"
            ),
            "treatment_continuation": (
                "exact persisted-session resume with bounded compaction policy"
            ),
            "estimand": (
                "declared-arm outcome contrast for the full LHOS policy bundle "
                "under a fixed custom DSH Harbor harness"
            ),
            "pure_context_reuse_claim": False,
            "one_shot_tasks_are_context_na": True,
            "verifier_artifact_isolation_guaranteed": False,
            "exclusion_policy": {
                "one_shot": "context_mechanism_na; retain only for outcome compatibility",
                "provider_censored": "exclude from paired outcome contrast",
                "infrastructure_resampled": "exclude from primary paired contrast",
                "uncontrolled_stochastic_verifier": "report separately or exclude",
            },
            "protocol_deviations": [
                "control and treatment use different continuation drivers",
                "control uses a direct worker launcher while treatment uses AgentOS",
                "pinned Docker Harbor verifier artifact isolation is unverified",
                "priority-parity arm order is balanced but not randomized",
            ],
            "verifier_artifact_isolation_note": (
                "Docker shared-verifier behavior at the pinned Harbor checkout "
                "is recorded as a limitation, not silently treated as isolated."
            ),
        },
        "task_content": (
            "official task payload"
            if not any(bool(task.get("local_pilot_network_override")) for task in tasks)
            else "official task payload with local-pilot network flag overrides"
        ),
        "environment": "local Docker pilot",
        "input_modality": "text-only",
        "local_pilot_manifest": (
            str(local_manifest_path) if local_manifest_path.is_file() else None
        ),
        "local_pilot_manifest_sha256": (
            _sha256_file(local_manifest_path) if local_manifest_path.is_file() else None
        ),
        "local_pilot_reason": local_manifest.get("reason"),
        "local_pilot_manifest_caveat": (
            "Network overrides are independently recomputed against LHTB Git "
            "HEAD because the generated manifest may inherit a dirty source "
            "task.toml."
        ),
        "tasks_root": str(tasks_root),
        "lhtb_source_commit": lhtb_source_commit,
        "harbor": _harbor_metadata(harbor_project),
        "harbor_prebuilt_pull_policy": pull_policy,
        "model": MODEL_LABEL,
        "reasoning_effort": REASONING_EFFORT,
        "agent": AGENT_IMPORT_PATH,
        "agent_timeout_seconds": configured_agent_timeout,
        "agent_timeout_mode": (
            "official_uniform_5400"
            if official_leaderboard_contract
            else "task_declared"
            if use_official_timeouts
            else "global"
        ),
        "time_to_verified_mode": time_to_verified_mode,
        "resolved_reward_threshold": DEFAULT_VERIFIED_REWARD_THRESHOLD,
        "verified_reward_threshold": DEFAULT_FULL_REWARD_THRESHOLD,
        "time_slice_seconds": effective_time_slice,
        "time_slice_mode": time_slice_mode,
        "time_slice_policy": {
            "mode": time_slice_mode,
            "requested_seconds": effective_time_slice,
            "sliced_task_count": len(sliced_task_names),
            "sliced_task_names": sliced_task_names,
            "full_budget_one_shot_task_count": len(
                full_budget_one_shot_task_names
            ),
            "full_budget_one_shot_task_names": full_budget_one_shot_task_names,
            "reason": (
                "Tasks with continue_until_timeout=true use the requested "
                "controller slice. One-shot tasks disable slicing so the "
                "configured arm budget is not truncated."
            ),
        },
        "semantic_context_control": dict(DEFAULT_SEMANTIC_CONTEXT_CONFIG),
        "worker_timeout_seconds": base_worker_timeout,
        "max_concurrency": max_concurrency,
        "resource_aware_pairs": resource_aware_pairs,
        "pair_admission": pair_admission,
        "n_attempts": 1,
        "timeout_multiplier": 1.0,
        "environment_delete": official_leaderboard_contract,
        "arm_order": "alternating AB/BA by task priority",
        "arms": {
            "dsh_fresh": (
                "Same Harbor same-conversation/binary path as LHOS, but a fresh "
                "DSH_HOME/session on every continuation; the original task "
                "instruction is reconstructed for the fresh session."
            ),
            "lhos_resume": (
                "Same Harbor same-conversation/binary path plus exact persisted "
                "DSH session resume; context reuse is claimed only if resume_gate "
                "passes."
            ),
        },
        "runtime": runtime,
        "selection": selection,
        "image_inventory": inventory,
        "tasks": tasks,
        "configs": configs,
        "attribution": (
            "This paired launcher tests fresh recomputation versus durable DSH "
            "same-session continuation inside the same LHTB task. It is a "
            "local-pilot result, not an official leaderboard score."
        ),
    }
    if isinstance(budget_sweep_metadata, dict):
        manifest["budget_sweep"] = dict(budget_sweep_metadata)
    if official_contract is not None:
        manifest["official_leaderboard_contract"] = official_contract
    _validate_controlled_pair_manifest(manifest)
    _validate_official_manifest_contract(manifest)
    _write_json(output / "manifest.json", manifest)
    return manifest


def _load_manifest(output: Path) -> dict[str, Any]:
    manifest = _load_json(output / "manifest.json")
    if manifest.get("schema_version") not in {
        MANIFEST_SCHEMA_V1,
        MANIFEST_SCHEMA_V2,
        MANIFEST_SCHEMA_V3,
    }:
        raise RuntimeError(f"missing or invalid manifest under {output}")
    return manifest


def _normalize_arm_mode(value: Any) -> str:
    mode = str(value or "both").strip().lower()
    if mode not in ARM_MODES:
        raise RuntimeError(
            f"invalid arm mode {value!r}; expected one of {', '.join(ARM_MODES)}"
        )
    return mode


def _manifest_run_arms(manifest: dict[str, Any]) -> tuple[str, ...]:
    mode = _normalize_arm_mode(manifest.get("run_arm", "both"))
    configured = manifest.get("run_arms")
    expected = ARM_MODE_TO_ARMS[mode]
    if configured is None:
        return expected
    if not isinstance(configured, (list, tuple)):
        raise RuntimeError("manifest run_arms must be a list")
    observed = tuple(str(item) for item in configured)
    if observed != expected:
        raise RuntimeError(
            f"manifest run_arms {observed!r} does not match run_arm={mode!r}"
        )
    return expected


def _set_manifest_run_arm(manifest: dict[str, Any], mode: str) -> tuple[str, ...]:
    normalized = _normalize_arm_mode(mode)
    active = ARM_MODE_TO_ARMS[normalized]
    manifest["run_arm"] = normalized
    manifest["run_arms"] = list(active)
    return active


def _docker_image_id(image: str) -> str | None:
    completed = _run(["docker", "image", "inspect", "--format", "{{.Id}}", image])
    return completed.stdout.strip() if completed.returncode == 0 else None


def prebuild(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output.resolve()
    manifest = _load_manifest(output)
    _validate_controlled_pair_manifest(manifest)
    _validate_official_manifest_contract(manifest)
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        _validate_official_leaderboard_contract(
            task_names=[str(task["name"]) for task in manifest.get("tasks", ())],
            configs=manifest.get("configs") or {},
        )
    tasks_root = Path(manifest["tasks_root"])
    logs = output / "logs" / "prebuild"
    logs.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for task in manifest["tasks"]:
        name = str(task["name"])
        image = str(task["docker_image"])
        inspect_error: dict[str, str] | None = None
        try:
            existing_id = _docker_image_id(image)
        except Exception as exc:
            # Docker may be unavailable (or its CLI may fail before a build
            # starts). Keep the task in the inventory and continue with the
            # remaining official images.
            existing_id = None
            inspect_error = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
        if bool(getattr(args, "missing_only", False)) and existing_id:
            records.append(
                {
                    "task_name": name,
                    "image": image,
                    "exit_code": 0,
                    "elapsed_ms": 0.0,
                    "image_id": existing_id,
                    "log": None,
                    "source": "existing_local_image",
                    "timed_out": False,
                    "timeout_seconds": 0.0,
                    "execution_error": None,
                }
            )
            continue
        started = time.monotonic()
        build_timeout = max(
            1800.0,
            float(task["official_build_timeout_seconds"] or 0),
        )
        build_command = [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "--tag",
            image,
            str(tasks_root / name / "environment"),
        ]
        timed_out = False
        execution_error = inspect_error
        try:
            completed = _run(
                build_command,
                timeout=build_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            # A single slow Dockerfile must not prevent the remaining official
            # LHTB images from being built and inventoried.
            timed_out = True
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            completed = subprocess.CompletedProcess(
                build_command,
                124,
                stdout=str(stdout),
                stderr=str(stderr) + f"\nTimed out after {build_timeout:.0f}s\n",
            )
            execution_error = {
                "type": "TimeoutExpired",
                "message": f"build timed out after {build_timeout:.0f}s",
            }
        except Exception as exc:
            # Treat Docker startup/CLI failures exactly like a failed build,
            # but do not let one task prevent the remaining tasks from being
            # attempted or from appearing in prebuild.json.
            execution_error = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            completed = subprocess.CompletedProcess(
                build_command,
                125,
                stdout="",
                stderr=f"{type(exc).__name__}: {exc}\n",
            )
        log = logs / f"{name}.log"
        log.write_text(
            (completed.stdout or "") + "\n" + (completed.stderr or ""),
            encoding="utf-8",
        )
        try:
            image_id = _docker_image_id(image)
        except Exception as exc:
            image_id = None
            execution_error = execution_error or {
                "type": type(exc).__name__,
                "message": str(exc),
            }
        record = {
            "task_name": name,
            "image": image,
            "exit_code": completed.returncode,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "image_id": image_id,
            "log": str(log),
            "source": "local_docker_build",
            "timed_out": timed_out,
            "timeout_seconds": build_timeout,
            "execution_error": execution_error,
        }
        records.append(record)
        # Continue building the remaining images after one task fails so the
        # inventory identifies every independently runnable case.
        if completed.returncode != 0 or not record["image_id"]:
            continue
    result = {
        "schema_version": (
            PREBUILD_SCHEMA_V1
            if manifest["schema_version"] == MANIFEST_SCHEMA_V1
            else PREBUILD_SCHEMA_V2
        ),
        "records": records,
        "complete": len(records) == len(manifest["tasks"])
        and all(item["exit_code"] == 0 and item["image_id"] for item in records),
        "official_score": False,
        "image_source": "locally built from local-pilot task Dockerfiles",
    }
    _write_json(output / "prebuild.json", result)
    if not result["complete"]:
        raise RuntimeError("prebuild did not complete; inspect prebuild.json")
    return result


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration_ms(started: str | None, finished: str | None) -> float | None:
    start = _parse_time(started)
    finish = _parse_time(finished)
    if start is None or finish is None:
        return None
    return round((finish - start).total_seconds() * 1000, 3)


def _safe_duration_ms(started: str | None, finished: str | None) -> float | None:
    """Return a duration without letting malformed timestamps break parsing."""

    try:
        return _duration_ms(started, finished)
    except (TypeError, ValueError, OverflowError):
        return None


def _reward(
    job_result: dict[str, Any],
    trial_result: dict[str, Any],
    trial_name: str | None = None,
) -> float | None:
    verifier = trial_result.get("verifier_result") or {}
    direct = verifier.get("reward")
    if direct is None and isinstance(verifier.get("rewards"), dict):
        direct = verifier["rewards"].get("reward")
    if direct is not None:
        try:
            parsed = float(direct)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) else None
    # Harbor can omit verifier_result in a partially persisted trial while
    # retaining the exact reward-to-trial mapping in job stats. Only use a
    # mapping that names this trial; never infer from an aggregate mean/max.
    if trial_name:
        evals = ((job_result.get("stats") or {}).get("evals") or {}).values()
        for evaluation in evals:
            if not isinstance(evaluation, dict):
                continue
            reward_stats = evaluation.get("reward_stats") or {}
            rewards = reward_stats.get("reward") if isinstance(reward_stats, dict) else None
            if not isinstance(rewards, dict):
                continue
            for reward_value, trial_names in rewards.items():
                if isinstance(trial_names, list) and trial_name in trial_names:
                    try:
                        parsed = float(reward_value)
                    except (TypeError, ValueError):
                        return None
                    return parsed if math.isfinite(parsed) else None
    # Do not infer reward from unrelated aggregate metrics. A missing verifier
    # reward is an invalid measurement, not a zero score.
    return None


def _process_reward_records(trial: Path | None) -> list[dict[str, Any]]:
    """Read Harbor process-reward checkpoints without trusting aggregate stats."""

    if trial is None:
        return []
    records: list[dict[str, Any]] = []
    jsonl = trial / "process_reward.jsonl"
    if jsonl.is_file():
        try:
            for line in jsonl.read_text(encoding="utf-8").splitlines():
                try:
                    value = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if isinstance(value, dict):
                    records.append(value)
        except OSError:
            pass
    checkpoint_root = trial / "process_reward_checkpoints"
    if checkpoint_root.is_dir():
        for path in sorted(checkpoint_root.glob("*/checkpoint.json")):
            value = _load_json(path)
            if value:
                records.append(value)
    # The same record is persisted in both JSONL and checkpoint.json. Keep the
    # first occurrence for each checkpoint id and preserve Harbor's chronology.
    deduplicated: dict[str, dict[str, Any]] = {}
    for record in records:
        checkpoint_id = str(record.get("checkpoint_id") or "")
        key = checkpoint_id or f"record-{len(deduplicated)}"
        deduplicated.setdefault(key, record)
    return list(deduplicated.values())


def _record_reward(record: dict[str, Any]) -> float | None:
    rewards = record.get("rewards")
    value: Any = rewards.get("reward") if isinstance(rewards, dict) else rewards
    if value is None:
        value = record.get("process_reward")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _first_verified_process_reward(
    trial: Path | None,
    *,
    threshold: float = DEFAULT_FULL_REWARD_THRESHOLD,
) -> dict[str, Any] | None:
    """Return the first full verifier checkpoint, if Harbor recorded one."""

    candidates = [
        record
        for record in _process_reward_records(trial)
        if str(record.get("checkpoint_kind", "timed")) in {"timed", "final"}
        and (_record_reward(record) is not None)
        and float(_record_reward(record) or 0.0) >= threshold
    ]
    def chronology(record: dict[str, Any]) -> float:
        timestamp = _parse_time(str(record.get("verifier_finished_at") or ""))
        if timestamp is None:
            return float("inf")
        try:
            return timestamp.timestamp()
        except (OverflowError, OSError, ValueError):
            return float("inf")

    candidates.sort(key=chronology)
    return candidates[0] if candidates else None


def _trial_dir(
    job_root: Path,
    job_result: dict[str, Any] | None = None,
) -> Path | None:
    """Find the trial belonging to this job, preferring Harbor's trial id."""

    candidates = [path.parent for path in job_root.glob("*/result.json")]
    if not candidates:
        return None
    result = job_result or _load_json(job_root / "result.json")
    referenced_names: set[str] = set()

    def collect(value: Any, key: str | None = None) -> None:
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                collect(child_value, str(child_key))
        elif isinstance(value, list):
            for child in value:
                collect(child, key)
        elif isinstance(value, str) and key in {
            "trial_name",
            "trial_id",
            "trial_name_id",
        }:
            referenced_names.add(value)

    collect(result)
    # Harbor stores the trial names in stats.evals[*].reward_stats as lists;
    # those values have no dedicated key, so include exact candidate matches.
    candidate_names = {candidate.name for candidate in candidates}

    def collect_matching(value: Any) -> None:
        if isinstance(value, str) and value in candidate_names:
            referenced_names.add(value)
        elif isinstance(value, dict):
            for child in value.values():
                collect_matching(child)
        elif isinstance(value, list):
            for child in value:
                collect_matching(child)

    collect_matching(result)
    referenced = [candidate for candidate in candidates if candidate.name in referenced_names]
    pool = referenced or candidates
    # A restarted job can leave multiple trial directories. Use the newest
    # result as a final fallback rather than lexicographically selecting stale
    # output from an earlier run.
    return max(
        pool,
        key=lambda candidate: (
            (candidate / "result.json").stat().st_mtime_ns,
            candidate.name,
        ),
    )


def _agent_results(trial_result: dict[str, Any]) -> list[dict[str, Any]]:
    step_results = trial_result.get("step_results")
    if isinstance(step_results, list):
        values = [
            step.get("agent_result")
            for step in step_results
            if isinstance(step, dict) and isinstance(step.get("agent_result"), dict)
        ]
        if values:
            return values
    top_level = trial_result.get("agent_result")
    return [top_level] if isinstance(top_level, dict) else []


def _sum_agent_field(agent_results: list[dict[str, Any]], field: str) -> int:
    return sum(int(item.get(field, 0) or 0) for item in agent_results)


def _natural_boundary_resume_evidence(
    observability: dict[str, Any],
    *,
    trial_exception_type: str | None = None,
    budget_exhausted: bool = False,
) -> dict[str, Any]:
    """Check a resume that was cancelled only by the outer agent budget.

    A natural (unsliced) continuation has no explicit max-token or slice
    checkpoint. Harbor can therefore cancel the in-flight second invocation
    while still persisting a valid continuation trajectory. We accept that
    narrow case only when the trial itself reports an AgentTimeoutError (or an
    equivalent budget flag), the resume invocation is explicitly marked as a
    CancelledError, and the semantic resume decision's pre-state event count is
    strictly below the final durable event count in the same session.
    """

    raw_invocations = observability.get("invocations", ())
    raw_decisions = observability.get("semantic_decisions", ())
    invocations = (
        raw_invocations
        if isinstance(raw_invocations, (list, tuple))
        else ()
    )
    decisions = (
        raw_decisions
        if isinstance(raw_decisions, (list, tuple))
        else ()
    )
    session_id = str(observability.get("session_id", "") or "")
    final_event_count = int(observability.get("event_count", 0) or 0)
    usage = observability.get("usage") or {}
    resume_invocations = [
        item
        for item in invocations
        if isinstance(item, dict)
        and item.get("resume") is True
    ]
    cancelled_resume = [
        item
        for item in resume_invocations
        if item.get("status") == "cancelled"
        and str(item.get("failure_type", "")) == "CancelledError"
    ]
    semantic_resume_decisions = [
        item
        for item in decisions
        if isinstance(item, dict)
        and str(item.get("action", "")) == "resume"
    ]
    decision = semantic_resume_decisions[-1] if semantic_resume_decisions else {}
    decision_event_count: int | None = None
    try:
        if decision:
            decision_event_count = int(decision.get("event_count"))
    except (TypeError, ValueError):
        decision_event_count = None
    resume_invocation = cancelled_resume[-1] if cancelled_resume else {}
    unsliced = (
        observability.get("time_slice_seconds") is None
        and observability.get("effective_time_slice_seconds") is None
        and not any(
            isinstance(item, dict)
            and (
                item.get("slice_preempted") is True
                or item.get("max_tokens_checkpoint") is True
            )
            for item in resume_invocations
        )
    )
    checks = {
        "trial_budget_exhausted": bool(
            budget_exhausted
            or trial_exception_type == "AgentTimeoutError"
        ),
        "cancelled_resume_invocation": bool(cancelled_resume),
        "natural_boundary": unsliced,
        "semantic_resume_decision": bool(decision),
        "semantic_control_artifact": bool(
            observability.get("semantic_control_artifact")
        ),
        "decision_event_count_valid": decision_event_count is not None,
        "event_growth_after_decision": bool(
            decision_event_count is not None
            and decision_event_count < final_event_count
        ),
        "same_session_decision": bool(
            session_id
            and decision.get("session_id") == session_id
        ),
        "same_session_invocation": bool(
            session_id
            and resume_invocation.get("resume_session_id") == session_id
        ),
        "model_activity": int(usage.get("model_calls", 0) or 0) > 0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "decision_event_count": decision_event_count,
        "final_event_count": final_event_count,
        "resume_invocation_status": resume_invocation.get("status"),
        "resume_invocation_failure_type": resume_invocation.get("failure_type"),
    }


def _resume_gate(
    observability: dict[str, Any],
    *,
    trial_exception_type: str | None = None,
    budget_exhausted: bool = False,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    raw_invocations = observability.get("invocations", ())
    invocations = (
        raw_invocations
        if isinstance(raw_invocations, (list, tuple))
        else ()
    )
    usage = observability.get("usage") or {}
    adaptive_context_control = bool(
        observability.get("semantic_context_control")
        or observability.get("resume_evidence_mode") == "adaptive_context_control"
    )
    durable_resumes = [
        item
        for item in invocations
        if isinstance(item, dict)
        and item.get("resume") is True
        and (
            (
                item.get("status") in {"completed", "slice_preempted"}
                and (
                    item.get("exit_code") in {0, 197}
                    or (
                        item.get("status") == "slice_preempted"
                        and item.get("exit_code") == 137
                        and item.get("slice_preempted") is True
                    )
                )
            )
            or (
                item.get("status") == "max_tokens_checkpoint"
                and item.get("exit_code") == 1
                and item.get("max_tokens_checkpoint") is True
            )
        )
    ]
    session_file_count = int(observability.get("session_file_count", 0) or 0)
    generation_count = int(
        observability.get("session_generation_count", 0) or 0
    )
    controlled_restarts = int(
        observability.get("controlled_restart_count", 0) or 0
    )
    session_topology_valid = (
        session_file_count == generation_count
        and generation_count >= 1
        and controlled_restarts == generation_count - 1
        if adaptive_context_control
        else session_file_count == 1
    )
    natural_boundary = _natural_boundary_resume_evidence(
        observability,
        trial_exception_type=trial_exception_type,
        budget_exhausted=budget_exhausted,
    )
    natural_boundary["checks"]["session_topology_valid"] = session_topology_valid
    natural_boundary["checks"]["valid_session_id"] = bool(
        _SESSION_ID.fullmatch(str(observability.get("session_id", "")))
    )
    natural_boundary["passed"] = all(natural_boundary["checks"].values())
    controlled_checks = {
        "controlled_pair_mode": observability.get("controlled_pair_mode") is True,
        "harbor_continue_mode": observability.get("harbor_continue_mode")
        == "same_conversation",
        "binary_feedback": observability.get("verifier_feedback_mode") == "binary",
    }
    if not controlled_pair_required:
        # Preserve the permissive interpretation for historical V1/V2 pilots.
        controlled_checks = {
            "controlled_pair_mode": observability.get("controlled_pair_mode", True)
            is True,
            "harbor_continue_mode": observability.get(
                "harbor_continue_mode", "same_conversation"
            )
            == "same_conversation",
            "binary_feedback": observability.get(
                "verifier_feedback_mode", "binary"
            )
            == "binary",
        }
    checks = {
        "schema": (observability.get("schema_version") == "lhos-lhtb-dsh-harbor-agent.v1"),
        "arm": observability.get("arm") == "lhos",
        "controller": observability.get("controller") == "longhorizonos",
        **controlled_checks,
        "resume_api": observability.get("resume_api") == "ctx.agents.resume",
        "invocation_count": int(observability.get("invocation_count", 0) or 0) >= 2,
        "resume_count": int(observability.get("resume_count", 0) or 0) >= 1,
        "session_reused": observability.get("session_reused") is True,
        "session_topology_valid": session_topology_valid,
        "valid_session_id": bool(_SESSION_ID.fullmatch(str(observability.get("session_id", "")))),
        "durable_resume_invocation": bool(
            durable_resumes or natural_boundary["passed"]
        ),
        "durable_events": int(observability.get("event_count", 0) or 0) > 0,
        "model_activity": int(usage.get("model_calls", 0) or 0) > 0,
        "semantic_control_evidence": (
            bool(observability.get("semantic_control_artifact"))
            and int(observability.get("semantic_decision_count", 0) or 0) >= 1
            if adaptive_context_control
            else True
        ),
    }
    passed = all(checks.values())
    return {
        "schema_version": "lhos-lhtb-dsh-resume-gate.v1",
        "passed": passed,
        "checks": checks,
        "controlled_pair_required": controlled_pair_required,
        "natural_boundary_resume": natural_boundary,
        "classification": (
            "natural_boundary_resume"
            if passed and natural_boundary["passed"]
            else "adaptive_context_control"
            if passed and adaptive_context_control
            else "verified_context_reuse"
            if passed
            else "direct_compatibility"
        ),
    }


def _fresh_gate(
    observability: dict[str, Any],
    *,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    controlled_checks = {
        "controlled_pair_mode": observability.get("controlled_pair_mode") is True,
        "harbor_continue_mode": observability.get("harbor_continue_mode")
        == "same_conversation",
        "binary_feedback": observability.get("verifier_feedback_mode") == "binary",
    }
    if not controlled_pair_required:
        controlled_checks = {
            "controlled_pair_mode": observability.get("controlled_pair_mode", True)
            is True,
            "harbor_continue_mode": observability.get(
                "harbor_continue_mode", "same_conversation"
            )
            == "same_conversation",
            "binary_feedback": observability.get(
                "verifier_feedback_mode", "binary"
            )
            == "binary",
        }
    checks = {
        "schema": (observability.get("schema_version") == "lhos-lhtb-dsh-harbor-agent.v1"),
        "arm": observability.get("arm") == "baseline",
        "controller": observability.get("controller") == "none",
        **controlled_checks,
        "resume_count_zero": int(observability.get("resume_count", 0) or 0) == 0,
        "session_not_reused": observability.get("session_reused") is False,
        "invocations_present": int(observability.get("invocation_count", 0) or 0) >= 1,
    }
    return {
        "schema_version": "lhos-lhtb-dsh-fresh-gate.v1",
        "passed": all(checks.values()),
        "checks": checks,
        "controlled_pair_required": controlled_pair_required,
    }


def _provider_censorship_reason(
    *,
    trial: Path | None,
    exception_info: dict[str, Any],
    observability: dict[str, Any],
) -> str | None:
    candidates = [
        str(exception_info.get("exception_type", "") or ""),
        str(exception_info.get("exception_message", "") or ""),
        json.dumps(
            observability.get("turn_end_reasons", ()),
            ensure_ascii=True,
            sort_keys=True,
        ),
    ]
    if trial is not None:
        invocation_root = trial / "agent" / "invocations"
        if invocation_root.is_dir():
            for path in sorted(invocation_root.glob("invocation-*/stderr.log"))[-3:]:
                try:
                    candidates.append(path.read_text(encoding="utf-8", errors="replace")[-8192:])
                except OSError:
                    continue
    for candidate in candidates:
        match = _PROVIDER_CENSORSHIP.search(candidate)
        if match is not None:
            return re.sub(r"\s+", "_", match.group(0).strip().lower())[:80]
    return None


def _job_was_censored(job_path: Path) -> bool:
    """Detect provider censorship (HTTP 451) inside a job's invocation stderr logs.

    DSH aborts with exit 134 when the model API returns 451; the Harbor worker may
    then crash before _trial_metrics can label the record, so scan the raw stderr
    logs as a fallback signal for censorship retries.
    """
    if not job_path or not job_path.exists():
        return False
    try:
        stderrs = sorted(job_path.glob("*/agent/invocations/invocation-*/stderr.log"))
        for stderr in stderrs[-5:]:
            try:
                text = stderr.read_text(encoding="utf-8", errors="replace")[-8192:]
                if _PROVIDER_CENSORSHIP.search(text):
                    return True
            except OSError:
                continue
    except OSError:
        pass
    return False


def _job_never_connected(job_path: Path) -> bool:
    """True when an arm FAILED yet never produced a single agent event.

    Under concurrency DSH (Node) can SIGABRT (exit 134) during the very first
    model request: no HTTP response ever arrives, so dsh-observability.json
    reports event_count=0 and there is no stderr censorship marker. This is a
    connection/startup failure (probabilistic), not a task failure, so a clean
    resample is authorized. Any positive event_count means the agent did
    connect at least once -> return False (do not mask real failures).
    """
    if not job_path or not job_path.exists():
        return False
    try:
        obs_files = sorted(job_path.glob("*/agent/dsh-observability.json"))
        if not obs_files:
            return False
        saw_zero = False
        for obs_path in obs_files:
            try:
                obs = json.loads(obs_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            top_ec = int(obs.get("event_count", 0) or 0)
            if top_ec > 0:
                return False
            for inv in (obs.get("invocations") or ()):
                if int(inv.get("event_count", 0) or 0) > 0:
                    return False
            saw_zero = True
        return saw_zero
    except OSError:
        return False


def _job_had_real_work(job_path: Path, arm: str) -> bool:
    """True when the job's own artifacts prove the agent produced model work.

    A crashed arm's failure record carries no parsed metrics, so zero-valued
    ``record["metrics"]`` is not evidence of "never connected".  Consult the
    durable job artifacts instead: any agent event/model call in the
    observability files, or any token/model-call/scored reward in the trial
    metrics.  Parse failures count as "no proof" and fall back to the legacy
    zero-metrics behaviour (retry remains authorized).
    """

    if not job_path or not job_path.exists():
        return False
    try:
        obs_files = sorted(job_path.glob("*/agent/dsh-observability.json"))
    except OSError:
        obs_files = []
    for obs_path in obs_files:
        try:
            obs = json.loads(obs_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if int(obs.get("event_count", 0) or 0) > 0:
            return True
        usage = obs.get("usage") or {}
        if isinstance(usage, dict) and int(usage.get("model_calls", 0) or 0) > 0:
            return True
    try:
        metrics = _trial_metrics(job_path, arm or "lhos_resume")
    except Exception:
        metrics = {}
    if isinstance(metrics, dict) and metrics:
        if int(metrics.get("token_units", 0) or 0) > 0:
            return True
        if int(metrics.get("model_calls", 0) or 0) > 0:
            return True
        if metrics.get("reward") is not None:
            return True
    return False


def _arm_never_started(record: dict[str, Any], job_path: Path) -> bool:
    """True when an arm ended BEFORE the agent produced any model work.

    Covers two probabilistic, resample-recoverable pre-agent failures:
      1. top-level NonZeroAgentExit (e.g. exit 134) with zero agent events;
      2. environment/compose startup failure recorded inside metrics, where
         token_units == 0 AND model_calls == 0 plus an explicit execution
         failure signal (the model never ran). A run that actually called the
         model (model_calls>0 or token_units>0) is a genuine task outcome and
         must never be retried here.
    """
    if not isinstance(record, dict):
        return False
    if record.get("execution_error") is not None and _job_never_connected(job_path):
        return True
    m = record.get("metrics") or {}
    if int(m.get("token_units", 0) or 0) == 0 and int(m.get("model_calls", 0) or 0) == 0:
        if (
            m.get("execution_error") is not None
            or m.get("exception_type") is not None
            or m.get("verification_status") == "execution_failure"
        ):
            # Zero-valued metrics in a failure record are not proof of
            # "never connected": the exception path builds the record without
            # running _trial_metrics, so a ran-and-crashed arm (agent exit 1
            # after real model work plus a scored verifier) would be
            # misclassified -- and the retry's clean resample would wipe its
            # legitimate partial result (observed: foldseek lost a scored
            # 0.333 this way).  Only trust zeros when the job's own artifacts
            # show no real work either.
            return not _job_had_real_work(job_path, str(record.get("arm") or ""))
    return False


def _trial_metrics(
    job_root: Path,
    arm: str,
    *,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    job_result = _load_json(job_root / "result.json")
    trial = _trial_dir(job_root, job_result)
    trial_result = _load_json(trial / "result.json") if trial else {}
    observability_path = trial / "agent" / "dsh-observability.json" if trial else None
    observability = (
        _load_json(observability_path) if observability_path and observability_path.exists() else {}
    )
    reward = _reward(job_result, trial_result, trial.name if trial else None)
    agent_results = _agent_results(trial_result)
    agent_result = agent_results[-1] if agent_results else {}
    metadata = agent_result.get("metadata") or {}
    usage = observability.get("usage") or metadata.get("dsh_usage_cumulative") or {}
    input_tokens = _sum_agent_field(agent_results, "n_input_tokens")
    cache_tokens = _sum_agent_field(agent_results, "n_cache_tokens")
    output_tokens = _sum_agent_field(agent_results, "n_output_tokens")
    # Crash-terminated trials can lose the agent_result block entirely
    # (harbor records exception_info instead), zeroing the n_* sums even
    # though the model produced real traffic.  Fall back per-field to the
    # agent-written cumulative observability usage so token accounting
    # survives the crash.  Observed on apex-openroad: output_tokens=0 was
    # recorded despite ~235K real output tokens in the phase trace, making an
    # accounting hole look like a mechanism bug.  Zero-valued observability
    # fields never overwrite (a genuine zero stays zero).
    if not output_tokens and int(usage.get("output_tokens", 0) or 0) > 0:
        output_tokens = int(usage.get("output_tokens", 0) or 0)
    if not input_tokens:
        observability_input = (
            int(usage.get("uncached_input_tokens", 0) or 0)
            + int(usage.get("cache_read_tokens", 0) or 0)
            + int(usage.get("cache_write_tokens", 0) or 0)
        )
        if observability_input > 0:
            input_tokens = observability_input
    if not cache_tokens and int(usage.get("cache_read_tokens", 0) or 0) > 0:
        cache_tokens = int(usage.get("cache_read_tokens", 0) or 0)
    token_units = int(usage.get("total_token_units") or input_tokens + output_tokens)
    parser_error = None
    if not trial_result:
        parser_error = "missing_or_invalid_trial_result"
    elif reward is None:
        parser_error = "missing_verifier_reward"
    exception_info = trial_result.get("exception_info") or {}
    exception_type = exception_info.get("exception_type")
    budget_exhausted = bool(
        exception_type == "AgentTimeoutError"
        or trial_result.get("budget_exhausted") is True
        or metadata.get("budget_exhausted") is True
    )
    gate = (
        _resume_gate(
            observability,
            trial_exception_type=exception_type,
            budget_exhausted=budget_exhausted,
            controlled_pair_required=controlled_pair_required,
        )
        if arm == "lhos_resume"
        else _fresh_gate(
            observability,
            controlled_pair_required=controlled_pair_required,
        )
    )
    censorship_reason = _provider_censorship_reason(
        trial=trial,
        exception_info=exception_info,
        observability=observability,
    )
    provider_censored = censorship_reason is not None
    execution_error = None
    if provider_censored:
        execution_error = {
            "type": "provider_censored",
            "message": "provider response was censored; pair must be resampled together",
        }
    elif exception_type and not budget_exhausted:
        execution_error = {
            "type": exception_type,
            "message": exception_info.get("exception_message"),
        }
    parse_valid = parser_error is None
    resource_measurement_valid = bool(
        token_units > 0
        or int(usage.get("model_calls", 0) or 0) > 0
        or int(usage.get("tool_calls", 0) or 0) > 0
    )
    resolved = bool(reward is not None and reward >= DEFAULT_VERIFIED_REWARD_THRESHOLD)
    verified = bool(reward is not None and reward >= DEFAULT_FULL_REWARD_THRESHOLD)
    agent_execution = trial_result.get("agent_execution") or {}
    verifier_timing = trial_result.get("verifier") or {}
    first_verified_checkpoint = (
        _first_verified_process_reward(trial) if verified else None
    )
    if verified and first_verified_checkpoint is not None:
        active_agent_time = first_verified_checkpoint.get("active_agent_time_sec")
        try:
            time_to_verified_ms = round(float(active_agent_time) * 1000, 3)
        except (TypeError, ValueError):
            time_to_verified_ms = _safe_duration_ms(
                agent_execution.get("started_at"),
                first_verified_checkpoint.get("verifier_started_at")
                or first_verified_checkpoint.get("verifier_finished_at"),
            )
        first_verified_at = (
            first_verified_checkpoint.get("verifier_finished_at")
            or first_verified_checkpoint.get("verifier_started_at")
        )
        time_to_verified_source = "process_reward_checkpoint"
    elif verified:
        # A final verifier duration is not agent work. Use the end of the
        # agent phase (or verifier start as a fallback), so TTV is not inflated
        # by the final verifier itself.
        first_verified_at = verifier_timing.get("started_at") or verifier_timing.get(
            "finished_at"
        )
        time_to_verified_ms = _safe_duration_ms(
            agent_execution.get("started_at"),
            agent_execution.get("finished_at")
            or verifier_timing.get("started_at")
            or verifier_timing.get("finished_at"),
        )
        time_to_verified_source = "agent_phase_end"
    else:
        first_verified_at = None
        time_to_verified_ms = None
        time_to_verified_source = None
    checkpoint_has_usage = bool(
        isinstance(first_verified_checkpoint, dict)
        and any(
            key in first_verified_checkpoint
            or key in (first_verified_checkpoint.get("usage") or {})
            for key in ("token_units", "total_token_units", "model_calls", "tool_calls")
        )
    )
    first_verified_usage_source = (
        "process_reward_checkpoint_usage"
        if checkpoint_has_usage
        else "checkpoint_time_final_cumulative_usage"
        if first_verified_checkpoint is not None
        else "final_cumulative"
        if verified
        else None
    )
    checkpoint_usage = (
        first_verified_checkpoint.get("usage")
        if isinstance(first_verified_checkpoint, dict)
        and isinstance(first_verified_checkpoint.get("usage"), dict)
        else {}
    )

    def checkpoint_or_cumulative(
        *keys: str,
        fallback: int,
    ) -> int | None:
        if not verified:
            return None
        sources = (
            first_verified_checkpoint or {},
            checkpoint_usage,
        )
        for source in sources:
            for key in keys:
                value = source.get(key)
                if value is None:
                    continue
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
        return fallback
    if verified:
        verification_status = "verified"
        verification_observation = "final_verifier"
    elif resolved:
        verification_status = "partially_resolved"
        verification_observation = "final_verifier_partial"
    elif provider_censored:
        verification_status = "provider_censored"
        verification_observation = "invalid_provider_sample"
    elif budget_exhausted:
        verification_status = "timeout"
        verification_observation = "no_verified_checkpoint"
    elif execution_error is not None:
        verification_status = "execution_failure"
        verification_observation = "no_verified_checkpoint"
    else:
        verification_status = "not_verified"
        verification_observation = "no_verified_checkpoint"
    result_eligible = bool(
        parse_valid
        and execution_error is None
        and resource_measurement_valid
        and not provider_censored
    )
    mechanism_eligible = bool(
        result_eligible
        and bool(gate.get("passed"))
    )
    return {
        "parse_valid": parse_valid,
        "resource_measurement_valid": resource_measurement_valid,
        "result_eligible": result_eligible,
        "mechanism_eligible": mechanism_eligible,
        # Backward-compatible alias: historical profiling measured the
        # resume mechanism subgroup, not the complete benchmark suite.
        "comparison_eligible": mechanism_eligible,
        "parser_error": parser_error,
        "execution_error": execution_error,
        "provider_censored": provider_censored,
        "provider_censorship_reason": censorship_reason,
        "budget_exhausted": budget_exhausted,
        "reward": reward,
        "resolved": resolved,
        "verified": verified,
        "verification_status": verification_status,
        "verification_observation": verification_observation,
        "first_verified_at": first_verified_at,
        "first_verified_checkpoint_id": (
            first_verified_checkpoint.get("checkpoint_id")
            if first_verified_checkpoint is not None
            else None
        ),
        "first_verified_checkpoint_active_agent_time_sec": (
            first_verified_checkpoint.get("active_agent_time_sec")
            if first_verified_checkpoint is not None
            else None
        ),
        "time_to_verified_source": time_to_verified_source,
        "time_to_verified_ms": time_to_verified_ms,
        "first_verified_usage_source": first_verified_usage_source,
        "first_verified_token_units": checkpoint_or_cumulative(
            "token_units",
            "total_token_units",
            fallback=token_units,
        ),
        "first_verified_model_calls": checkpoint_or_cumulative(
            "model_calls",
            fallback=int(usage.get("model_calls", 0) or 0),
        ),
        "first_verified_tool_calls": checkpoint_or_cumulative(
            "tool_calls",
            fallback=int(usage.get("tool_calls", 0) or 0),
        ),
        "exception_type": exception_type,
        "exception_message": exception_info.get("exception_message"),
        "input_tokens": input_tokens,
        "cache_tokens": cache_tokens,
        "output_tokens": output_tokens,
        "token_units": token_units,
        "uncached_input_tokens": int(usage.get("uncached_input_tokens", 0) or 0),
        "cache_read_tokens": int(usage.get("cache_read_tokens", 0) or 0),
        "cache_write_tokens": int(usage.get("cache_write_tokens", 0) or 0),
        "reasoning_tokens": int(usage.get("reasoning_tokens", 0) or 0),
        "model_calls": int(usage.get("model_calls", 0) or 0),
        "tool_calls": int(usage.get("tool_calls", 0) or 0),
        "dsh_invocations": int(
            observability.get("invocation_count") or metadata.get("dsh_invocations_cumulative") or 0
        ),
        "dsh_resumes": int(
            observability.get("resume_count") or metadata.get("dsh_resumes_cumulative") or 0
        ),
        "dsh_session_id": observability.get("session_id") or metadata.get("dsh_session_id"),
        "dsh_session_reused": bool(
            observability.get("session_reused") or metadata.get("dsh_session_reused")
        ),
        "dsh_resume_evidence_mode": (
            observability.get("resume_evidence_mode")
            or metadata.get("dsh_resume_evidence_mode")
        ),
        "dsh_semantic_context_control": bool(
            observability.get("semantic_context_control")
            or metadata.get("dsh_semantic_context_control")
        ),
        "dsh_session_generation_count": int(
            observability.get("session_generation_count")
            or metadata.get("dsh_session_generation_count")
            or 0
        ),
        "dsh_controlled_restarts": int(
            observability.get("controlled_restart_count")
            or metadata.get("dsh_controlled_restarts_cumulative")
            or 0
        ),
        "dsh_semantic_decisions": int(
            observability.get("semantic_decision_count")
            or metadata.get("dsh_semantic_decisions_cumulative")
            or 0
        ),
        "dsh_max_tokens_checkpoints": int(
            observability.get("max_tokens_checkpoints")
            or metadata.get("dsh_max_tokens_checkpoints_cumulative")
            or 0
        ),
        "agent_elapsed_ms": _duration_ms(
            (trial_result.get("agent_execution") or {}).get("started_at"),
            (trial_result.get("agent_execution") or {}).get("finished_at"),
        ),
        "harbor_total_elapsed_ms": _duration_ms(
            trial_result.get("started_at"),
            trial_result.get("finished_at"),
        ),
        "continuation_gate": gate,
        "natural_boundary_resume_evidence": (
            gate.get("natural_boundary_resume", {})
            if arm == "lhos_resume"
            else {}
        ),
        "control_classification": (
            gate.get("classification", "fresh_recomputation")
            if arm == "lhos_resume"
            else "fresh_recomputation"
        ),
        "trial_dir": "" if trial is None else str(trial),
        "observability": ("" if observability_path is None else str(observability_path)),
        "job_result": str(job_root / "result.json"),
    }


def _credential_env(
    credential_env: str,
    *,
    arm: str,
) -> tuple[dict[str, str], tuple[str, ...]]:
    secret = os.environ.get(credential_env, "")
    if not secret:
        raise RuntimeError(f"missing credential environment variable: {credential_env}")
    env = dict(os.environ)
    python_path = os.pathsep.join((str(REPO_ROOT), str(REPO_ROOT / "src")))
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = python_path if not inherited else python_path + os.pathsep + inherited
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["DOCKER_DEFAULT_PLATFORM"] = "linux/amd64"
    env["HB_VERIFIER_FEEDBACK_MODE"] = "binary"
    env.pop("HB_PROCESS_REWARD", None)
    # Keep the Harbor continuation protocol identical for both arms. The
    # treatment difference is implemented inside the custom agent, not by
    # selecting a different Harbor feedback path.
    env["HB_CONTINUE_MODE"] = "same_conversation"
    return env, (secret,)


def _redact(value: str, secrets: tuple[str, ...]) -> str:
    result = value
    for secret in secrets:
        if secret:
            result = result.replace(secret, "[REDACTED]")
    return result


def _worker_command(
    *,
    harbor_project: Path,
    config: Path,
    jobs_dir: Path,
    timeout_seconds: float,
    status: Path,
) -> list[str]:
    return [
        sys.executable,
        str(REPO_ROOT / "scripts" / "harbor_job_worker.py"),
        "--harbor-project",
        str(harbor_project),
        "--config",
        str(config),
        "--jobs-dir",
        str(jobs_dir),
        "--timeout-seconds",
        str(timeout_seconds),
        "--status",
        str(status),
    ]


def _sanitize_status(path: Path, secrets: tuple[str, ...]) -> dict[str, Any]:
    if not path.is_file():
        return {
            "status": "invalid",
            "status_read_error": "missing_worker_status",
            "infrastructure_interrupted": True,
        }
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {
            "status": "invalid",
            "status_read_error": f"worker_status_read_error:{type(exc).__name__}",
            "infrastructure_interrupted": True,
        }
    if not raw.strip():
        return {
            "status": "invalid",
            "status_read_error": "empty_worker_status",
            "infrastructure_interrupted": True,
        }
    redacted = _redact(raw, secrets)
    try:
        value = json.loads(redacted)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {
            "status": "invalid",
            "status_read_error": "invalid_worker_status_json",
            "infrastructure_interrupted": True,
        }
    if not isinstance(value, dict):
        return {
            "status": "invalid",
            "status_read_error": "non_object_worker_status",
            "infrastructure_interrupted": True,
        }
    if redacted != raw:
        _write_json(path, value)
    return value


def _windows_control_interrupted(exit_code: Any) -> bool:
    try:
        normalized = int(exit_code)
    except (TypeError, ValueError, OverflowError):
        return False
    return normalized in {
        WINDOWS_CONTROL_C_EXIT,
        WINDOWS_CONTROL_C_EXIT - (1 << 32),
    }


def _worker_infrastructure_reason(
    worker: dict[str, Any],
    *,
    launcher_exit_code: int | None,
) -> str | None:
    if _windows_control_interrupted(launcher_exit_code):
        return "launcher_control_c_exit"
    if worker.get("status_read_error"):
        return str(worker["status_read_error"])
    if worker.get("infrastructure_interrupted") is True:
        return "worker_reported_infrastructure_interrupt"
    if _windows_control_interrupted(worker.get("exit_code")):
        return "harbor_control_c_exit"
    status = str(worker.get("status", "") or "").strip().lower()
    if status in {"running", "starting"}:
        return f"worker_status_left_{status}"
    if status == "infrastructure_interrupted":
        return "worker_status_infrastructure_interrupted"
    return None


def _raise_if_infrastructure_interrupted(
    *,
    task_name: str,
    arm: str,
    worker: dict[str, Any],
    launcher_exit_code: int | None,
) -> None:
    reason = _worker_infrastructure_reason(
        worker,
        launcher_exit_code=launcher_exit_code,
    )
    if reason is None:
        return
    raise InfrastructureInterruptedError(
        f"{task_name}/{arm}: infrastructure interrupted ({reason})",
        reason=reason,
        worker=worker,
        launcher_exit_code=launcher_exit_code,
    )


def _run_fresh_arm(
    *,
    task_name: str,
    config: Path,
    job_name: str,
    harbor_project: Path,
    jobs_dir: Path,
    output: Path,
    worker_timeout_seconds: float,
    credential_env: str,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    env, secrets = _credential_env(credential_env, arm="dsh_fresh")
    status = output / "worker-status" / f"{task_name}.dsh_fresh.json"
    command = _worker_command(
        harbor_project=harbor_project,
        config=config,
        jobs_dir=jobs_dir,
        timeout_seconds=worker_timeout_seconds,
        status=status,
    )
    started = time.monotonic()
    try:
        completed = _run(
            command,
            cwd=REPO_ROOT,
            env=env,
            timeout=worker_timeout_seconds + 120,
        )
    except subprocess.TimeoutExpired as exc:
        worker = _sanitize_status(status, secrets)
        raise InfrastructureInterruptedError(
            f"{task_name}/dsh_fresh: worker wrapper exceeded outer timeout",
            reason="worker_wrapper_timeout",
            worker=worker,
        ) from exc
    elapsed = round((time.monotonic() - started) * 1000, 3)
    log = output / "logs" / f"{task_name}.dsh_fresh.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(
        _redact((completed.stdout or "") + "\n" + (completed.stderr or ""), secrets),
        encoding="utf-8",
    )
    worker = _sanitize_status(status, secrets)
    _raise_if_infrastructure_interrupted(
        task_name=task_name,
        arm="dsh_fresh",
        worker=worker,
        launcher_exit_code=completed.returncode,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{task_name}: dsh_fresh Harbor worker exited {completed.returncode}; inspect {log}"
        )
    worker_exit = worker.get("exit_code")
    if worker_exit != 0:
        raise RuntimeError(
            f"{task_name}: dsh_fresh Harbor worker exited {worker_exit}; inspect {log}"
        )
    return {
        "arm": "dsh_fresh",
        "task_name": task_name,
        "goal_state": "n/a",
        "elapsed_ms": elapsed,
        "launcher_exit_code": completed.returncode,
        "worker": worker,
        "metrics": _trial_metrics(
            jobs_dir / job_name,
            "dsh_fresh",
            controlled_pair_required=controlled_pair_required,
        ),
        "log": str(log),
    }


def _lhos_verification_outcome(
    task_name: str,
    evaluation: dict[str, Any],
) -> VerificationOutcome:
    return VerificationOutcome(
        passed=bool(evaluation["verified"]),
        artifact_id=f"lhtb://{task_name}/result",
        version=1,
        content=json.dumps(evaluation, sort_keys=True),
        evidence_note="Harbor hidden verifier and DSH resume evidence",
        details=evaluation,
    )


def _find_observability(job_root: Path) -> dict[str, Any] | None:
    """Locate the live DSH progress artifact for a Harbor job, if any.

    The full observability file (``agent/dsh-observability.json``) is only
    refreshed at event boundaries (resume/verifier/invocation end), so it is a
    STALE SNAPSHOT while a single long invocation runs -- reading it would make
    an outer controller blind to a 3600s session. The agent now also publishes
    a live ``dsh-heartbeat.json`` every ~10s while an invocation is running;
    prefer it whenever present, and fall back to the full observability file.
    """
    if not job_root.is_dir():
        return None

    # Bounded lookup first: live artifacts always live at
    # <job>/<trial>/agent/<name>, so an unbounded rglob on every poll would
    # needlessly descend into per-generation dsh-home trees (node_modules,
    # dangling symlinks -- which can also raise OSError mid-iteration and
    # kill the monitor task).  Fall back to the recursive scan only when the
    # bounded probe finds nothing (defensive against unexpected layouts).
    def _candidate_paths(name: str) -> list[Path]:
        try:
            bounded = sorted(job_root.glob(f"*/agent/{name}"))
        except OSError:
            bounded = []
        if bounded:
            return bounded
        try:
            return sorted(job_root.rglob(name))
        except OSError:
            return []

    for path in _candidate_paths("dsh-heartbeat.json"):
        try:
            payload = _load_json(path)
        except Exception:
            continue
        if isinstance(payload, dict) and payload.get("kind") == "dsh-heartbeat":
            return payload
    for path in _candidate_paths("dsh-observability.json"):
        try:
            payload = _load_json(path)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _kill_lhos_containers(task_name: str) -> None:
    """Force-remove running Harbor compose containers owned by a task.

    Used after a proactive budget/no-progress abort so a cancelled worker does
    not leave its Docker containers alive and burning compute. The container
    names embed the task name (``<task>__<hash>-main-1``); match on the leading
    task prefix and never on a broad ``name=`` that could hit other tasks.
    """
    safe = re.sub(r"[^A-Za-z0-9_.-]", "-", task_name)
    prefix = safe[:40]
    try:
        completed = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"name={prefix}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return
    container_ids = [
        line.strip() for line in completed.stdout.splitlines() if line.strip()
    ]
    if not container_ids:
        return
    for cid in container_ids:
        try:
            subprocess.run(
                ["docker", "rm", "-f", cid],
                capture_output=True,
                timeout=30,
            )
        except Exception:
            pass


async def _monitor_lhos(
    job_root: Path,
    *,
    enabled: bool,
    token_budget_units: int,
    control_inert_token_units: int,
    no_progress_window_seconds: float,
    poll_seconds: float,
    quality_stall_token_units: int = 0,
    quality_stall_samples: int = 4,
    quality_stall_max_error_ratio: float = 0.5,
) -> str | None:
    """Proactive resource governance for the LHOS arm.

    Polls the live Harbor observability file and returns an abort reason when
    the run must be stopped early, or ``None`` when it may keep going (which
    also covers the case where the run simply finished first). Only three
    deterministic rules survive validation against all 46 real runs:

    1. token_budget_exceeded  - hard 80M ceiling (only super-mario 120.9M).
    2. semantic_control_inert - control claimed but zero decisions past a 60M
       spend (super-mario; apex-law 58.5M / reward 0.79 is spared).
    3. no_progress            - no new events and no new tokens over a window.

    A fourth rule (invalid-topology retry loops) was tried and REMOVED because
    the real data proved it would kill two of LHOS's best results: grammar-fuzz
    (0.899) and poc-exploit-craft (0.892) also resume with invalid topology.
    Without a runtime reward signal, no token/topology threshold can separate
    them from the genuinely wasteful runs, so the safe choice is to not guess.
    """
    if not enabled:
        return None
    last_event_count: int | None = None
    last_token_units: int | None = None
    stalled_samples = 0
    stall_window = max(1.0, float(no_progress_window_seconds))
    step = max(1.0, float(poll_seconds))
    # Progressive-quality state: last observed cumulative quality probe values.
    q_last_distinct_writes: int | None = None
    q_last_test_calls: int | None = None
    q_last_error_calls: int | None = None
    q_stall_samples = 0
    # Give the first invocation time to create the observability file before
    # the first sample; a premature "no progress" verdict must be impossible.
    await asyncio.sleep(step)
    while True:
        obs = _find_observability(job_root)
        if obs is not None:
            usage = obs.get("usage") or {}
            token_units = int(usage.get("total_token_units") or 0)
            # Budget guards must compare the RUN TOTAL, not the current
            # generation: the heartbeat's "usage" block resets on every
            # compacted restart, so a multi-generation run would never trip
            # the ceiling (observed: 45.9M/71.6M runs with no generation
            # anywhere near the cap).  "usage_cumulative" is the
            # cross-generation total written by the agent; the full
            # observability snapshot's "usage" is already cumulative, and a
            # legacy heartbeat without the new key degrades to per-generation
            # (previous behaviour), hence max().
            usage_cumulative = obs.get("usage_cumulative")
            budget_token_units = token_units
            if isinstance(usage_cumulative, dict):
                budget_token_units = max(
                    token_units,
                    int(usage_cumulative.get("total_token_units") or 0),
                )
            event_count = int(obs.get("event_count") or 0)
            invocations = int(obs.get("invocation_count") or 0)
            semantic_control = bool(obs.get("semantic_context_control"))
            decisions = int(obs.get("semantic_decision_count") or 0)
            # 1) Hard per-task token ceiling. Only super-mario (120.9M) crosses
            #    this among all 46 real runs; no high-reward run is endangered.
            if token_budget_units > 0 and budget_token_units > token_budget_units:
                return f"token_budget_exceeded:{budget_token_units}"
            # 2) Semantic control claimed but inert past a large spend. The 60M
            #    floor spares apex-law (58.5M, reward 0.79, completed normally)
            #    while catching super-mario (120.9M, reward 0, burned to timeout).
            if (
                semantic_control
                and decisions == 0
                and control_inert_token_units > 0
                and budget_token_units > control_inert_token_units
            ):
                return f"semantic_control_inert:{budget_token_units}:{invocations}"
            # 3) Stall detection: neither events nor tokens grew over the window.
            if last_event_count is not None:
                # Heartbeat counters are per-generation: a compacted restart
                # resets them toward zero.  A decrease is lifecycle evidence,
                # not a stall -- re-baseline and give the new generation a
                # fresh window instead of counting the reset as "no progress".
                counter_reset = (
                    event_count < last_event_count
                    or token_units < last_token_units
                )
                grew = (
                    event_count > last_event_count
                    or token_units > last_token_units
                )
                if counter_reset:
                    stalled_samples = 0
                elif not grew:
                    stalled_samples += 1
                    if stalled_samples * step >= stall_window:
                        return (
                            f"no_progress:{event_count}:{token_units}"
                        )
                else:
                    stalled_samples = 0
            last_event_count = event_count
            last_token_units = token_units
            # 4) Progressive-quality stall (漏洞3): the agent keeps running
            #    tests/builds but they keep failing while the distinct artifact
            #    set stops growing. This is the only runtime signal that can
            #    separate a spinning microscopy (55M, 0.06) from a progressing
            #    grammar-fuzz (47M, 0.899) -- pure token/topology thresholds
            #    cannot (validated offline in ACTIVE-STOPLOSS-20260831.md v4).
            #    Off by default; a tuned ablation enables it. Conservative:
            #    needs `quality_stall_samples` consecutive growing-test but
            #    stalled-write polls past the token floor.
            quality = obs.get("quality") or {}
            q_distinct_writes = int(quality.get("cumulative_distinct_writes") or 0)
            q_test_calls = int(quality.get("cumulative_test_calls") or 0)
            q_error_calls = int(quality.get("cumulative_error_calls") or 0)
            if (
                quality_stall_token_units > 0
                and token_units > quality_stall_token_units
                and q_distinct_writes > 0
                and q_test_calls > 0
                and q_error_calls > 0
                and (q_error_calls / max(1, q_test_calls)) > quality_stall_max_error_ratio
                and q_last_distinct_writes is not None
            ):
                grew_writes = q_distinct_writes > q_last_distinct_writes
                ran_tests = q_test_calls > q_last_test_calls
                if grew_writes:
                    # New distinct artifacts -> the agent is advancing; reset.
                    q_stall_samples = 0
                elif ran_tests:
                    # Tests run but keep failing and no new artifacts: stall++.
                    q_stall_samples += 1
                    if q_stall_samples >= max(1, quality_stall_samples):
                        return (
                            "quality_stall:"
                            f"{q_distinct_writes}:{q_test_calls}:{q_error_calls}:{token_units}"
                        )
                # Same values between two heartbeat samples: neither progress
                # nor a new data point; keep the running count (do not reset).
            if q_last_distinct_writes is not None:
                q_last_distinct_writes = q_distinct_writes
                q_last_test_calls = q_test_calls
                q_last_error_calls = q_error_calls
            else:
                q_last_distinct_writes = q_distinct_writes
                q_last_test_calls = q_test_calls
                q_last_error_calls = q_error_calls
        await asyncio.sleep(step)


async def _run_lhos_arm(
    *,
    task_name: str,
    config: Path,
    job_name: str,
    harbor_project: Path,
    jobs_dir: Path,
    output: Path,
    worker_timeout_seconds: float,
    credential_env: str,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    env, secrets = _credential_env(credential_env, arm="lhos_resume")
    status = output / "worker-status" / f"{task_name}.lhos_resume.json"

    def command(_task_id: str) -> list[str]:
        return _worker_command(
            harbor_project=harbor_project,
            config=config,
            jobs_dir=jobs_dir,
            timeout_seconds=worker_timeout_seconds,
            status=status,
        )

    executor = subprocess_task_executor(
        command,
        cwd=str(REPO_ROOT),
        env=env,
        timeout_seconds=worker_timeout_seconds + 120,
        poll_seconds=0.1,
    )
    runtime = AgentOS(":memory:")
    runtime.add_agent(Agent("harbor", executor=executor, executor_api="context_v1"))
    goal = Goal(f"lhtb-dsh-{task_name}", executor_api="context_v1")
    evaluation: dict[str, Any] = {}

    def verify(_context: Any, dispatched_task: str) -> VerificationOutcome:
        nonlocal evaluation
        if dispatched_task != task_name:
            raise RuntimeError(f"expected task {task_name!r}, received {dispatched_task!r}")
        evaluation = _trial_metrics(
            jobs_dir / job_name,
            "lhos_resume",
            controlled_pair_required=controlled_pair_required,
        )
        return _lhos_verification_outcome(task_name, evaluation)

    goal.task(
        task_name,
        agent="harbor",
        verify=verify,
        executor_api="context_v1",
        max_attempts=1,
        inputs=(f"lhtb://{task_name}/base",),
        outputs=(f"lhtb://{task_name}/result",),
    )
    started = time.monotonic()
    result = None
    run_error: BaseException | None = None
    budget_abort: str | None = None
    try:
        run_task = asyncio.create_task(
            runtime.run_async(
                goal,
                max_dispatches=1,
                max_steps=2,
                max_concurrency=1,
                adaptive=True,
                max_parallelism=1,
            )
        )
        monitor_task = asyncio.create_task(
            _monitor_lhos(
                jobs_dir / job_name,
                enabled=LHOS_ACTIVE_GOVERNANCE_ENABLED,
                token_budget_units=LHOS_TOKEN_BUDGET_UNITS,
                control_inert_token_units=LHOS_CONTROL_INERT_TOKEN_UNITS,
                no_progress_window_seconds=LHOS_NO_PROGRESS_WINDOW_SECONDS,
                poll_seconds=LHOS_NO_PROGRESS_POLL_SECONDS,
                quality_stall_token_units=LHOS_QUALITY_STALL_TOKEN_UNITS,
                quality_stall_samples=LHOS_QUALITY_STALL_SAMPLES,
                quality_stall_max_error_ratio=LHOS_QUALITY_STALL_MAX_ERROR_RATIO,
            )
        )
        done, _pending = await asyncio.wait(
            {run_task, monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if monitor_task in done:
            # Proactive stop-loss fired: cancel the run (which preempts the
            # child process through the context_v1 cancellation token) and
            # make sure the task's Docker containers are actually gone.
            budget_abort = monitor_task.result()
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await run_task
            _kill_lhos_containers(task_name)
        else:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task
            result = run_task.result()
    except BaseException as exc:
        run_error = exc
    finally:
        runtime.close()
    elapsed = round((time.monotonic() - started) * 1000, 3)
    log = output / "logs" / f"{task_name}.lhos_resume.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    worker = _sanitize_status(status, secrets)
    log.write_text(
        _redact(
            str(worker.get("stdout_tail", "")) + "\n" + str(worker.get("stderr_tail", "")),
            secrets,
        ),
        encoding="utf-8",
    )
    if budget_abort is None:
        _raise_if_infrastructure_interrupted(
            task_name=task_name,
            arm="lhos_resume",
            worker=worker,
            launcher_exit_code=None,
        )
    # When the monitor stopped the run (budget_abort set), the worker was
    # deliberately killed mid-flight: its status file legitimately still says
    # "running" and its exit_code may be null.  Both would otherwise mask the
    # real budget_abort outcome as a "worker_status_left_running" infra flake
    # and burn a full spurious retry on top (observed on dicom/epidemic/gdal).
    if run_error is not None:
        raise run_error
    evaluation = evaluation or _trial_metrics(
        jobs_dir / job_name,
        "lhos_resume",
        controlled_pair_required=controlled_pair_required,
    )
    if budget_abort is not None:
        # Record the proactive stop as an explicit, attributable outcome: the
        # run was stopped by the OS controller, not by an agent or infra error.
        evaluation["budget_abort_reason"] = budget_abort
        evaluation["budget_exhausted"] = True
        evaluation["verification_status"] = "budget_abort"
    worker_exit = worker.get("exit_code")
    if worker_exit != 0 and budget_abort is None:
        raise RuntimeError(
            f"{task_name}: lhos_resume Harbor worker exited {worker_exit}; inspect {log}"
        )
    return {
        "arm": "lhos_resume",
        "task_name": task_name,
        "goal_state": result.goal_state if result is not None else "unknown",
        "elapsed_ms": elapsed,
        "worker": worker,
        "metrics": evaluation,
        "run_result": result.as_dict() if result is not None else {},
        "log": str(log),
    }


def _image_ids_match(
    output: Path,
    manifest: dict[str, Any],
    *,
    strict: bool = True,
) -> dict[str, str]:
    if not strict:
        return {}
    prebuild_result = _load_json(output / "prebuild.json")
    prebuilt = {
        str(item["image"]): item["image_id"]
        for item in prebuild_result.get("records", ())
        if item.get("image_id")
    }
    errors: dict[str, str] = {}
    for task in manifest["tasks"]:
        name = str(task["name"])
        prepared = task.get("local_image_ids", {})
        for image in task.get(
            "required_docker_images",
            (task["docker_image"],),
        ):
            normalized = str(image)
            expected = prebuilt.get(normalized) or prepared.get(normalized)
            try:
                current = _docker_image_id(normalized)
            except Exception as exc:
                errors[name] = (
                    f"{name}: Docker image inspect failed for {normalized}: "
                    f"{type(exc).__name__}: {exc}"
                )
                break
            if not current:
                errors[name] = (
                    f"{name}: required local image is missing: {normalized}; "
                    "run prebuild or prepare with --local-images-only"
                )
                break
            if expected and current != expected:
                errors[name] = f"{name}: local image changed after prepare/prebuild: {normalized}"
                break
    if strict and errors:
        raise RuntimeError(next(iter(errors.values())))
    return errors


def _runtime_pins_match(manifest: dict[str, Any]) -> None:
    runtime = manifest["runtime"]
    checks = (
        ("node", Path(runtime["node_host_path"]), runtime["node_sha256"]),
        ("DSH entry", Path(runtime["dsh_host_path"]), runtime["dsh_entry_sha256"]),
        ("StepFun patch", Path(runtime["patch_host_path"]), runtime["patch_sha256"]),
        (
            "custom Harbor agent",
            Path(runtime["agent_module"]),
            runtime["agent_module_sha256"],
        ),
    )
    for label, path, expected in checks:
        if not path.is_file() or _sha256_file(path) != expected:
            raise RuntimeError(f"{label} changed after prepare; create a new prepared run")


def _arm_order(priority: int) -> tuple[str, str]:
    return ARMS if priority % 2 == 1 else tuple(reversed(ARMS))


def _arm_failure_record(
    *,
    task_name: str,
    arm: str,
    exc: BaseException,
    secret: str = "",
) -> dict[str, Any]:
    error = {
        "type": type(exc).__name__,
        "message": _redact(str(exc), (secret,)),
    }
    return {
        "status": "failed",
        "arm": arm,
        "task_name": task_name,
        "parser_error": None,
        "execution_error": error,
        # Keep the legacy fields for consumers of the earlier pilot schema.
        "error_type": error["type"],
        "error": error["message"],
        "metrics": {
            "parse_valid": False,
            "result_eligible": False,
            "mechanism_eligible": False,
            "comparison_eligible": False,
            "parser_error": None,
            "execution_error": error,
            "error": error["message"],
        },
    }


def _task_worker_timeout_seconds(
    task: dict[str, Any],
    manifest: dict[str, Any],
) -> float:
    """Return the outer Harbor worker budget for one task.

    Official-timeout runs need an outer budget larger than the task's agent
    timeout so Harbor can finish its final verifier and artifact collection.
    Historical fixed-budget pilots retain their manifest-wide worker timeout.
    """

    configured = task.get("worker_timeout_seconds")
    if configured is not None:
        return float(configured)
    base = float(manifest.get("worker_timeout_seconds") or DEFAULT_WORKER_TIMEOUT_SECONDS)
    if manifest.get("agent_timeout_mode") not in {"task_declared", "official_per_task"}:
        return base
    agent_timeout = float(task.get("official_agent_timeout_seconds") or 0)
    verifier_timeout = float(task.get("official_verifier_timeout_seconds") or 0)
    return max(base, agent_timeout + verifier_timeout + 300.0)


def _infrastructure_failure_record(
    *,
    task_name: str,
    arm: str,
    exc: InfrastructureInterruptedError,
    partial_metrics: dict[str, Any],
) -> dict[str, Any]:
    error = {
        "type": "infrastructure_interrupted",
        "message": str(exc),
        "reason": exc.reason,
        "launcher_exit_code": exc.launcher_exit_code,
    }
    return {
        "status": "failed",
        "arm": arm,
        "task_name": task_name,
        "failure_classification": "infrastructure_interrupted",
        "infrastructure_interrupted": True,
        "parser_error": None,
        "execution_error": error,
        "error_type": error["type"],
        "error": error["message"],
        "worker": exc.worker,
        "metrics": {
            **partial_metrics,
            "parse_valid": False,
            "result_eligible": False,
            "mechanism_eligible": False,
            "comparison_eligible": False,
            "infrastructure_interrupted": True,
            "execution_error": error,
            "error": error["message"],
        },
    }


def _safe_descendant(root: Path, path: Path) -> Path:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise RuntimeError(
            f"refusing to operate outside {resolved_root}: {resolved_path}"
        ) from exc
    return resolved_path


def _compose_project_name(trial_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]", "-", str(trial_name).lower())
    if not normalized or not re.match(r"^[a-z0-9]", normalized):
        raise RuntimeError(f"cannot derive a safe Compose project from {trial_name!r}")
    return normalized


def _docker_ids(
    resource: str,
    project_name: str,
) -> tuple[list[str], dict[str, Any] | None]:
    command = [
        "docker",
        resource,
        "ls" if resource in {"network", "volume"} else "-aq",
    ]
    if resource in {"network", "volume"}:
        command.extend(
            [
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project_name}",
            ]
        )
    else:
        command.extend(
            [
                "--filter",
                f"label=com.docker.compose.project={project_name}",
            ]
        )
    completed = _run(command, timeout=30)
    if completed.returncode != 0:
        return [], {
            "type": "docker_query_failed",
            "resource": resource,
            "exit_code": completed.returncode,
            "message": (completed.stderr or completed.stdout or "").strip()[-1000:],
        }
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()], None


def _cleanup_compose_project(project_name: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project_name):
        return {
            "project_name": project_name,
            "complete": False,
            "errors": [{"type": "invalid_compose_project"}],
        }
    errors: list[dict[str, Any]] = []
    removed: dict[str, list[str]] = {
        "containers": [],
        "networks": [],
        "volumes": [],
    }
    container_ids, error = _docker_ids("ps", project_name)
    if error is not None:
        errors.append(error)
    elif container_ids:
        completed = _run(["docker", "rm", "-f", *container_ids], timeout=60)
        if completed.returncode == 0:
            removed["containers"] = container_ids
        else:
            errors.append(
                {
                    "type": "docker_container_cleanup_failed",
                    "exit_code": completed.returncode,
                    "message": (completed.stderr or completed.stdout or "").strip()[-1000:],
                }
            )
    for resource, remove_command, output_key in (
        ("network", "network", "networks"),
        ("volume", "volume", "volumes"),
    ):
        ids, error = _docker_ids(resource, project_name)
        if error is not None:
            errors.append(error)
            continue
        if not ids:
            continue
        completed = _run(
            ["docker", remove_command, "rm", *ids],
            timeout=60,
        )
        if completed.returncode == 0:
            removed[output_key] = ids
        else:
            errors.append(
                {
                    "type": f"docker_{resource}_cleanup_failed",
                    "exit_code": completed.returncode,
                    "message": (completed.stderr or completed.stdout or "").strip()[-1000:],
                }
            )
    return {
        "project_name": project_name,
        "complete": not errors,
        "removed": removed,
        "errors": errors,
    }


def _partial_metrics(job_root: Path, arm: str) -> dict[str, Any]:
    if not job_root.is_dir():
        return {}
    try:
        metrics = _trial_metrics(job_root, arm)
    except Exception as exc:
        return {
            "parse_valid": False,
            "result_eligible": False,
            "mechanism_eligible": False,
            "comparison_eligible": False,
            "parser_error": f"partial_metric_parse_failed:{type(exc).__name__}",
        }
    return {
        **metrics,
        "result_eligible": False,
        "mechanism_eligible": False,
        "comparison_eligible": False,
    }


def _move_if_present(source: Path, destination: Path) -> str | None:
    if not source.exists():
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise RuntimeError(f"retry archive already exists: {destination}")
    shutil.move(str(source), str(destination))
    return str(destination)


def _prepare_infrastructure_retry(
    *,
    task_name: str,
    arm: str,
    controller_attempt: int,
    exc: InfrastructureInterruptedError,
    output: Path,
    jobs_dir: Path,
    job_name: str,
    secret: str,
) -> dict[str, Any]:
    job_root = _safe_descendant(jobs_dir, jobs_dir / job_name)
    partial_metrics = _partial_metrics(job_root, arm)
    trial_names = sorted(
        child.name
        for child in job_root.iterdir()
        if job_root.is_dir() and child.is_dir()
    ) if job_root.is_dir() else []
    projects: list[str] = []
    cleanup_records: list[dict[str, Any]] = []
    cleanup_complete = True
    try:
        projects = [_compose_project_name(name) for name in trial_names]
    except RuntimeError as project_error:
        cleanup_complete = False
        cleanup_records.append(
            {
                "complete": False,
                "errors": [
                    {
                        "type": type(project_error).__name__,
                        "message": str(project_error),
                    }
                ],
            }
        )
    if cleanup_complete:
        for project in projects:
            cleanup = _cleanup_compose_project(project)
            cleanup_records.append(cleanup)
            cleanup_complete = cleanup_complete and bool(cleanup["complete"])

    attempt_root = (
        output
        / "controller-attempts"
        / task_name
        / arm
        / f"attempt-{controller_attempt}"
    )
    archive_error: dict[str, str] | None = None
    archived_job: str | None = None
    archived_status: str | None = None
    archived_log: str | None = None
    if cleanup_complete:
        try:
            archive_job = _safe_descendant(
                jobs_dir,
                jobs_dir
                / "_lhos-controller-attempts"
                / task_name
                / arm
                / f"attempt-{controller_attempt}"
                / job_name,
            )
            archived_job = _move_if_present(job_root, archive_job)
            archived_status = _move_if_present(
                output / "worker-status" / f"{task_name}.{arm}.json",
                attempt_root / "worker-status.json",
            )
            archived_log = _move_if_present(
                output / "logs" / f"{task_name}.{arm}.log",
                attempt_root / "launcher.log",
            )
        except Exception as archive_exc:
            cleanup_complete = False
            archive_error = {
                "type": type(archive_exc).__name__,
                "message": _redact(str(archive_exc), (secret,)),
            }

    ledger_path = (
        output
        / "controller-attempts"
        / task_name
        / arm
        / f"attempt-{controller_attempt}.json"
    )
    ledger = {
        "schema_version": "lhos-lhtb-controller-attempt.v1",
        "task_name": task_name,
        "arm": arm,
        "controller_attempt": controller_attempt,
        "ledger_path": str(ledger_path),
        "classification": "infrastructure_interrupted",
        "reason": exc.reason,
        "message": _redact(str(exc), (secret,)),
        "launcher_exit_code": exc.launcher_exit_code,
        "worker": exc.worker,
        "partial_metrics": partial_metrics,
        "trial_names": trial_names,
        "compose_projects": projects,
        "cleanup": cleanup_records,
        "archive_error": archive_error,
        "archived_job": archived_job,
        "archived_status": archived_status,
        "archived_log": archived_log,
        "retry_authorized": cleanup_complete,
        "recorded_at": datetime.now().astimezone().isoformat(),
    }
    _write_json(ledger_path, ledger)
    return ledger


def _existing_terminal_arm_record(
    path: Path,
    *,
    task_name: str,
    arm: str,
    parity_sha256: str,
) -> dict[str, Any] | None:
    """Return a safe checkpoint or request a Harbor-backed replay of an incomplete one."""

    if not path.is_file():
        return None
    record, parser_error = _load_arm_record(path)
    if parser_error is not None:
        print(
            f"warning: replaying {task_name}/{arm}: persisted arm record is invalid",
            file=sys.stderr,
        )
        return None
    status = str(record.get("status", "") or "")
    if status not in TERMINAL_ARM_STATUSES:
        print(
            f"warning: replaying {task_name}/{arm}: non-terminal status={status!r}",
            file=sys.stderr,
        )
        return None
    identity_errors: list[str] = []
    if record.get("task_name") != task_name:
        identity_errors.append("task_name")
    if record.get("arm") != arm:
        identity_errors.append("arm")
    if record.get("config_parity_sha256") != parity_sha256:
        identity_errors.append("config_parity_sha256")
    if identity_errors:
        raise RuntimeError(
            f"{task_name}/{arm}: persisted terminal record identity mismatch: "
            + ", ".join(identity_errors)
        )
    return record


def _run_progress_payload(
    output: Path,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    active_arms = _manifest_run_arms(manifest)
    arm_mode = _normalize_arm_mode(manifest.get("run_arm", "both"))
    tasks: list[dict[str, Any]] = []
    completed_arm_count = 0
    failed_arm_count = 0
    invalid_arm_count = 0
    pending_arm_count = 0
    completed_pair_count = 0
    terminal_pair_count = 0
    metric_keys = (
        "reward",
        "resolved",
        "verified",
        "verification_status",
        "parse_valid",
        "result_eligible",
        "mechanism_eligible",
        "comparison_eligible",
        "provider_censored",
        "token_units",
        "uncached_input_tokens",
        "cache_read_tokens",
        "output_tokens",
        "model_calls",
        "tool_calls",
        "dsh_invocations",
        "dsh_controlled_restarts",
        "agent_elapsed_ms",
        "harbor_total_elapsed_ms",
        "time_to_verified_ms",
    )
    for task in manifest.get("tasks", ()):
        name = str(task["name"])
        arms: dict[str, Any] = {}
        active_statuses: list[str] = []
        for arm in ARMS:
            path = output / "runs" / name / f"{arm}.json"
            if arm not in active_arms:
                arms[arm] = {
                    "status": "not_selected",
                    "record": str(path) if path.is_file() else None,
                    "parser_error": None,
                    "execution_error": None,
                    "metrics": {},
                }
                continue
            if not path.is_file():
                status = "pending"
                record: dict[str, Any] = {}
                parser_error = None
                pending_arm_count += 1
            else:
                record, parser_error = _load_arm_record(path)
                if parser_error is not None:
                    status = "invalid"
                    invalid_arm_count += 1
                else:
                    status = str(record.get("status", "unknown") or "unknown")
                    if status == "completed":
                        completed_arm_count += 1
                    elif status == "failed":
                        failed_arm_count += 1
                    else:
                        pending_arm_count += 1
            active_statuses.append(status)
            raw_metrics = record.get("metrics", {})
            metrics = raw_metrics if isinstance(raw_metrics, dict) else {}
            arms[arm] = {
                "status": status,
                "record": str(path) if path.is_file() else None,
                "parser_error": parser_error,
                "execution_error": (
                    record.get("execution_error") or metrics.get("execution_error")
                ),
                "metrics": {
                    key: metrics.get(key)
                    for key in metric_keys
                    if key in metrics
                },
            }
        terminal = bool(active_statuses) and all(
            status in TERMINAL_ARM_STATUSES for status in active_statuses
        )
        complete = bool(active_statuses) and all(
            status == "completed" for status in active_statuses
        )
        terminal_pair_count += int(terminal)
        completed_pair_count += int(complete)
        if arm_mode == "both":
            pair_status = (
                "completed"
                if complete
                else "terminal_with_failure"
                if terminal
                else "invalid_checkpoint"
                if "invalid" in active_statuses
                else "in_progress"
                if any(status != "pending" for status in active_statuses)
                else "pending"
            )
        else:
            pair_status = (
                "arm_completed"
                if complete
                else "arm_failed"
                if terminal
                else "invalid_checkpoint"
                if "invalid" in active_statuses
                else "in_progress"
                if any(status != "pending" for status in active_statuses)
                else "pending"
            )
        tasks.append(
            {
                "task_name": name,
                "priority": task.get("priority"),
                "configured_time_slice_seconds": task.get(
                    "configured_time_slice_seconds"
                ),
                "time_slice_policy": task.get("time_slice_policy"),
                "continuation_boundary_mode": task.get(
                    "continuation_boundary_mode"
                ),
                "pair_status": pair_status,
                "arms": arms,
            }
        )
    arm_count = len(tasks) * len(active_arms)
    return {
        "schema_version": RUN_PROGRESS_SCHEMA_V1,
        "updated_at": datetime.now().astimezone().isoformat(),
        "arm_mode": arm_mode,
        "selected_arms": list(active_arms),
        "task_count": len(tasks),
        "arm_count": arm_count,
        "completed_arm_count": completed_arm_count,
        "failed_arm_count": failed_arm_count,
        "invalid_arm_count": invalid_arm_count,
        "pending_arm_count": pending_arm_count,
        "terminal_pair_count": terminal_pair_count,
        "completed_pair_count": completed_pair_count,
        "tasks": tasks,
    }


def _write_run_progress(output: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    progress = _run_progress_payload(output, manifest)
    _write_json(output / "progress.json", progress)
    return progress


def _run_task_pair(
    *,
    task: dict[str, Any],
    manifest: dict[str, Any],
    output: Path,
    jobs_dir: Path,
    credential_env: str,
) -> list[dict[str, Any]]:
    name = str(task["name"])
    priority = int(task["priority"])
    configs = manifest["configs"][name]
    harbor_project = Path(manifest["harbor"]["project"])
    worker_timeout_seconds = _task_worker_timeout_seconds(task, manifest)
    active_arms = _manifest_run_arms(manifest)
    controlled_pair_required = _controlled_pair_enabled(manifest)
    records: list[dict[str, Any]] = []
    for arm in _arm_order(priority):
        if arm not in active_arms:
            continue
        path = output / "runs" / name / f"{arm}.json"
        existing = _existing_terminal_arm_record(
            path,
            task_name=name,
            arm=arm,
            parity_sha256=str(configs["parity_sha256"]),
        )
        if existing is not None:
            records.append(existing)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        config = Path(configs[arm])
        job_name = str(configs[f"{arm}_job_name"])
        controller_ledgers: list[dict[str, Any]] = []
        record: dict[str, Any] | None = None
        controller_attempt_count = 0
        for controller_attempt in range(
            1,
            max(MAX_INFRASTRUCTURE_ARM_RETRIES, MAX_CENSORSHIP_RETRIES,
                MAX_CONNECTION_RETRIES) + 2,
        ):
            controller_attempt_count = controller_attempt
            try:
                if arm == "dsh_fresh":
                    record = _run_fresh_arm(
                        task_name=name,
                        config=config,
                        job_name=job_name,
                        harbor_project=harbor_project,
                        jobs_dir=jobs_dir,
                        output=output,
                        worker_timeout_seconds=worker_timeout_seconds,
                        credential_env=credential_env,
                        controlled_pair_required=controlled_pair_required,
                    )
                else:
                    record = asyncio.run(
                        _run_lhos_arm(
                            task_name=name,
                            config=config,
                            job_name=job_name,
                            harbor_project=harbor_project,
                            jobs_dir=jobs_dir,
                            output=output,
                            worker_timeout_seconds=worker_timeout_seconds,
                            credential_env=credential_env,
                            controlled_pair_required=controlled_pair_required,
                        )
                    )
            except InfrastructureInterruptedError as exc:
                secret = os.environ.get(credential_env, "")
                try:
                    ledger = _prepare_infrastructure_retry(
                        task_name=name,
                        arm=arm,
                        controller_attempt=controller_attempt,
                        exc=exc,
                        output=output,
                        jobs_dir=jobs_dir,
                        job_name=job_name,
                        secret=secret,
                    )
                except Exception as cleanup_exc:
                    partial_metrics = _partial_metrics(
                        jobs_dir / job_name,
                        arm,
                    )
                    ledger_path = (
                        output
                        / "controller-attempts"
                        / name
                        / arm
                        / f"attempt-{controller_attempt}.json"
                    )
                    ledger = {
                        "schema_version": "lhos-lhtb-controller-attempt.v1",
                        "task_name": name,
                        "arm": arm,
                        "controller_attempt": controller_attempt,
                        "ledger_path": str(ledger_path),
                        "classification": "infrastructure_interrupted",
                        "reason": exc.reason,
                        "message": _redact(str(exc), (secret,)),
                        "partial_metrics": partial_metrics,
                        "retry_authorized": False,
                        "cleanup_exception": {
                            "type": type(cleanup_exc).__name__,
                            "message": _redact(str(cleanup_exc), (secret,)),
                        },
                        "recorded_at": datetime.now().astimezone().isoformat(),
                    }
                    _write_json(ledger_path, ledger)
                controller_ledgers.append(ledger)
                if (
                    controller_attempt <= MAX_INFRASTRUCTURE_ARM_RETRIES
                    and ledger.get("retry_authorized") is True
                ):
                    print(
                        f"warning: retrying {name}/{arm} once after "
                        f"infrastructure interruption: {exc.reason}",
                        file=sys.stderr,
                    )
                    continue
                record = _infrastructure_failure_record(
                    task_name=name,
                    arm=arm,
                    exc=exc,
                    partial_metrics=dict(ledger.get("partial_metrics") or {}),
                )
                # An infrastructure-interrupted arm has its own retry budget
                # (MAX_INFRASTRUCTURE_ARM_RETRIES) and must NOT fall through
                # into the censorship / never-connected classifiers below:
                # its zero-token failure record would otherwise be mistaken
                # for a connection abort and retried up to
                # MAX_CONNECTION_RETRIES more times, multiplying both
                # wall-clock and token waste on a genuine infra fault.
                break
            except Exception as exc:
                secret = os.environ.get(credential_env, "")
                record = _arm_failure_record(
                    task_name=name,
                    arm=arm,
                    exc=exc,
                    secret=secret,
                )
            # provider_censored retry: resample from scratch
            _censored = False
            if record is not None:
                _metrics = record.get("metrics") or {}
                if _metrics.get("provider_censored") is True:
                    _censored = True
            if not _censored and _job_was_censored(jobs_dir / job_name):
                _censored = True
            if _censored and controller_attempt <= MAX_CENSORSHIP_RETRIES:
                    print(
                        f"warning: provider censored {name}/{arm}, "
                        f"retrying (attempt {controller_attempt}/{MAX_CENSORSHIP_RETRIES})",
                        file=sys.stderr,
                    )
                    import shutil
                    job_path = jobs_dir / job_name
                    # Capture the wiped attempt's burned tokens into the retry
                    # ledger BEFORE deleting the job dir, so controller-attempt
                    # cost accounting (and the pair token comparison) stays
                    # honest -- a censored attempt can burn tens of minutes of
                    # model calls before the 451 surfaces.
                    partial_metrics = _partial_metrics(job_path, arm)
                    if job_path.exists():
                        shutil.rmtree(job_path, ignore_errors=True)
                    record_path = output / "runs" / name / f"{arm}.json"
                    if record_path.exists():
                        record_path.unlink()
                    controller_ledgers.append({
                        "classification": "provider_censored",
                        "controller_attempt": controller_attempt,
                        "partial_metrics": partial_metrics,
                        "recorded_at": datetime.now().astimezone().isoformat(),
                    })
                    continue
            # connection-level abort: arm failed without ever producing an
            # agent event (exit 134 during first request). Resample cleanly.
            _connect_fail = _arm_never_started(
                record, jobs_dir / job_name
            )
            if (
                _connect_fail
                and controller_attempt <= MAX_CONNECTION_RETRIES
            ):
                print(
                    f"warning: never-connected abort {name}/{arm}, "
                    f"clean resample (attempt "
                    f"{controller_attempt}/{MAX_CONNECTION_RETRIES})",
                    file=sys.stderr,
                )
                import shutil
                job_path = jobs_dir / job_name
                # Same accounting fix as the censorship branch: keep the
                # aborted attempt's partial metrics before the clean resample
                # wipes the job dir (usually token-less, but not guaranteed).
                partial_metrics = _partial_metrics(job_path, arm)
                if job_path.exists():
                    shutil.rmtree(job_path, ignore_errors=True)
                record_path = output / "runs" / name / f"{arm}.json"
                if record_path.exists():
                    record_path.unlink()
                controller_ledgers.append({
                    "classification": "never_connected_abort",
                    "controller_attempt": controller_attempt,
                    "partial_metrics": partial_metrics,
                    "recorded_at": datetime.now().astimezone().isoformat(),
                })
                continue
            break
        if record is None:
            record = _arm_failure_record(
                task_name=name,
                arm=arm,
                exc=RuntimeError("arm controller returned no terminal record"),
                secret=os.environ.get(credential_env, ""),
            )
        record["controller_attempt_count"] = controller_attempt_count
        record["controller_attempt_ledger"] = controller_ledgers
        record["infrastructure_retry_count"] = max(
            0,
            controller_attempt_count - 1,
        )
        record["infrastructure_resampled"] = bool(controller_ledgers)
        record["status"] = record.get("status", "completed")
        record["execution_order"] = [
            arm_name for arm_name in _arm_order(priority) if arm_name in active_arms
        ]
        record["config_parity_sha256"] = configs["parity_sha256"]
        record["docker_image_id"] = _docker_image_id(str(task["docker_image"]))
        record["configured_time_slice_seconds"] = task.get(
            "configured_time_slice_seconds"
        )
        record["time_slice_policy"] = task.get("time_slice_policy")
        record["continuation_boundary_mode"] = task.get(
            "continuation_boundary_mode"
        )
        _write_json(path, record)
        _write_run_progress(output, manifest)
        records.append(record)
    _write_run_progress(output, manifest)
    return records


def _is_reparse_point(info: os.stat_result) -> bool:
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return stat.S_ISLNK(info.st_mode) or bool(attributes & reparse_flag)


def _scan_secret(roots: tuple[Path, ...], secret: str) -> dict[str, Any]:
    """Scan regular files without following Windows reparse points."""

    report: dict[str, Any] = {
        "schema_version": "lhos-secret-scan.v2",
        "scanned_roots": [_redact(str(path), (secret,)) for path in roots],
        "scanned_file_count": 0,
        "exact_key_hit_count": 0,
        "hits": [],
        "skipped": [],
        "skipped_count": 0,
        "errors": [],
        "error_count": 0,
        "scan_complete": True,
        "passed": True,
    }
    if not secret:
        report["enabled"] = False
        return report

    report["enabled"] = True
    needle = secret.encode()
    overlap = max(0, len(needle) - 1)
    stack: list[tuple[Path, os.stat_result | None]] = [
        (Path(root), None) for root in reversed(roots)
    ]

    def display(path: Path) -> str:
        return _redact(str(path), (secret,))

    def record_skip(path: Path, reason: str) -> None:
        report["skipped"].append(
            {
                "path": display(path),
                "reason": reason,
            }
        )

    def record_error(path: Path, operation: str, exc: BaseException) -> None:
        report["errors"].append(
            {
                "path": display(path),
                "operation": operation,
                "type": type(exc).__name__,
                "message": _redact(str(exc), (secret,)),
            }
        )
        report["scan_complete"] = False

    def scan_file(path: Path) -> None:
        try:
            with path.open("rb") as handle:
                tail = b""
                while True:
                    try:
                        chunk = handle.read(1024 * 1024)
                    except OSError as exc:
                        record_error(path, "read", exc)
                        return
                    if not chunk:
                        break
                    data = tail + chunk
                    if needle in data:
                        report["hits"].append(display(path))
                        break
                    tail = data[-overlap:] if overlap else b""
        except FileNotFoundError:
            record_skip(path, "missing_after_enumeration")
            return
        except OSError as exc:
            record_error(path, "open", exc)
            return
        report["scanned_file_count"] += 1

    while stack:
        path, known_info = stack.pop()
        if ".git" in path.parts:
            record_skip(path, "git_metadata")
            continue
        try:
            info = known_info or path.lstat()
        except FileNotFoundError:
            record_skip(path, "missing_after_enumeration")
            continue
        except OSError as exc:
            record_error(path, "lstat", exc)
            continue
        if _is_reparse_point(info):
            record_skip(path, "reparse_point")
            continue
        if stat.S_ISREG(info.st_mode):
            scan_file(path)
            continue
        if not stat.S_ISDIR(info.st_mode):
            record_skip(path, "non_regular_file")
            continue

        children: list[tuple[Path, os.stat_result | None]] = []
        try:
            with os.scandir(path) as entries:
                while True:
                    try:
                        entry = next(entries)
                    except StopIteration:
                        break
                    except OSError as exc:
                        record_error(path, "scandir_iter", exc)
                        break
                    child = Path(entry.path)
                    if entry.name == ".git":
                        record_skip(child, "git_metadata")
                        continue
                    try:
                        child_info = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        record_skip(child, "missing_after_enumeration")
                        continue
                    except OSError as exc:
                        record_error(child, "direntry_stat", exc)
                        continue
                    children.append((child, child_info))
        except FileNotFoundError:
            record_skip(path, "missing_after_enumeration")
            continue
        except OSError as exc:
            record_error(path, "scandir", exc)
            continue
        stack.extend(reversed(children))

    report["exact_key_hit_count"] = len(report["hits"])
    report["skipped_count"] = len(report["skipped"])
    report["error_count"] = len(report["errors"])
    report["passed"] = report["exact_key_hit_count"] == 0
    return report


def _live_compose_projects() -> set[str]:
    """Names of compose projects with at least one existing container."""

    try:
        completed = _run(
            ["docker", "ps", "-a", "--format", "{{.Label \"com.docker.compose.project\"}}"],
            timeout=30,
        )
    except Exception:
        return set()
    if completed.returncode != 0:
        return set()
    return {
        line.strip()
        for line in (completed.stdout or "").splitlines()
        if line.strip()
    }


def _sweep_orphaned_trial_containers(
    jobs_dir: Path,
    *,
    live_projects: set[str] | None = None,
) -> list[str]:
    """Tear down compose projects orphaned by a previously interrupted run.

    Trial containers are dockerd-managed: they survive the death of the whole
    runner process tree (observed: two v3-confirm containers idled 1.5h after
    the runner died).  At startup -- before any new arm launches -- every live
    container matching a trial directory under THIS run's jobs dir is by
    definition an orphan.  The sweep is scoped strictly to this jobs dir
    (per-output), so concurrent runs on other outputs are never touched.
    """

    swept: list[str] = []
    if not jobs_dir.is_dir():
        return swept
    if live_projects is None:
        live_projects = _live_compose_projects()
    if not live_projects:
        return swept
    try:
        job_dirs = sorted(p for p in jobs_dir.iterdir() if p.is_dir())
    except OSError:
        return swept
    for job_dir in job_dirs:
        if job_dir.name.startswith("_"):
            continue
        try:
            trial_dirs = sorted(
                p for p in job_dir.iterdir() if p.is_dir() and "__" in p.name
            )
        except OSError:
            continue
        for trial_dir in trial_dirs:
            try:
                project = _compose_project_name(trial_dir.name)
            except RuntimeError:
                continue
            if project not in live_projects:
                continue
            result = _cleanup_compose_project(project)
            removed = result.get("removed") or {}
            if any(removed.values()):
                swept.append(project)
                print(
                    "warning: swept orphaned compose project "
                    f"{project} from a previous interrupted run",
                    file=sys.stderr,
                )
    return swept


def _persist_task_controller_failure(
    *,
    output: Path,
    task: dict[str, Any],
    manifest: dict[str, Any],
    exc: BaseException,
    credential_env: str,
    arms: tuple[str, ...] | None = None,
) -> None:
    """Persist both arm failures when a task controller fails before dispatch."""

    name = str(task["name"])
    secret = os.environ.get(credential_env, "")
    configs = manifest.get("configs") or {}
    manifest_configs = configs.get(name, {}) if isinstance(configs, dict) else {}
    selected_arms = arms or _manifest_run_arms(manifest)
    for arm in selected_arms:
        path = output / "runs" / name / f"{arm}.json"
        if path.exists():
            continue
        record = _arm_failure_record(
            task_name=name,
            arm=arm,
            exc=exc,
            secret=secret,
        )
        priority = int(task.get("priority", 1) or 1)
        record["execution_order"] = [
            arm_name
            for arm_name in _arm_order(priority)
            if arm_name in selected_arms
        ]
        if manifest_configs.get("parity_sha256"):
            record["config_parity_sha256"] = manifest_configs["parity_sha256"]
        record["docker_image_id"] = None
        _write_json(path, record)


def _persist_pair_admission(
    output: Path,
    manifest: dict[str, Any],
    plan: dict[str, Any],
) -> None:
    manifest["resource_aware_pairs"] = bool(plan.get("enabled"))
    manifest["pair_admission"] = plan
    _write_json(output / "pair-admission.json", plan)
    _write_json(output / "manifest.json", manifest)


def _run_task_batch(
    *,
    tasks: list[dict[str, Any]],
    max_workers: int,
    manifest: dict[str, Any],
    output: Path,
    jobs_dir: Path,
    credential_env: str,
) -> None:
    futures: dict[Any, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for task in tasks:
            future = pool.submit(
                _run_task_pair,
                task=task,
                manifest=manifest,
                output=output,
                jobs_dir=jobs_dir,
                credential_env=credential_env,
            )
            futures[future] = task
        for future in as_completed(futures):
            # _run_task_pair persists per-arm failures. Keep collecting the
            # remaining tasks if an unexpected controller exception escapes.
            try:
                future.result()
            except Exception as exc:
                task = futures[future]
                _persist_task_controller_failure(
                    output=output,
                    task=task,
                    manifest=manifest,
                    exc=exc,
                    credential_env=credential_env,
                    arms=_manifest_run_arms(manifest),
                )
                print(
                    f"warning: task worker failed: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )


def _select_work_conserving_admissions(
    pending: list[dict[str, Any]],
    running: list[dict[str, Any]],
    *,
    capacity_cpus: float,
    capacity_memory_mb: float,
    max_workers: int,
) -> list[int]:
    """Indices into ``pending`` that may start right now.

    Work-conserving refill for the resource-aware path.  Invariants: running
    pairs never exceed ``max_workers`` and summed cpu/memory never exceed
    capacity.  An unknown-resource (exclusive) task may only start on an empty
    system, and once one reaches the queue head nothing else is admitted until
    it runs (drain mode), so exclusives cannot starve.  Known tasks are
    backfilled in queue order whenever they fit; a task that fits capacity at
    all is guaranteed to be admitted once enough running tasks finish, so the
    loop always terminates.  Utilization is never lower than static barrier
    waves: any task a wave would have started at wave-open can start here the
    moment its predecessors release capacity, instead of waiting for the
    slowest wave member.
    """

    if not pending or len(running) >= max_workers:
        return []
    if any(not bool(req["known"]) for req in running):
        # An exclusive task occupies the whole system by definition.
        return []
    if not bool(pending[0]["known"]):
        # Exclusive head: admit it only onto an empty system and drain
        # otherwise; later tasks may not jump ahead of it.
        return [] if running else [0]
    used_cpus = sum(float(req["cpus"]) for req in running)
    used_memory_mb = sum(float(req["memory_mb"]) for req in running)
    admitted: list[int] = []
    for index, req in enumerate(pending):
        if len(running) + len(admitted) >= max_workers:
            break
        if not bool(req["known"]):
            # Not yet at the head: skip it in backfill; it advances toward the
            # head as earlier tasks finish and cannot starve (finite queue).
            continue
        cpus = float(req["cpus"])
        memory_mb = float(req["memory_mb"])
        if used_cpus + cpus > float(capacity_cpus):
            continue
        if used_memory_mb + memory_mb > float(capacity_memory_mb):
            continue
        used_cpus += cpus
        used_memory_mb += memory_mb
        admitted.append(index)
    return admitted


def _run_task_batch_work_conserving(
    *,
    tasks: list[dict[str, Any]],
    max_workers: int,
    capacity: dict[str, Any],
    manifest: dict[str, Any],
    output: Path,
    jobs_dir: Path,
    credential_env: str,
) -> None:
    """Run admitted pairs, refilling freed capacity the moment a pair finishes.

    Replaces static wave execution for the resource-aware path.  The admission
    plan remains the capacity proof of record; this scheduler re-checks
    cpu/memory/concurrency on every refill, so no instantaneous combination of
    running pairs ever exceeds capacity.
    """

    capacity_cpus = _resource_number(capacity.get("cpus"))
    capacity_memory_mb = _resource_number(capacity.get("memory_mb"))
    if capacity_cpus is None or capacity_memory_mb is None:
        raise RuntimeError(
            "work-conserving execution requires numeric cpu/memory capacity"
        )
    if max_workers < 1:
        raise RuntimeError("max concurrency must be at least one")
    pending = list(tasks)
    futures: dict[Any, dict[str, Any]] = {}
    footprints: dict[Any, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while pending or futures:
            if pending:
                chosen = _select_work_conserving_admissions(
                    [_task_pair_resource_requirement(task) for task in pending],
                    [footprints[future] for future in futures],
                    capacity_cpus=float(capacity_cpus),
                    capacity_memory_mb=float(capacity_memory_mb),
                    max_workers=max_workers,
                )
                for index in sorted(chosen, reverse=True):
                    task = pending.pop(index)
                    future = pool.submit(
                        _run_task_pair,
                        task=task,
                        manifest=manifest,
                        output=output,
                        jobs_dir=jobs_dir,
                        credential_env=credential_env,
                    )
                    futures[future] = task
                    footprints[future] = _task_pair_resource_requirement(task)
            if not futures:
                # Unreachable: every admitted task passed the capacity check,
                # so on an empty system the head always fits (or is exclusive
                # and admits onto the empty system).  Guard anyway.
                raise RuntimeError(
                    "work-conserving scheduler deadlock: "
                    f"{len(pending)} pending tasks but none admissible"
                )
            done, _pending = wait(tuple(futures), return_when=FIRST_COMPLETED)
            for future in done:
                task = futures.pop(future)
                footprints.pop(future, None)
                try:
                    future.result()
                except Exception as exc:
                    _persist_task_controller_failure(
                        output=output,
                        task=task,
                        manifest=manifest,
                        exc=exc,
                        credential_env=credential_env,
                        arms=_manifest_run_arms(manifest),
                    )
                    print(
                        f"warning: task worker failed: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )


def run_pairs(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output.resolve()
    jobs_dir = args.jobs_dir.resolve()
    jobs_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest(output)
    requested_arm_mode = getattr(args, "arm", None)
    prepared_arm_mode = _normalize_arm_mode(manifest.get("run_arm", "both"))
    materialization = manifest.get("materialization")
    if (
        requested_arm_mode is not None
        and isinstance(materialization, dict)
        and _normalize_arm_mode(requested_arm_mode) != prepared_arm_mode
    ):
        raise RuntimeError(
            "run --arm cannot override an arm-specific materialized manifest"
        )
    arm_mode = _normalize_arm_mode(
        requested_arm_mode
        if requested_arm_mode is not None
        else prepared_arm_mode
    )
    active_arms = _set_manifest_run_arm(manifest, arm_mode)
    if arm_mode != "both":
        unexpected = [
            str(output / "runs" / str(task["name"]) / f"{arm}.json")
            for task in manifest.get("tasks", ())
            for arm in ARMS
            if arm not in active_arms
            and (output / "runs" / str(task["name"]) / f"{arm}.json").exists()
        ]
        if unexpected:
            raise RuntimeError(
                "arm-only output already contains counterpart records; "
                "use materialize-arms with clean targets"
            )
    _runtime_pins_match(manifest)
    _validate_controlled_pair_manifest(manifest)
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        _validate_official_leaderboard_contract(
            task_names=[str(task["name"]) for task in manifest.get("tasks", ())],
            configs=manifest.get("configs") or {},
        )
        _validate_official_manifest_contract(manifest)
    image_errors = _image_ids_match(output, manifest, strict=False)
    prepared_admission = manifest.get("pair_admission")
    prepared_resource_aware = bool(
        isinstance(prepared_admission, dict) and prepared_admission.get("enabled")
    )
    resource_aware_pairs = bool(
        getattr(args, "resource_aware_pairs", False)
        or manifest.get("resource_aware_pairs")
        or prepared_resource_aware
    )
    if resource_aware_pairs:
        _hydrate_manifest_task_resources(manifest)
    requested_max = getattr(args, "max_concurrency", None)
    prepared_max = int(manifest["max_concurrency"])
    if resource_aware_pairs:
        max_workers = int(requested_max if requested_max is not None else prepared_max)
    else:
        requested_legacy_max = (
            int(requested_max) if requested_max is not None else prepared_max
        )
        # Local override: the legacy dynamic thread pool refills a slot the
        # instant a pair finishes (no wave head-of-line blocking), so honor the
        # CLI --max-concurrency instead of clamping to 2.
        max_workers = max(1, requested_legacy_max)
    if max_workers < 1:
        raise RuntimeError("max concurrency must be at least one")
    pair_capacity_cpus = getattr(args, "pair_capacity_cpus", None)
    pair_capacity_memory_mb = getattr(args, "pair_capacity_memory_mb", None)
    if not resource_aware_pairs and (
        pair_capacity_cpus is not None or pair_capacity_memory_mb is not None
    ):
        raise RuntimeError("pair capacity overrides require --resource-aware-pairs")

    runnable_tasks: list[dict[str, Any]] = []
    task_by_name: dict[str, dict[str, Any]] = {}
    for task in manifest["tasks"]:
        name = str(task["name"])
        task_by_name[name] = task
        if name in image_errors:
            _persist_task_controller_failure(
                output=output,
                task=task,
                manifest=manifest,
                exc=RuntimeError(image_errors[name]),
                credential_env=args.credential_env,
                arms=active_arms,
            )
            print(f"warning: skipping {name}: {image_errors[name]}", file=sys.stderr)
            continue
        runnable_tasks.append(task)

    if resource_aware_pairs:
        prepared_capacity = (
            prepared_admission.get("capacity", {})
            if isinstance(prepared_admission, dict)
            else {}
        )
        if not isinstance(prepared_capacity, dict):
            prepared_capacity = {}
        if (
            pair_capacity_cpus is None
            and prepared_capacity.get("cpus_source") == "override"
        ):
            pair_capacity_cpus = prepared_capacity.get("cpus")
        if (
            pair_capacity_memory_mb is None
            and prepared_capacity.get("memory_mb_source") == "override"
        ):
            pair_capacity_memory_mb = prepared_capacity.get("memory_mb")
        plan = _resource_aware_pair_plan(
            runnable_tasks,
            capacity=_docker_pair_capacity(
                cpus_override=pair_capacity_cpus,
                memory_mb_override=pair_capacity_memory_mb,
            ),
            max_concurrency=max_workers,
        )
    else:
        plan = _legacy_pair_plan(runnable_tasks, max_concurrency=max_workers)

    admission_rejections = {
        str(decision["task_name"]): decision
        for decision in plan["decisions"]
        if not bool(decision.get("admitted"))
    }
    for name, decision in admission_rejections.items():
        detail = str(
            decision.get("detail")
            or f"{name}: resource admission rejected ({decision.get('reason')})"
        )
        _persist_task_controller_failure(
            output=output,
            task=task_by_name[name],
            manifest=manifest,
            exc=RuntimeError(detail),
            credential_env=args.credential_env,
            arms=active_arms,
        )
        print(f"warning: skipping {name}: {detail}", file=sys.stderr)

    priority_by_name = {
        str(task["name"]): task.get("priority") for task in manifest["tasks"]
    }
    for name, error in image_errors.items():
        plan["decisions"].append(
            {
                "task_name": name,
                "priority": priority_by_name.get(name),
                "wave": None,
                "admitted": False,
                "exclusive": False,
                "reason": "required_image_unavailable",
                "detail": error,
            }
        )
    plan["decisions"].sort(
        key=lambda decision: _task_admission_sort_key(
            {
                "priority": decision.get("priority"),
                "name": decision.get("task_name", ""),
            }
        )
    )
    plan["task_count"] = len(manifest["tasks"])
    plan["runnable_task_count"] = sum(
        bool(decision.get("admitted"))
        for decision in plan["decisions"]
    )
    skipped_task_names = sorted(
        set(image_errors) | set(admission_rejections)
    )
    plan["skipped_task_count"] = len(skipped_task_names)
    plan["skipped_task_names"] = skipped_task_names
    plan["status"] = "planned"
    _persist_pair_admission(output, manifest, plan)
    _write_run_progress(output, manifest)
    # Reap containers orphaned by a previous interrupted invocation of this
    # output dir before launching anything new (they hold memory and would
    # otherwise idle forever).
    swept_orphans = _sweep_orphaned_trial_containers(jobs_dir)
    if swept_orphans:
        plan["orphan_containers_swept"] = swept_orphans
        _persist_pair_admission(output, manifest, plan)
    batch_started = time.perf_counter()
    batch_timeline: dict[str, Any] = {
        "schema_version": "lhos-lhtb-batch-timeline.v1",
        "arm_mode": arm_mode,
        "selected_arms": list(active_arms),
        "started_at": datetime.now().astimezone().isoformat(),
        "status": "running",
        "waves": [],
    }
    _write_json(output / "batch-timeline.json", batch_timeline)

    if resource_aware_pairs:
        # Work-conserving execution: refill freed capacity the moment a pair
        # finishes instead of holding a whole static wave behind its slowest
        # member (the legacy dynamic pool already had this property; the
        # resource-aware path now keeps it too).  The admission plan above
        # stays the capacity proof of record; the scheduler re-checks
        # cpu/memory/concurrency on every refill, so no instantaneous
        # combination of running pairs ever exceeds capacity.  Queue order is
        # shortest-pair-first by configured worker timeout, then admission
        # order -- arm alternation via priority parity is unaffected.
        admitted_task_names = {
            str(decision["task_name"])
            for decision in plan["decisions"]
            if bool(decision.get("admitted"))
        }
        admitted_tasks = sorted(
            (
                task_by_name[name]
                for name in admitted_task_names
                if name in task_by_name
            ),
            key=lambda task: (
                _task_worker_timeout_seconds(task, manifest),
                _task_admission_sort_key(task),
            ),
        )
        plan["execution_policy"] = "work_conserving_backfill"
        if admitted_tasks:
            started = time.perf_counter()
            timeline_wave = {
                "wave": 1,
                "task_names": [str(task["name"]) for task in admitted_tasks],
                "max_workers": max_workers,
                "execution": "work_conserving_backfill",
                "started_at": datetime.now().astimezone().isoformat(),
                "status": "running",
            }
            batch_timeline["waves"].append(timeline_wave)
            _write_json(output / "batch-timeline.json", batch_timeline)
            _persist_pair_admission(output, manifest, plan)
            _run_task_batch_work_conserving(
                tasks=admitted_tasks,
                max_workers=max_workers,
                capacity=plan["capacity"],
                manifest=manifest,
                output=output,
                jobs_dir=jobs_dir,
                credential_env=args.credential_env,
            )
            timeline_wave["elapsed_seconds"] = time.perf_counter() - started
            timeline_wave["completed_at"] = datetime.now().astimezone().isoformat()
            timeline_wave["status"] = "completed"
            _write_json(output / "batch-timeline.json", batch_timeline)
            _persist_pair_admission(output, manifest, plan)
    elif runnable_tasks:
        # Local override: shortest-job-first submission. The dynamic pool then
        # drains many short pairs early instead of holding every slot on the
        # few hour-scale apex pairs at the head of the priority list. Terminal
        # pairs are skipped in _run_task_pair, so only outstanding work is
        # reordered; per-task pairing fairness is unaffected.
        runnable_tasks = sorted(
            runnable_tasks,
            key=lambda t: (
                _task_worker_timeout_seconds(t, manifest),
                int(t.get("priority", sys.maxsize)),
                str(t.get("name", "")),
            ),
        )
        started = time.perf_counter()
        timeline_wave = {
            "wave": 1,
            "task_names": [str(task["name"]) for task in runnable_tasks],
            "max_workers": max_workers,
            "started_at": datetime.now().astimezone().isoformat(),
            "status": "running",
        }
        batch_timeline["waves"].append(timeline_wave)
        _write_json(output / "batch-timeline.json", batch_timeline)
        _run_task_batch(
            tasks=runnable_tasks,
            max_workers=max_workers,
            manifest=manifest,
            output=output,
            jobs_dir=jobs_dir,
            credential_env=args.credential_env,
        )
        timeline_wave["elapsed_seconds"] = time.perf_counter() - started
        timeline_wave["completed_at"] = datetime.now().astimezone().isoformat()
        timeline_wave["status"] = "completed"
        _write_json(output / "batch-timeline.json", batch_timeline)
    plan["status"] = "completed"
    plan["completed_at"] = datetime.now().astimezone().isoformat()
    _persist_pair_admission(output, manifest, plan)
    _write_run_progress(output, manifest)
    batch_timeline["status"] = "completed"
    batch_timeline["finished_at"] = datetime.now().astimezone().isoformat()
    batch_timeline["elapsed_seconds"] = time.perf_counter() - batch_started
    _write_json(output / "batch-timeline.json", batch_timeline)
    result = (
        summarize_output(output)
        if arm_mode == "both"
        else summarize_arm_output(output, active_arms[0])
    )
    secret = os.environ.get(args.credential_env, "")
    scan = _scan_secret((output, jobs_dir), secret)
    _write_json(output / "secret-scan.json", scan)
    if scan["exact_key_hit_count"]:
        raise RuntimeError("credential found in artifacts; see secret-scan.json")
    return result


def _delta(baseline: dict[str, Any], lhos: dict[str, Any], key: str) -> float:
    return float(baseline.get(key, 0) or 0) - float(lhos.get(key, 0) or 0)


PROFILE_METRICS = (
    "token_units",
    "uncached_input_tokens",
    "cache_read_tokens",
    "output_tokens",
    "model_calls",
    "tool_calls",
    "dsh_invocations",
    "dsh_controlled_restarts",
    "dsh_max_tokens_checkpoints",
    "agent_elapsed_ms",
    "harbor_total_elapsed_ms",
    "launcher_elapsed_ms",
    "time_to_verified_ms",
    "first_verified_token_units",
    "first_verified_model_calls",
    "first_verified_tool_calls",
)
INFRASTRUCTURE_RETRY_COST_METRICS = (
    "token_units",
    "uncached_input_tokens",
    "cache_read_tokens",
    "output_tokens",
    "model_calls",
    "tool_calls",
    "agent_elapsed_ms",
    "harbor_total_elapsed_ms",
)


def _controller_attempt_cost(record: dict[str, Any]) -> dict[str, Any]:
    ledgers = record.get("controller_attempt_ledger", ())
    rows = [item for item in ledgers if isinstance(item, dict)]
    totals: dict[str, float] = {}
    for key in INFRASTRUCTURE_RETRY_COST_METRICS:
        totals[key] = round(
            sum(
                float((item.get("partial_metrics") or {}).get(key, 0) or 0)
                for item in rows
            ),
            3,
        )
    return {
        "interrupted_attempt_count": len(rows),
        "metrics": totals,
        "ledger_paths": [
            str(item.get("ledger_path"))
            for item in rows
            if item.get("ledger_path")
        ],
    }


def _aggregate_controller_attempt_cost(
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    arms: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        costs = [
            pair.get(arm, {}).get("controller_attempt_cost", {})
            for pair in pairs
        ]
        arms[arm] = {
            "interrupted_attempt_count": sum(
                int(cost.get("interrupted_attempt_count", 0) or 0)
                for cost in costs
            ),
            "metrics": {
                key: round(
                    sum(
                        float((cost.get("metrics") or {}).get(key, 0) or 0)
                        for cost in costs
                    ),
                    3,
                )
                for key in INFRASTRUCTURE_RETRY_COST_METRICS
            },
            "ledger_paths": [
                str(path)
                for cost in costs
                for path in cost.get("ledger_paths", ())
                if path
            ],
        }
    return {
        "excluded_from_mechanism_profiling": True,
        "arms": arms,
        "combined": {
            "interrupted_attempt_count": sum(
                int(arms[arm]["interrupted_attempt_count"])
                for arm in ARMS
            ),
            "metrics": {
                key: round(
                    sum(
                        float(arms[arm]["metrics"][key])
                        for arm in ARMS
                    ),
                    3,
                )
                for key in INFRASTRUCTURE_RETRY_COST_METRICS
            },
        },
    }


def _aggregate_profiling(
    pairs: list[dict[str, Any]],
    *,
    eligibility_key: str = "mechanism_eligible",
) -> dict[str, Any]:
    eligible = [
        pair
        for pair in pairs
        if bool(
            pair.get(
                eligibility_key,
                pair.get("comparison_eligible", False),
            )
        )
    ]
    metrics: dict[str, dict[str, Any]] = {}
    for key in PROFILE_METRICS:
        comparable = [
            pair
            for pair in eligible
            if pair["dsh_fresh"].get(key) is not None and pair["lhos_resume"].get(key) is not None
        ]
        fresh_total = sum(float(pair["dsh_fresh"][key]) for pair in comparable)
        lhos_total = sum(float(pair["lhos_resume"][key]) for pair in comparable)
        delta = fresh_total - lhos_total
        metrics[key] = {
            "pair_count": len(comparable),
            "fresh_total": round(fresh_total, 3),
            "lhos_total": round(lhos_total, 3),
            "fresh_minus_lhos": round(delta, 3),
            "saving_percent": (round(delta / fresh_total * 100.0, 3) if fresh_total else None),
            "lhos_wins": sum(
                float(pair["lhos_resume"][key]) < float(pair["dsh_fresh"][key])
                for pair in comparable
            ),
            "fresh_wins": sum(
                float(pair["dsh_fresh"][key]) < float(pair["lhos_resume"][key])
                for pair in comparable
            ),
            "ties": sum(
                float(pair["dsh_fresh"][key]) == float(pair["lhos_resume"][key])
                for pair in comparable
            ),
        }
    reward_pairs = [
        pair
        for pair in eligible
        if pair["dsh_fresh"].get("reward") is not None
        and pair["lhos_resume"].get("reward") is not None
    ]
    verified_pairs = [
        pair
        for pair in eligible
        if pair["dsh_fresh"].get("time_to_verified_ms") is not None
        and pair["lhos_resume"].get("time_to_verified_ms") is not None
    ]
    return {
        "schema_version": "lhos-lhtb-dsh-profiling.v1",
        "eligibility_key": eligibility_key,
        "eligible_pair_count": len(eligible),
        "eligible_task_names": [pair["task_name"] for pair in eligible],
        # Historical aliases retain their mechanism-subgroup meaning.
        "comparison_eligible_pair_count": len(eligible),
        "comparison_eligible_task_names": [pair["task_name"] for pair in eligible],
        "reward_pair_count": len(reward_pairs),
        "same_reward_pair_count": sum(
            abs(float(pair["dsh_fresh"]["reward"]) - float(pair["lhos_resume"]["reward"])) <= 1e-12
            for pair in reward_pairs
        ),
        "time_to_verified_pair_count": len(verified_pairs),
        "fresh_verified_before_agent_budget": sum(
            bool(pair["dsh_fresh"].get("verified_before_agent_budget"))
            for pair in eligible
        ),
        "lhos_verified_before_agent_budget": sum(
            bool(pair["lhos_resume"].get("verified_before_agent_budget"))
            for pair in eligible
        ),
        "both_resolved": sum(
            bool(pair["dsh_fresh"].get("resolved")) and bool(pair["lhos_resume"].get("resolved"))
            for pair in eligible
        ),
        "fresh_only_resolved": sum(
            bool(pair["dsh_fresh"].get("resolved"))
            and not bool(pair["lhos_resume"].get("resolved"))
            for pair in eligible
        ),
        "lhos_only_resolved": sum(
            not bool(pair["dsh_fresh"].get("resolved"))
            and bool(pair["lhos_resume"].get("resolved"))
            for pair in eligible
        ),
        "neither_resolved": sum(
            not bool(pair["dsh_fresh"].get("resolved"))
            and not bool(pair["lhos_resume"].get("resolved"))
            for pair in eligible
        ),
        "metrics": metrics,
    }


def _budget_seconds_for_pair(pair: dict[str, Any]) -> int | None:
    """Return the configured per-arm budget used for timeout stratification."""

    value = pair.get("configured_agent_timeout_seconds")
    if value is None:
        value = pair.get("official_agent_timeout_seconds")
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    return int(numeric) if numeric.is_integer() else round(numeric)


def _finite_reward(metrics: dict[str, Any]) -> float:
    """Use official-style zero imputation for an assigned arm outcome."""

    value = metrics.get("reward")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    numeric = float(value)
    return numeric if math.isfinite(numeric) else 0.0


def _budget_tier_profiling(
    pairs: list[dict[str, Any]],
    *,
    solved_threshold: float = DEFAULT_VERIFIED_REWARD_THRESHOLD,
) -> dict[str, Any]:
    """Report paired outcome contrasts separately for each budget condition.

    The primary tier contrast is assignment-level ITT: every task pair stays in
    its declared budget stratum and missing/provider/error outcomes are scored
    as zero.  An observed-only view and a continuation/mechanism view are
    included as secondary diagnostics, so post-treatment resume gates cannot
    silently become the headline denominator.
    """

    grouped: dict[int | str, list[dict[str, Any]]] = {}
    for pair in pairs:
        budget = _budget_seconds_for_pair(pair)
        key: int | str = budget if budget is not None else "unknown"
        grouped.setdefault(key, []).append(pair)

    def numeric_mean(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 12) if values else None

    tiers: list[dict[str, Any]] = []
    for tier in sorted(grouped, key=lambda value: (value == "unknown", value)):
        cohort = grouped[tier]
        fresh_itt = [_finite_reward(pair.get("dsh_fresh", {})) for pair in cohort]
        lhos_itt = [_finite_reward(pair.get("lhos_resume", {})) for pair in cohort]
        pair_deltas = [
            lhos - fresh for fresh, lhos in zip(fresh_itt, lhos_itt, strict=True)
        ]
        observed = [
            pair
            for pair in cohort
            if isinstance(pair.get("dsh_fresh", {}).get("reward"), (int, float))
            and not isinstance(pair.get("dsh_fresh", {}).get("reward"), bool)
            and isinstance(pair.get("lhos_resume", {}).get("reward"), (int, float))
            and not isinstance(pair.get("lhos_resume", {}).get("reward"), bool)
        ]
        continuation = [
            pair for pair in cohort if pair.get("continuation_boundary_mode") != "one_shot"
        ]
        mechanism = [pair for pair in continuation if pair.get("mechanism_eligible")]
        observed_fresh = [_finite_reward(pair.get("dsh_fresh", {})) for pair in observed]
        observed_lhos = [_finite_reward(pair.get("lhos_resume", {})) for pair in observed]
        observed_deltas = [
            lhos - fresh
            for fresh, lhos in zip(observed_fresh, observed_lhos, strict=True)
        ]

        def outcome_counts(
            fresh_values: list[float],
            lhos_values: list[float],
        ) -> dict[str, int]:
            return {
                "lhos_wins": sum(
                    lhos > fresh
                    for fresh, lhos in zip(fresh_values, lhos_values, strict=True)
                ),
                "fresh_wins": sum(
                    fresh > lhos
                    for fresh, lhos in zip(fresh_values, lhos_values, strict=True)
                ),
                "ties": sum(
                    fresh == lhos
                    for fresh, lhos in zip(fresh_values, lhos_values, strict=True)
                ),
            }

        tier_result: dict[str, Any] = {
            "budget_seconds": tier if isinstance(tier, int) else None,
            "budget_label": str(tier),
            "pair_count": len(cohort),
            "complete_pair_count": sum(bool(pair.get("complete")) for pair in cohort),
            "continuation_pair_count": len(continuation),
            "one_shot_pair_count": len(cohort) - len(continuation),
            "result_eligible_pair_count": sum(
                bool(pair.get("result_eligible")) for pair in cohort
            ),
            "mechanism_eligible_pair_count": len(mechanism),
            "outcome_itt": {
                "imputation": "missing_or_invalid_reward=0.0",
                "fresh_mean_reward": numeric_mean(fresh_itt),
                "lhos_mean_reward": numeric_mean(lhos_itt),
                "lhos_minus_fresh_mean_reward": numeric_mean(pair_deltas),
                "fresh_resolved_count": sum(value >= solved_threshold for value in fresh_itt),
                "lhos_resolved_count": sum(value >= solved_threshold for value in lhos_itt),
                "fresh_resolved_rate": round(
                    sum(value >= solved_threshold for value in fresh_itt) / len(cohort),
                    12,
                )
                if cohort
                else None,
                "lhos_resolved_rate": round(
                    sum(value >= solved_threshold for value in lhos_itt) / len(cohort),
                    12,
                )
                if cohort
                else None,
                **outcome_counts(fresh_itt, lhos_itt),
            },
            "observed_reward_pairs": {
                "pair_count": len(observed),
                "fresh_mean_reward": numeric_mean(observed_fresh),
                "lhos_mean_reward": numeric_mean(observed_lhos),
                "lhos_minus_fresh_mean_reward": numeric_mean(observed_deltas),
                **outcome_counts(observed_fresh, observed_lhos),
            },
            "mechanism_cohort": {
                "pair_count": len(mechanism),
                "task_names": [pair.get("task_name") for pair in mechanism],
                "full_lhos_policy_effect_note": (
                    "descriptive post-treatment cohort; not the primary ITT denominator"
                ),
            },
        }
        tiers.append(tier_result)
    return {
        "schema_version": "lhos-lhtb-budget-tier-profiling.v1",
        "primary_estimand": (
            "task-level paired ITT reward contrast (LHOS minus fresh) within each "
            "configured agent-timeout stratum"
        ),
        "solved_threshold": solved_threshold,
        "tiers": tiers,
    }


def _official_leaderboard_score(
    reward_rows: list[tuple[str, Any]],
    *,
    expected_task_count: int = OFFICIAL_LEADERBOARD_TASK_COUNT,
    solved_threshold: float = OFFICIAL_LEADERBOARD_SOLVED_THRESHOLD,
    expected_task_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Compute the public metrics only when the complete expected suite is present."""

    task_names = [str(name) for name, _reward_value in reward_rows]
    valid: list[tuple[str, float]] = []
    scored: list[tuple[str, float]] = []
    missing_or_invalid: list[str] = []
    for name, reward_value in reward_rows:
        if isinstance(reward_value, bool) or not isinstance(reward_value, (int, float)):
            missing_or_invalid.append(str(name))
            scored.append((str(name), 0.0))
            continue
        reward = float(reward_value)
        if not math.isfinite(reward):
            missing_or_invalid.append(str(name))
            scored.append((str(name), 0.0))
            continue
        valid.append((str(name), reward))
        scored.append((str(name), reward))
    expected_names = (
        {str(name) for name in expected_task_names}
        if expected_task_names is not None
        else None
    )
    complete_task_set = bool(
        len(reward_rows) == expected_task_count
        and len(set(task_names)) == expected_task_count
        and (
            expected_names is None
            or (len(expected_names) == expected_task_count and set(task_names) == expected_names)
        )
    )
    observed_mean = (
        sum(reward for _name, reward in valid) / len(valid) if valid else None
    )
    observed_solved = sum(reward >= solved_threshold for _name, reward in valid)
    contract_mean = (
        sum(reward for _name, reward in scored) / expected_task_count
        if complete_task_set and expected_task_count > 0
        else None
    )
    contract_solved = sum(reward >= solved_threshold for _name, reward in scored)
    return {
        "schema_version": "lhtb-official-leaderboard-score.v1",
        "official_score": False,
        "claim_status": "public_metric_projection_only",
        "ranking_eligible": False,
        "complete": complete_task_set,
        "expected_task_count": expected_task_count,
        "observed_task_count": len(reward_rows),
        "reward_count": len(valid),
        "error_reward_count": len(missing_or_invalid),
        "error_reward_value": 0.0,
        "missing_or_invalid_reward_task_names": missing_or_invalid,
        "mean_reward": round(contract_mean, 12) if contract_mean is not None else None,
        "solved_count": contract_solved if complete_task_set else None,
        "solved_reward_threshold": solved_threshold,
        "observed_mean_reward": (
            round(observed_mean, 12) if observed_mean is not None else None
        ),
        "observed_solved_count": observed_solved,
    }


def _refresh_arm_metrics(
    record: dict[str, Any],
    arm: str,
    *,
    controlled_pair_required: bool = False,
) -> dict[str, Any]:
    """Reparse durable Harbor output so summarize uses the current gates."""

    persisted = record.get("metrics", {})
    if record.get("status") != "completed" or not isinstance(persisted, dict):
        return persisted if isinstance(persisted, dict) else {}
    job_result = persisted.get("job_result")
    if not job_result:
        return persisted
    job_result_path = Path(str(job_result))
    if not job_result_path.is_file():
        return persisted
    try:
        if controlled_pair_required:
            return _trial_metrics(
                job_result_path.parent,
                arm,
                controlled_pair_required=True,
            )
        return _trial_metrics(job_result_path.parent, arm)
    except Exception as exc:
        return {
            **persisted,
            "parse_valid": False,
            "result_eligible": False,
            "mechanism_eligible": False,
            "comparison_eligible": False,
            "parser_error": f"metric_reparse_failed:{type(exc).__name__}",
        }


def _annotate_time_to_verified(
    metrics: dict[str, Any],
    task: dict[str, Any],
) -> dict[str, Any]:
    """Add task-budget context to parsed verification timing."""

    annotated = dict(metrics)
    configured = task.get("configured_agent_timeout_seconds")
    if configured is None:
        configured = task.get("official_agent_timeout_seconds")
    try:
        budget_ms = float(configured) * 1000.0 if configured is not None else None
    except (TypeError, ValueError):
        budget_ms = None
    elapsed = annotated.get("agent_elapsed_ms")
    early = (
        bool(annotated.get("verified"))
        and budget_ms is not None
        and elapsed is not None
        and float(elapsed) < budget_ms
    )
    observed_budget_exhausted = bool(
        not annotated.get("verified")
        and budget_ms is not None
        and elapsed is not None
        # Harbor timestamp precision and cleanup can place the measured agent
        # span a few milliseconds either side of the configured ceiling.
        and float(elapsed) >= budget_ms * 0.99
    )
    if (
        observed_budget_exhausted
        and annotated.get("verification_status") == "not_verified"
    ):
        annotated["verification_status"] = "timeout"
    annotated["budget_exhausted"] = bool(
        annotated.get("budget_exhausted") or observed_budget_exhausted
    )
    annotated["time_to_verified_censoring"] = (
        "right_censored_at_agent_budget"
        if observed_budget_exhausted
        else None
    )
    annotated["time_to_verified_budget_ms"] = budget_ms
    annotated["verified_before_agent_budget"] = early if annotated.get("verified") else None
    annotated["time_to_verified_source"] = (
        annotated.get("time_to_verified_source")
        or annotated.get("verification_observation")
        if annotated.get("verified")
        else None
    )
    return annotated


def summarize_arm_output(
    output: Path,
    arm: str,
) -> dict[str, Any]:
    """Summarize one isolated arm without inventing a missing counterpart."""

    manifest = _load_manifest(output)
    normalized_arm = str(arm)
    if normalized_arm not in ARMS:
        raise RuntimeError(f"invalid arm for arm summary: {arm!r}")
    active_arms = _manifest_run_arms(manifest)
    if active_arms != (normalized_arm,):
        raise RuntimeError(
            f"arm summary requires a single selected arm, got {active_arms!r}"
        )
    _validate_controlled_pair_manifest(manifest)
    controlled_pair_required = _controlled_pair_enabled(manifest)
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        _validate_official_leaderboard_contract(
            task_names=[str(task["name"]) for task in manifest.get("tasks", ())],
            configs=manifest.get("configs") or {},
        )
        _validate_official_manifest_contract(manifest)
    progress = _write_run_progress(output, manifest)
    timeline = _load_json(output / "batch-timeline.json")
    rows: list[dict[str, Any]] = []
    totals = {
        key: 0.0
        for key in PROFILE_METRICS
    }
    completed = failed = pending = invalid = 0
    eligible = mechanism_eligible = 0
    for task in manifest.get("tasks", ()):
        name = str(task["name"])
        path = output / "runs" / name / f"{normalized_arm}.json"
        if not path.is_file():
            record, parser_error = {}, None
        else:
            record, parser_error = _load_arm_record(path)
        if not path.is_file():
            status = "pending"
            pending += 1
            metrics: dict[str, Any] = {}
        elif parser_error is not None:
            status = "invalid"
            invalid += 1
            metrics: dict[str, Any] = {}
        elif not record:
            status = "pending"
            pending += 1
            metrics = {}
        else:
            status = str(record.get("status", "unknown") or "unknown")
            if status == "completed":
                completed += 1
            elif status == "failed":
                failed += 1
            else:
                pending += 1
            metrics = _annotate_time_to_verified(
                _refresh_arm_metrics(
                    record,
                    normalized_arm,
                    controlled_pair_required=controlled_pair_required,
                ),
                task,
            )
            if metrics.get("launcher_elapsed_ms") is None:
                metrics["launcher_elapsed_ms"] = record.get("elapsed_ms")
            for key in PROFILE_METRICS:
                value = metrics.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    totals[key] += float(value)
            eligible += int(bool(metrics.get("result_eligible")))
            mechanism_eligible += int(bool(metrics.get("mechanism_eligible")))
        rows.append(
            {
                "task_name": name,
                "priority": task.get("priority"),
                "status": status,
                "record": str(path) if path.is_file() else None,
                "parser_error": parser_error,
                "execution_error": (
                    record.get("execution_error")
                    or metrics.get("execution_error")
                ),
                "config_parity_sha256": record.get(
                    "config_parity_sha256",
                    (manifest.get("configs", {}).get(name, {}) or {}).get(
                        "parity_sha256"
                    ),
                ),
                "docker_image_id": record.get("docker_image_id"),
                "continuation_boundary_mode": task.get(
                    "continuation_boundary_mode"
                ),
                "paired_verifier_seed_sha256": task.get(
                    "paired_verifier_seed_sha256"
                ),
                "metrics": metrics,
            }
        )
    result = {
        "schema_version": "lhos-lhtb-arm-result.v1",
        "official_score": False,
        "claim_status": manifest.get(
            "claim_status", "custom_harness_controlled_experiment"
        ),
        "ranking_eligible": False,
        "estimand": manifest.get("estimand"),
        "budget_condition": manifest.get("budget_condition"),
        "controlled_pair_experiment": manifest.get("controlled_pair_experiment"),
        "arm": normalized_arm,
        "arm_mode": _normalize_arm_mode(manifest.get("run_arm", "both")),
        "selected_arms": list(active_arms),
        "benchmark": manifest.get("benchmark"),
        "task_content": manifest.get("task_content"),
        "lhtb_source_commit": manifest.get("lhtb_source_commit"),
        "model": manifest.get("model"),
        "reasoning_effort": manifest.get("reasoning_effort"),
        "agent_timeout_seconds": manifest.get("agent_timeout_seconds"),
        "agent_timeout_mode": manifest.get("agent_timeout_mode"),
        "time_slice_seconds": manifest.get("time_slice_seconds"),
        "time_slice_mode": manifest.get("time_slice_mode"),
        "runtime": manifest.get("runtime"),
        "task_count": len(rows),
        "completed_task_count": completed,
        "failed_task_count": failed,
        "pending_task_count": pending,
        "invalid_task_count": invalid,
        "result_eligible_task_count": eligible,
        "mechanism_eligible_task_count": mechanism_eligible,
        "progress": progress,
        "batch_timeline": timeline,
        "totals": {
            key: round(value, 3)
            for key, value in totals.items()
        },
        "tasks": rows,
        "attribution": (
            "This is an isolated single-arm batch. The absent counterpart is "
            "not a failure and must be supplied by merge-arms."
        ),
    }
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        result["official_leaderboard_contract"] = {
            **official_contract,
            "score": _official_leaderboard_score(
                [
                    (str(row["task_name"]), (row.get("metrics") or {}).get("reward"))
                    for row in rows
                ],
                expected_task_names=[str(task["name"]) for task in manifest["tasks"]],
            ),
        }
    _write_json(output / "arm-result.json", result)
    _write_json(output / "result.json", result)
    _write_json(output / "batch-timeline.json", timeline)
    lines = [
        f"# LHTB arm batch: {normalized_arm}",
        "",
        f"- Tasks: {len(rows)}",
        f"- Completed: {completed}",
        f"- Failed: {failed}",
        f"- Pending: {pending}",
        f"- Result eligible: {eligible}",
        f"- Mechanism eligible: {mechanism_eligible}",
        "",
        "The missing counterpart arm is intentionally not classified as failed.",
    ]
    contract_score = (result.get("official_leaderboard_contract") or {}).get("score")
    if isinstance(contract_score, dict):
        lines.extend(
            [
                "",
                "Public metric projection only (custom/non-comparable):",
                (
                    f"mean_reward={_fmt(contract_score.get('mean_reward'))}, "
                    f"solved@0.95={_fmt(contract_score.get('solved_count'))}, "
                    f"errors={_fmt(contract_score.get('error_reward_count'))}"
                ),
            ]
        )
    (output / "ARM-RESULTS.zh-CN.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    return result


def _merge_identity_fields(manifest: dict[str, Any]) -> dict[str, Any]:
    harbor = manifest.get("harbor")
    if isinstance(harbor, dict):
        # Isolated arm batches intentionally use different Compose project
        # directories. Compare the pinned Harbor identity, not that path.
        harbor = {
            key: value
            for key, value in harbor.items()
            if key not in {"project", "path", "worktree"}
        }
    identity = {
        key: manifest.get(key)
        for key in (
            "benchmark",
            "task_content",
            "lhtb_source_commit",
            "model",
            "reasoning_effort",
            "agent_timeout_seconds",
            "agent_timeout_mode",
            "time_to_verified_mode",
            "time_slice_seconds",
            "time_slice_mode",
            "time_slice_policy",
            "runtime",
            "agent",
            "claim_status",
            "ranking_eligible",
            "estimand",
            "harbor",
            "harbor_prebuilt_pull_policy",
            "local_pilot_manifest_sha256",
            "n_attempts",
            "timeout_multiplier",
            "environment_delete",
            "controlled_pair_experiment",
        )
    }
    identity["harbor"] = harbor
    return identity


def _arm_rows_by_name(result: dict[str, Any], arm: str) -> dict[str, dict[str, Any]]:
    if result.get("arm") != arm:
        raise RuntimeError(
            f"arm result identity mismatch: expected {arm!r}, got {result.get('arm')!r}"
        )
    rows = result.get("tasks")
    if not isinstance(rows, list):
        raise RuntimeError(f"{arm} arm result has no task rows")
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("task_name"):
            raise RuntimeError(f"{arm} arm result contains an invalid task row")
        name = str(row["task_name"])
        if name in output:
            raise RuntimeError(f"{arm} arm result contains duplicate task {name!r}")
        output[name] = row
    return output


def merge_arm_outputs(
    *,
    fresh_output: Path,
    lhos_output: Path,
    output: Path,
) -> dict[str, Any]:
    """Strictly merge two isolated arm batches into one paired result."""

    fresh_output = fresh_output.resolve()
    lhos_output = lhos_output.resolve()
    output = output.resolve()
    if fresh_output == lhos_output:
        raise RuntimeError("fresh and LHOS outputs must be different directories")
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"merge output already exists and is not empty: {output}")

    fresh_manifest = _load_manifest(fresh_output)
    lhos_manifest = _load_manifest(lhos_output)
    _validate_controlled_pair_manifest(fresh_manifest)
    _validate_controlled_pair_manifest(lhos_manifest)
    fresh_result = _load_json(fresh_output / "arm-result.json")
    lhos_result = _load_json(lhos_output / "arm-result.json")
    if not fresh_result or not lhos_result:
        raise RuntimeError(
            "both arm outputs must contain arm-result.json; run arm-only batches first"
        )
    for arm_result, manifest, expected_arm in (
        (fresh_result, fresh_manifest, "dsh_fresh"),
        (lhos_result, lhos_manifest, "lhos_resume"),
    ):
        for field in (
            "benchmark",
            "task_content",
            "lhtb_source_commit",
            "model",
            "reasoning_effort",
            "agent_timeout_seconds",
            "agent_timeout_mode",
            "time_slice_seconds",
            "time_slice_mode",
        ):
            if arm_result.get(field) != manifest.get(field):
                raise RuntimeError(
                    f"{expected_arm}: arm result/manifest mismatch for {field}"
                )
    _arm_rows_by_name(fresh_result, "dsh_fresh")
    _arm_rows_by_name(lhos_result, "lhos_resume")
    if _manifest_run_arms(fresh_manifest) != ("dsh_fresh",):
        raise RuntimeError("fresh output manifest is not arm=fresh")
    if _manifest_run_arms(lhos_manifest) != ("lhos_resume",):
        raise RuntimeError("LHOS output manifest is not arm=lhos")

    if _merge_identity_fields(fresh_manifest) != _merge_identity_fields(lhos_manifest):
        raise RuntimeError("fresh/LHOS manifest model, budget, boundary, or runtime mismatch")

    fresh_materialization = fresh_manifest.get("materialization")
    lhos_materialization = lhos_manifest.get("materialization")
    if isinstance(fresh_materialization, dict) or isinstance(lhos_materialization, dict):
        if not (
            isinstance(fresh_materialization, dict)
            and isinstance(lhos_materialization, dict)
        ):
            raise RuntimeError("fresh/LHOS materialization provenance is incomplete")
        for field in ("source_manifest_sha256", "common_identity_sha256", "task_names"):
            if fresh_materialization.get(field) != lhos_materialization.get(field):
                raise RuntimeError(
                    f"fresh/LHOS materialization provenance mismatch for {field}"
                )

    fresh_tasks = {
        str(task["name"]): task for task in fresh_manifest.get("tasks", ())
    }
    lhos_tasks = {
        str(task["name"]): task for task in lhos_manifest.get("tasks", ())
    }
    if list(fresh_tasks) != list(lhos_tasks):
        raise RuntimeError("fresh/LHOS task ordering or task set mismatch")
    fresh_rows = _arm_rows_by_name(fresh_result, "dsh_fresh")
    lhos_rows = _arm_rows_by_name(lhos_result, "lhos_resume")
    if set(fresh_rows) != set(lhos_rows) or set(fresh_rows) != set(fresh_tasks):
        raise RuntimeError("fresh/LHOS arm result task sets do not match manifest")

    validation_rows: list[dict[str, Any]] = []
    for name, fresh_task in fresh_tasks.items():
        lhos_task = lhos_tasks[name]
        task_fields = (
            "task_toml_sha256",
            "task_content_sha256",
            "docker_image",
            "official_agent_timeout_seconds",
            "configured_agent_timeout_seconds",
            "configured_time_slice_seconds",
            "time_slice_policy",
            "continuation_boundary_mode",
            "paired_verifier_seed_sha256",
            "stochastic_verifier",
            "stochastic_pair_controlled",
        )
        for field in task_fields:
            if fresh_task.get(field) != lhos_task.get(field):
                raise RuntimeError(f"{name}: task field mismatch for {field}")
        fresh_row = fresh_rows[name]
        lhos_row = lhos_rows[name]
        parity_values = (
            fresh_row.get("config_parity_sha256"),
            lhos_row.get("config_parity_sha256"),
        )
        if not parity_values[0] or parity_values[0] != parity_values[1]:
            raise RuntimeError(f"{name}: config parity mismatch between arms")
        image_values = (
            fresh_row.get("docker_image_id"),
            lhos_row.get("docker_image_id"),
        )
        if not image_values[0] or image_values[0] != image_values[1]:
            raise RuntimeError(f"{name}: Docker image ID mismatch between arms")
        seed_values = (
            fresh_task.get("paired_verifier_seed_sha256"),
            lhos_task.get("paired_verifier_seed_sha256"),
        )
        if seed_values[0] != seed_values[1]:
            raise RuntimeError(f"{name}: verifier seed hash mismatch between arms")
        if (
            fresh_task.get("stochastic_verifier")
            and fresh_task.get("stochastic_pair_controlled")
            and not seed_values[0]
        ):
            raise RuntimeError(
                f"{name}: controlled stochastic verifier is missing a paired seed hash"
            )
        validation_rows.append(
            {
                "task_name": name,
                "config_parity_sha256": parity_values[0],
                "docker_image_id": image_values[0],
                "paired_verifier_seed_sha256": seed_values[0],
                "continuation_boundary_mode": fresh_task.get(
                    "continuation_boundary_mode"
                ),
                "official_agent_timeout_seconds": fresh_task.get(
                    "official_agent_timeout_seconds"
                ),
            }
        )

    output.mkdir(parents=True, exist_ok=True)
    merged_manifest = copy.deepcopy(fresh_manifest)
    _set_manifest_run_arm(merged_manifest, "both")
    merged_design = merged_manifest.get("controlled_pair_experiment")
    if isinstance(merged_design, dict):
        merged_design["observed_execution_layout"] = "separate_arm_batches"
        merged_design["execution_layout"] = "separate_arm_batches"
        merged_design["validity"] = "partial"
        merged_design["execution_layout_note"] = (
            "The two arm-only batches were executed and merged separately; "
            "same-batch interleaving was not observed."
        )
    merged_manifest["arm_batch_merge"] = {
        "schema_version": "lhos-lhtb-arm-merge.v1",
        "fresh_output": str(fresh_output),
        "lhos_output": str(lhos_output),
        "execution_layout_observed": "separate_arm_batches",
        "validation": validation_rows,
    }
    _write_json(output / "manifest.json", merged_manifest)
    pair_admission = _load_json(fresh_output / "pair-admission.json")
    if pair_admission:
        _write_json(output / "pair-admission.json", pair_admission)
    timeline = {
        "schema_version": "lhos-lhtb-merged-batch-timeline.v1",
        "status": "merged",
        "fresh": _load_json(fresh_output / "batch-timeline.json"),
        "lhos": _load_json(lhos_output / "batch-timeline.json"),
        "merged_at": datetime.now().astimezone().isoformat(),
    }
    _write_json(output / "batch-timeline.json", timeline)
    for arm, source_root in (
        ("dsh_fresh", fresh_output),
        ("lhos_resume", lhos_output),
    ):
        for name in fresh_tasks:
            source = source_root / "runs" / name / f"{arm}.json"
            if not source.is_file():
                raise RuntimeError(f"{name}/{arm}: arm record is missing")
            destination = output / "runs" / name / f"{arm}.json"
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(source.read_bytes())

    result = summarize_output(output)
    result["arm_batch_merge"] = merged_manifest["arm_batch_merge"]
    result["batch_timeline"] = timeline
    result["merge_validation"] = validation_rows
    _write_json(output / "result.json", result)
    (output / "RESULTS.zh-CN.md").write_text(
        _render_summary_v2(result),
        encoding="utf-8",
    )
    return result


def materialize_arm_outputs(
    *,
    prepared_output: Path,
    fresh_output: Path,
    lhos_output: Path,
    task_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Create two clean arm-specific outputs from one prepared manifest."""

    source = prepared_output.resolve()
    targets = {
        "fresh": fresh_output.resolve(),
        "lhos": lhos_output.resolve(),
    }
    if targets["fresh"] == targets["lhos"]:
        raise RuntimeError("fresh and LHOS targets must be different directories")
    for parent_name, child_name in (("fresh", "lhos"), ("lhos", "fresh")):
        try:
            targets[child_name].relative_to(targets[parent_name])
        except ValueError:
            continue
        raise RuntimeError(
            "fresh and LHOS targets must not contain one another"
        )
    if not source.is_dir():
        raise RuntimeError(f"prepared output does not exist: {source}")
    source_manifest = _load_manifest(source)
    _validate_controlled_pair_manifest(source_manifest)
    if _manifest_run_arms(source_manifest) != ARMS:
        raise RuntimeError("prepared output must be a both-arm manifest")
    _runtime_pins_match(source_manifest)
    source_manifest_path = source / "manifest.json"
    source_manifest_sha256 = _sha256_file(source_manifest_path)
    for target in targets.values():
        if target.exists():
            raise RuntimeError(f"materialization target already exists: {target}")
        try:
            target.relative_to(source)
        except ValueError:
            pass
        else:
            raise RuntimeError(
                f"materialization target must not be inside prepared output: {target}"
            )

    source_tasks = {
        str(task["name"]): task for task in source_manifest.get("tasks", ())
    }
    selected = _normalize_task_names(task_names)
    if not selected:
        selected = list(source_tasks)
    missing = [name for name in selected if name not in source_tasks]
    if missing:
        raise RuntimeError(f"materialization tasks are missing from prepared output: {missing}")
    selected_tasks: list[dict[str, Any]] = []
    source_priorities: dict[str, Any] = {}
    for priority, name in enumerate(selected, start=1):
        task = copy.deepcopy(source_tasks[name])
        source_priorities[name] = task.get("priority")
        task["source_priority"] = task.get("priority")
        task["priority"] = priority
        selected_tasks.append(task)

    source_configs = source_manifest.get("configs")
    if not isinstance(source_configs, dict):
        raise RuntimeError("prepared manifest has no configs mapping")
    for task in selected_tasks:
        name = str(task["name"])
        config = source_configs.get(name)
        if not isinstance(config, dict):
            raise RuntimeError(f"{name}: prepared config mapping is missing")
        parity_values: set[str] = set()
        for arm in ARMS:
            config_path = Path(str(config.get(arm, "")))
            if not config_path.is_file():
                raise RuntimeError(f"{name}/{arm}: prepared config is missing")
            payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError(f"{name}/{arm}: prepared config is invalid")
            parity = _parity_hash(payload)
            if parity != config.get("parity_sha256"):
                raise RuntimeError(f"{name}: prepared config parity hash mismatch")
            parity_values.add(parity)
        if len(parity_values) != 1:
            raise RuntimeError(f"{name}: prepared fresh/LHOS configs are not equivalent")

    source_plan = source_manifest.get("pair_admission")
    if not isinstance(source_plan, dict):
        source_plan = {}
    resource_aware = bool(
        source_plan.get("enabled", source_manifest.get("resource_aware_pairs", False))
    )
    max_concurrency = int(
        source_plan.get("max_concurrency", source_manifest.get("max_concurrency", 1))
    )
    if resource_aware:
        capacity = source_plan.get("capacity")
        if not isinstance(capacity, dict):
            raise RuntimeError("prepared resource-aware manifest has no capacity")
        pair_admission = _resource_aware_pair_plan(
            selected_tasks,
            capacity=capacity,
            max_concurrency=max_concurrency,
        )
    else:
        pair_admission = _legacy_pair_plan(
            selected_tasks,
            max_concurrency=max_concurrency,
        )
    pair_admission["materialized_from_manifest_sha256"] = source_manifest_sha256
    pair_admission["status"] = "prepared"

    source_inventory = _load_json(source / "image-inventory.json")
    selected_set = set(selected)
    if source_inventory:
        images: list[dict[str, Any]] = []
        for raw_item in source_inventory.get("images", ()):
            if not isinstance(raw_item, dict):
                continue
            selected_image_tasks = [
                str(name)
                for name in raw_item.get("task_names", ())
                if str(name) in selected_set
            ]
            if not selected_image_tasks:
                continue
            item = copy.deepcopy(raw_item)
            item["task_names"] = selected_image_tasks
            images.append(item)
        source_inventory["images"] = images
        source_inventory["image_count"] = len(images)
        source_inventory["available_image_count"] = sum(
            bool(item.get("available")) for item in images
        )
        source_inventory["missing_image_count"] = sum(
            not bool(item.get("available")) for item in images
        )
        source_inventory["available_task_names"] = [
            name for name in selected if name in source_inventory.get("available_task_names", ())
        ]
        source_inventory["missing_task_names"] = [
            name for name in selected if name in source_inventory.get("missing_task_names", ())
        ]
        source_inventory["available_task_count"] = len(
            source_inventory["available_task_names"]
        )
        source_inventory["missing_task_count"] = len(
            source_inventory["missing_task_names"]
        )
        source_inventory["available_images"] = [
            copy.deepcopy(item) for item in images if item.get("available")
        ]
        source_inventory["missing_images"] = [
            copy.deepcopy(item) for item in images if not item.get("available")
        ]

    source_prebuild = _load_json(source / "prebuild.json")
    if not source_prebuild or source_prebuild.get("complete") is not True:
        raise RuntimeError("prepared output does not contain a complete prebuild.json")
    source_prebuild["records"] = [
        item
        for item in source_prebuild.get("records", ())
        if isinstance(item, dict)
        and str(item.get("task_name", "")) in selected_set
    ]
    prebuild_by_task = {
        str(item.get("task_name")): item for item in source_prebuild["records"]
    }
    for task in selected_tasks:
        name = str(task["name"])
        record = prebuild_by_task.get(name)
        if (
            not isinstance(record, dict)
            or record.get("exit_code") != 0
            or not record.get("image_id")
        ):
            raise RuntimeError(f"{name}: complete prebuild record is missing")
        expected_image_ids = task.get("local_image_ids") or {}
        expected_image_id = expected_image_ids.get(str(record.get("image")))
        if expected_image_id and expected_image_id != record.get("image_id"):
            raise RuntimeError(f"{name}: prebuild image ID does not match manifest")
    source_prebuild["task_count"] = len(selected)
    source_prebuild["image_count"] = len(
        {str(item.get("image")) for item in source_prebuild["records"]}
    )
    source_prebuild["complete"] = (
        len(prebuild_by_task) == len(selected)
        and all(
            item.get("exit_code") == 0 and item.get("image_id")
            for item in source_prebuild["records"]
        )
    )

    common_identity = hashlib.sha256(
        json.dumps(
            {
                "manifest": _merge_identity_fields(source_manifest),
                "tasks": [
                    {
                        key: task.get(key)
                        for key in (
                            "name",
                            "task_toml_sha256",
                            "task_content_sha256",
                            "docker_image",
                            "paired_verifier_seed_sha256",
                            "continuation_boundary_mode",
                            "official_agent_timeout_seconds",
                            "configured_agent_timeout_seconds",
                            "configured_time_slice_seconds",
                        )
                    }
                    for task in selected_tasks
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    created: list[Path] = []
    try:
        for mode, target in targets.items():
            target.mkdir(parents=True, exist_ok=False)
            created.append(target)
            manifest = copy.deepcopy(source_manifest)
            for stale_key in (
                "arm_batch_merge",
                "batch_timeline",
                "run_progress",
            ):
                manifest.pop(stale_key, None)
            manifest["tasks"] = copy.deepcopy(selected_tasks)
            official_contract = manifest.get("official_leaderboard_contract")
            if (
                isinstance(official_contract, dict)
                and official_contract.get("enabled") is True
                and (
                    len(selected) != OFFICIAL_LEADERBOARD_TASK_COUNT
                    or selected_set != set(source_tasks)
                )
            ):
                manifest["official_leaderboard_contract"] = {
                    **official_contract,
                    "enabled": False,
                    "config_validated": False,
                    "materialized_subset": True,
                    "materialized_task_count": len(selected),
                    "official_score": False,
                    "official_score_reason": (
                        "A materialized subset is not the complete 46-task contract."
                    ),
                }
            manifest["selection"] = {
                **(manifest.get("selection") or {}),
                "mode": "materialized_subset",
                "requested_task_count": len(selected),
                "requested_task_names": list(selected),
                "configured_task_count": len(selected),
                "configured_task_names": list(selected),
                "excluded_materialization_task_names": [
                    name for name in source_tasks if name not in selected_set
                ],
                "source_priority_by_task": source_priorities,
            }
            manifest["configs"] = {}
            _set_manifest_run_arm(manifest, mode)
            materialized_design = manifest.get("controlled_pair_experiment")
            if isinstance(materialized_design, dict):
                materialized_design["observed_execution_layout"] = (
                    "separate_arm_batches"
                )
                materialized_design["execution_layout"] = "separate_arm_batches"
                materialized_design["validity"] = "partial"
                materialized_design["execution_layout_note"] = (
                    "This arm was materialized for a separate batch; counterpart "
                    "timing/provider state is not interleaved."
                )
            manifest["pair_admission"] = copy.deepcopy(pair_admission)
            manifest["resource_aware_pairs"] = resource_aware
            manifest["materialization"] = {
                "schema_version": "lhos-lhtb-arm-materialization.v1",
                "source_output": str(source),
                "source_manifest_sha256": source_manifest_sha256,
                "common_identity_sha256": common_identity,
                "arm_mode": mode,
                "task_names": list(selected),
            }
            config_root = target / "configs"
            config_root.mkdir(parents=True, exist_ok=True)
            for task in manifest["tasks"]:
                name = str(task["name"])
                source_config = source_configs[name]
                output_config: dict[str, Any] = {}
                parity_values: set[str] = set()
                for arm in ARMS:
                    payload = yaml.safe_load(
                        Path(str(source_config[arm])).read_text(encoding="utf-8")
                    )
                    old_job_name = str(source_config[f"{arm}_job_name"])
                    payload["job_name"] = f"{old_job_name}-materialized-{mode}"
                    destination = config_root / f"{name}.{arm}.yaml"
                    destination.write_text(
                        yaml.safe_dump(payload, sort_keys=False),
                        encoding="utf-8",
                    )
                    output_config[arm] = str(destination)
                    output_config[f"{arm}_job_name"] = payload["job_name"]
                    parity_values.add(_parity_hash(payload))
                if len(parity_values) != 1:
                    raise RuntimeError(f"{name}: materialized configs diverged")
                output_config["parity_sha256"] = parity_values.pop()
                manifest["configs"][name] = output_config
            _write_json(target / "manifest.json", manifest)
            _write_json(target / "pair-admission.json", copy.deepcopy(pair_admission))
            if source_inventory:
                _write_json(target / "image-inventory.json", copy.deepcopy(source_inventory))
            if source_prebuild:
                _write_json(target / "prebuild.json", copy.deepcopy(source_prebuild))
            missing_images = _load_json(source / "missing-local-images.json")
            if missing_images:
                missing_images["missing_task_names"] = [
                    name
                    for name in selected
                    if name in missing_images.get("missing_task_names", ())
                ]
                _write_json(target / "missing-local-images.json", missing_images)
            for directory in ("runs", "worker-status", "logs", "controller-attempts"):
                (target / directory).mkdir(parents=True, exist_ok=True)
            _write_json(
                target / "materialization.json",
                {
                    **manifest["materialization"],
                    "runtime_pins_validated": True,
                    "task_count": len(selected),
                },
            )
    except Exception:
        for target in created:
            shutil.rmtree(target, ignore_errors=True)
        raise

    return {
        "schema_version": "lhos-lhtb-arm-materialization.v1",
        "prepared_output": str(source),
        "fresh_output": str(targets["fresh"]),
        "lhos_output": str(targets["lhos"]),
        "task_names": list(selected),
        "task_count": len(selected),
        "source_manifest_sha256": source_manifest_sha256,
        "common_identity_sha256": common_identity,
        "pair_admission": pair_admission,
        "runtime_pins_validated": True,
    }


def summarize_output(output: Path) -> dict[str, Any]:
    manifest = _load_manifest(output)
    _validate_controlled_pair_manifest(manifest)
    controlled_pair_required = _controlled_pair_enabled(manifest)
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        _validate_official_leaderboard_contract(
            task_names=[str(task["name"]) for task in manifest.get("tasks", ())],
            configs=manifest.get("configs") or {},
        )
        _validate_official_manifest_contract(manifest)
    progress = _write_run_progress(output, manifest)
    pair_admission = _load_json(output / "pair-admission.json")
    if not pair_admission:
        manifest_admission = manifest.get("pair_admission")
        pair_admission = (
            manifest_admission if isinstance(manifest_admission, dict) else {}
        )
    pairs: list[dict[str, Any]] = []
    for task in manifest["tasks"]:
        name = str(task["name"])
        fresh_record, fresh_parser_error = _load_arm_record(
            output / "runs" / name / "dsh_fresh.json"
        )
        lhos_record, lhos_parser_error = _load_arm_record(
            output / "runs" / name / "lhos_resume.json"
        )
        fresh = {
            **_annotate_time_to_verified(
                _refresh_arm_metrics(
                    fresh_record,
                    "dsh_fresh",
                    controlled_pair_required=controlled_pair_required,
                ),
                task,
            ),
            "launcher_elapsed_ms": fresh_record.get("elapsed_ms"),
            "controller_attempt_cost": _controller_attempt_cost(fresh_record),
            "controller_attempt_count": fresh_record.get("controller_attempt_count", 1),
            "infrastructure_retry_count": fresh_record.get(
                "infrastructure_retry_count",
                0,
            ),
            "infrastructure_resampled": bool(
                fresh_record.get("infrastructure_resampled")
            ),
        }
        lhos = {
            **_annotate_time_to_verified(
                _refresh_arm_metrics(
                    lhos_record,
                    "lhos_resume",
                    controlled_pair_required=controlled_pair_required,
                ),
                task,
            ),
            "launcher_elapsed_ms": lhos_record.get("elapsed_ms"),
            "controller_attempt_cost": _controller_attempt_cost(lhos_record),
            "controller_attempt_count": lhos_record.get("controller_attempt_count", 1),
            "infrastructure_retry_count": lhos_record.get(
                "infrastructure_retry_count",
                0,
            ),
            "infrastructure_resampled": bool(
                lhos_record.get("infrastructure_resampled")
            ),
        }
        fresh_parser_error = fresh_parser_error or fresh_record.get("parser_error")
        lhos_parser_error = lhos_parser_error or lhos_record.get("parser_error")
        fresh_execution_error = fresh_record.get("execution_error") or fresh.get("execution_error")
        lhos_execution_error = lhos_record.get("execution_error") or lhos.get("execution_error")
        complete = bool(fresh_record and lhos_record)
        metrics_available = bool(
            complete
            and not fresh_parser_error
            and not lhos_parser_error
            and not fresh_execution_error
            and not lhos_execution_error
            and fresh.get("parse_valid")
            and lhos.get("parse_valid")
        )
        parity_valid = bool(
            fresh_record.get("status") == "completed"
            and lhos_record.get("status") == "completed"
            and fresh_record.get("config_parity_sha256")
            == lhos_record.get("config_parity_sha256")
            and fresh_record.get("docker_image_id")
            == lhos_record.get("docker_image_id")
        )
        provider_censored = bool(
            fresh.get("provider_censored")
            or lhos.get("provider_censored")
        )
        infrastructure_resampled = bool(
            fresh.get("infrastructure_resampled")
            or lhos.get("infrastructure_resampled")
        )
        operational_recovered = bool(
            infrastructure_resampled
            and metrics_available
            and parity_valid
            and not provider_censored
        )
        stochastic_verifier = bool(
            task.get(
                "stochastic_verifier",
                name in STOCHASTIC_VERIFIER_COHORT,
            )
        )
        stochastic_pair_controlled = bool(
            task.get(
                "stochastic_pair_controlled",
                not stochastic_verifier,
            )
        )
        result_eligible = bool(
            metrics_available
            and fresh.get(
                "result_eligible",
                fresh.get("comparison_eligible"),
            )
            and lhos.get(
                "result_eligible",
                lhos.get("comparison_eligible"),
            )
            and parity_valid
            and not provider_censored
            and not infrastructure_resampled
            and stochastic_pair_controlled
        )
        resume_verified = bool(lhos.get("continuation_gate", {}).get("passed"))
        continuation_boundary_mode = str(
            task.get("continuation_boundary_mode")
            or _continuation_boundary_mode(task)
        )
        continuation_capable = bool(task.get("continue_until_timeout"))
        mechanism_eligible = bool(
            result_eligible
            and continuation_capable
            and fresh.get(
                "mechanism_eligible",
                fresh.get("comparison_eligible"),
            )
            and lhos.get(
                "mechanism_eligible",
                lhos.get("comparison_eligible"),
            )
            and resume_verified
        )
        observed = (
            {key: _delta(fresh, lhos, key) for key in PROFILE_METRICS}
            if result_eligible
            else None
        )
        pairs.append(
            {
                "task_name": name,
                "official_agent_timeout_seconds": task.get(
                    "official_agent_timeout_seconds"
                ),
                "configured_agent_timeout_seconds": task.get(
                    "configured_agent_timeout_seconds"
                ),
                "configured_time_slice_seconds": task.get(
                    "configured_time_slice_seconds"
                ),
                "time_slice_policy": task.get("time_slice_policy"),
                "continuation_boundary_mode": continuation_boundary_mode,
                "continue_until_timeout": continuation_capable,
                "time_to_verified_supported": bool(
                    task.get("time_to_verified_supported")
                ),
                "complete": complete,
                "result_eligible": result_eligible,
                "mechanism_eligible": mechanism_eligible,
                "comparison_eligible": mechanism_eligible,
                "provider_censored": provider_censored,
                "infrastructure_resampled": infrastructure_resampled,
                "operational_recovered": operational_recovered,
                "stochastic_verifier": stochastic_verifier,
                "stochastic_pair_controlled": stochastic_pair_controlled,
                "stochastic_control_mode": task.get(
                    "stochastic_control_mode",
                    "not_declared_stochastic"
                    if not stochastic_verifier
                    else "uncontrolled_stochastic_cohort",
                ),
                "paired_verifier_seed_sha256": task.get(
                    "paired_verifier_seed_sha256"
                ),
                "provider_resample": {
                    "attempted": False,
                    "max_pair_resamples": 1,
                    "reason": (
                        "fail_closed_no_automatic_pair_resample"
                        if provider_censored
                        else "not_required"
                    ),
                },
                "parser_error": fresh_parser_error or lhos_parser_error,
                "fresh_parser_error": fresh_parser_error,
                "lhos_parser_error": lhos_parser_error,
                "fresh_execution_error": fresh_execution_error,
                "lhos_execution_error": lhos_execution_error,
                "same_task_model_timeout": bool(
                    fresh_record.get("config_parity_sha256")
                    and fresh_record.get("config_parity_sha256")
                    == lhos_record.get("config_parity_sha256")
                ),
                "same_image_id": bool(
                    fresh_record.get("docker_image_id")
                    and fresh_record.get("docker_image_id") == lhos_record.get("docker_image_id")
                ),
                "dsh_fresh": fresh,
                "lhos_resume": {
                    **lhos,
                    "goal_state": lhos_record.get("goal_state"),
                },
                "resume_verified": resume_verified,
                "reported_lhos_mode": (
                    "one_shot_direct_compatibility"
                    if continuation_boundary_mode == "one_shot"
                    else
                    str(
                        lhos.get(
                            "control_classification",
                            "verified_context_reuse",
                        )
                    )
                    if resume_verified
                    else "direct_compatibility"
                ),
                "observed_fresh_minus_lhos": observed,
                "official_score": False,
            }
        )
    completed = [pair for pair in pairs if pair["complete"]]
    full_suite_profiling = _aggregate_profiling(
        pairs,
        eligibility_key="result_eligible",
    )
    mechanism_profiling = _aggregate_profiling(
        pairs,
        eligibility_key="mechanism_eligible",
    )
    continuation_boundary_profiling = {
        mode: {
            "pair_count": sum(
                pair["continuation_boundary_mode"] == mode
                for pair in pairs
            ),
            "full_suite": _aggregate_profiling(
                [
                    pair
                    for pair in pairs
                    if pair["continuation_boundary_mode"] == mode
                ],
                eligibility_key="result_eligible",
            ),
            "mechanism": _aggregate_profiling(
                [
                    pair
                    for pair in pairs
                    if pair["continuation_boundary_mode"] == mode
                ],
                eligibility_key="mechanism_eligible",
            ),
        }
        for mode in CONTINUATION_BOUNDARY_MODES
    }
    infrastructure_retry_cost = _aggregate_controller_attempt_cost(pairs)
    budget_tier_profiling = _budget_tier_profiling(pairs)
    result = {
        "schema_version": (
            "lhos-lhtb-dsh-software5-result.v1"
            if manifest["schema_version"] == MANIFEST_SCHEMA_V1
            else "lhos-lhtb-dsh-result.v2"
        ),
        "benchmark": manifest["benchmark"],
        "official_score": False,
        "claim_status": manifest.get(
            "claim_status", "custom_harness_controlled_experiment"
        ),
        "ranking_eligible": False,
        "estimand": manifest.get("estimand"),
        "budget_condition": manifest.get("budget_condition"),
        "controlled_pair_experiment": manifest.get("controlled_pair_experiment"),
        "task_content": manifest["task_content"],
        "environment": manifest["environment"],
        "lhtb_source_commit": manifest["lhtb_source_commit"],
        "model": manifest["model"],
        "reasoning_effort": manifest["reasoning_effort"],
        "agent_timeout_seconds": manifest["agent_timeout_seconds"],
        "agent_timeout_mode": manifest.get("agent_timeout_mode", "global"),
        "time_to_verified_mode": bool(manifest.get("time_to_verified_mode", False)),
        "time_slice_seconds": manifest.get("time_slice_seconds"),
        "time_slice_mode": manifest.get("time_slice_mode"),
        "time_slice_policy": manifest.get("time_slice_policy"),
        "semantic_context_control": manifest.get(
            "semantic_context_control",
            {},
        ),
        "resolved_reward_threshold": manifest.get(
            "resolved_reward_threshold", DEFAULT_VERIFIED_REWARD_THRESHOLD
        ),
        "verified_reward_threshold": manifest.get(
            "verified_reward_threshold", DEFAULT_FULL_REWARD_THRESHOLD
        ),
        "max_concurrency": manifest["max_concurrency"],
        "effective_max_concurrency": pair_admission.get(
            "max_concurrency", manifest["max_concurrency"]
        ),
        "resource_aware_pairs": bool(
            pair_admission.get("enabled", manifest.get("resource_aware_pairs", False))
        ),
        "pair_admission": pair_admission,
        "batch_timeline": _load_json(output / "batch-timeline.json"),
        "run_progress": progress,
        "harbor": manifest["harbor"],
        "runtime": manifest["runtime"],
        "pair_count": len(pairs),
        "complete_pair_count": len(completed),
        "result_eligible_pair_count": sum(
            bool(pair["result_eligible"]) for pair in pairs
        ),
        "mechanism_eligible_pair_count": sum(
            bool(pair["mechanism_eligible"]) for pair in pairs
        ),
        "comparison_eligible_pair_count": sum(
            bool(pair["mechanism_eligible"]) for pair in pairs
        ),
        "provider_censored_pair_count": sum(
            bool(pair["provider_censored"]) for pair in pairs
        ),
        "provider_censored_task_names": [
            pair["task_name"] for pair in pairs if pair["provider_censored"]
        ],
        "parser_error_pair_count": sum(bool(pair["parser_error"]) for pair in pairs),
        "execution_failure_pair_count": sum(
            bool(pair["fresh_execution_error"] or pair["lhos_execution_error"]) for pair in pairs
        ),
        "fresh_resolved": sum(bool(pair["dsh_fresh"].get("resolved")) for pair in pairs),
        "lhos_resolved": sum(bool(pair["lhos_resume"].get("resolved")) for pair in pairs),
        "fresh_verified": sum(bool(pair["dsh_fresh"].get("verified")) for pair in pairs),
        "lhos_verified": sum(bool(pair["lhos_resume"].get("verified")) for pair in pairs),
        "resume_verified_count": sum(bool(pair["resume_verified"]) for pair in pairs),
        "infrastructure_resampled_arm_count": sum(
            int(bool(pair["dsh_fresh"].get("infrastructure_resampled")))
            + int(bool(pair["lhos_resume"].get("infrastructure_resampled")))
            for pair in pairs
        ),
        "infrastructure_resampled_pair_count": sum(
            bool(pair["infrastructure_resampled"]) for pair in pairs
        ),
        "operational_recovered_pair_count": sum(
            bool(pair["operational_recovered"]) for pair in pairs
        ),
        "operational_recovered_task_names": [
            pair["task_name"] for pair in pairs if pair["operational_recovered"]
        ],
        "stochastic_pair_count": sum(
            bool(pair["stochastic_verifier"]) for pair in pairs
        ),
        "uncontrolled_stochastic_pair_count": sum(
            bool(
                pair["stochastic_verifier"]
                and not pair["stochastic_pair_controlled"]
            )
            for pair in pairs
        ),
        "uncontrolled_stochastic_task_names": [
            pair["task_name"]
            for pair in pairs
            if pair["stochastic_verifier"]
            and not pair["stochastic_pair_controlled"]
        ],
        "infrastructure_retry_cost": infrastructure_retry_cost,
        "budget_tier_profiling": budget_tier_profiling,
        "full_suite_profiling": full_suite_profiling,
        "mechanism_profiling": mechanism_profiling,
        "continuation_boundary_profiling": continuation_boundary_profiling,
        # Backward-compatible alias retains the mechanism-only interpretation.
        "profiling": mechanism_profiling,
        "pairs": pairs,
        "attribution": manifest["attribution"],
    }
    official_contract = manifest.get("official_leaderboard_contract")
    if isinstance(official_contract, dict) and official_contract.get("enabled") is True:
        result["official_leaderboard_contract"] = {
            **official_contract,
            "scores": {
                arm: _official_leaderboard_score(
                    [
                        (str(pair["task_name"]), pair[arm].get("reward"))
                        for pair in pairs
                    ],
                    expected_task_names=[str(task["name"]) for task in manifest["tasks"]],
                )
                for arm in ARMS
            },
        }
    _write_json(output / "result.json", result)
    (output / "RESULTS.zh-CN.md").write_text(
        _render_summary_v2(result),
        encoding="utf-8",
    )
    return result


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if value.is_integer():
            return f"{value:,.0f}"
        return f"{value:,.6f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _render_summary(result: dict[str, Any]) -> str:
    timeout_mode = result.get("agent_timeout_mode", "global")
    timeout_value = result.get("agent_timeout_seconds")
    timeout_text = (
        "per-task task.toml declared"
        if timeout_mode == "task_declared"
        else "per-task configured"
        if timeout_value is None
        else f"{timeout_value}s"
    )
    lines = [
        f"# LHTB {result['pair_count']} 题 DSH 双臂本地 Pilot",
        "",
        "## 实验边界",
        "",
        "```text",
        f"task content: {result['task_content']}",
        f"LHTB commit: {result['lhtb_source_commit']}",
        f"Harbor: {result['harbor']['version']} @ {result['harbor']['commit']}",
        f"model: {result['model']}",
        f"reasoning: {result['reasoning_effort']}",
        f"per-arm agent timeout: {timeout_text}",
        f"max task-pair concurrency: {result['max_concurrency']}",
        "environment: local Docker pilot",
        "official leaderboard score: false",
        "```",
        "",
        "Baseline 每次 verifier rejection 后使用全新 DSH_HOME/session。LHOS arm",
        "只有在同一 session JSONL 被复用、事件数增长且至少一次 resume invocation",
        "完成后，才标记为 `verified_context_reuse`。",
        "",
        "## 结果",
        "",
        "| Task | Fresh resolved | LHOS resolved | LHOS mode | Fresh tokens | "
        "LHOS tokens | Fresh calls | LHOS calls | Fresh tools | LHOS tools |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for pair in result["pairs"]:
        fresh = pair["dsh_fresh"]
        lhos = pair["lhos_resume"]
        lines.append(
            f"| `{pair['task_name']}` | "
            f"{'yes' if fresh.get('resolved') else 'no'} | "
            f"{'yes' if lhos.get('resolved') else 'no'} | "
            f"`{pair['reported_lhos_mode']}` | "
            f"{_fmt(fresh.get('token_units'))} | "
            f"{_fmt(lhos.get('token_units'))} | "
            f"{_fmt(fresh.get('model_calls'))} | "
            f"{_fmt(lhos.get('model_calls'))} | "
            f"{_fmt(fresh.get('tool_calls'))} | "
            f"{_fmt(lhos.get('tool_calls'))} |"
        )
    lines.extend(
        [
            "",
            "## 汇总",
            "",
            f"- 完成 pair: {result['complete_pair_count']}/{result['pair_count']}",
            f"- Fresh resolved: {result['fresh_resolved']}",
            f"- LHOS resolved: {result['lhos_resolved']}",
            f"- Resume gate passed: {result['resume_verified_count']}",
            "",
            "若 resume gate 未通过，该题只证明 custom DSH agent、Harbor、Docker",
            "verifier 与 LHOS 外层 authority 的 direct compatibility，不得宣传",
            "context reuse 或 OS 加速。即使 gate 通过，单次 paired trajectory 的",
            "token/time 差异仍需多次 AB/BA 重复后才能作为因果结论。",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_summary_v2(result: dict[str, Any]) -> str:
    profile = result["mechanism_profiling"]
    full_profile = result["full_suite_profiling"]
    timeout_value = result.get("agent_timeout_seconds")
    timeout_mode = result.get("agent_timeout_mode", "global")
    timeout_text = (
        "per-task task.toml declared"
        if timeout_mode == "task_declared"
        else "uniform official 5400"
        if timeout_mode == "official_uniform_5400"
        else "per-task configured"
        if timeout_value is None
        else f"{timeout_value}s"
    )
    lines = [
        f"# LHTB {result['pair_count']} 题 DSH 双臂本地 Pilot",
        "",
        "## 实验边界",
        "",
        "```text",
        f"task content: {result['task_content']}",
        f"LHTB commit: {result['lhtb_source_commit']}",
        f"Harbor: {result['harbor']['version']} @ {result['harbor']['commit']}",
        f"model: {result['model']}",
        f"reasoning: {result['reasoning_effort']}",
        f"agent timeout mode: {result.get('agent_timeout_mode', 'global')}",
        f"per-arm agent timeout: {timeout_text}",
        f"time-to-verified mode: {result.get('time_to_verified_mode', False)}",
        f"max task-pair concurrency: {result['max_concurrency']}",
        "environment: local Docker pilot",
        "official leaderboard score: false",
        "```",
        "",
        "Baseline 在 verifier rejection 后使用新的 DSH session。LHOS arm 只有在同一",
        "session JSONL 被复用、事件继续增长且 resume gate 通过时，才计为",
        "`verified_context_reuse` 并进入 mechanism profiling。Continuation 边界",
        "可能来自 natural Harness continuation 或 forced time slice；one-shot 题",
        "只进入 full-suite result profiling，不宣称 semantic resume 加速。",
        "",
        "## 逐题结果",
        "",
        "| Task | Boundary | Result | Mechanism | Fresh status | LHOS status | Fresh reward | LHOS reward | LHOS mode | "
        "Fresh tokens | LHOS tokens | Token delta | Fresh tools | LHOS tools | "
        "Fresh TTV ms | LHOS TTV ms |",
        "|---|---|---:|---:|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for pair in result["pairs"]:
        fresh = pair["dsh_fresh"]
        lhos = pair["lhos_resume"]
        delta = pair.get("observed_fresh_minus_lhos") or {}
        lines.append(
            f"| `{pair['task_name']}` | "
            f"`{pair['continuation_boundary_mode']}` | "
            f"{'yes' if pair['result_eligible'] else 'no'} | "
            f"{'yes' if pair['mechanism_eligible'] else 'no'} | "
            f"`{fresh.get('verification_status', 'unknown')}` | "
            f"`{lhos.get('verification_status', 'unknown')}` | "
            f"{_fmt(fresh.get('reward'))} | "
            f"{_fmt(lhos.get('reward'))} | "
            f"`{pair['reported_lhos_mode']}` | "
            f"{_fmt(fresh.get('token_units'))} | "
            f"{_fmt(lhos.get('token_units'))} | "
            f"{_fmt(delta.get('token_units'))} | "
            f"{_fmt(fresh.get('tool_calls'))} | "
            f"{_fmt(lhos.get('tool_calls'))} | "
            f"{_fmt(fresh.get('time_to_verified_ms'))} | "
            f"{_fmt(lhos.get('time_to_verified_ms'))} |"
        )

    budget_profile = result.get("budget_tier_profiling") or {}
    tiers = budget_profile.get("tiers") or []
    lines.extend(
        [
            "",
            "## Budget-Tier Outcome ITT",
            "",
            "Missing/invalid arm rewards are imputed as zero for this assignment-level "
            "view. LHOS-minus-fresh is the primary contrast; one-shot tasks remain "
            "in outcome ITT but are mechanism-NA.",
            "",
            "| Budget | Pairs | Continuation | One-shot | Fresh mean | LHOS mean | LHOS-Fresh | Fresh resolved | LHOS resolved | LHOS wins | Fresh wins | Ties |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for tier in tiers:
        outcome = tier.get("outcome_itt") or {}
        lines.append(
            f"| `{tier.get('budget_label')}` | {tier.get('pair_count', 0)} | "
            f"{tier.get('continuation_pair_count', 0)} | "
            f"{tier.get('one_shot_pair_count', 0)} | "
            f"{_fmt(outcome.get('fresh_mean_reward'))} | "
            f"{_fmt(outcome.get('lhos_mean_reward'))} | "
            f"{_fmt(outcome.get('lhos_minus_fresh_mean_reward'))} | "
            f"{_fmt(outcome.get('fresh_resolved_rate'))} | "
            f"{_fmt(outcome.get('lhos_resolved_rate'))} | "
            f"{outcome.get('lhos_wins', 0)} | {outcome.get('fresh_wins', 0)} | "
            f"{outcome.get('ties', 0)} |"
        )

    lines.extend(
        [
            "",
            "## Full-Suite Result Profiling",
            "",
            "| Metric | Pair count | Fresh total | LHOS total | Saved | Saved % | "
            "LHOS wins | Fresh wins | Ties |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for key in PROFILE_METRICS:
        metric = full_profile["metrics"][key]
        lines.append(
            f"| `{key}` | {metric['pair_count']} | "
            f"{_fmt(metric['fresh_total'])} | "
            f"{_fmt(metric['lhos_total'])} | "
            f"{_fmt(metric['fresh_minus_lhos'])} | "
            f"{_fmt(metric['saving_percent'])} | "
            f"{metric['lhos_wins']} | {metric['fresh_wins']} | "
            f"{metric['ties']} |"
        )

    lines.extend(
        [
            "",
            "## Continuation Resume-Mechanism Profiling",
            "",
            "| Metric | Pair count | Fresh total | LHOS total | Saved | Saved % | "
            "LHOS wins | Fresh wins | Ties |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for key in PROFILE_METRICS:
        metric = profile["metrics"][key]
        lines.append(
            f"| `{key}` | {metric['pair_count']} | "
            f"{_fmt(metric['fresh_total'])} | "
            f"{_fmt(metric['lhos_total'])} | "
            f"{_fmt(metric['fresh_minus_lhos'])} | "
            f"{_fmt(metric['saving_percent'])} | "
            f"{metric['lhos_wins']} | {metric['fresh_wins']} | "
            f"{metric['ties']} |"
        )

    contract = result.get("official_leaderboard_contract")
    if isinstance(contract, dict):
        lines.extend(
            [
                "",
                "## Public metric projection (not a leaderboard score)",
                "",
                (
                    "Shared public metric fields and post-hoc metrics are checked; "
                    "modified-Harbor semantics are not validated, and this custom "
                    "harness/local-pilot result is not leaderboard-comparable."
                ),
                "",
                "| Arm | Complete | Mean reward (46, errors=0) | Solved (reward >= 0.95) | Errors |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for arm, score in (contract.get("scores") or {}).items():
            if not isinstance(score, dict):
                continue
            lines.append(
                f"| `{arm}` | {score.get('complete', False)} | "
                f"{_fmt(score.get('mean_reward'))} | "
                f"{_fmt(score.get('solved_count'))} | "
                f"{_fmt(score.get('error_reward_count'))} |"
            )

    boundary_profiles = result["continuation_boundary_profiling"]
    lines.extend(
        [
            "",
            "## Continuation Boundary Cohorts",
            "",
            "| Boundary | All pairs | Result eligible | Mechanism eligible |",
            "|---|---:|---:|---:|",
        ]
    )
    for mode in CONTINUATION_BOUNDARY_MODES:
        cohort = boundary_profiles[mode]
        lines.append(
            f"| `{mode}` | {cohort['pair_count']} | "
            f"{cohort['full_suite']['eligible_pair_count']} | "
            f"{cohort['mechanism']['eligible_pair_count']} |"
        )

    retry_cost = result["infrastructure_retry_cost"]
    lines.extend(
        [
            "",
            "## Infrastructure Retry Cost",
            "",
            "These costs came from interrupted controller attempts and are exposed "
            "separately from the agent-mechanism profiling above.",
            "",
            "| Arm | Interrupted attempts | Token units | Model calls | Tool calls | "
            "Agent elapsed ms | Harbor elapsed ms |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for arm in ARMS:
        cost = retry_cost["arms"][arm]
        metrics = cost["metrics"]
        lines.append(
            f"| `{arm}` | {cost['interrupted_attempt_count']} | "
            f"{_fmt(metrics['token_units'])} | "
            f"{_fmt(metrics['model_calls'])} | "
            f"{_fmt(metrics['tool_calls'])} | "
            f"{_fmt(metrics['agent_elapsed_ms'])} | "
            f"{_fmt(metrics['harbor_total_elapsed_ms'])} |"
        )

    lines.extend(
        [
            "",
            "## 汇总",
            "",
            f"- 完成 pair: {result['complete_pair_count']}/{result['pair_count']}",
            f"- Full-suite result eligible: {result['result_eligible_pair_count']}",
            f"- Resume-mechanism eligible: {result['mechanism_eligible_pair_count']}",
            f"- Resume gate 通过: {result['resume_verified_count']}",
            f"- Provider-censored invalid pairs: {result['provider_censored_pair_count']}",
            f"- Infrastructure-resampled arms: "
            f"{result['infrastructure_resampled_arm_count']}",
            f"- Infrastructure-resampled invalid pairs: "
            f"{result['infrastructure_resampled_pair_count']}",
            f"- Operationally recovered after infrastructure retry: "
            f"{result['operational_recovered_pair_count']}",
            f"- Uncontrolled stochastic invalid pairs: "
            f"{result['uncontrolled_stochastic_pair_count']}",
            f"- Parser error pair: {result['parser_error_pair_count']}",
            f"- Execution failure pair: {result['execution_failure_pair_count']}",
            f"- Fresh resolved: {result['fresh_resolved']}",
            f"- LHOS resolved: {result['lhos_resolved']}",
            f"- 有效 reward 相同: {profile['same_reward_pair_count']}/"
            f"{profile['reward_pair_count']}",
            f"- time-to-verified pairs: {profile['time_to_verified_pair_count']}",
            f"- Fresh verified before budget: "
            f"{profile['fresh_verified_before_agent_budget']}",
            f"- LHOS verified before budget: "
            f"{profile['lhos_verified_before_agent_budget']}",
            "",
            "未通过 resume gate 的题只能证明 DSH、Harbor、Docker、verifier 与",
            "LHOS 外层控制面的直接兼容性，不计入 context reuse 加速结论。单次",
            "paired trajectory 仍是 pilot；因果结论需要多次 AB/BA 重复和置信区间。",
        ]
    )
    return "\n".join(lines) + "\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--lhtb-root", type=Path, required=True)
    prepare_parser.add_argument(
        "--tasks-root",
        type=Path,
        default=Path(r"D:\LHTB-local-pilot\tasks"),
    )
    prepare_parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path(r"D:\LHTB-dsh-runtime"),
    )
    prepare_parser.add_argument(
        "--patch",
        type=Path,
        default=(
            REPO_ROOT
            / "benchmarks"
            / "real_dsh_dynamic_coding"
            / "stepfun-3.7-pi-ai.cordis.patch.yml"
        ),
    )
    prepare_parser.add_argument("--output", type=Path, required=True)
    selection = prepare_parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--all",
        action="store_true",
        help="Discover and configure all 46 task.toml files under tasks_root.",
    )
    selection.add_argument(
        "--task-names",
        nargs="+",
        help="Explicit ordered task names; comma-separated values are also accepted.",
    )
    prepare_parser.add_argument(
        "--official-leaderboard-contract",
        "--official-contract",
        "--official-protocol",
        dest="official_leaderboard_contract",
        action="store_true",
        help=(
            "Audit shared public leaderboard settings: all 46 tasks, one attempt, "
            "uniform 5400s agent budget, timeout multiplier 1.0, Docker delete=true, "
            "mean reward, and solved@0.95. This does not validate modified-Harbor "
            "semantics or make a custom/local-pilot result leaderboard-comparable."
        ),
    )
    prepare_parser.add_argument(
        "--official-model-yaml",
        "--official-model-config",
        dest="official_model_yaml",
        type=Path,
        default=None,
        help=(
            "Optionally validate and record one unchanged, Git-tracked "
            "configs/leaderboard/*.yaml from the pinned LHTB checkout. The "
            "reference does not make the custom runner model- or harness-equivalent."
        ),
    )
    prepare_parser.add_argument(
        "--local-images-only",
        action="store_true",
        help=(
            "Generate configs only for selected tasks whose required Docker "
            "images already exist locally; missing images are still reported."
        ),
    )
    prepare_parser.add_argument(
        "--agent-timeout-seconds",
        type=int,
        default=DEFAULT_AGENT_TIMEOUT_SECONDS,
    )
    prepare_parser.add_argument(
        "--task-declared-timeouts",
        "--use-official-agent-timeouts",
        "--official-timeouts",
        dest="use_official_agent_timeouts",
        action="store_true",
        help=(
            "Use each selected task's [agent].timeout_sec instead of one global "
            "timeout. The older --official-timeouts spelling remains an alias."
        ),
    )
    prepare_parser.add_argument(
        "--time-to-verified",
        action="store_true",
        help=(
            "Prepare a full-budget run that records time/token/tool/model "
            "usage at the first verified result when available."
        ),
    )
    time_slice_selection = prepare_parser.add_mutually_exclusive_group()
    time_slice_selection.add_argument(
        "--time-slice-seconds",
        type=int,
        default=None,
        help=(
            "Optional DSH slice budget. Defaults to the historical "
            f"{DEFAULT_TIME_SLICE_SECONDS}s pilot slice; --time-to-verified "
            "defaults to no artificial slice. One-shot tasks automatically "
            "retain their full arm budget."
        ),
    )
    time_slice_selection.add_argument(
        "--no-time-slice",
        action="store_true",
        help=(
            "Disable controller time slicing for every selected task. By default "
            "only continue_until_timeout tasks are sliced."
        ),
    )
    prepare_parser.add_argument(
        "--worker-timeout-seconds",
        type=int,
        default=DEFAULT_WORKER_TIMEOUT_SECONDS,
    )
    prepare_parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
    )
    prepare_parser.add_argument(
        "--resource-aware-pairs",
        action="store_true",
        help=(
            "Schedule independent task pairs in deterministic CPU/memory "
            "admission waves. Required for --max-concurrency above 2."
        ),
    )
    prepare_parser.add_argument(
        "--pair-capacity-cpus",
        type=float,
        default=None,
        help="Override Docker CPU capacity used for resource-aware pair admission.",
    )
    prepare_parser.add_argument(
        "--pair-capacity-memory-mb",
        type=int,
        default=None,
        help="Override Docker memory capacity used for resource-aware pair admission.",
    )

    prebuild_parser = subparsers.add_parser("prebuild")
    prebuild_parser.add_argument("--output", type=Path, required=True)
    prebuild_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Reuse already-present exact image tags and build only missing ones.",
    )

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--jobs-dir", type=Path, required=True)
    run_parser.add_argument("--credential-env", default="STEPFUN_API_KEY")
    run_parser.add_argument(
        "--arm",
        choices=ARM_MODES,
        default=None,
        help=(
            "Run both paired arms (default), or isolate one arm into this "
            "output/jobs directory for a later merge-arms."
        ),
    )
    run_parser.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Override the prepared task-pair concurrency limit.",
    )
    run_parser.add_argument(
        "--resource-aware-pairs",
        action="store_true",
        help="Enable deterministic CPU/memory admission waves for this run.",
    )
    run_parser.add_argument(
        "--pair-capacity-cpus",
        type=float,
        default=None,
        help="Override Docker CPU capacity used for resource-aware pair admission.",
    )
    run_parser.add_argument(
        "--pair-capacity-memory-mb",
        type=int,
        default=None,
        help="Override Docker memory capacity used for resource-aware pair admission.",
    )

    summarize_parser = subparsers.add_parser("summarize")
    summarize_parser.add_argument("--output", type=Path, required=True)

    merge_parser = subparsers.add_parser("merge-arms")
    merge_parser.add_argument("--fresh-output", type=Path, required=True)
    merge_parser.add_argument("--lhos-output", type=Path, required=True)
    merge_parser.add_argument("--output", type=Path, required=True)

    materialize_parser = subparsers.add_parser("materialize-arms")
    materialize_parser.add_argument("--prepared-output", type=Path, required=True)
    materialize_parser.add_argument("--fresh-output", type=Path, required=True)
    materialize_parser.add_argument("--lhos-output", type=Path, required=True)
    materialize_parser.add_argument(
        "--task-names",
        nargs="+",
        help=(
            "Optional ordered subset; comma-separated values are accepted. "
            "Defaults to every task in the prepared manifest."
        ),
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "prepare":
        result = prepare(args)
    elif args.command == "prebuild":
        result = prebuild(args)
    elif args.command == "run":
        result = run_pairs(args)
    elif args.command == "merge-arms":
        result = merge_arm_outputs(
            fresh_output=args.fresh_output,
            lhos_output=args.lhos_output,
            output=args.output,
        )
    elif args.command == "materialize-arms":
        result = materialize_arm_outputs(
            prepared_output=args.prepared_output,
            fresh_output=args.fresh_output,
            lhos_output=args.lhos_output,
            task_names=args.task_names,
        )
    else:
        result = summarize_output(args.output.resolve())
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
