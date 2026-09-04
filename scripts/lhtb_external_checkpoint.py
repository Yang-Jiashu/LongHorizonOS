"""Externally checkpoint and score an unsliced paired LHTB run.

The watcher observes the agent's already-mounted, atomic ``invocation.json``
records.  It does not inject a callback, prompt, time slice, verifier pass, or
environment variable into either experimental arm.  At a requested cumulative
DSH-active time it briefly freezes the exact Compose ``main`` container,
commits a crash-consistent filesystem view, immediately unfreezes the live
container, and converts ``/app`` from the committed view into a content-
addressed workspace manifest.

Freezing is an external measurement intervention: Harbor's wall-clock agent
deadline continues while the container is paused.  Every snapshot records the
freeze interval and is therefore an extended-budget diagnostic, not an
official leaderboard checkpoint.  The two arms receive the same mechanism,
but workspace-size-dependent freeze cost can still create differential bias.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import posixpath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import uuid4

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CUTOFFS = (3_600, 5_400, 10_800, 14_400, 18_000, 21_600, 28_800)
ARMS = ("dsh_fresh", "lhos_resume")
INDEX_SCHEMA = "lhos-lhtb-external-checkpoint-index.v1"
SNAPSHOT_SCHEMA = "lhos-lhtb-external-workspace-checkpoint.v1"
WORKSPACE_SCHEMA = "lhos-lhtb-content-addressed-workspace.v1"
SCORE_SCHEMA = "lhos-lhtb-external-checkpoint-score.v1"
FREEZE_SCHEMA = "lhos-lhtb-external-checkpoint-freeze.v1"
RESTORE_AGENT = "scripts.lhtb_snapshot_restore_agent:LHTBSnapshotRestoreAgent"
DEFAULT_WORKSPACE = PurePosixPath("/app")
ARCHIVE_TARGET = PurePosixPath("/opt/lhtb-checkpoint/workspace.tar")
_TERMINAL_CAPTURE_STATUSES = frozenset({"captured", "not_reached", "failed", "late_unavailable"})


@dataclass(frozen=True, slots=True)
class ArmSpec:
    task_name: str
    arm: str
    job_name: str
    config_path: Path
    config_sha256: str
    docker_image: str
    docker_image_id: str
    task_root: Path
    task_content_sha256: str
    credential_env: str
    workspace: PurePosixPath

    @property
    def key(self) -> str:
        return f"{self.task_name}/{self.arm}"


@dataclass(frozen=True, slots=True)
class ActiveTrial:
    job_root: Path
    trial_dir: Path
    trial_name: str
    active_seconds: float
    completed_active_seconds: float
    running_invocation: int | None
    invocation_count: int


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.write("\n")
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


def _harbor_source_hash(project: Path) -> str:
    """Hash executable Harbor source/config while excluding cache artifacts."""

    roots = (project / "src" / "harbor",)
    candidates: list[Path] = []
    for root in roots:
        if root.is_dir():
            candidates.extend(
                item
                for item in root.rglob("*")
                if item.is_file()
                and "__pycache__" not in item.parts
                and item.suffix.lower() in {".py", ".yaml", ".yml", ".toml"}
            )
    for name in ("pyproject.toml", "uv.lock"):
        candidate = project / name
        if candidate.is_file():
            candidates.append(candidate)
    if not candidates:
        raise RuntimeError(f"Harbor source is missing below {project}")
    digest = hashlib.sha256()
    for item in sorted(set(candidates)):
        relative = item.relative_to(project).as_posix().encode("utf-8")
        content = item.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _run(
    command: list[str],
    *,
    timeout: float = 120,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
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


def _checked_run(
    command: list[str],
    *,
    timeout: float = 120,
) -> str:
    completed = _run(command, timeout=timeout)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1200:]
        raise RuntimeError(
            f"command failed ({command[0]} {command[1] if len(command) > 1 else ''}): "
            f"{detail or completed.returncode}"
        )
    return completed.stdout.strip()


def _docker_image_id(image: str) -> str:
    value = _checked_run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        timeout=30,
    )
    if not value.startswith("sha256:"):
        raise RuntimeError(f"Docker returned an invalid image id for {image}")
    return value


def _parse_cutoffs(raw_values: Iterable[str | int]) -> tuple[int, ...]:
    values: set[int] = set()
    for raw in raw_values:
        for item in str(raw).split(","):
            if not item.strip():
                continue
            try:
                value = int(item)
            except ValueError as exc:
                raise RuntimeError(f"invalid cutoff: {item!r}") from exc
            if value <= 0:
                raise RuntimeError("cutoffs must be positive seconds")
            values.add(value)
    if not values:
        raise RuntimeError("at least one cutoff is required")
    return tuple(sorted(values))


def _load_specs(output: Path) -> tuple[dict[str, Any], list[ArmSpec]]:
    manifest_path = output / "manifest.json"
    manifest = _load_json(manifest_path)
    if not manifest:
        raise RuntimeError(f"missing or invalid manifest: {manifest_path}")
    if (
        manifest.get("time_slice_mode") != "disabled"
        or manifest.get("time_slice_seconds") is not None
    ):
        raise RuntimeError("external checkpoints require a prepared unsliced run")
    tasks_root = Path(str(manifest.get("tasks_root", ""))).resolve()
    configs = manifest.get("configs") or {}
    specs: list[ArmSpec] = []
    for task in manifest.get("tasks", ()):
        name = str(task.get("name", ""))
        task_configs = configs.get(name) if isinstance(configs, dict) else None
        if not name or not isinstance(task_configs, dict):
            raise RuntimeError(f"manifest has incomplete config metadata for {name!r}")
        if task.get("configured_time_slice_seconds") is not None:
            raise RuntimeError(f"{name}: configured_time_slice_seconds must be null")
        if str(task.get("verifier_environment_mode", "same")) != "same":
            raise RuntimeError(
                f"{name}: external workspace replay currently requires a shared verifier"
            )
        image = str(task.get("docker_image", ""))
        image_ids = task.get("local_image_ids") or {}
        image_id = str(image_ids.get(image, "")) if isinstance(image_ids, dict) else ""
        if not image or not image_id:
            raise RuntimeError(f"{name}: prepared Docker image provenance is missing")
        task_root = tasks_root / name
        for arm in ARMS:
            config_path = Path(str(task_configs.get(arm, ""))).resolve()
            job_name = str(task_configs.get(f"{arm}_job_name", ""))
            if not config_path.is_file() or not job_name:
                raise RuntimeError(f"{name}/{arm}: prepared config is missing")
            config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            agent = (config.get("agents") or [{}])[0]
            kwargs = agent.get("kwargs") or {}
            credential_env = str(kwargs.get("credential_env", "STEPFUN_API_KEY"))
            raw_workspace = str(kwargs.get("workdir", DEFAULT_WORKSPACE.as_posix()))
            workspace = PurePosixPath(raw_workspace)
            if (
                not workspace.is_absolute()
                or ".." in workspace.parts
                or workspace == PurePosixPath("/")
            ):
                raise RuntimeError(f"{name}/{arm}: agent workdir is unsafe")
            specs.append(
                ArmSpec(
                    task_name=name,
                    arm=arm,
                    job_name=job_name,
                    config_path=config_path,
                    config_sha256=_sha256_file(config_path),
                    docker_image=image,
                    docker_image_id=image_id,
                    task_root=task_root,
                    task_content_sha256=str(task.get("task_content_sha256", "")),
                    credential_env=credential_env,
                    workspace=workspace,
                )
            )
    if not specs:
        raise RuntimeError("manifest selects no task arms")
    return manifest, specs


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp_duration(started_at: str | None, finished_at: str | None) -> float | None:
    started = _parse_timestamp(started_at)
    finished = _parse_timestamp(finished_at)
    if started is None or finished is None:
        return None
    return (finished - started).total_seconds()


def _invocation_active_time(
    trial_dir: Path,
    *,
    observed_at: datetime | None = None,
) -> tuple[float, int | None, int, float]:
    now = (observed_at or datetime.now(UTC)).astimezone(UTC)
    records: list[tuple[int, dict[str, Any]]] = []
    for path in sorted((trial_dir / "agent" / "invocations").glob("invocation-*/invocation.json")):
        record = _load_json(path)
        try:
            invocation = int(record.get("invocation"))
        except (TypeError, ValueError):
            continue
        records.append((invocation, record))
    records.sort(key=lambda item: item[0])
    completed_active = 0.0
    running_active = 0.0
    running: list[int] = []
    for invocation, record in records:
        status = str(record.get("status", ""))
        if status == "running":
            started_at = _parse_timestamp(record.get("started_at"))
            if started_at is None:
                raise RuntimeError(
                    f"{trial_dir.name}: running invocation {invocation} has no timestamp"
                )
            running_active += max(0.0, (now - started_at).total_seconds())
            running.append(invocation)
            continue
        try:
            elapsed_ms = float(record.get("elapsed_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"{trial_dir.name}: invalid elapsed_ms in invocation {invocation}"
            ) from exc
        if not math.isfinite(elapsed_ms) or elapsed_ms < 0:
            raise RuntimeError(f"{trial_dir.name}: invalid elapsed_ms in invocation {invocation}")
        completed_active += elapsed_ms / 1000.0
    if len(running) > 1:
        raise RuntimeError(f"{trial_dir.name}: multiple invocations are marked running")
    return (
        completed_active + running_active,
        (running[0] if running else None),
        len(records),
        completed_active,
    )


def _find_trial(spec: ArmSpec, jobs_dir: Path) -> ActiveTrial | None:
    job_root = jobs_dir / spec.job_name
    if not job_root.is_dir():
        return None
    candidates: list[ActiveTrial] = []
    for child in sorted(job_root.iterdir()):
        if not child.is_dir() or not (child / "config.json").is_file():
            continue
        config = _load_json(child / "config.json")
        trial_name = str(config.get("trial_name", ""))
        if trial_name != child.name:
            continue
        agent_kwargs = (config.get("agent") or {}).get("kwargs") or {}
        expected_agent_arm = "baseline" if spec.arm == "dsh_fresh" else "lhos"
        if str(agent_kwargs.get("arm", "")) != expected_agent_arm:
            continue
        active, running, count, completed_active = _invocation_active_time(child)
        if count:
            candidates.append(
                ActiveTrial(
                    job_root=job_root,
                    trial_dir=child,
                    trial_name=trial_name,
                    active_seconds=active,
                    completed_active_seconds=completed_active,
                    running_invocation=running,
                    invocation_count=count,
                )
            )
    running_candidates = [item for item in candidates if item.running_invocation is not None]
    if len(running_candidates) > 1:
        raise RuntimeError(f"{spec.key}: multiple active trial directories found")
    if running_candidates:
        return running_candidates[0]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.trial_dir.stat().st_mtime_ns)


def _capture_readiness(
    trial: ActiveTrial | None,
    cutoff: int,
    *,
    excluded_active_seconds: float = 0.0,
    excluded_completed_seconds: float = 0.0,
) -> tuple[str, str | None]:
    if trial is None:
        return "pending", None
    active_seconds = max(0.0, trial.active_seconds - excluded_active_seconds)
    completed_seconds = max(
        0.0,
        trial.completed_active_seconds - excluded_completed_seconds,
    )
    if active_seconds < cutoff:
        return "pending", None
    if trial.running_invocation is None:
        return "late_unavailable", "cutoff_crossed_without_running_invocation"
    if completed_seconds >= cutoff:
        return "late_unavailable", "watcher_missed_cutoff_before_current_invocation"
    return "due", None


def _freeze_offsets(
    checkpoint_root: Path,
    spec: ArmSpec,
    trial: ActiveTrial,
) -> tuple[float, float]:
    total = 0.0
    completed = 0.0
    ledger_root = (
        checkpoint_root / "_capture-freezes" / spec.task_name / spec.arm / trial.trial_name
    )
    for path in sorted(ledger_root.glob("*.json")):
        metadata = _load_json(path)
        if metadata.get("schema_version") != FREEZE_SCHEMA:
            continue
        try:
            duration = float(metadata.get("freeze_wall_seconds") or 0.0)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(duration) or duration < 0:
            continue
        total += duration
        if metadata.get("running_invocation") != trial.running_invocation:
            completed += duration
    return total, completed


def _write_freeze_ledger(
    path: Path,
    *,
    spec: ArmSpec,
    trial: ActiveTrial,
    cutoff: int,
    container_id: str,
    requested_at: str | None,
    acknowledged_at: str | None,
    unpaused_at: str,
) -> dict[str, Any]:
    started_at = acknowledged_at or requested_at
    duration = _timestamp_duration(started_at, unpaused_at)
    if duration is None or duration < 0:
        raise RuntimeError("cannot derive a valid checkpoint freeze duration")
    payload = {
        "schema_version": FREEZE_SCHEMA,
        "task_name": spec.task_name,
        "arm": spec.arm,
        "trial_name": trial.trial_name,
        "target_active_seconds": cutoff,
        "running_invocation": trial.running_invocation,
        "container_id": container_id,
        "freeze_requested_at": requested_at,
        "freeze_acknowledged_at": acknowledged_at,
        "unpaused_at": unpaused_at,
        "freeze_wall_seconds": duration,
        "recorded_at": _now(),
    }
    _atomic_write_json(path, payload)
    return payload


def _compose_project_name(trial_name: str) -> str:
    value = trial_name.lower()
    if not re.match(r"^[a-z0-9]", value):
        value = "0" + value
    return re.sub(r"[^a-z0-9_-]", "-", value)


def _container_id_for_trial(trial: ActiveTrial) -> str:
    project = _compose_project_name(trial.trial_name)
    stdout = _checked_run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--filter",
            "label=com.docker.compose.service=main",
            "--format",
            "{{.ID}}",
        ],
        timeout=30,
    )
    ids = [line.strip() for line in stdout.splitlines() if line.strip()]
    if len(ids) != 1:
        raise RuntimeError(
            f"{trial.trial_name}: expected one running Compose main container, found {len(ids)}"
        )
    return ids[0]


def _docker_inspect(container_id: str) -> dict[str, Any]:
    value = json.loads(_checked_run(["docker", "inspect", container_id], timeout=30))
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise RuntimeError(f"Docker returned invalid inspect data for {container_id}")
    return value[0]


def _normalized_host_path(value: Any) -> str:
    return str(value or "").replace("\\", "/").rstrip("/").lower()


def _validate_container_identity(
    spec: ArmSpec,
    trial: ActiveTrial,
    container_id: str,
) -> dict[str, Any]:
    inspect = _docker_inspect(container_id)
    labels = (inspect.get("Config") or {}).get("Labels") or {}
    project = _compose_project_name(trial.trial_name)
    if labels.get("com.docker.compose.project") != project:
        raise RuntimeError(f"{spec.key}: Compose project label mismatch")
    if labels.get("com.docker.compose.service") != "main":
        raise RuntimeError(f"{spec.key}: Compose service label mismatch")
    if inspect.get("Image") != spec.docker_image_id:
        raise RuntimeError(f"{spec.key}: live container image differs from prepared image id")
    state = inspect.get("State") or {}
    if state.get("Running") is not True or state.get("Paused") is True:
        raise RuntimeError(f"{spec.key}: container is not running and unpaused")
    mounts = inspect.get("Mounts") or []
    agent_mounts = [
        mount for mount in mounts if str(mount.get("Destination", "")).rstrip("/") == "/logs/agent"
    ]
    if len(agent_mounts) != 1 or agent_mounts[0].get("Type") != "bind":
        raise RuntimeError(f"{spec.key}: exact trial agent-log bind mount is missing")
    expected_suffix = _normalized_host_path(trial.trial_dir / "agent")
    observed_source = _normalized_host_path(agent_mounts[0].get("Source"))
    expected_parts = expected_suffix.split("/")[-4:]
    if not observed_source.endswith("/".join(expected_parts)):
        raise RuntimeError(f"{spec.key}: agent-log bind does not belong to the trial")
    for mount in mounts:
        raw_destination = str(mount.get("Destination", ""))
        try:
            destination = PurePosixPath(raw_destination)
        except (TypeError, ValueError):
            continue
        if destination == spec.workspace or destination in spec.workspace.parents:
            raise RuntimeError(
                f"{spec.key}: workspace is covered by a mount and cannot be committed"
            )
        try:
            destination.relative_to(spec.workspace)
        except ValueError:
            continue
        raise RuntimeError(f"{spec.key}: nested workspace mount would be absent from the snapshot")
    return inspect


def _safe_tar_path(raw_name: str) -> str:
    name = str(raw_name).replace("\\", "/")
    while name.startswith("./"):
        name = name[2:]
    name = name.rstrip("/")
    if name in {"", "."}:
        return "."
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\x00" in name:
        raise RuntimeError(f"Docker workspace archive contains an unsafe path: {raw_name!r}")
    return path.as_posix()


def _entry_type(member: tarfile.TarInfo) -> str:
    if member.isfile():
        return "file"
    if member.isdir():
        return "directory"
    if member.issym():
        return "symlink"
    if member.islnk():
        return "hardlink"
    if member.isfifo():
        return "fifo"
    if member.ischr():
        return "character_device"
    if member.isblk():
        return "block_device"
    raise RuntimeError(
        f"Docker workspace archive contains unsupported tar type for {member.name!r}"
    )


def _scan_and_store_blob(
    source: BinaryIO,
    *,
    blob_root: Path,
    expected_size: int,
    secret: bytes,
) -> tuple[str, bool]:
    blob_root.mkdir(parents=True, exist_ok=True)
    temporary = blob_root / f".blob.{os.getpid()}.{uuid4().hex}.tmp"
    digest = hashlib.sha256()
    written = 0
    secret_hit = False
    overlap = max(0, len(secret) - 1)
    tail = b""
    try:
        with temporary.open("wb") as handle:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                written += len(chunk)
                if secret:
                    data = tail + chunk
                    secret_hit = secret_hit or secret in data
                    tail = data[-overlap:] if overlap else b""
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        if written != expected_size:
            raise RuntimeError(
                f"Docker workspace tar member changed size ({written} != {expected_size})"
            )
        hexdigest = digest.hexdigest()
        if secret_hit:
            return hexdigest, True
        destination = blob_root / hexdigest
        if destination.exists():
            if destination.stat().st_size != written:
                raise RuntimeError(f"content-addressed blob collision at {hexdigest}")
            temporary.unlink()
        else:
            os.replace(temporary, destination)
            with suppress(OSError):
                destination.chmod(0o444)
        return hexdigest, secret_hit
    finally:
        temporary.unlink(missing_ok=True)


def _member_payload(member: tarfile.TarInfo, path: str, entry_type: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": path,
        "type": entry_type,
        "mode": int(member.mode),
        "uid": int(member.uid),
        "gid": int(member.gid),
        "uname": str(member.uname or ""),
        "gname": str(member.gname or ""),
        "mtime": float(member.mtime),
    }
    if member.pax_headers:
        payload["pax_headers"] = {
            str(key): str(value)
            for key, value in sorted(member.pax_headers.items())
            if key not in {"path", "linkpath"}
        }
    if entry_type in {"symlink", "hardlink"}:
        payload["linkname"] = str(member.linkname)
    if entry_type in {"character_device", "block_device"}:
        payload["devmajor"] = int(member.devmajor)
        payload["devminor"] = int(member.devminor)
    return payload


def _resolved_workspace_link(path: str, linkname: str) -> str:
    raw = str(linkname).replace("\\", "/")
    if not raw or "\x00" in raw or PurePosixPath(raw).is_absolute():
        raise RuntimeError(f"workspace link {path!r} has an unsafe target")
    parent = "" if path == "." else PurePosixPath(path).parent.as_posix()
    normalized = posixpath.normpath(posixpath.join(parent, raw))
    if normalized == ".." or normalized.startswith("../") or normalized.startswith("/"):
        raise RuntimeError(f"workspace link {path!r} escapes /app")
    return _safe_tar_path(normalized)


def _validate_workspace_entries(entries: list[dict[str, Any]]) -> None:
    paths: dict[str, dict[str, Any]] = {}
    for entry in entries:
        path = _safe_tar_path(str(entry.get("path", "")))
        if path in paths:
            raise RuntimeError(f"workspace archive contains duplicate path {path!r}")
        paths[path] = entry
        entry_type = str(entry.get("type", ""))
        if entry_type in {"fifo", "character_device", "block_device"}:
            raise RuntimeError(f"workspace archive contains unsafe special file {path!r}")

    symlinks = {
        path: _resolved_workspace_link(path, str(entry.get("linkname", "")))
        for path, entry in paths.items()
        if entry.get("type") == "symlink"
    }
    for path in paths:
        if path == ".":
            continue
        parents = PurePosixPath(path).parents
        if any(parent.as_posix() in symlinks for parent in parents):
            raise RuntimeError(f"workspace member {path!r} is nested below a symlink")

    for path, entry in paths.items():
        if entry.get("type") != "hardlink":
            continue
        target = _safe_tar_path(str(entry.get("linkname", "")))
        target_entry = paths.get(target)
        seen = {path}
        while target_entry is not None and target_entry.get("type") == "hardlink":
            if target in seen:
                raise RuntimeError(f"workspace hardlink cycle includes {path!r}")
            seen.add(target)
            target = _safe_tar_path(str(target_entry.get("linkname", "")))
            target_entry = paths.get(target)
        if target_entry is None or target_entry.get("type") != "file":
            raise RuntimeError(f"workspace hardlink {path!r} has an invalid target {target!r}")


def _ingest_workspace(
    container_id: str,
    *,
    blob_root: Path,
    secret: bytes,
    workspace: PurePosixPath = DEFAULT_WORKSPACE,
) -> dict[str, Any]:
    """Ingest the task workdir from a stopped committed container into a CAS."""

    with tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(
            ["docker", "cp", f"{container_id}:{workspace.as_posix()}/.", "-"],
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        if process.stdout is None:
            process.kill()
            raise RuntimeError("docker cp did not expose its tar stream")
        entries: list[dict[str, Any]] = []
        secret_paths: list[str] = []
        total_file_bytes = 0
        try:
            with tarfile.open(fileobj=process.stdout, mode="r|*") as archive:
                for member in archive:
                    path = _safe_tar_path(member.name)
                    entry_type = _entry_type(member)
                    payload = _member_payload(member, path, entry_type)
                    if entry_type == "file":
                        extracted = archive.extractfile(member)
                        if extracted is None:
                            raise RuntimeError(f"cannot read workspace file {path!r}")
                        digest, secret_hit = _scan_and_store_blob(
                            extracted,
                            blob_root=blob_root,
                            expected_size=int(member.size),
                            secret=secret,
                        )
                        payload.update(
                            {
                                "size": int(member.size),
                                "sha256": digest,
                            }
                        )
                        total_file_bytes += int(member.size)
                        if secret_hit:
                            secret_paths.append(path)
                    entries.append(payload)
        except BaseException:
            process.kill()
            process.wait(timeout=30)
            raise
        finally:
            process.stdout.close()
        exit_code = process.wait(timeout=120)
        stderr.seek(0)
        error_text = stderr.read().decode("utf-8", errors="replace")
    if exit_code != 0:
        raise RuntimeError(
            f"docker cp workspace export failed: {error_text.strip()[-1200:] or exit_code}"
        )
    if secret_paths:
        raise RuntimeError(
            "provider credential detected in workspace snapshot; refusing to publish "
            f"({len(secret_paths)} file(s))"
        )
    _validate_workspace_entries(entries)
    canonical = json.dumps(
        entries,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "schema_version": WORKSPACE_SCHEMA,
        "workspace": workspace.as_posix(),
        "entries": entries,
        "entry_count": len(entries),
        "file_count": sum(item["type"] == "file" for item in entries),
        "total_file_bytes": total_file_bytes,
        "tree_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def _workspace_manifest_hash(manifest: dict[str, Any]) -> str:
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise RuntimeError("workspace manifest entries are invalid")
    if not all(isinstance(entry, dict) for entry in entries):
        raise RuntimeError("workspace manifest contains a non-object entry")
    _validate_workspace_entries(entries)
    canonical = json.dumps(
        entries,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _snapshot_path(root: Path, spec: ArmSpec, cutoff: int) -> Path:
    return root / spec.task_name / spec.arm / f"active-{cutoff:08d}"


def _safe_remove_container(container_id: str | None) -> dict[str, Any]:
    if not container_id:
        return {"attempted": False, "succeeded": True}
    completed = _run(["docker", "rm", "-f", container_id], timeout=60)
    return {
        "attempted": True,
        "succeeded": completed.returncode == 0,
        "exit_code": completed.returncode,
    }


def _safe_remove_image(image_id: str | None) -> dict[str, Any]:
    if not image_id:
        return {"attempted": False, "succeeded": True}
    completed = _run(["docker", "image", "rm", image_id], timeout=120)
    return {
        "attempted": True,
        "succeeded": completed.returncode == 0,
        "exit_code": completed.returncode,
    }


def _ensure_unpaused(container_id: str, *, attempts: int = 3) -> None:
    errors: list[str] = []
    for _ in range(max(1, attempts)):
        completed = _run(["docker", "unpause", container_id], timeout=30)
        if completed.returncode != 0:
            errors.append((completed.stderr or completed.stdout or "").strip()[-400:])
        try:
            state = _docker_inspect(container_id).get("State") or {}
        except Exception as exc:
            errors.append(f"inspect:{type(exc).__name__}:{exc}")
        else:
            if state.get("Running") is True and state.get("Paused") is not True:
                return
            errors.append(f"state:running={state.get('Running')},paused={state.get('Paused')}")
        time.sleep(0.2)
    raise RuntimeError(
        "failed to restore the primary experiment container to an unpaused state: "
        + " | ".join(error for error in errors if error)[-1200:]
    )


def _capture_snapshot(
    *,
    spec: ArmSpec,
    trial: ActiveTrial,
    cutoff: int,
    checkpoint_root: Path,
    experiment_output: Path,
    harbor_project: Path,
    harbor_commit: str,
    harbor_source_sha256: str,
    prior_freeze_seconds: float,
    prior_completed_freeze_seconds: float,
) -> dict[str, Any]:
    final = _snapshot_path(checkpoint_root, spec, cutoff)
    if final.exists():
        metadata = _load_json(final / "metadata.json")
        if metadata.get("schema_version") != SNAPSHOT_SCHEMA:
            raise RuntimeError(f"existing snapshot is invalid: {final}")
        return metadata
    final.parent.mkdir(parents=True, exist_ok=True)
    temporary = final.parent / f".{final.name}.{uuid4().hex}.tmp"
    temporary.mkdir(parents=False, exist_ok=False)
    blob_root = checkpoint_root / "_blobs" / "sha256"
    container_id: str | None = None
    committed_image_id: str | None = None
    extraction_container_id: str | None = None
    paused = False
    pause_attempted = False
    pause_observed = False
    freeze_requested_at: str | None = None
    freeze_acknowledged_at: str | None = None
    unpaused_at: str | None = None
    freeze_active_before = trial.active_seconds
    freeze_active_after: float | None = None
    commit_started = 0.0
    commit_elapsed = 0.0
    freeze_ledger: dict[str, Any] | None = None
    freeze_ledger_path = (
        checkpoint_root
        / "_capture-freezes"
        / spec.task_name
        / spec.arm
        / trial.trial_name
        / f"cutoff-{cutoff:08d}-{uuid4().hex}.json"
    )
    try:
        refreshed = _find_trial(spec, trial.job_root.parent)
        if (
            refreshed is None
            or refreshed.trial_dir != trial.trial_dir
            or refreshed.running_invocation is None
        ):
            raise RuntimeError(f"{spec.key}: DSH invocation ended before checkpoint freeze")
        container_id = _container_id_for_trial(refreshed)
        inspect = _validate_container_identity(spec, refreshed, container_id)
        freeze_active_before_raw, _, _, _ = _invocation_active_time(refreshed.trial_dir)
        freeze_active_before = max(
            0.0,
            freeze_active_before_raw - prior_freeze_seconds,
        )
        freeze_requested_at = _now()
        pause_attempted = True
        _checked_run(["docker", "pause", container_id], timeout=30)
        paused = True
        pause_observed = True
        freeze_acknowledged_at = _now()
        freeze_active_after_raw, running_after, _, completed_active_after_raw = (
            _invocation_active_time(refreshed.trial_dir)
        )
        freeze_active_after = max(
            0.0,
            freeze_active_after_raw - prior_freeze_seconds,
        )
        completed_active_after = max(
            0.0,
            completed_active_after_raw - prior_completed_freeze_seconds,
        )
        if running_after != refreshed.running_invocation:
            raise RuntimeError(f"{spec.key}: invocation identity changed during freeze")
        if completed_active_after >= cutoff:
            raise RuntimeError(
                f"{spec.key}: cutoff predates the running invocation; exact state is unavailable"
            )
        paused_inspect = _docker_inspect(container_id)
        if (paused_inspect.get("State") or {}).get("Paused") is not True:
            raise RuntimeError(f"{spec.key}: Docker did not confirm the paused state")
        commit_started = time.monotonic()
        committed_image_id = _checked_run(
            # The container is already explicitly paused so the freeze
            # boundary can be timed. New Docker CLIs reject ``--pause=false``
            # after printing a deprecation warning; ``--no-pause`` is the
            # canonical boolean-negation form.
            ["docker", "commit", "--no-pause", container_id],
            timeout=1800,
        )
        commit_elapsed = time.monotonic() - commit_started
        if not committed_image_id.startswith("sha256:"):
            raise RuntimeError("docker commit returned an invalid image id")
        _ensure_unpaused(container_id)
        paused = False
        unpaused_at = _now()
        freeze_ledger = _write_freeze_ledger(
            freeze_ledger_path,
            spec=spec,
            trial=refreshed,
            cutoff=cutoff,
            container_id=container_id,
            requested_at=freeze_requested_at,
            acknowledged_at=freeze_acknowledged_at,
            unpaused_at=unpaused_at,
        )

        extraction_container_id = _checked_run(
            ["docker", "create", committed_image_id],
            timeout=120,
        )
        secret_value = os.environ.get(spec.credential_env, "")
        workspace_manifest = _ingest_workspace(
            extraction_container_id,
            blob_root=blob_root,
            secret=secret_value.encode("utf-8"),
            workspace=spec.workspace,
        )
        workspace_manifest["blob_store_relative_to_snapshot"] = Path(
            os.path.relpath(blob_root, temporary)
        ).as_posix()
        _atomic_write_json(temporary / "workspace-manifest.json", workspace_manifest)
        cleanup_container = _safe_remove_container(extraction_container_id)
        if cleanup_container["succeeded"]:
            extraction_container_id = None
        else:
            raise RuntimeError("temporary extraction container could not be removed")
        cleanup_image = _safe_remove_image(committed_image_id)
        committed_image_removed = cleanup_image["succeeded"]
        if not committed_image_removed:
            raise RuntimeError("temporary committed image could not be removed")

        upper_active = freeze_active_after or freeze_active_before
        metadata = {
            "schema_version": SNAPSHOT_SCHEMA,
            "snapshot_id": hashlib.sha256(
                (
                    f"{spec.key}:{cutoff}:{workspace_manifest['tree_sha256']}:"
                    f"{spec.config_sha256}:{spec.docker_image_id}"
                ).encode()
            ).hexdigest(),
            "experiment_output": str(experiment_output),
            "checkpoint_root": str(checkpoint_root),
            "snapshot_dir": str(final),
            "task_name": spec.task_name,
            "arm": spec.arm,
            "job_name": spec.job_name,
            "trial_name": refreshed.trial_name,
            "trial_dir": str(refreshed.trial_dir),
            "target_active_seconds": cutoff,
            "raw_active_seconds_before_pause": freeze_active_before_raw,
            "observed_active_seconds_before_pause": freeze_active_before,
            "raw_active_seconds_after_pause_ack": freeze_active_after_raw,
            "observed_active_seconds_after_pause_ack": upper_active,
            "prior_instrumentation_freeze_seconds_excluded": prior_freeze_seconds,
            "prior_completed_freeze_seconds_excluded": (prior_completed_freeze_seconds),
            "capture_lateness_upper_seconds": max(0.0, upper_active - cutoff),
            "active_time_source": "atomic_dsh_invocation_records_plus_utc_running_age",
            "active_time_adjustment": "subtract_prior_instrumentation_freezes",
            "running_invocation": refreshed.running_invocation,
            "invocation_count": refreshed.invocation_count,
            "freeze_requested_at": freeze_requested_at,
            "freeze_acknowledged_at": freeze_acknowledged_at,
            "unpaused_at": unpaused_at,
            "freeze_wall_seconds": freeze_ledger["freeze_wall_seconds"],
            "freeze_ledger": str(freeze_ledger_path),
            "commit_elapsed_seconds": commit_elapsed,
            "consistency": "docker_pause_then_commit_of_container_writable_layer",
            "workspace": spec.workspace.as_posix(),
            "workspace_mount_exclusion_check": "passed",
            "workspace_manifest": "workspace-manifest.json",
            "workspace_tree_sha256": workspace_manifest["tree_sha256"],
            "workspace_entry_count": workspace_manifest["entry_count"],
            "workspace_file_count": workspace_manifest["file_count"],
            "workspace_total_file_bytes": workspace_manifest["total_file_bytes"],
            "container_id": container_id,
            "container_name": str(inspect.get("Name", "")).lstrip("/"),
            "compose_project": _compose_project_name(refreshed.trial_name),
            "docker_image": spec.docker_image,
            "docker_image_id": spec.docker_image_id,
            "committed_image_id": committed_image_id,
            "committed_image_removed": committed_image_removed,
            "extraction_container_cleanup": cleanup_container,
            "config_path": str(spec.config_path),
            "config_sha256": spec.config_sha256,
            "task_root": str(spec.task_root),
            "task_content_sha256": spec.task_content_sha256,
            "credential_env": spec.credential_env,
            "harbor_project": str(harbor_project),
            "harbor_commit": harbor_commit,
            "harbor_source_sha256": harbor_source_sha256,
            "credential_scan_enabled": bool(secret_value),
            "credential_scan_passed": True,
            "harness_modified": False,
            "time_slice_enabled": False,
            "measurement_intervention": "container_freeze",
            "official_leaderboard_checkpoint": False,
            "captured_at": _now(),
        }
        _atomic_write_json(temporary / "metadata.json", metadata)
        os.replace(temporary, final)
        return metadata
    finally:
        if pause_attempted and container_id:
            try:
                state = _docker_inspect(container_id).get("State") or {}
            except Exception:
                state = {}
            if state.get("Paused") is True:
                pause_observed = True
            if paused or state.get("Paused") is True:
                _ensure_unpaused(container_id)
                paused = False
                unpaused_at = unpaused_at or _now()
            if pause_observed and freeze_ledger is None and unpaused_at is not None:
                _write_freeze_ledger(
                    freeze_ledger_path,
                    spec=spec,
                    trial=trial,
                    cutoff=cutoff,
                    container_id=container_id,
                    requested_at=freeze_requested_at,
                    acknowledged_at=freeze_acknowledged_at,
                    unpaused_at=unpaused_at,
                )
        _safe_remove_container(extraction_container_id)
        _safe_remove_image(committed_image_id)
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)


def _terminal_arm_record(output: Path, spec: ArmSpec) -> dict[str, Any] | None:
    path = output / "runs" / spec.task_name / f"{spec.arm}.json"
    if not path.is_file():
        return None
    value = _load_json(path)
    if value.get("status") not in {"completed", "failed"}:
        return None
    return value


def _initial_index(
    *,
    output: Path,
    jobs_dir: Path,
    checkpoint_root: Path,
    manifest: dict[str, Any],
    specs: list[ArmSpec],
    cutoffs: tuple[int, ...],
    poll_seconds: float,
    harbor_source_sha256: str,
) -> dict[str, Any]:
    arms: dict[str, Any] = {}
    for spec in specs:
        cutoff_state: dict[str, Any] = {}
        captured_trial_names: set[str] = set()
        for cutoff in cutoffs:
            snapshot = _snapshot_path(checkpoint_root, spec, cutoff)
            metadata = _load_json(snapshot / "metadata.json")
            if metadata.get("schema_version") == SNAPSHOT_SCHEMA:
                if metadata.get("trial_name"):
                    captured_trial_names.add(str(metadata["trial_name"]))
                cutoff_state[str(cutoff)] = {
                    "status": "captured",
                    "attempts": 1,
                    "snapshot": str(snapshot),
                    "snapshot_id": metadata.get("snapshot_id"),
                    "observed_active_seconds_after_pause_ack": metadata.get(
                        "observed_active_seconds_after_pause_ack"
                    ),
                }
            else:
                cutoff_state[str(cutoff)] = {
                    "status": "pending",
                    "attempts": 0,
                }
        if len(captured_trial_names) > 1:
            raise RuntimeError(f"{spec.key}: snapshots span multiple trial identities")
        arms[spec.key] = {
            "task_name": spec.task_name,
            "arm": spec.arm,
            "job_name": spec.job_name,
            "config_sha256": spec.config_sha256,
            "docker_image_id": spec.docker_image_id,
            "trial_name": next(iter(captured_trial_names), None),
            "last_observed_active_seconds": 0.0,
            "last_running_invocation": None,
            "cutoffs": cutoff_state,
        }
    return {
        "schema_version": INDEX_SCHEMA,
        "status": "watching",
        "experiment_output": str(output),
        "jobs_dir": str(jobs_dir),
        "checkpoint_root": str(checkpoint_root),
        "manifest_schema_version": manifest.get("schema_version"),
        "manifest_sha256_at_watcher_start": _sha256_file(output / "manifest.json"),
        "harbor_source_sha256": harbor_source_sha256,
        "cutoffs_active_seconds": list(cutoffs),
        "poll_seconds": poll_seconds,
        "capture_consistency": "docker_pause_then_commit",
        "capture_scheduling": "parallel_per_due_arm",
        "harness_modified": False,
        "time_slice_enabled": False,
        "methodological_warning": (
            "Docker freeze time consumes Harbor's wall-clock agent budget; scores are "
            "external extended-budget diagnostics, not official leaderboard checkpoints."
        ),
        "started_at": _now(),
        "updated_at": _now(),
        "arms": arms,
    }


def _index_complete(index: dict[str, Any]) -> bool:
    arms = index.get("arms") or {}
    return bool(arms) and all(
        state.get("status") in _TERMINAL_CAPTURE_STATUSES
        for arm in arms.values()
        for state in (arm.get("cutoffs") or {}).values()
    )


def _primary_arms_terminal(output: Path, specs: list[ArmSpec]) -> bool:
    return all(_terminal_arm_record(output, spec) is not None for spec in specs)


def watch(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output.resolve()
    jobs_dir = args.jobs_dir.resolve()
    checkpoint_root = (
        args.checkpoint_root.resolve()
        if args.checkpoint_root is not None
        else output / "external-checkpoints"
    )
    if checkpoint_root == output or output not in checkpoint_root.parents:
        raise RuntimeError("checkpoint root must be a descendant of the experiment output")
    cutoffs = _parse_cutoffs(args.cutoff or DEFAULT_CUTOFFS)
    poll_seconds = float(args.poll_seconds)
    if poll_seconds <= 0 or poll_seconds > 30:
        raise RuntimeError("poll seconds must be in (0, 30]")
    manifest, specs = _load_specs(output)
    missing_scan_credentials = sorted(
        {spec.credential_env for spec in specs if not os.environ.get(spec.credential_env)}
    )
    if missing_scan_credentials:
        raise RuntimeError(
            "checkpoint watcher requires the provider credential in its process "
            "environment so workspace artifacts can be scanned"
        )
    maximum_budget = int(float(manifest.get("agent_timeout_seconds") or 0))
    if maximum_budget and max(cutoffs) > maximum_budget:
        raise RuntimeError("a requested cutoff exceeds the prepared agent timeout")
    harbor = manifest.get("harbor") or {}
    harbor_project = Path(str(harbor.get("project", ""))).resolve()
    harbor_commit = str(harbor.get("commit", ""))
    if not harbor_project.is_dir() or not harbor_commit:
        raise RuntimeError("manifest Harbor provenance is incomplete")
    observed_commit = _checked_run(
        ["git", "-C", str(harbor_project), "rev-parse", "HEAD"],
        timeout=30,
    )
    if observed_commit != harbor_commit:
        raise RuntimeError("Harbor commit changed after experiment preparation")
    harbor_source_sha256 = _harbor_source_hash(harbor_project)
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    index_path = checkpoint_root / "index.json"
    if index_path.exists():
        index = _load_json(index_path)
        if index.get("schema_version") != INDEX_SCHEMA:
            raise RuntimeError(f"invalid existing checkpoint index: {index_path}")
        if index.get("cutoffs_active_seconds") != list(cutoffs):
            raise RuntimeError("existing checkpoint index uses different cutoffs")
        expected_keys = {spec.key for spec in specs}
        if set((index.get("arms") or {}).keys()) != expected_keys:
            raise RuntimeError("existing checkpoint index selects different task arms")
        index["status"] = "watching"
        index["resumed_at"] = _now()
        for arm_state in (index.get("arms") or {}).values():
            for cutoff_state in (arm_state.get("cutoffs") or {}).values():
                if cutoff_state.get("status") == "capturing":
                    cutoff_state["status"] = "pending"
                    cutoff_state["recovered_after_watcher_restart"] = True
    else:
        index = _initial_index(
            output=output,
            jobs_dir=jobs_dir,
            checkpoint_root=checkpoint_root,
            manifest=manifest,
            specs=specs,
            cutoffs=cutoffs,
            poll_seconds=poll_seconds,
            harbor_source_sha256=harbor_source_sha256,
        )
    _atomic_write_json(index_path, index)

    max_attempts = int(args.max_capture_attempts)
    if max_attempts < 1:
        raise RuntimeError("max capture attempts must be positive")
    executor = ThreadPoolExecutor(max_workers=max(1, int(args.capture_concurrency)))
    inflight: dict[Future[dict[str, Any]], tuple[ArmSpec, int]] = {}
    inflight_keys: set[str] = set()
    spec_by_key = {spec.key: spec for spec in specs}
    try:
        while True:
            for future, (spec, cutoff) in list(inflight.items()):
                if not future.done():
                    continue
                inflight.pop(future)
                inflight_keys.discard(spec.key)
                state = index["arms"][spec.key]["cutoffs"][str(cutoff)]
                try:
                    metadata = future.result()
                except Exception as exc:
                    state["last_error"] = {
                        "type": type(exc).__name__,
                        "message": str(exc)[:1200],
                        "recorded_at": _now(),
                    }
                    state["status"] = (
                        "failed" if int(state.get("attempts", 0)) >= max_attempts else "pending"
                    )
                else:
                    state.update(
                        {
                            "status": "captured",
                            "snapshot": metadata["snapshot_dir"],
                            "snapshot_id": metadata["snapshot_id"],
                            "observed_active_seconds_after_pause_ack": metadata[
                                "observed_active_seconds_after_pause_ack"
                            ],
                            "capture_lateness_upper_seconds": metadata[
                                "capture_lateness_upper_seconds"
                            ],
                            "freeze_wall_seconds": metadata["freeze_wall_seconds"],
                        }
                    )
                index["updated_at"] = _now()
                _atomic_write_json(index_path, index)

            for key, arm_state in index["arms"].items():
                if key in inflight_keys:
                    continue
                spec = spec_by_key[key]
                trial = _find_trial(spec, jobs_dir)
                if trial is not None:
                    known_trial_name = arm_state.get("trial_name")
                    if known_trial_name is None:
                        arm_state["trial_name"] = trial.trial_name
                    elif known_trial_name != trial.trial_name:
                        arm_state["trial_identity_changed"] = {
                            "original": known_trial_name,
                            "observed": trial.trial_name,
                            "recorded_at": _now(),
                        }
                        for cutoff_state in arm_state["cutoffs"].values():
                            cutoff_state["prior_status"] = cutoff_state.get("status")
                            cutoff_state["status"] = "failed"
                            cutoff_state["reason"] = "trial_identity_changed"
                        continue
                    freeze_seconds, completed_freeze_seconds = _freeze_offsets(
                        checkpoint_root,
                        spec,
                        trial,
                    )
                    adjusted_active = max(0.0, trial.active_seconds - freeze_seconds)
                    adjusted_completed = max(
                        0.0,
                        trial.completed_active_seconds - completed_freeze_seconds,
                    )
                    arm_state["last_observed_raw_active_seconds"] = trial.active_seconds
                    arm_state["last_observed_active_seconds"] = adjusted_active
                    arm_state["instrumentation_freeze_seconds_excluded"] = freeze_seconds
                    arm_state["last_running_invocation"] = trial.running_invocation
                    arm_state["last_trial_dir"] = str(trial.trial_dir)
                    arm_state["last_observed_at"] = _now()
                pending = [
                    cutoff
                    for cutoff in cutoffs
                    if arm_state["cutoffs"][str(cutoff)]["status"] == "pending"
                ]
                if not pending:
                    continue
                terminal = _terminal_arm_record(output, spec)
                if terminal is not None and (trial is None or trial.running_invocation is None):
                    final_active = 0.0 if trial is None else adjusted_active
                    for cutoff in pending:
                        state = arm_state["cutoffs"][str(cutoff)]
                        state.update(
                            {
                                "status": (
                                    "late_unavailable" if final_active >= cutoff else "not_reached"
                                ),
                                "terminal_arm_status": terminal.get("status"),
                                "final_observed_active_seconds": final_active,
                                "recorded_at": _now(),
                            }
                        )
                    continue
                cutoff = pending[0]
                freeze_seconds = 0.0 if trial is None else freeze_seconds
                completed_freeze_seconds = 0.0 if trial is None else completed_freeze_seconds
                readiness, reason = _capture_readiness(
                    trial,
                    cutoff,
                    excluded_active_seconds=freeze_seconds,
                    excluded_completed_seconds=completed_freeze_seconds,
                )
                if readiness == "pending":
                    continue
                if readiness == "late_unavailable":
                    state = arm_state["cutoffs"][str(cutoff)]
                    state.update(
                        {
                            "status": "late_unavailable",
                            "reason": reason,
                            "first_late_observed_active_seconds": adjusted_active,
                            "completed_active_seconds": adjusted_completed,
                            "recorded_at": _now(),
                        }
                    )
                    continue
                state = arm_state["cutoffs"][str(cutoff)]
                state["status"] = "capturing"
                state["attempts"] = int(state.get("attempts", 0)) + 1
                state["capture_dispatched_at"] = _now()
                state["dispatch_active_seconds"] = adjusted_active
                future = executor.submit(
                    _capture_snapshot,
                    spec=spec,
                    trial=trial,
                    cutoff=cutoff,
                    checkpoint_root=checkpoint_root,
                    experiment_output=output,
                    harbor_project=harbor_project,
                    harbor_commit=harbor_commit,
                    harbor_source_sha256=harbor_source_sha256,
                    prior_freeze_seconds=freeze_seconds,
                    prior_completed_freeze_seconds=completed_freeze_seconds,
                )
                inflight[future] = (spec, cutoff)
                inflight_keys.add(spec.key)

            index["updated_at"] = _now()
            if _index_complete(index) and not inflight and _primary_arms_terminal(output, specs):
                index["status"] = "complete"
                index["finished_at"] = _now()
                _atomic_write_json(index_path, index)
                return index
            _atomic_write_json(index_path, index)
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        index["status"] = "interrupted"
        index["interrupted_at"] = _now()
        _atomic_write_json(index_path, index)
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=False)


def _blob_root(snapshot_dir: Path, workspace_manifest: dict[str, Any]) -> Path:
    relative = workspace_manifest.get("blob_store_relative_to_snapshot")
    if not isinstance(relative, str) or not relative:
        raise RuntimeError("workspace manifest has no relative blob store")
    root = (snapshot_dir / Path(relative)).resolve()
    checkpoint_root = Path(
        str(_load_json(snapshot_dir / "metadata.json").get("checkpoint_root", ""))
    ).resolve()
    expected = (checkpoint_root / "_blobs" / "sha256").resolve()
    if root != expected:
        raise RuntimeError("workspace manifest blob store escapes the checkpoint root")
    return root


def _tar_type(entry_type: str) -> bytes:
    mapping = {
        "file": tarfile.REGTYPE,
        "directory": tarfile.DIRTYPE,
        "symlink": tarfile.SYMTYPE,
        "hardlink": tarfile.LNKTYPE,
        "fifo": tarfile.FIFOTYPE,
        "character_device": tarfile.CHRTYPE,
        "block_device": tarfile.BLKTYPE,
    }
    try:
        return mapping[entry_type]
    except KeyError as exc:
        raise RuntimeError(f"unsupported workspace entry type: {entry_type!r}") from exc


def materialize_snapshot(snapshot_dir: Path, archive_path: Path) -> dict[str, Any]:
    snapshot_dir = snapshot_dir.resolve()
    metadata = _load_json(snapshot_dir / "metadata.json")
    workspace = _load_json(snapshot_dir / "workspace-manifest.json")
    if metadata.get("schema_version") != SNAPSHOT_SCHEMA:
        raise RuntimeError("snapshot metadata schema is invalid")
    if workspace.get("schema_version") != WORKSPACE_SCHEMA:
        raise RuntimeError("workspace manifest schema is invalid")
    observed_tree = _workspace_manifest_hash(workspace)
    if observed_tree != workspace.get("tree_sha256") or observed_tree != metadata.get(
        "workspace_tree_sha256"
    ):
        raise RuntimeError("workspace manifest integrity check failed")
    blobs = _blob_root(snapshot_dir, workspace)
    archive_path = archive_path.resolve()
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive_path.with_name(f".{archive_path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        with tarfile.open(temporary, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for raw in workspace["entries"]:
                if not isinstance(raw, dict):
                    raise RuntimeError("workspace manifest contains a non-object entry")
                path = _safe_tar_path(str(raw.get("path", "")))
                name = "." if path == "." else f"./{path}"
                info = tarfile.TarInfo(name=name)
                entry_type = str(raw.get("type", ""))
                info.type = _tar_type(entry_type)
                info.mode = int(raw.get("mode", 0))
                info.uid = int(raw.get("uid", 0))
                info.gid = int(raw.get("gid", 0))
                info.uname = str(raw.get("uname", ""))
                info.gname = str(raw.get("gname", ""))
                info.mtime = float(raw.get("mtime", 0))
                pax_headers = raw.get("pax_headers")
                if isinstance(pax_headers, dict):
                    info.pax_headers = {
                        str(key): str(value)
                        for key, value in pax_headers.items()
                        if key not in {"path", "linkpath"}
                    }
                if entry_type in {"symlink", "hardlink"}:
                    info.linkname = str(raw.get("linkname", ""))
                if entry_type in {"character_device", "block_device"}:
                    info.devmajor = int(raw.get("devmajor", 0))
                    info.devminor = int(raw.get("devminor", 0))
                if entry_type == "file":
                    digest = str(raw.get("sha256", ""))
                    blob = blobs / digest
                    if not re.fullmatch(r"[0-9a-f]{64}", digest) or not blob.is_file():
                        raise RuntimeError(f"workspace blob is missing for {path!r}")
                    if _sha256_file(blob) != digest:
                        raise RuntimeError(f"workspace blob integrity failed for {path!r}")
                    expected_size = int(raw.get("size", -1))
                    if blob.stat().st_size != expected_size:
                        raise RuntimeError(f"workspace blob size failed for {path!r}")
                    info.size = expected_size
                    with blob.open("rb") as handle:
                        archive.addfile(info, handle)
                else:
                    info.size = 0
                    archive.addfile(info)
        digest = _sha256_file(temporary)
        size = temporary.stat().st_size
        os.replace(temporary, archive_path)
        return {
            "schema_version": "lhos-lhtb-materialized-workspace.v1",
            "snapshot_id": metadata.get("snapshot_id"),
            "workspace_tree_sha256": observed_tree,
            "archive_path": str(archive_path),
            "archive_sha256": digest,
            "archive_bytes": size,
            "materialized_at": _now(),
        }
    finally:
        temporary.unlink(missing_ok=True)


def materialize(args: argparse.Namespace) -> dict[str, Any]:
    result = materialize_snapshot(args.snapshot, args.archive)
    if args.metadata is not None:
        _atomic_write_json(args.metadata.resolve(), result)
    return result


def _validate_score_provenance(
    snapshot_dir: Path,
    metadata: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    experiment_output = Path(str(metadata.get("experiment_output", ""))).resolve()
    manifest, specs = _load_specs(experiment_output)
    matching = [
        spec
        for spec in specs
        if spec.task_name == metadata.get("task_name") and spec.arm == metadata.get("arm")
    ]
    if len(matching) != 1:
        raise RuntimeError("snapshot does not map to one prepared task arm")
    spec = matching[0]
    checks = {
        "snapshot_schema": metadata.get("schema_version") == SNAPSHOT_SCHEMA,
        "config_sha256": spec.config_sha256 == metadata.get("config_sha256"),
        "task_content_sha256": (
            _directory_hash(spec.task_root) == metadata.get("task_content_sha256")
        ),
        "docker_image_id": _docker_image_id(spec.docker_image) == metadata.get("docker_image_id"),
        "workspace": spec.workspace.as_posix() == metadata.get("workspace"),
    }
    harbor_project = Path(str(metadata.get("harbor_project", ""))).resolve()
    commit = _checked_run(
        ["git", "-C", str(harbor_project), "rev-parse", "HEAD"],
        timeout=30,
    )
    checks["harbor_commit"] = commit == metadata.get("harbor_commit")
    checks["harbor_source_sha256"] = _harbor_source_hash(harbor_project) == metadata.get(
        "harbor_source_sha256"
    )
    workspace = _load_json(snapshot_dir / "workspace-manifest.json")
    tree = _workspace_manifest_hash(workspace)
    checks["workspace_manifest"] = (
        tree == workspace.get("tree_sha256") == metadata.get("workspace_tree_sha256")
    )
    if not all(checks.values()):
        failed = ", ".join(key for key, passed in checks.items() if not passed)
        raise RuntimeError(f"checkpoint score provenance failed: {failed}")
    return manifest, checks, spec.config_path


def _replay_config(
    original_config: Path,
    *,
    archive_path: Path,
    archive_sha256: str,
    job_name: str,
    workspace: PurePosixPath = DEFAULT_WORKSPACE,
) -> dict[str, Any]:
    config = yaml.safe_load(original_config.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise RuntimeError("prepared Harbor config is invalid")
    config["job_name"] = job_name
    config["n_attempts"] = 1
    config["n_concurrent_trials"] = 1
    retry = config.setdefault("retry", {})
    retry["max_retries"] = 0
    environment = config.setdefault("environment", {})
    environment["delete"] = False
    mounts = list(environment.get("mounts") or [])
    if any(str(item.get("target", "")) == ARCHIVE_TARGET.as_posix() for item in mounts):
        raise RuntimeError("prepared config already uses the checkpoint archive mount")
    mounts.append(
        {
            "type": "bind",
            "source": archive_path.resolve().as_posix(),
            "target": ARCHIVE_TARGET.as_posix(),
            "read_only": True,
            "bind": {"create_host_path": False},
        }
    )
    environment["mounts"] = mounts
    config["agents"] = [
        {
            "name": None,
            "import_path": RESTORE_AGENT,
            "model_name": None,
            "override_timeout_sec": 120,
            "override_setup_timeout_sec": 120,
            "max_timeout_sec": 120,
            "kwargs": {
                "archive_path": ARCHIVE_TARGET.as_posix(),
                "workspace": workspace.as_posix(),
                "archive_sha256": archive_sha256,
            },
            "env": {},
        }
    ]
    return config


def _trial_reward(job_root: Path) -> tuple[float | None, Path | None, dict[str, Any]]:
    job_result = _load_json(job_root / "result.json")
    trial_names: set[str] = set()

    def collect(value: Any, key: str | None = None) -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                collect(child, str(child_key))
        elif isinstance(value, list):
            for child in value:
                collect(child, key)
        elif isinstance(value, str) and key in {"trial_name", "trial_id"}:
            trial_names.add(value)

    collect(job_result)
    candidates = (
        [
            child
            for child in job_root.iterdir()
            if child.is_dir() and (child / "result.json").is_file()
        ]
        if job_root.is_dir()
        else []
    )
    referenced = [item for item in candidates if item.name in trial_names]
    pool = referenced or candidates
    if not pool:
        return None, None, {}
    trial = max(pool, key=lambda item: (item / "result.json").stat().st_mtime_ns)
    result = _load_json(trial / "result.json")
    verifier = result.get("verifier_result") or {}
    reward: Any = verifier.get("reward")
    if reward is None and isinstance(verifier.get("rewards"), dict):
        reward = verifier["rewards"].get("reward")
    try:
        parsed = float(reward)
    except (TypeError, ValueError, OverflowError):
        parsed = None
    if parsed is not None and not math.isfinite(parsed):
        parsed = None
    return parsed, trial, result


def score(args: argparse.Namespace) -> dict[str, Any]:
    snapshot_dir = args.snapshot.resolve()
    metadata = _load_json(snapshot_dir / "metadata.json")
    if metadata.get("schema_version") != SNAPSHOT_SCHEMA:
        raise RuntimeError("snapshot metadata is invalid")
    manifest, provenance_checks, original_config = _validate_score_provenance(
        snapshot_dir,
        metadata,
    )
    default_score_root = (
        Path(str(metadata["experiment_output"]))
        / "external-checkpoint-scores"
        / str(metadata["task_name"])
        / str(metadata["arm"])
        / f"active-{int(metadata['target_active_seconds']):08d}"
    )
    score_root = (
        args.score_output.resolve()
        if args.score_output is not None
        else default_score_root.resolve()
    )
    if score_root.exists() and any(score_root.iterdir()):
        raise RuntimeError(f"score output already exists and is non-empty: {score_root}")
    score_root.mkdir(parents=True, exist_ok=True)
    materialized_archive = score_root / "input" / "workspace.tar"
    materialization = materialize_snapshot(snapshot_dir, materialized_archive)
    job_name = (
        f"lhtb-checkpoint-{metadata['task_name']}-{metadata['arm']}-"
        f"t{int(metadata['target_active_seconds'])}"
    )
    config_payload = _replay_config(
        original_config,
        archive_path=materialized_archive,
        archive_sha256=materialization["archive_sha256"],
        job_name=job_name,
        workspace=PurePosixPath(str(metadata["workspace"])),
    )
    config_path = score_root / "replay-config.yaml"
    config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False),
        encoding="utf-8",
    )
    jobs_dir = score_root / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    harbor_project = Path(str(manifest["harbor"]["project"])).resolve()
    env = dict(os.environ)
    env.pop(str(metadata.get("credential_env", "STEPFUN_API_KEY")), None)
    env["HB_CONTINUE_MODE"] = "same_conversation"
    env["HB_VERIFIER_FEEDBACK_MODE"] = "binary"
    python_path = os.pathsep.join((str(REPO_ROOT), str(REPO_ROOT / "src")))
    inherited = env.get("PYTHONPATH")
    env["PYTHONPATH"] = python_path if not inherited else python_path + os.pathsep + inherited
    worker_status_path = score_root / "worker-status.json"
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "harbor_job_worker.py"),
        "--harbor-project",
        str(harbor_project),
        "--config",
        str(config_path),
        "--jobs-dir",
        str(jobs_dir),
        "--timeout-seconds",
        str(float(args.timeout_seconds)),
        "--status",
        str(worker_status_path),
    ]
    started = time.monotonic()
    completed = _run(
        command,
        timeout=float(args.timeout_seconds) + 120,
        cwd=REPO_ROOT,
        env=env,
    )
    elapsed = time.monotonic() - started
    log_path = score_root / "harbor.log"
    worker_status = _load_json(worker_status_path)
    log_path.write_text(
        (completed.stdout or "")
        + "\n"
        + (completed.stderr or "")
        + "\n"
        + str(worker_status.get("stdout_tail", ""))
        + "\n"
        + str(worker_status.get("stderr_tail", "")),
        encoding="utf-8",
    )
    reward, trial_dir, trial_result = _trial_reward(jobs_dir / job_name)
    verifier = trial_result.get("verifier_result") or {}
    result = {
        "schema_version": SCORE_SCHEMA,
        "snapshot_id": metadata.get("snapshot_id"),
        "snapshot_dir": str(snapshot_dir),
        "task_name": metadata.get("task_name"),
        "arm": metadata.get("arm"),
        "target_active_seconds": metadata.get("target_active_seconds"),
        "capture_lateness_upper_seconds": metadata.get("capture_lateness_upper_seconds"),
        "freeze_wall_seconds": metadata.get("freeze_wall_seconds"),
        "reward": reward,
        "rewards": verifier.get("rewards"),
        "parse_valid": reward is not None,
        "provenance_checks": provenance_checks,
        "provenance_valid": all(provenance_checks.values()),
        "workspace_materialization": materialization,
        "replay_config": str(config_path),
        "replay_config_sha256": _sha256_file(config_path),
        "restore_agent": RESTORE_AGENT,
        "restore_agent_sha256": _sha256_file(
            REPO_ROOT / "scripts" / "lhtb_snapshot_restore_agent.py"
        ),
        "worker_launcher_exit_code": completed.returncode,
        "harbor_exit_code": worker_status.get("exit_code"),
        "worker_status": str(worker_status_path),
        "harbor_elapsed_seconds": elapsed,
        "harbor_log": str(log_path),
        "job_root": str(jobs_dir / job_name),
        "trial_dir": None if trial_dir is None else str(trial_dir),
        "original_verifier_reused": True,
        "primary_harness_modified": False,
        "time_slice_enabled": False,
        "official_leaderboard_score": False,
        "methodological_bias": (
            "The live container was frozen for snapshot consistency and Harbor's "
            "wall-clock budget continued during that freeze."
        ),
        "scored_at": _now(),
    }
    _atomic_write_json(score_root / "score.json", result)
    if not args.keep_archive:
        materialized_archive.unlink(missing_ok=True)
        result["workspace_materialization"]["archive_retained"] = False
        _atomic_write_json(score_root / "score.json", result)
    return result


def score_all(args: argparse.Namespace) -> dict[str, Any]:
    checkpoint_root = args.checkpoint_root.resolve()
    index = _load_json(checkpoint_root / "index.json")
    if index.get("schema_version") != INDEX_SCHEMA:
        raise RuntimeError("checkpoint index is invalid")
    experiment_output = Path(str(index.get("experiment_output", ""))).resolve()
    _, specs = _load_specs(experiment_output)
    nonterminal = [
        spec.key for spec in specs if _terminal_arm_record(experiment_output, spec) is None
    ]
    if nonterminal:
        raise RuntimeError("post-hoc checkpoint scoring is disabled while primary arms are running")
    snapshots: list[Path] = []
    for arm in (index.get("arms") or {}).values():
        for state in (arm.get("cutoffs") or {}).values():
            if state.get("status") == "captured" and state.get("snapshot"):
                snapshots.append(Path(str(state["snapshot"])).resolve())
    snapshots = sorted(set(snapshots))
    if not snapshots:
        raise RuntimeError("checkpoint index contains no captured snapshots")
    ledger_path = checkpoint_root / "score-all.json"
    ledger: dict[str, Any] = {
        "schema_version": "lhos-lhtb-external-checkpoint-score-all.v1",
        "status": "running",
        "checkpoint_root": str(checkpoint_root),
        "snapshot_count": len(snapshots),
        "started_at": _now(),
        "records": [],
    }
    _atomic_write_json(ledger_path, ledger)

    def run_one(snapshot: Path) -> dict[str, Any]:
        metadata = _load_json(snapshot / "metadata.json")
        expected_score = (
            experiment_output
            / "external-checkpoint-scores"
            / str(metadata["task_name"])
            / str(metadata["arm"])
            / f"active-{int(metadata['target_active_seconds']):08d}"
        )
        existing = _load_json(expected_score / "score.json")
        if existing.get("schema_version") == SCORE_SCHEMA:
            return existing
        return score(
            argparse.Namespace(
                snapshot=snapshot,
                score_output=None,
                timeout_seconds=float(args.timeout_seconds),
                keep_archive=bool(args.keep_archive),
            )
        )

    futures: dict[Future[dict[str, Any]], Path] = {}
    with ThreadPoolExecutor(max_workers=max(1, int(args.max_concurrency))) as executor:
        for snapshot in snapshots:
            futures[executor.submit(run_one, snapshot)] = snapshot
        for future, snapshot in list(futures.items()):
            try:
                value = future.result()
            except Exception as exc:
                record = {
                    "snapshot": str(snapshot),
                    "status": "failed",
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc)[:1200],
                    },
                }
            else:
                record = {
                    "snapshot": str(snapshot),
                    "status": "scored",
                    "score": value,
                }
            ledger["records"].append(record)
            _atomic_write_json(ledger_path, ledger)
    failed = sum(item["status"] == "failed" for item in ledger["records"])
    ledger.update(
        {
            "status": "complete" if failed == 0 else "completed_with_failures",
            "scored_count": len(ledger["records"]) - failed,
            "failed_count": failed,
            "finished_at": _now(),
        }
    )
    _atomic_write_json(ledger_path, ledger)
    return ledger


def status(args: argparse.Namespace) -> dict[str, Any]:
    root = args.checkpoint_root.resolve()
    index = _load_json(root / "index.json")
    if index.get("schema_version") != INDEX_SCHEMA:
        raise RuntimeError(f"invalid checkpoint index below {root}")
    return index


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    watch_parser = subparsers.add_parser(
        "watch",
        help="watch an unsliced run and capture official active-time cutoffs",
    )
    watch_parser.add_argument("--output", type=Path, required=True)
    watch_parser.add_argument("--jobs-dir", type=Path, required=True)
    watch_parser.add_argument("--checkpoint-root", type=Path)
    watch_parser.add_argument(
        "--cutoff",
        action="append",
        default=None,
        help="active seconds; repeat or pass comma-separated values",
    )
    watch_parser.add_argument("--poll-seconds", type=float, default=0.25)
    watch_parser.add_argument("--capture-concurrency", type=int, default=2)
    watch_parser.add_argument("--max-capture-attempts", type=int, default=3)
    watch_parser.set_defaults(handler=watch)

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="materialize one content-addressed snapshot as a replay tar",
    )
    materialize_parser.add_argument("--snapshot", type=Path, required=True)
    materialize_parser.add_argument("--archive", type=Path, required=True)
    materialize_parser.add_argument("--metadata", type=Path)
    materialize_parser.set_defaults(handler=materialize)

    score_parser = subparsers.add_parser(
        "score",
        help="restore one snapshot and run the pinned original Harbor verifier",
    )
    score_parser.add_argument("--snapshot", type=Path, required=True)
    score_parser.add_argument("--score-output", type=Path)
    score_parser.add_argument("--timeout-seconds", type=float, default=7_200)
    score_parser.add_argument("--keep-archive", action="store_true")
    score_parser.set_defaults(handler=score)

    score_all_parser = subparsers.add_parser(
        "score-all",
        help="score all captured snapshots after every primary arm is terminal",
    )
    score_all_parser.add_argument("--checkpoint-root", type=Path, required=True)
    score_all_parser.add_argument("--timeout-seconds", type=float, default=7_200)
    score_all_parser.add_argument("--max-concurrency", type=int, default=2)
    score_all_parser.add_argument("--keep-archive", action="store_true")
    score_all_parser.set_defaults(handler=score_all)

    status_parser = subparsers.add_parser("status", help="print watcher state")
    status_parser.add_argument("--checkpoint-root", type=Path, required=True)
    status_parser.set_defaults(handler=status)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = args.handler(args)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
