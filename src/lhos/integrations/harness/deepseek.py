"""DeepSeek Harness CLI adapter with semantic phase contracts.

This adapter is the production boundary for the current DSH rc.8 integration:

* one phase is a real LongHorizonOS Task with declared inputs/outputs;
* Node DSH is the directly managed child (no Python-worker -> Node nesting);
* provider usage, tools, retries, failures, and candidate access sets are
  projected from the durable DSH session log;
* the exact Scheduler/Attempt/Context binding is persisted with every attempt;
* only an independent Task verifier can publish Evidence.

DSH ``headless`` is still one-shot. Native mid-turn cancel/resume requires a
future in-process DSH bridge; this adapter deliberately does not claim it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from pydantic import BaseModel, ConfigDict, Field

from lhos.provenance import ProvenanceOperation
from lhos.sdk import Agent, Goal
from lhos.sdk.errors import ConfigurationError, ExecutionError
from lhos.sdk.verification import VerificationOutcome

from .process import ManagedProcessResult, run_managed_process
from .protocol import (
    HarnessExecutionBinding,
    HarnessFailure,
    HarnessFailureClass,
    HarnessPhaseEvent,
    HarnessPhaseKind,
    HarnessRetryScope,
    HarnessUsage,
)

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SENSITIVE_ENV_NAME = re.compile(
    r"(?:^|_)(?:API_?KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIALS?)(?:_|$)",
    re.IGNORECASE,
)
_PATH_KEYS = ("file_path", "path", "file", "target", "directory", "root")
_READ_TOOLS = frozenset({"read", "grep", "glob"})
_WRITE_TOOLS = frozenset({"edit", "write", "rm", "remove", "delete"})
_UNKNOWN_IO_TOOLS = frozenset({"pwsh", "bash", "shell", "terminal"})
# DSH may persist internal snapshot JSONL next to the durable session. Only the
# real ``session-*`` file carries the task conversation; internal bare-UUID
# snapshots must be excluded so usage/events are never double counted.
_SESSION_FILE_ID = re.compile(r"session-[A-Za-z0-9-]+\Z")


def _is_durable_session_file(path: Path) -> bool:
    """True only for a real durable DSH session JSONL (first row is a session
    header whose id uses the ``session-`` prefix). Internal snapshots and torn
    files are skipped so token accounting is not inflated by replay copies."""

    try:
        with _long_path(path).open("r", encoding="utf-8") as stream:
            first = stream.readline()
    except OSError:
        return False
    try:
        payload = json.loads(first)
    except (TypeError, ValueError):
        return False
    return (
        isinstance(payload, dict)
        and payload.get("type") == "session"
        and _SESSION_FILE_ID.fullmatch(str(payload.get("id", "")) or "") is not None
    )


def _now_ms() -> int:
    return round(datetime.now(UTC).timestamp() * 1000)


def _safe_json(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _long_path(path: Path) -> Path:
    if os.name != "nt":
        return path
    resolved = str(path.resolve())
    return Path(resolved if resolved.startswith("\\\\?\\") else "\\\\?\\" + resolved)


def _redact(value: str, secrets: tuple[str, ...], *, limit: int = 4000) -> str:
    result = str(value)[-limit:]
    for secret in secrets:
        if secret:
            result = result.replace(secret, "[REDACTED]")
    return result


def _redact_value(value: Any, secrets: tuple[str, ...]) -> Any:
    """Recursively remove selected credentials from persisted trace values."""

    if isinstance(value, str):
        result = value
        for secret in secrets:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        return result
    if isinstance(value, Mapping):
        return {str(key): _redact_value(item, secrets) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_redact_value(item, secrets) for item in value)
    if isinstance(value, list):
        return [_redact_value(item, secrets) for item in value]
    return value


def _redact_trace(
    trace: DeepSeekTraceSummary,
    secrets: tuple[str, ...],
) -> DeepSeekTraceSummary:
    tools = tuple(
        tool.model_copy(
            update={"arguments": _redact_value(tool.arguments, secrets)},
        )
        for tool in trace.tool_calls
    )
    return trace.model_copy(
        update={
            "tool_calls": tools,
            "turn_end_reasons": _redact_value(trace.turn_end_reasons, secrets),
        }
    )


def _atomic_write_text(path: Path, value: str) -> None:
    target = _long_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _long_path(path.parent / f".tmp-{uuid4().hex[:8]}")
    try:
        temporary.write_text(value, encoding="utf-8")
        os.replace(temporary, target)
    finally:
        with suppress(OSError):
            temporary.unlink()


def _workspace_uri(workspace: Path, raw_path: str) -> str | None:
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = workspace / candidate
    try:
        relative = candidate.resolve().relative_to(workspace.resolve())
    except (OSError, ValueError):
        return None
    return "workspace://" + relative.as_posix()


def _tool_access(
    name: str,
    arguments: Mapping[str, Any],
    workspace: Path,
) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    normalized = str(name).strip().lower()
    paths = tuple(
        uri
        for key in _PATH_KEYS
        if (uri := _workspace_uri(workspace, str(arguments.get(key, "") or "")))
    )
    command = str(arguments.get("command", "") or "").strip().lower()
    if normalized in _READ_TOOLS:
        reads = tuple(sorted(set(paths)))
        return reads, (), not bool(reads)
    if normalized in _WRITE_TOOLS:
        writes = tuple(sorted(set(paths)))
        return (), writes, not bool(writes)
    if normalized == "str_replace_editor":
        if command in {"view", "grep"}:
            reads = tuple(sorted(set(paths)))
            return reads, (), not bool(reads)
        if command in {"create", "str_replace", "insert", "replace"}:
            writes = tuple(sorted(set(paths)))
            return (), writes, not bool(writes)
    if normalized in _UNKNOWN_IO_TOOLS:
        return (), (), True
    # Any tool not covered by an explicit typed contract may perform hidden
    # filesystem, network, subprocess, or database effects.  A path-shaped
    # argument is not proof of provenance, so unknown tools fail closed.
    return (), (), True


class DeepSeekToolCall(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    call_id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    started_at_ms: int = 0
    ended_at_ms: int | None = None
    duration_ms: int | None = None
    is_error: bool = False
    read_set: tuple[str, ...] = ()
    write_set: tuple[str, ...] = ()
    unknown_io: bool = False


class DeepSeekTraceSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str = ""
    provider: str = ""
    model: str = ""
    api: str = ""
    session_files: tuple[str, ...] = ()
    event_count: int = 0
    usage: HarnessUsage = Field(default_factory=HarnessUsage)
    retries: int = 0
    retry_delay_ms: int = 0
    tool_calls: tuple[DeepSeekToolCall, ...] = ()
    read_set: tuple[str, ...] = ()
    write_set: tuple[str, ...] = ()
    unknown_io: bool = False
    turn_end_reasons: tuple[dict[str, Any], ...] = ()
    events: tuple[HarnessPhaseEvent, ...] = ()


class DeepSeekAttemptRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = "deepseek-harness-attempt.v1"
    attempt_record_id: str
    phase_id: str
    phase_version: int
    attempt_number: int
    binding: HarnessExecutionBinding
    prompt_sha256: str
    command_sha256: str
    patch_sha256: str
    credential_fingerprint: str
    node_version: str = ""
    dsh_version: str = ""
    dsh_home: str
    process: dict[str, Any]
    trace: DeepSeekTraceSummary
    usage_total: HarnessUsage = Field(default_factory=HarnessUsage)
    failure: HarnessFailure | None = None
    completed: bool = False
    stdout_tail: str = ""
    stderr_tail: str = ""

    @property
    def usage(self) -> HarnessUsage:
        """Cumulative usage across all adapter subattempts in this Attempt."""

        return self.usage_total


@dataclass(frozen=True, slots=True)
class DeepSeekRetryPolicy:
    max_attempts: int = 2
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0

    def delay(self, attempt_number: int, failure: HarnessFailure) -> float:
        if failure.retry_after_seconds is not None:
            return min(self.max_backoff_seconds, failure.retry_after_seconds)
        power = max(0, attempt_number - 1)
        return min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (2**power),
        )


@dataclass(frozen=True, slots=True)
class DeepSeekPatchRoute:
    provider: str
    model: str
    reasoning_effort: str
    credential_env: str
    base_url_expression: str
    api: str


class _CordisPatchLoader(yaml.SafeLoader):
    pass


def _cordis_js_scalar(loader: yaml.SafeLoader, node: yaml.Node) -> str:
    return str(loader.construct_scalar(node))


_CordisPatchLoader.add_constructor(
    "tag:yaml.org,2002:js",
    _cordis_js_scalar,
)


def inspect_deepseek_patch(path: Path) -> DeepSeekPatchRoute:
    try:
        rows = yaml.load(path.read_text(encoding="utf-8"), Loader=_CordisPatchLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"cannot parse DeepSeek Harness patch: {path}") from exc
    if not isinstance(rows, list):
        raise ConfigurationError("DeepSeek Harness patch must be a YAML row list")
    default_config: Mapping[str, Any] | None = None
    llm_config: Mapping[str, Any] | None = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        config = row.get("config")
        if not isinstance(config, Mapping):
            continue
        if row.get("id") == "agent-default-model":
            default_config = config
        elif row.get("id") == "llm-pi-ai":
            llm_config = config
    if default_config is None or llm_config is None:
        raise ConfigurationError(
            "DeepSeek Harness patch must declare agent-default-model and llm-pi-ai"
        )
    provider = str(default_config.get("provider", "") or "").strip()
    model = str(default_config.get("model", "") or "").strip()
    providers = llm_config.get("providers")
    provider_config = providers.get(provider) if isinstance(providers, Mapping) else None
    if not provider or not model or not isinstance(provider_config, Mapping):
        raise ConfigurationError(
            "DeepSeek Harness patch has an incomplete default provider/model route"
        )
    return DeepSeekPatchRoute(
        provider=provider,
        model=model,
        reasoning_effort=str(provider_config.get("reasoning", "") or "").strip(),
        credential_env=str(provider_config.get("apiKeyEnv", "") or "").strip(),
        base_url_expression=str(provider_config.get("baseURL", "") or "").strip(),
        api=str(provider_config.get("api", "") or "").strip(),
    )


@lru_cache(maxsize=32)
def _runtime_versions(node: Path, dsh: Path) -> tuple[str, str]:
    def run(command: list[str], label: str) -> str:
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ConfigurationError(f"cannot execute {label} version check") from exc
        output = (completed.stdout or completed.stderr or "").strip().splitlines()
        if completed.returncode != 0 or not output:
            raise ConfigurationError(
                f"{label} version check failed with exit code {completed.returncode}"
            )
        return output[-1].strip()

    return (
        run([str(node.resolve()), "--version"], "Node"),
        run([str(node.resolve()), str(dsh.resolve()), "--version"], "DeepSeek Harness"),
    )


@dataclass(frozen=True, slots=True)
class DeepSeekHarnessConfig:
    node: Path
    dsh: Path
    patch: Path
    provider: str
    model: str
    reasoning_effort: str
    credential_env: str
    base_url: str
    base_url_env: str
    credential_pool_env: str | None = None
    profile: str = "headless"
    timeout_seconds: float = 900.0
    retry: DeepSeekRetryPolicy = field(default_factory=DeepSeekRetryPolicy)
    dsh_home_root: Path = field(
        default_factory=lambda: Path(tempfile.gettempdir()) / "lhos-dsh-adapter"
    )
    permission_mode: str = "workspace-write"
    telemetry_disabled: bool = True
    tools_mode: str = "native"
    extra_env: Mapping[str, str] = field(default_factory=dict)
    allowed_secret_env: tuple[str, ...] = ()
    validate_patch_route: bool = True
    require_trace_route: bool = True
    validate_runtime_versions: bool = True
    minimum_node_major: int = 22
    max_command_chars: int | None = None
    # A caller that already selected a credential (for example a benchmark
    # worker with a key-slot allocator) can pass it directly. Keeping the
    # value in memory avoids mutating the parent process environment; it is
    # never serialized and only its fingerprint is written to an attempt.
    credential_values: tuple[str, ...] = field(default=(), repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in (self.credential_env, self.base_url_env):
            if not _ENV_NAME.fullmatch(name):
                raise ConfigurationError(f"invalid environment variable name: {name!r}")
        if self.credential_pool_env is not None and not _ENV_NAME.fullmatch(
            self.credential_pool_env
        ):
            raise ConfigurationError(
                f"invalid credential pool environment variable: {self.credential_pool_env!r}"
            )
        if self.timeout_seconds <= 0:
            raise ConfigurationError("timeout_seconds must be positive")
        if self.retry.max_attempts < 1:
            raise ConfigurationError("retry.max_attempts must be >= 1")
        if any(not str(value).strip() for value in self.credential_values):
            raise ConfigurationError("credential_values must contain non-empty strings")
        for name in self.allowed_secret_env:
            if not _ENV_NAME.fullmatch(str(name)):
                raise ConfigurationError(
                    f"invalid allowed secret environment variable name: {name!r}"
                )
        if self.max_command_chars is not None and self.max_command_chars < 1:
            raise ConfigurationError("max_command_chars must be positive or None")
        if self.minimum_node_major < 1:
            raise ConfigurationError("minimum_node_major must be >= 1")

    def validate_runtime(self) -> None:
        for label, path in (
            ("node", self.node),
            ("dsh", self.dsh),
            ("patch", self.patch),
        ):
            if not Path(path).is_file():
                raise ConfigurationError(f"{label} path does not exist: {path}")
        route = inspect_deepseek_patch(self.patch) if self.validate_patch_route else None
        if route is not None:
            mismatches: list[str] = []
            for field_name, configured, observed in (
                ("provider", self.provider, route.provider),
                ("model", self.model, route.model),
                ("reasoning_effort", self.reasoning_effort, route.reasoning_effort),
                ("credential_env", self.credential_env, route.credential_env),
            ):
                if str(configured).strip() != str(observed).strip():
                    mismatches.append(f"{field_name}: config={configured!r}, patch={observed!r}")
            expression = route.base_url_expression
            if expression.startswith(("http://", "https://")):
                if expression.rstrip("/") != self.base_url.rstrip("/"):
                    mismatches.append(f"base_url: config={self.base_url!r}, patch={expression!r}")
            elif self.base_url_env not in expression:
                mismatches.append(
                    f"base_url_env: config={self.base_url_env!r}, patch={expression!r}"
                )
            if mismatches:
                raise ConfigurationError(
                    "DeepSeek Harness config does not match Cordis patch: " + "; ".join(mismatches)
                )
        if self.validate_runtime_versions:
            node_version, _dsh_version = _runtime_versions(self.node, self.dsh)
            match = re.fullmatch(r"v?([0-9]+)(?:\..*)?", node_version)
            major = int(match.group(1)) if match is not None else 0
            if major < self.minimum_node_major:
                raise ConfigurationError(
                    "DeepSeek Harness requires Node "
                    f">={self.minimum_node_major}; observed {node_version!r}"
                )


Verifier = Callable[[Any, str], VerificationOutcome] | Callable[[], VerificationOutcome]


@dataclass(frozen=True, slots=True)
class DeepSeekHarnessPhase:
    phase_id: str
    prompt: str
    dependencies: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    artifact_id: str = ""
    version: int = 1
    verifier: Verifier | None = None
    required_specializations: tuple[str, ...] = ("python",)
    required_tools: tuple[str, ...] = ()
    max_attempts: int = 1
    harness_max_attempts: int = 1
    resume_session_id: str | None = None
    resume_dsh_home: Path | None = None

    def __post_init__(self) -> None:
        if not self.phase_id.strip():
            raise ConfigurationError("DeepSeek Harness phase_id must be non-empty")
        if not self.prompt.strip():
            raise ConfigurationError(f"phase {self.phase_id!r} prompt must be non-empty")
        if self.version < 1:
            raise ConfigurationError("phase version must be >= 1")
        if self.max_attempts < 1:
            raise ConfigurationError("phase max_attempts must be >= 1")
        if self.harness_max_attempts < 1:
            raise ConfigurationError("phase harness_max_attempts must be >= 1")
        if self.resume_session_id:
            raise ConfigurationError(
                "DeepSeek headless adapter does not support native resume; "
                "use an authenticated DSH bridge or start a fresh Attempt"
            )


def _event_time(value: Any) -> datetime:
    try:
        timestamp = float(value) / 1000.0
        return datetime.fromtimestamp(timestamp, UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        return datetime.now(UTC)


def _event_int(value: Any, *, default: int = -1) -> int:
    """Coerce an optional DSH integer without making trace parsing fatal."""

    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _usage_from_payload(value: Any) -> HarnessUsage | None:
    """Parse provider usage defensively and reject negative counters."""

    if not isinstance(value, Mapping):
        return None

    def counter(name: str) -> int:
        return max(0, _event_int(value.get(name), default=0))

    return HarnessUsage(
        uncached_input_tokens=counter("inputTokens"),
        output_tokens=counter("outputTokens"),
        reasoning_tokens=counter("reasoningTokens"),
        cache_read_tokens=counter("cacheReadTokens"),
        cache_write_tokens=counter("cacheWriteTokens"),
        model_calls=1,
    )


def parse_deepseek_sessions(
    session_root: Path,
    *,
    workspace: Path,
    binding: HarnessExecutionBinding | None = None,
    phase_seq_start: int = 0,
    idempotency_namespace: str | None = None,
    event_session_id: str | None = None,
) -> DeepSeekTraceSummary:
    """Parse durable DSH JSONL defensively and deduplicate final usage."""

    scan_root = _long_path(session_root)
    files = sorted(scan_root.rglob("*.jsonl")) if scan_root.exists() else []
    files = [path for path in files if _is_durable_session_file(path)]
    # One committed usage record per logical provider call.  Each value also
    # retains its durable event position so MODEL_CALL telemetry can be
    # materialized *after* final-message-vs-chunk deduplication.
    final_usage: dict[tuple[str, int, int, int], tuple[int, datetime, HarnessUsage]] = {}
    chunk_usage: dict[tuple[str, int, int, int], tuple[int, datetime, HarnessUsage]] = {}
    tool_by_id: dict[tuple[str, str], DeepSeekToolCall] = {}
    retry_generation: dict[tuple[str, int, int], int] = {}
    seen_retries: set[tuple[str, str, int]] = set()
    seen_retry_starts: set[tuple[str, str, int]] = set()
    retries = 0
    retry_delay_ms = 0
    event_count = 0
    provider = ""
    model = ""
    api = ""
    session_id = ""
    reasons: list[dict[str, Any]] = []
    if phase_seq_start < 0:
        raise ValueError("phase_seq_start must be >= 0")
    namespace = (
        str(idempotency_namespace).strip()
        if idempotency_namespace is not None
        else (binding.attempt_id if binding is not None else "")
    )
    if binding is not None and not namespace:
        raise ValueError("idempotency_namespace must be non-empty when binding is provided")
    stable_event_session_id = str(event_session_id or "").strip()
    malformed_rows = 0
    observations: list[
        tuple[
            str,
            int,
            datetime,
            HarnessPhaseKind,
            HarnessUsage,
            tuple[str, ...],
            tuple[str, ...],
            dict[str, Any],
        ]
    ] = []

    def observe(
        source_file: str,
        source_position: int,
        phase: HarnessPhaseKind,
        *,
        emitted_at: datetime,
        usage_delta: HarnessUsage | None = None,
        read_set: tuple[str, ...] = (),
        write_set: tuple[str, ...] = (),
        details: dict[str, Any] | None = None,
    ) -> None:
        if binding is None:
            return
        observations.append(
            (
                source_file,
                source_position,
                emitted_at,
                phase,
                usage_delta or HarnessUsage(),
                read_set,
                write_set,
                details or {},
            )
        )

    for path in files:
        try:
            stream = path.open("r", encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        with stream:
            for index, raw in enumerate(stream):
                if not raw.strip():
                    continue
                try:
                    event = json.loads(raw)
                except (TypeError, ValueError):
                    # A torn final row or an interior malformed row must not make
                    # already durable partial usage unavailable.
                    malformed_rows += 1
                    continue
                if not isinstance(event, dict):
                    malformed_rows += 1
                    continue
                if index == 0 and event.get("type") == "session":
                    session_id = str(event.get("id", "") or session_id)
                    continue
                event_count += 1
                event_type = str(event.get("type", ""))
                data = event.get("data", {})
                if not isinstance(data, dict):
                    malformed_rows += 1
                    continue
                turn = _event_int(data.get("turn"))
                step = _event_int(data.get("step"))
                base_key = (str(path), turn, step)
                generation = retry_generation.get(base_key, 0)
                key = (*base_key, generation)
                emitted_at = _event_time(event.get("time"))
                source_position = _event_int(event.get("seq"), default=index)
                if source_position < 0:
                    source_position = index

                if event_type == "turn/start":
                    observe(
                        str(path),
                        source_position,
                        HarnessPhaseKind.READY,
                        emitted_at=emitted_at,
                    )
                elif event_type == "assistant/message":
                    usage = _usage_from_payload(data.get("usage"))
                    if usage is not None:
                        # A replayed message overwrites the same logical call;
                        # only the final committed usage becomes an event.
                        final_usage[key] = (source_position, emitted_at, usage)
                    message = data.get("message", {})
                    source = message.get("source", {}) if isinstance(message, dict) else {}
                    if isinstance(source, dict):
                        provider = provider or str(source.get("provider", "") or "")
                        model = model or str(source.get("model", "") or "")
                        replay = source.get("replayState", {})
                        response = replay.get("response", {}) if isinstance(replay, dict) else {}
                        if isinstance(response, dict):
                            provider = provider or str(response.get("provider", "") or "")
                            model = model or str(response.get("model", "") or "")
                            api = api or str(response.get("api", "") or "")
                elif event_type == "assistant/chunk":
                    chunk = data.get("chunk", {})
                    if (
                        isinstance(chunk, dict)
                        and chunk.get("type") == "usage"
                        and (usage := _usage_from_payload(chunk.get("usage"))) is not None
                    ):
                        # Kept as a fallback for preempted/torn steps. A later
                        # assistant/message for the same provider attempt supersedes it.
                        chunk_usage[key] = (source_position, emitted_at, usage)
                elif event_type == "llm/retry":
                    retry_id = str(data.get("retryId", "") or "")
                    retry_number = max(0, _event_int(data.get("retry"), default=0))
                    retry_key = (str(path), retry_id, retry_number)
                    if retry_key not in seen_retries:
                        seen_retries.add(retry_key)
                        retries += 1
                        if key not in final_usage and key not in chunk_usage:
                            # The durable retry boundary proves that a provider
                            # call occurred even when the failed adapter
                            # emitted no usage payload. Preserve the call count
                            # while leaving its token buckets authoritatively
                            # unknown/zero.
                            chunk_usage[key] = (
                                source_position,
                                emitted_at,
                                HarnessUsage(model_calls=1),
                            )
                        try:
                            delay = max(0, int(float(data.get("delayMs", 0) or 0)))
                        except (TypeError, ValueError, OverflowError):
                            delay = 0
                        retry_delay_ms += delay
                elif event_type == "llm/retry-started":
                    retry_id = str(data.get("retryId", "") or "")
                    retry_number = max(0, _event_int(data.get("retry"), default=0))
                    retry_key = (str(path), retry_id, retry_number)
                    if retry_key not in seen_retry_starts:
                        seen_retry_starts.add(retry_key)
                        retry_generation[base_key] = generation + 1
                elif event_type == "tool/call":
                    call_id = str(data.get("callId", "") or "")
                    if not call_id:
                        # Preserve anonymous calls without letting every empty
                        # callId collapse into one dictionary entry.
                        call_id = f"anonymous-{source_position}"
                    name = str(data.get("name", "") or "")
                    arguments = _safe_json(str(data.get("arguments", "") or ""))
                    reads, writes, unknown = _tool_access(name, arguments, workspace)
                    tool_key = (str(path), call_id)
                    tool = DeepSeekToolCall(
                        call_id=call_id,
                        name=name,
                        arguments=arguments,
                        started_at_ms=max(0, _event_int(event.get("time"), default=0)),
                        read_set=reads,
                        write_set=writes,
                        unknown_io=unknown,
                    )
                    # Replayed tool/call rows are one logical call.
                    if tool_key not in tool_by_id:
                        tool_by_id[tool_key] = tool
                        observe(
                            str(path),
                            source_position,
                            HarnessPhaseKind.TOOL_CALL,
                            emitted_at=emitted_at,
                            usage_delta=HarnessUsage(tool_calls=1),
                            read_set=reads,
                            write_set=writes,
                            details={
                                "call_id": call_id,
                                "name": name,
                                "unknown_io": unknown,
                            },
                        )
                elif event_type == "tool/result":
                    message = data.get("message", {})
                    source = message.get("source", {}) if isinstance(message, dict) else {}
                    call_id = (
                        str(source.get("callId", "") or "") if isinstance(source, dict) else ""
                    )
                    tool_key = (str(path), call_id)
                    tool = tool_by_id.get(tool_key)
                    if tool is not None:
                        ended = max(0, _event_int(event.get("time"), default=0))
                        content = message.get("content", ()) if isinstance(message, dict) else ()
                        if not isinstance(content, (list, tuple)):
                            content = ()
                        is_error = any(
                            isinstance(item, dict) and item.get("isError") is True
                            for item in content
                        )
                        tool_by_id[tool_key] = tool.model_copy(
                            update={
                                "ended_at_ms": ended,
                                "duration_ms": max(0, ended - tool.started_at_ms),
                                "is_error": is_error,
                            }
                        )
                elif event_type == "turn/end":
                    reason = data.get("reason")
                    if isinstance(reason, dict):
                        reasons.append(dict(reason))

    usage_by_step = dict(chunk_usage)
    usage_by_step.update(final_usage)
    # Materialize MODEL_CALL only after fallback selection, so a chunk plus its
    # final assistant/message contributes exactly one call and one token count.
    for (
        source_file,
        turn,
        step,
        generation,
    ), (source_position, emitted_at, usage) in usage_by_step.items():
        observe(
            source_file,
            source_position,
            HarnessPhaseKind.MODEL_CALL,
            emitted_at=emitted_at,
            usage_delta=usage,
            details={"turn": turn, "step": step, "retry_generation": generation},
        )

    phase_events: list[HarnessPhaseEvent] = []
    cumulative = HarnessUsage()
    parent_event_id: str | None = None
    observations.sort(key=lambda item: (item[0], item[1], item[2]))
    for offset, (
        _source_file,
        _source_position,
        emitted_at,
        phase,
        delta,
        reads_delta,
        writes_delta,
        details,
    ) in enumerate(observations):
        phase_seq = phase_seq_start + offset
        cumulative = cumulative.plus(delta)
        event = HarnessPhaseEvent(
            session_id=stable_event_session_id or session_id or binding.attempt_id,  # type: ignore[union-attr]
            phase_seq=phase_seq,
            emitted_at=emitted_at,
            phase=phase,
            binding=binding,  # type: ignore[arg-type]
            usage_delta=delta,
            usage_cumulative=cumulative,
            read_set_delta=reads_delta,
            write_set_delta=writes_delta,
            idempotency_key=f"{namespace}:{phase_seq}:{phase.value}",
            parent_event_id=parent_event_id,
            details=details,
        )
        phase_events.append(event)
        parent_event_id = event.event_id

    totals = HarnessUsage()
    for _source_position, _emitted_at, usage in usage_by_step.values():
        totals = totals.plus(usage)
    totals = totals.model_copy(update={"tool_calls": len(tool_by_id)})
    tools = tuple(
        sorted(
            tool_by_id.values(),
            key=lambda item: (item.started_at_ms, item.call_id),
        )
    )
    reads = tuple(sorted({path for tool in tools for path in tool.read_set}))
    writes = tuple(sorted({path for tool in tools for path in tool.write_set}))
    return DeepSeekTraceSummary(
        session_id=session_id,
        provider=provider,
        model=model,
        api=api,
        session_files=tuple(str(path) for path in files),
        event_count=event_count,
        usage=totals,
        retries=retries,
        retry_delay_ms=retry_delay_ms,
        tool_calls=tools,
        read_set=reads,
        write_set=writes,
        unknown_io=malformed_rows > 0 or any(tool.unknown_io for tool in tools),
        turn_end_reasons=tuple(reasons),
        events=tuple(phase_events),
    )


def classify_deepseek_failure(
    process: ManagedProcessResult,
    trace: DeepSeekTraceSummary,
) -> HarnessFailure | None:
    reason_kinds = tuple(
        str(reason.get("kind", "") or "").strip().lower()
        for reason in trace.turn_end_reasons
        if isinstance(reason, Mapping)
    )
    structured_reason = json.dumps(
        trace.turn_end_reasons,
        ensure_ascii=True,
        sort_keys=True,
    )
    # DSH headless writes the assistant's successful final text to stdout.
    # Error classification must therefore prefer structured turn/end state and
    # stderr; otherwise a valid answer that merely discusses "429" or
    # "network errors" is misclassified as a provider failure.
    text = "\n".join(
        item
        for item in (
            process.stderr_tail,
            structured_reason if trace.turn_end_reasons else "",
            process.stdout_tail
            if process.exit_code not in (None, 0)
            and not process.stderr_tail
            and not trace.turn_end_reasons
            else "",
        )
        if item
    ).strip()
    lowered = text.lower()
    status_match = re.search(r"\b([1-5][0-9]{2})\b", text)
    status_code = int(status_match.group(1)) if status_match else None
    retry_after_match = re.search(
        r"\bretry[- ]after\b\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)",
        lowered,
    )
    retry_after_seconds = (
        float(retry_after_match.group(1)) if retry_after_match is not None else None
    )
    if process.terminated_by == "semantic_interrupt":
        return HarnessFailure.from_message(
            HarnessFailureClass.PREEMPTED,
            text or "DeepSeek Harness was preempted",
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
        )
    if process.timed_out:
        return HarnessFailure.from_message(
            HarnessFailureClass.TIMEOUT,
            text or "DeepSeek Harness exceeded its wall-clock timeout",
            retryable=True,
            retry_scope=HarnessRetryScope.ATTEMPT,
        )
    if process.terminated_by == "pipe_drain_timeout":
        return HarnessFailure.from_message(
            HarnessFailureClass.SANDBOX_ERROR,
            "DeepSeek Harness left descendant processes holding output pipes",
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
        )
    if process.exit_code == 0:
        if trace.usage.model_calls == 0:
            return HarnessFailure.from_message(
                HarnessFailureClass.PROTOCOL_MALFORMED,
                "DeepSeek Harness completed without committed provider usage",
                retryable=False,
                retry_scope=HarnessRetryScope.NONE,
            )
        if reason_kinds and reason_kinds[-1] != "completed":
            return HarnessFailure.from_message(
                HarnessFailureClass.PROTOCOL_MALFORMED,
                f"DeepSeek Harness exited successfully with turn reason {reason_kinds[-1]!r}",
                retryable=False,
                retry_scope=HarnessRetryScope.NONE,
            )
        return None
    if any(kind in {"cancelled", "aborted", "interrupted"} for kind in reason_kinds):
        return HarnessFailure.from_message(
            HarnessFailureClass.CANCELLED,
            text or "DeepSeek Harness turn was cancelled",
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
        )
    if "max-tokens" in reason_kinds:
        return HarnessFailure.from_message(
            HarnessFailureClass.INVALID_REQUEST,
            text or "DeepSeek Harness reached its maximum token limit",
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
        )
    if "blocked" in reason_kinds:
        return HarnessFailure.from_message(
            HarnessFailureClass.INVALID_REQUEST,
            text or "DeepSeek Harness blocked the turn before execution",
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
        )
    if status_code == 451 or any(
        token in lowered
        for token in (
            "censorship_blocked",
            "content policy",
            "content_policy",
            "moderation blocked",
        )
    ):
        return HarnessFailure.from_message(
            HarnessFailureClass.CONTENT_POLICY,
            text,
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
            status_code=status_code or 451,
        )
    if any(token in lowered for token in ("unauthorized", "invalid api key", "authentication")):
        return HarnessFailure.from_message(
            HarnessFailureClass.AUTH,
            text,
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
            status_code=status_code,
        )
    if any(token in lowered for token in ("rpm exhausted", "tpm exhausted", "rate limit", "429")):
        return HarnessFailure.from_message(
            HarnessFailureClass.RATE_LIMIT,
            text,
            retryable=True,
            retry_scope=HarnessRetryScope.PROVIDER,
            status_code=status_code or 429,
            retry_after_seconds=retry_after_seconds,
        )
    if "quota_exceeded" in lowered:
        return HarnessFailure.from_message(
            HarnessFailureClass.RATE_LIMIT,
            text,
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
            status_code=status_code,
        )
    if status_code is not None and 500 <= status_code <= 599:
        return HarnessFailure.from_message(
            HarnessFailureClass.PROVIDER_5XX,
            text,
            retryable=True,
            retry_scope=HarnessRetryScope.PROVIDER,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
        )
    if any(
        token in lowered for token in ("connection reset", "timed out", "dns", "econn", "network")
    ):
        return HarnessFailure.from_message(
            HarnessFailureClass.NETWORK_TRANSIENT,
            text,
            retryable=True,
            retry_scope=HarnessRetryScope.ATTEMPT,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
        )
    if status_code is not None and 400 <= status_code <= 499:
        return HarnessFailure.from_message(
            HarnessFailureClass.INVALID_REQUEST,
            text,
            retryable=False,
            retry_scope=HarnessRetryScope.NONE,
            status_code=status_code,
        )
    if process.exit_code not in (None, 0):
        return HarnessFailure.from_message(
            HarnessFailureClass.EXIT_NONZERO,
            text or f"DeepSeek Harness exited with code {process.exit_code}",
            retryable=True,
            retry_scope=HarnessRetryScope.ATTEMPT,
            status_code=status_code,
        )
    return None


class DeepSeekHarnessAdapter:
    """Compile semantic phase contracts into preemptible DSH executions."""

    def __init__(
        self,
        config: DeepSeekHarnessConfig,
        *,
        workspace: Path,
        run_root: Path,
        phases: tuple[DeepSeekHarnessPhase, ...],
        extra_env: Mapping[str, str] | None = None,
    ) -> None:
        config.validate_runtime()
        self.config = config
        self.workspace = workspace.resolve()
        self.run_root = run_root.resolve()
        self.phases = {phase.phase_id: phase for phase in phases}
        self.extra_env = dict(extra_env or {})
        self.patch_sha256 = hashlib.sha256(config.patch.read_bytes()).hexdigest()
        self.node_version, self.dsh_version = (
            _runtime_versions(config.node, config.dsh)
            if config.validate_runtime_versions
            else ("", "")
        )
        self._latest: dict[str, DeepSeekAttemptRecord] = {}
        self._validate_phases()

    def _validate_phases(self) -> None:
        if not self.phases:
            raise ConfigurationError("DeepSeek Harness adapter requires at least one phase")
        for phase in self.phases.values():
            missing = sorted(set(phase.dependencies) - set(self.phases))
            if missing:
                raise ConfigurationError(
                    f"phase {phase.phase_id!r} has unknown dependencies: {missing}"
                )
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(phase_id: str) -> None:
            if phase_id in visiting:
                raise ConfigurationError("DeepSeek Harness phase graph contains a cycle")
            if phase_id in visited:
                return
            visiting.add(phase_id)
            for dependency in self.phases[phase_id].dependencies:
                visit(dependency)
            visiting.remove(phase_id)
            visited.add(phase_id)

        for phase_id in self.phases:
            visit(phase_id)

    def _credential_keys(self) -> tuple[str, ...]:
        if self.config.credential_values:
            return tuple(str(item) for item in self.config.credential_values)
        source = self.config.credential_pool_env or self.config.credential_env
        raw = os.environ.get(source, "").strip()
        keys = tuple(item.strip() for item in raw.split(",") if item.strip())
        if not keys:
            raise ConfigurationError(f"{source} is required for DeepSeek Harness")
        return keys

    def _environment(self, key: str, dsh_home: Path) -> dict[str, str]:
        env = dict(os.environ)
        env.pop("LHOS_DSH_API_KEYS", None)
        for name in {
            "DEEPSEEK_API_KEY",
            "STEPFUN_API_KEY",
            self.config.credential_env,
            self.config.credential_pool_env,
        }:
            if name:
                env.pop(name, None)
        env[self.config.credential_env] = key
        env[self.config.base_url_env] = self.config.base_url
        env["DSH_HOME"] = str(dsh_home)
        env["DSH_PERMISSION_MODE"] = self.config.permission_mode
        env["DSH_TELEMETRY_DISABLED"] = "1" if self.config.telemetry_disabled else "0"
        env["DSH_TOOLS_MODE"] = self.config.tools_mode
        env["PYTHONIOENCODING"] = "utf-8"
        env.update({str(k): str(v) for k, v in self.config.extra_env.items()})
        env.update({str(k): str(v) for k, v in self.extra_env.items()})
        # Keep credential and route authority with the adapter even when a
        # caller supplies a broad inherited environment for tools.
        for name in {
            "LHOS_DSH_API_KEYS",
            "DEEPSEEK_API_KEY",
            "STEPFUN_API_KEY",
            self.config.credential_env,
            self.config.credential_pool_env,
        }:
            if name:
                env.pop(name, None)
        allowed_secrets = {
            self.config.credential_env,
            *(str(name) for name in self.config.allowed_secret_env),
        }
        for name in tuple(env):
            if _SENSITIVE_ENV_NAME.search(name) and name not in allowed_secrets:
                env.pop(name, None)
        env[self.config.credential_env] = key
        env[self.config.base_url_env] = self.config.base_url
        env["DSH_HOME"] = str(dsh_home)
        env["DSH_PERMISSION_MODE"] = self.config.permission_mode
        env["DSH_TELEMETRY_DISABLED"] = "1" if self.config.telemetry_disabled else "0"
        env["DSH_TOOLS_MODE"] = self.config.tools_mode
        return env

    def _home_for(
        self,
        binding: HarnessExecutionBinding,
        phase: DeepSeekHarnessPhase,
        attempt_number: int,
    ) -> Path:
        basis = (
            f"{binding.graph_id}|{binding.claim_id}|{binding.attempt_id}|"
            f"{phase.phase_id}|{phase.version}|{attempt_number}"
        )
        digest = hashlib.sha256(basis.encode()).hexdigest()[:20]
        home = self.config.dsh_home_root.resolve() / f"session-{digest}"
        home.mkdir(parents=True, exist_ok=True)
        return home

    def _command(
        self,
        phase: DeepSeekHarnessPhase,
    ) -> list[str]:
        command = [
            str(self.config.node.resolve()),
            str(self.config.dsh.resolve()),
            "--profile",
            self.config.profile,
            "--patch",
            str(self.config.patch.resolve()),
        ]
        if phase.resume_session_id:
            command.extend(("--resume", phase.resume_session_id))
        command.append(phase.prompt)
        limit = self.config.max_command_chars
        if limit is None:
            limit = 30_000 if os.name == "nt" else 1_000_000
        command_chars = sum(len(item) + 3 for item in command)
        if command_chars > limit:
            raise ConfigurationError(
                "DeepSeek headless command exceeds the safe argv limit "
                f"({command_chars} > {limit}); use a DSH bridge/task-file transport"
            )
        return command

    def _record_provenance(
        self,
        context: Any,
        phase: DeepSeekHarnessPhase,
        trace: DeepSeekTraceSummary,
    ) -> None:
        for uri in trace.read_set:
            context.record(
                ProvenanceOperation.READ,
                resource_uri=uri,
                source="deepseek-harness",
                metadata={"phase_id": phase.phase_id, "authority": "typed_tool_candidate"},
            )
        for uri in trace.write_set:
            context.record(
                ProvenanceOperation.WRITE,
                resource_uri=uri,
                source="deepseek-harness",
                metadata={"phase_id": phase.phase_id, "authority": "typed_tool_candidate"},
            )
        for tool in trace.tool_calls:
            context.record_tool(
                tool.name,
                action_id=tool.call_id or None,
                known=not tool.unknown_io,
                phase_id=phase.phase_id,
                is_error=tool.is_error,
            )
        if trace.model:
            context.record_model(
                trace.model,
                prompt_hash=hashlib.sha256(phase.prompt.encode()).hexdigest(),
                provider=trace.provider,
                api=trace.api,
                reasoning_effort=self.config.reasoning_effort,
            )
        if trace.unknown_io:
            context.observe_unknown(
                resource_hint="dsh://unknown-tool-access",
                phase_id=phase.phase_id,
            )

    def _record_paths(
        self,
        *,
        phase_id: str,
        binding: HarnessExecutionBinding,
        attempt_number: int,
    ) -> tuple[Path, Path, Path]:
        attempts = self.run_root / "attempts"
        attempts.mkdir(parents=True, exist_ok=True)
        phase_hash = hashlib.sha256(str(phase_id).encode()).hexdigest()[:10]
        attempt_hash = hashlib.sha256(binding.attempt_id.encode()).hexdigest()[:16]
        stem = f"p-{phase_hash}-x-{attempt_hash}-a{attempt_number}"
        return (
            attempts / f"{stem}.json",
            attempts / f"{stem}.events.jsonl",
            attempts / f"{stem}.intent.json",
        )

    def _write_intent(
        self,
        *,
        phase: DeepSeekHarnessPhase,
        binding: HarnessExecutionBinding,
        attempt_number: int,
        dsh_home: Path,
        command: list[str],
        credential_fingerprint: str,
        starting: HarnessPhaseEvent,
        pid: int,
    ) -> Path:
        _record_path, _event_path, intent_path = self._record_paths(
            phase_id=phase.phase_id,
            binding=binding,
            attempt_number=attempt_number,
        )
        payload = {
            "schema_version": "deepseek-harness-intent.v1",
            "status": "running",
            "recorded_at_ms": _now_ms(),
            "phase_id": phase.phase_id,
            "phase_version": phase.version,
            "attempt_number": attempt_number,
            "binding": binding.model_dump(mode="json"),
            "pid": pid,
            "dsh_home": str(dsh_home),
            "command_sha256": hashlib.sha256("\0".join(command).encode()).hexdigest(),
            "patch_sha256": self.patch_sha256,
            "prompt_sha256": hashlib.sha256(phase.prompt.encode()).hexdigest(),
            "credential_fingerprint": credential_fingerprint,
            "node_version": self.node_version,
            "dsh_version": self.dsh_version,
            "starting_event": starting.model_dump(mode="json"),
        }
        _atomic_write_text(
            intent_path,
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
        )
        return intent_path

    def _write_record(self, record: DeepSeekAttemptRecord) -> Path:
        path, event_path, intent_path = self._record_paths(
            phase_id=record.phase_id,
            binding=record.binding,
            attempt_number=record.attempt_number,
        )
        _atomic_write_text(
            event_path,
            "\n".join(
                json.dumps(
                    event.model_dump(mode="json"),
                    ensure_ascii=True,
                    sort_keys=True,
                )
                for event in record.trace.events
            )
            + ("\n" if record.trace.events else ""),
        )
        _atomic_write_text(
            path,
            json.dumps(
                record.model_dump(mode="json"),
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            ),
        )
        with suppress(OSError):
            intent_path.unlink()
        return path

    @staticmethod
    async def _wait_retry_delay(
        context: Any,
        delay_seconds: float,
        partial_record: DeepSeekAttemptRecord,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + max(0.0, delay_seconds)
        cancellation_token = getattr(context, "cancellation_token", None)
        while True:
            if cancellation_token is not None and bool(
                getattr(cancellation_token, "request_pending", False)
            ):
                attach_partial = getattr(
                    cancellation_token,
                    "attach_partial_outcome",
                    None,
                )
                if callable(attach_partial):
                    attach_partial(partial_record)
                context.raise_if_interrupted()
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.05, remaining))

    async def execute(self, context: Any, task_id: str) -> DeepSeekAttemptRecord:
        normalized_task_id = str(task_id)
        phase = self.phases.get(normalized_task_id)
        if phase is None:
            raise ConfigurationError(
                f"DeepSeek Harness task {normalized_task_id!r} is not a declared phase"
            )
        context_task_id = str(getattr(context, "task_id", "") or "")
        if context_task_id != normalized_task_id:
            raise ConfigurationError(
                "DeepSeek Harness execution context/task mismatch: "
                f"context={context_task_id!r}, requested={normalized_task_id!r}"
            )
        keys = self._credential_keys()
        last: DeepSeekAttemptRecord | None = None
        usage_total = HarnessUsage()
        attempt_limit = min(
            self.config.retry.max_attempts,
            phase.harness_max_attempts,
        )
        binding = HarnessExecutionBinding.from_execution_context(
            context,
            workspace_id=str(self.workspace),
            session_cursor=phase.resume_session_id,
        )
        for attempt_number in range(1, attempt_limit + 1):
            key = keys[(attempt_number - 1) % len(keys)]
            credential_fingerprint = hashlib.sha256(key.encode()).hexdigest()[:16]
            event_namespace = f"{binding.attempt_id}:a{attempt_number}"
            dsh_home = (
                phase.resume_dsh_home.resolve()
                if phase.resume_dsh_home is not None
                else self._home_for(binding, phase, attempt_number)
            )
            command = self._command(phase)
            starting = HarnessPhaseEvent(
                session_id=binding.attempt_id,
                phase_seq=0,
                emitted_at=datetime.now(UTC),
                phase=HarnessPhaseKind.STARTING,
                binding=binding,
                idempotency_key=f"{event_namespace}:0:starting",
                details={
                    "provider": self.config.provider,
                    "model": self.config.model,
                    "reasoning_effort": self.config.reasoning_effort,
                },
            )

            def persist_intent(
                pid: int,
                *,
                _attempt_number: int = attempt_number,
                _dsh_home: Path = dsh_home,
                _command: list[str] = command,
                _credential_fingerprint: str = credential_fingerprint,
                _starting: HarnessPhaseEvent = starting,
            ) -> None:
                self._write_intent(
                    phase=phase,
                    binding=binding,
                    attempt_number=_attempt_number,
                    dsh_home=_dsh_home,
                    command=_command,
                    credential_fingerprint=_credential_fingerprint,
                    starting=_starting,
                    pid=pid,
                )

            process = await run_managed_process(
                command,
                cwd=self.workspace,
                env=self._environment(key, dsh_home),
                timeout_seconds=self.config.timeout_seconds,
                cancellation_token=getattr(context, "cancellation_token", None),
                on_started=persist_intent,
            )
            trace = parse_deepseek_sessions(
                dsh_home / "sessions",
                workspace=self.workspace,
                binding=binding,
                # Sequence zero is reserved for the adapter's durable
                # STARTING event.  Parser events must continue strictly after
                # it so replay consumers never see duplicate sequence ids.
                phase_seq_start=1,
                idempotency_namespace=event_namespace,
                event_session_id=binding.attempt_id,
            )
            trace = _redact_trace(trace, keys)
            safe_process = replace(
                process,
                stdout_tail=_redact(process.stdout_tail, keys),
                stderr_tail=_redact(process.stderr_tail, keys),
            )
            failure = classify_deepseek_failure(safe_process, trace)
            if failure is None and self.config.require_trace_route:
                route_mismatches: list[str] = []
                for field_name, configured, observed in (
                    ("provider", self.config.provider, trace.provider),
                    ("model", self.config.model, trace.model),
                ):
                    if not observed or str(observed).strip() != str(configured).strip():
                        route_mismatches.append(
                            f"{field_name}: config={configured!r}, trace={observed!r}"
                        )
                if route_mismatches:
                    failure = HarnessFailure.from_message(
                        HarnessFailureClass.PROTOCOL_MALFORMED,
                        "DeepSeek Harness trace route mismatch: " + "; ".join(route_mismatches),
                        retryable=False,
                        retry_scope=HarnessRetryScope.NONE,
                    )
            events = (starting, *trace.events)
            wall_delta = HarnessUsage(wall_time_ms=process.elapsed_ms)
            cumulative = (events[-1].usage_cumulative if events else HarnessUsage()).plus(
                wall_delta
            )
            trace_usage = trace.usage.plus(wall_delta)
            trace = trace.model_copy(
                update={"usage": trace_usage},
            )
            usage_total = usage_total.plus(trace_usage)
            terminal_phase = (
                HarnessPhaseKind.PREEMPTED
                if failure is not None and failure.failure_class is HarnessFailureClass.PREEMPTED
                else HarnessPhaseKind.FAILED
                if failure is not None
                else HarnessPhaseKind.COMPLETED
            )
            terminal = HarnessPhaseEvent(
                session_id=binding.attempt_id,
                phase_seq=len(events),
                phase=terminal_phase,
                binding=binding,
                usage_delta=wall_delta,
                usage_cumulative=cumulative,
                failure=failure,
                idempotency_key=(f"{event_namespace}:{len(events)}:{terminal_phase.value}"),
                parent_event_id=events[-1].event_id if events else None,
                artifact_refs=phase.outputs,
                details={"dsh_session_id": trace.session_id},
            )
            trace = trace.model_copy(update={"events": (*events, terminal)})
            record = DeepSeekAttemptRecord(
                attempt_record_id=uuid4().hex,
                phase_id=phase.phase_id,
                phase_version=phase.version,
                attempt_number=attempt_number,
                binding=binding,
                prompt_sha256=hashlib.sha256(phase.prompt.encode()).hexdigest(),
                command_sha256=hashlib.sha256("\0".join(command).encode()).hexdigest(),
                patch_sha256=self.patch_sha256,
                credential_fingerprint=credential_fingerprint,
                node_version=self.node_version,
                dsh_version=self.dsh_version,
                dsh_home=str(dsh_home),
                process={
                    "pid": process.pid,
                    "exit_code": process.exit_code,
                    "elapsed_ms": process.elapsed_ms,
                    "timed_out": process.timed_out,
                    "terminated_by": process.terminated_by,
                    "hard_killed": process.hard_killed,
                },
                trace=trace,
                usage_total=usage_total,
                failure=failure,
                completed=failure is None,
                stdout_tail=safe_process.stdout_tail,
                stderr_tail=safe_process.stderr_tail,
            )
            self._write_record(record)
            self._latest[phase.phase_id] = record
            self._record_provenance(context, phase, trace)
            last = record

            if process.terminated_by == "semantic_interrupt":
                cancellation_token = getattr(context, "cancellation_token", None)
                attach_partial = getattr(
                    cancellation_token,
                    "attach_partial_outcome",
                    None,
                )
                if callable(attach_partial):
                    attach_partial(record)
                context.raise_if_interrupted()
            if failure is None:
                return record
            if not failure.retryable or attempt_number >= attempt_limit:
                break
            await self._wait_retry_delay(
                context,
                self.config.retry.delay(attempt_number, failure),
                record,
            )

        assert last is not None
        failure = last.failure
        summary = (
            "unknown DeepSeek Harness failure"
            if failure is None
            else f"{failure.failure_class.value}: {failure.summary}"
        )
        error = ExecutionError(
            f"DeepSeek Harness phase {phase.phase_id!r} failed after "
            f"{last.attempt_number} attempt(s): {summary}"
        )
        error.partial_outcome = last
        raise error

    def latest_record(self, phase_id: str) -> DeepSeekAttemptRecord | None:
        return self._latest.get(str(phase_id))

    def agent(
        self,
        name: str = "deepseek-harness",
        *,
        max_concurrency: int = 1,
    ) -> Agent:
        return Agent(
            name,
            executor=self.execute,
            executor_api="context_v1",
            specializations=("python", "harness"),
            supported_tools=("filesystem", "shell"),
            max_concurrency=max_concurrency,
            model=self.config.model,
        )

    def goal(
        self,
        goal_id: str,
        *,
        agent_name: str = "deepseek-harness",
    ) -> Goal:
        goal = Goal(goal_id, executor_api="context_v1")
        tasks: dict[str, Any] = {}
        pending = set(self.phases)
        while pending:
            ready = sorted(
                phase_id
                for phase_id in pending
                if set(self.phases[phase_id].dependencies) <= set(tasks)
            )
            if not ready:
                raise ConfigurationError("DeepSeek Harness phase graph cannot be compiled")
            for phase_id in ready:
                phase = self.phases[phase_id]
                tasks[phase_id] = goal.task(
                    phase_id,
                    agent=agent_name,
                    depends_on=tuple(tasks[item] for item in phase.dependencies),
                    verify=phase.verifier,
                    required_specializations=phase.required_specializations,
                    required_tools=phase.required_tools,
                    max_attempts=phase.max_attempts,
                    inputs=phase.inputs,
                    outputs=phase.outputs,
                    metadata={
                        "harness": {
                            "kind": "deepseek",
                            "provider": self.config.provider,
                            "model": self.config.model,
                            "reasoning_effort": self.config.reasoning_effort,
                            "phase_version": phase.version,
                        }
                    },
                    executor_api="context_v1",
                )
                pending.remove(phase_id)
        return goal


__all__ = [
    "DeepSeekAttemptRecord",
    "DeepSeekHarnessAdapter",
    "DeepSeekHarnessConfig",
    "DeepSeekHarnessPhase",
    "DeepSeekPatchRoute",
    "DeepSeekRetryPolicy",
    "DeepSeekToolCall",
    "DeepSeekTraceSummary",
    "classify_deepseek_failure",
    "inspect_deepseek_patch",
    "parse_deepseek_sessions",
]
