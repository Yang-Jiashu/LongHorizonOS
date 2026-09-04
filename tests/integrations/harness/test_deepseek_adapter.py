"""Direct execution tests for the DeepSeek Harness adapter."""

from __future__ import annotations

import asyncio
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
    inspect_deepseek_patch,
)
from lhos.integrations.harness.deepseek import _atomic_write_text, _long_path
from lhos.provenance import ExecutionContext
from lhos.runtimes.multi_agent.worker_pool import (
    CooperativeCancellationToken,
    CooperativeInterrupt,
)
from lhos.sdk.errors import ConfigurationError


def _context() -> ExecutionContext:
    context = ExecutionContext(
        "graph-1",
        task_id="task-1",
        attempt_id="attempt-1",
        semantic_epoch=0,
        source="test",
    )
    context.graph_version = 1
    context.agent_id = "agent-1"
    context.claim_id = "claim-1"
    context.process_id = "process-1"
    return context


def test_atomic_record_write_uses_short_temp_names_under_long_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path
    while len(str(parent)) < 215:
        parent /= "long-parent-segment"
    parent.mkdir(parents=True)
    paths = (
        parent / "p-1234567890-x-1234567890abcdef-a1.intent.json",
        parent / "p-abcdefghij-x-fedcba0987654321-a1.intent.json",
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda path: _atomic_write_text(path, "ok"), paths))

    assert [_long_path(path).read_text(encoding="utf-8") for path in paths] == [
        "ok",
        "ok",
    ]
    assert not list(_long_path(parent).glob(".tmp-*"))


def test_headless_phase_rejects_fake_native_resume() -> None:
    with pytest.raises(ConfigurationError, match="does not support native resume"):
        DeepSeekHarnessPhase(
            phase_id="task-1",
            prompt="do work",
            resume_session_id="session-1",
        )


def test_patch_route_is_authoritative_and_mismatch_fails_fast(tmp_path: Path) -> None:
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text("raise SystemExit(0)\n", encoding="utf-8")
    patch = tmp_path / "patch.yml"
    patch.write_text(
        """\
- id: agent-default-model
  config:
    provider: stepfun
    model: step-3.7-flash
- id: llm-pi-ai
  config:
    providers:
      stepfun:
        apiKeyEnv: STEPFUN_API_KEY
        api: openai-completions
        baseURL: https://api.stepfun.com/step_plan/v1
        reasoning: medium
""",
        encoding="utf-8",
    )

    route = inspect_deepseek_patch(patch)
    assert route.provider == "stepfun"
    assert route.model == "step-3.7-flash"
    assert route.reasoning_effort == "medium"

    with pytest.raises(ConfigurationError, match="does not match Cordis patch"):
        DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=Path(sys.executable),
                dsh=dsh,
                patch=patch,
                provider="stepfun",
                model="wrong-model",
                reasoning_effort="medium",
                credential_env="STEPFUN_API_KEY",
                base_url="https://api.stepfun.com/step_plan/v1",
                base_url_env="STEPFUN_BASE_URL",
                credential_values=("secret-value",),
            ),
            workspace=tmp_path,
            run_root=tmp_path / "run",
            phases=(DeepSeekHarnessPhase(phase_id="task-1", prompt="do work"),),
        )


def test_headless_command_limit_fails_before_process_start(tmp_path: Path) -> None:
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text("raise SystemExit(99)\n", encoding="utf-8")
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=Path(sys.executable),
            dsh=dsh,
            patch=patch,
            provider="test",
            model="test-model",
            reasoning_effort="low",
            credential_env="DEEPSEEK_API_KEY",
            base_url="https://example.invalid/v1",
            base_url_env="DEEPSEEK_BASE_URL",
            credential_values=("secret-value",),
            validate_patch_route=False,
            require_trace_route=False,
            validate_runtime_versions=False,
            max_command_chars=64,
        ),
        workspace=tmp_path,
        run_root=tmp_path / "run",
        phases=(
            DeepSeekHarnessPhase(
                phase_id="task-1",
                prompt="x" * 200,
            ),
        ),
    )

    with pytest.raises(ConfigurationError, match="safe argv limit"):
        asyncio.run(adapter.execute(_context(), "task-1"))

    assert not (tmp_path / "run" / "attempts").exists()


def test_runtime_version_check_rejects_node_below_minimum(tmp_path: Path) -> None:
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text(
        "import sys\nif '--version' in sys.argv: print('0.1.0-rc.8')\n",
        encoding="utf-8",
    )
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="requires Node >=22"):
        DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=Path(sys.executable),
                dsh=dsh,
                patch=patch,
                provider="test",
                model="test-model",
                reasoning_effort="low",
                credential_env="DEEPSEEK_API_KEY",
                base_url="https://example.invalid/v1",
                base_url_env="DEEPSEEK_BASE_URL",
                credential_values=("secret-value",),
                validate_patch_route=False,
                require_trace_route=False,
            ),
            workspace=tmp_path,
            run_root=tmp_path / "run",
            phases=(DeepSeekHarnessPhase(phase_id="task-1", prompt="do work"),),
        )


def test_direct_adapter_owns_process_and_redacts_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LHOS_AUDIT_UNRELATED_SECRET", "parent-secret-value")
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text(
        "import json, os, sys\n"
        "assert 'LHOS_AUDIT_UNRELATED_SECRET' not in os.environ\n"
        "home = os.environ['DSH_HOME']\n"
        "path = os.path.join(home, 'sessions', 'project')\n"
        "os.makedirs(path, exist_ok=True)\n"
        "events = [\n"
        " {'type':'session','id':'session-1'},\n"
        " {'type':'assistant/message','seq':1,'time':1,'data':{'turn':1,'step':1,'usage':{'inputTokens':4,'outputTokens':2}}},\n"
        " {'type':'tool/call','seq':2,'time':2,'data':{'callId':'call-1','name':'pwsh','arguments':json.dumps({'command':'echo secret-value'})}},\n"
        " {'type':'turn/end','seq':3,'time':3,'data':{'reason':{'kind':'completed'}}},\n"
        "]\n"
        "with open(os.path.join(path, 'session.jsonl'), 'w', encoding='utf-8') as f:\n"
        " for event in events: f.write(json.dumps(event) + '\\n')\n"
        "print('provider secret-value')\n",
        encoding="utf-8",
    )
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    run_root = tmp_path / "run"
    config = DeepSeekHarnessConfig(
        node=Path(sys.executable),
        dsh=dsh,
        patch=patch,
        provider="test",
        model="test-model",
        reasoning_effort="low",
        credential_env="DEEPSEEK_API_KEY",
        base_url="https://example.invalid/v1",
        base_url_env="DEEPSEEK_BASE_URL",
        credential_values=("secret-value",),
        retry=DeepSeekRetryPolicy(max_attempts=1),
        dsh_home_root=tmp_path / "dsh-home",
        extra_env={"DEEPSEEK_API_KEY": "wrong-value", "LHOS_DSH_API_KEYS": "wrong-value"},
        validate_patch_route=False,
        require_trace_route=False,
        validate_runtime_versions=False,
    )
    adapter = DeepSeekHarnessAdapter(
        config,
        workspace=workspace,
        run_root=run_root,
        phases=(DeepSeekHarnessPhase(phase_id="task-1", prompt="do work"),),
    )

    record = asyncio.run(adapter.execute(_context(), "task-1"))

    assert "secret-value" not in repr(config)
    assert record.completed is True
    assert record.trace.usage.total_token_units == 6
    assert [event.phase_seq for event in record.trace.events] == list(
        range(len(record.trace.events))
    )
    assert "secret-value" not in record.stdout_tail
    assert record.credential_fingerprint != "secret-value"
    assert record.trace.session_id == "session-1"
    assert record.trace.events[-1].usage_cumulative == record.trace.usage
    assert record.trace.events[-1].usage_delta.wall_time_ms > 0
    assert "secret-value" not in json.dumps(record.model_dump(mode="json"))
    assert not list((run_root / "attempts").glob("*.intent.json"))
    assert len(list((run_root / "attempts").glob("*.events.jsonl"))) == 1


def test_adapter_rejects_context_task_mismatch_before_start(tmp_path: Path) -> None:
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text("raise SystemExit(99)\n", encoding="utf-8")
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=Path(sys.executable),
            dsh=dsh,
            patch=patch,
            provider="test",
            model="test-model",
            reasoning_effort="low",
            credential_env="DEEPSEEK_API_KEY",
            base_url="https://example.invalid/v1",
            base_url_env="DEEPSEEK_BASE_URL",
            credential_values=("secret-value",),
            retry=DeepSeekRetryPolicy(max_attempts=1),
            validate_patch_route=False,
            require_trace_route=False,
            validate_runtime_versions=False,
        ),
        workspace=tmp_path,
        run_root=tmp_path / "run",
        phases=(DeepSeekHarnessPhase(phase_id="other-task", prompt="do work"),),
    )

    with pytest.raises(ConfigurationError, match="context/task mismatch"):
        asyncio.run(adapter.execute(_context(), "other-task"))

    assert not (tmp_path / "run" / "attempts").exists()


def test_adapter_retry_records_distinct_event_namespaces(tmp_path: Path) -> None:
    count_file = tmp_path / "count.txt"
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text(
        "import json, os, pathlib, sys\n"
        f"count_file=pathlib.Path({str(count_file)!r})\n"
        "count=int(count_file.read_text()) + 1 if count_file.exists() else 1\n"
        "count_file.write_text(str(count), encoding='utf-8')\n"
        "if count == 1:\n"
        " print('429 rate limit retry-after: 0.01', file=sys.stderr)\n"
        " raise SystemExit(1)\n"
        "home=pathlib.Path(os.environ['DSH_HOME'])\n"
        "path=home / 'sessions' / 'project'\n"
        "path.mkdir(parents=True, exist_ok=True)\n"
        "events=[\n"
        " {'type':'session','id':'session-2'},\n"
        " {'type':'assistant/message','seq':1,'time':1,'data':{'turn':1,'step':1,'usage':{'inputTokens':3,'outputTokens':1}}},\n"
        " {'type':'turn/end','seq':2,'time':2,'data':{'reason':{'kind':'completed'}}},\n"
        "]\n"
        "with (path / 'session.jsonl').open('w', encoding='utf-8') as f:\n"
        " for event in events: f.write(json.dumps(event) + '\\n')\n",
        encoding="utf-8",
    )
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")
    run_root = tmp_path / "run"
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=Path(sys.executable),
            dsh=dsh,
            patch=patch,
            provider="test",
            model="test-model",
            reasoning_effort="low",
            credential_env="DEEPSEEK_API_KEY",
            base_url="https://example.invalid/v1",
            base_url_env="DEEPSEEK_BASE_URL",
            credential_values=("secret-value",),
            retry=DeepSeekRetryPolicy(
                max_attempts=2,
                initial_backoff_seconds=0.01,
                max_backoff_seconds=0.01,
            ),
            dsh_home_root=tmp_path / "dsh-home",
            validate_patch_route=False,
            require_trace_route=False,
            validate_runtime_versions=False,
        ),
        workspace=tmp_path,
        run_root=run_root,
        phases=(
            DeepSeekHarnessPhase(
                phase_id="task-1",
                prompt="do work",
                max_attempts=2,
                harness_max_attempts=2,
            ),
        ),
    )

    record = asyncio.run(adapter.execute(_context(), "task-1"))
    event_files = sorted((run_root / "attempts").glob("*.events.jsonl"))
    all_events = [
        [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for path in event_files
    ]

    assert record.attempt_number == 2
    assert count_file.read_text(encoding="utf-8") == "2"
    assert len(event_files) == 2
    assert all(":a1:" in event["idempotency_key"] for event in all_events[0])
    assert all(":a2:" in event["idempotency_key"] for event in all_events[1])


async def test_preempt_preserves_partial_usage_and_attaches_outcome(
    tmp_path: Path,
) -> None:
    dsh = tmp_path / "fake_dsh.py"
    dsh.write_text(
        "import json, os, pathlib, time\n"
        "home=pathlib.Path(os.environ['DSH_HOME'])\n"
        "path=home / 'sessions' / 'project'\n"
        "path.mkdir(parents=True, exist_ok=True)\n"
        "events=[\n"
        " {'type':'session','id':'session-partial'},\n"
        " {'type':'assistant/chunk','seq':1,'time':1,'data':{'turn':1,'step':1,'chunk':{'type':'usage','usage':{'inputTokens':9,'outputTokens':2}}}},\n"
        "]\n"
        "with (path / 'session.jsonl').open('w', encoding='utf-8') as f:\n"
        " for event in events: f.write(json.dumps(event) + '\\n')\n"
        "time.sleep(120)\n",
        encoding="utf-8",
    )
    patch = tmp_path / "patch.yml"
    patch.write_text("profile: test\n", encoding="utf-8")
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=Path(sys.executable),
            dsh=dsh,
            patch=patch,
            provider="test",
            model="test-model",
            reasoning_effort="low",
            credential_env="DEEPSEEK_API_KEY",
            base_url="https://example.invalid/v1",
            base_url_env="DEEPSEEK_BASE_URL",
            credential_values=("secret-value",),
            retry=DeepSeekRetryPolicy(max_attempts=1),
            dsh_home_root=tmp_path / "dsh-home",
            validate_patch_route=False,
            require_trace_route=False,
            validate_runtime_versions=False,
        ),
        workspace=tmp_path,
        run_root=tmp_path / "run",
        phases=(DeepSeekHarnessPhase(phase_id="task-1", prompt="do work"),),
    )
    context = _context()
    token = CooperativeCancellationToken(
        "claim-1",
        loop=asyncio.get_running_loop(),
    )
    context.bind_cancellation_token(token)

    async def request_preempt() -> None:
        await asyncio.sleep(0.2)
        token.request(action="preempt", interrupt_id="interrupt-1")

    request_task = asyncio.create_task(request_preempt())
    with pytest.raises(CooperativeInterrupt):
        await adapter.execute(context, "task-1")
    await request_task
    record = adapter.latest_record("task-1")

    assert record is not None
    assert record.failure is not None
    assert record.failure.failure_class.value == "preempted"
    assert record.trace.usage.uncached_input_tokens == 9
    assert record.trace.usage.output_tokens == 2
    assert record.trace.usage.model_calls == 1
    assert token.partial_outcome == record
