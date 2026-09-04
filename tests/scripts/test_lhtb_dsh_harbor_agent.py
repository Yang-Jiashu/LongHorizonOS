from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def agent_module(monkeypatch: pytest.MonkeyPatch):
    """Load the custom agent with a tiny Harbor API double.

    Harbor is an external benchmark dependency and is intentionally not part of
    the LHOS package dependencies. The production import is exercised by Harbor;
    these tests isolate the adapter contract from that optional installation.
    """

    class BaseInstalledAgent:
        def __init__(
            self,
            logs_dir,
            model_name=None,
            version=None,
            extra_env=None,
            **_kwargs,
        ):
            self.logs_dir = Path(logs_dir)
            self.model_name = model_name
            self._version = version
            self._extra_env = dict(extra_env or {})

        def version(self):
            return self._version

    class NonZeroAgentExitCodeError(RuntimeError):
        pass

    class BaseEnvironment:
        pass

    class AgentContext:
        def __init__(self):
            self.n_input_tokens = None
            self.n_cache_tokens = None
            self.n_output_tokens = None
            self.cost_usd = None
            self.metadata = None

    modules = {
        "harbor": types.ModuleType("harbor"),
        "harbor.agents": types.ModuleType("harbor.agents"),
        "harbor.agents.installed": types.ModuleType("harbor.agents.installed"),
        "harbor.agents.installed.base": types.ModuleType("harbor.agents.installed.base"),
        "harbor.environments": types.ModuleType("harbor.environments"),
        "harbor.environments.base": types.ModuleType("harbor.environments.base"),
        "harbor.models": types.ModuleType("harbor.models"),
        "harbor.models.agent": types.ModuleType("harbor.models.agent"),
        "harbor.models.agent.context": types.ModuleType("harbor.models.agent.context"),
    }
    modules["harbor.agents.installed.base"].BaseInstalledAgent = BaseInstalledAgent
    modules["harbor.agents.installed.base"].NonZeroAgentExitCodeError = NonZeroAgentExitCodeError
    modules["harbor.environments.base"].BaseEnvironment = BaseEnvironment
    modules["harbor.models.agent.context"].AgentContext = AgentContext
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    src_root = Path(__file__).resolve().parents[2] / "src"
    monkeypatch.syspath_prepend(str(src_root))
    module_name = "_test_lhtb_dsh_harbor_agent"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[2] / "scripts" / "lhtb_dsh_harbor_agent.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


def _agent(
    module,
    tmp_path: Path,
    *,
    arm: str = "baseline",
    time_slice_seconds: float | None = None,
    **kwargs,
):
    return module.LHTBDeepSeekHarnessAgent(
        logs_dir=tmp_path / "agent",
        model_name="stepfun/step-3.7-flash",
        arm=arm,
        time_slice_seconds=time_slice_seconds,
        **kwargs,
    )


def _write_session(
    home: Path,
    *,
    session_id: str,
    turn: int,
    append: bool = False,
) -> None:
    session = home / "sessions" / "app" / session_id
    session.mkdir(parents=True, exist_ok=True)
    path = session / "session.jsonl"
    rows = [
        {
            "type": "turn/start",
            "seq": (turn - 1) * 2,
            "time": turn * 10,
            "data": {"turn": turn},
        },
        {
            "type": "assistant/chunk",
            "seq": (turn - 1) * 2 + 1,
            "time": turn * 10 + 1,
            "data": {
                "turn": turn,
                "step": 1,
                "chunk": {
                    "type": "usage",
                    "usage": {"inputTokens": turn + 2, "outputTokens": 1},
                },
            },
        },
    ]
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8") as stream:
        if not append:
            stream.write(
                json.dumps(
                    {
                        "type": "session",
                        "id": session_id,
                        "cwd": "/app",
                    }
                )
                + "\n"
            )
        for row in rows:
            stream.write(json.dumps(row) + "\n")


def test_arm_and_credential_boundaries(agent_module, tmp_path, monkeypatch):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    with pytest.raises(ValueError, match="HB_CONTINUE_MODE"):
        _agent(agent_module, tmp_path, arm="lhos")

    credential = "resolved-only-in-memory"
    agent = agent_module.LHTBDeepSeekHarnessAgent(
        logs_dir=tmp_path / "agent",
        model_name="stepfun/step-3.7-flash",
        extra_env={"STEPFUN_API_KEY": credential},
    )
    assert agent._credential_transport == "ephemeral-mode-0600-file"
    setup = agent._credential_shell_setup(
        agent_module.PurePosixPath("/tmp/.lhos-dsh-credential-test")
    )
    assert "STEPFUN_API_KEY" in setup
    assert credential not in setup
    assert credential not in json.dumps(agent._write_observability.__annotations__)

    with pytest.raises(ValueError, match="unexpected sensitive"):
        agent_module.LHTBDeepSeekHarnessAgent(
            logs_dir=tmp_path / "agent-2",
            model_name="stepfun/step-3.7-flash",
            extra_env={"OTHER_API_KEY": "must-not-be-read"},
        )


def test_controlled_pair_baseline_uses_same_harbor_mode_but_fresh_session(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    baseline = _agent(
        agent_module,
        tmp_path,
        arm="baseline",
        controlled_pair_mode=True,
    )
    assert baseline.controlled_pair_mode is True
    assert baseline.semantic_context_control is False

    calls = []

    async def fake_execute(**kwargs):
        calls.append(kwargs)

    baseline._execute = fake_execute
    environment = object()
    context = agent_module.AgentContext()
    asyncio.run(baseline.run("original task", environment, context))
    asyncio.run(
        baseline.resume_after_verifier_rejection(
            "binary rejection", context
        )
    )

    assert len(calls) == 2
    assert calls[0]["resume_session_id"] is None
    assert calls[1]["resume_session_id"] is None
    assert calls[1]["continuation_action"] == "fresh_recomputation"
    assert calls[1]["instruction"] == "original task\n\nbinary rejection"

    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    with pytest.raises(ValueError, match="controlled_pair_mode"):
        _agent(
            agent_module,
            tmp_path / "missing-mode",
            arm="baseline",
            controlled_pair_mode=True,
        )


def test_credential_uses_ephemeral_file_not_exec_environment(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    credential = "resolved-only-in-memory"
    agent = agent_module.LHTBDeepSeekHarnessAgent(
        logs_dir=tmp_path / "agent",
        model_name="stepfun/step-3.7-flash",
        extra_env={"STEPFUN_API_KEY": credential},
    )

    class CredentialEnvironment:
        capabilities = SimpleNamespace(mounted=True)
        task_env_config = SimpleNamespace(allow_internet=True)

        def __init__(self):
            self.uploads = []
            self.exec_calls = []

        async def upload_file(self, source_path, target_path):
            source = Path(source_path)
            self.uploads.append(
                {
                    "source": source,
                    "target": target_path,
                    "content": source.read_text(encoding="utf-8"),
                }
            )

        async def exec(self, *, command, **kwargs):
            self.exec_calls.append({"command": command, **kwargs})
            if "id -u" in command and "id -g" in command:
                return SimpleNamespace(
                    return_code=0,
                    stdout="1001:1002",
                    stderr="",
                )
            return SimpleNamespace(return_code=0, stdout="", stderr="")

    environment = CredentialEnvironment()
    asyncio.run(agent.install(environment))

    assert len(environment.uploads) == 1
    upload = environment.uploads[0]
    assert upload["content"] == credential
    assert not upload["source"].exists()
    assert upload["target"].startswith("/tmp/.lhos-dsh-credential-")
    assert all(credential not in call["command"] for call in environment.exec_calls)
    assert all(call.get("env") is None for call in environment.exec_calls)
    assert any("chown 1001:1002" in call["command"] for call in environment.exec_calls)
    assert any("chmod 600" in call["command"] for call in environment.exec_calls)
    assert any("rm -f" in call["command"] for call in environment.exec_calls)


def test_command_is_container_local_and_resume_is_explicit(agent_module, tmp_path, monkeypatch):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    baseline = _agent(agent_module, tmp_path)
    command = baseline._build_command(
        instruction_file=agent_module.PurePosixPath("/logs/agent/instructions/a.txt"),
        dsh_home=agent_module.PurePosixPath("/logs/agent/dsh-runs/a/dsh-home"),
        stdout_file=agent_module.PurePosixPath("/logs/agent/out.log"),
        stderr_file=agent_module.PurePosixPath("/logs/agent/err.log"),
        resume_session_id=None,
        runtime_patch=None,
    )
    assert "/app" not in command
    assert "--profile headless" in command
    assert "--resume" not in command
    assert "STEPFUN_API_KEY" not in command
    assert "must-not-be-read" not in command

    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    lhos = _agent(agent_module, tmp_path / "lhos", arm="lhos")
    resumed = lhos._build_command(
        instruction_file=agent_module.PurePosixPath("/logs/agent/instructions/b.txt"),
        dsh_home=agent_module.PurePosixPath("/logs/agent/dsh-home"),
        stdout_file=agent_module.PurePosixPath("/logs/agent/out.log"),
        stderr_file=agent_module.PurePosixPath("/logs/agent/err.log"),
        resume_session_id="session-abc-1",
        runtime_patch=agent_module.PurePosixPath("/logs/agent/dsh-home/lhos-resume.patch.yml"),
    )
    assert "--resume session-abc-1" not in resumed
    assert "LHOS_DSH_RESUME_SESSION_ID" in resumed
    assert "LHOS_DSH_RESUME_TASK_FILE" in resumed
    assert "ctx.agents.resume" not in resumed


def test_lhos_resume_reuses_session_and_accumulates_usage(agent_module, tmp_path, monkeypatch):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class FakeEnvironment:
        capabilities = SimpleNamespace(mounted=True)
        task_env_config = SimpleNamespace(allow_internet=True)
        session_id = "trial-1"
        environment_name = "test-task"

        def __init__(self):
            self.calls: list[str] = []

        async def exec(self, *, command, **_kwargs):
            self.calls.append(command)
            home = tmp_path / "agent" / "dsh-home"
            session = home / "sessions" / "app" / "session-1"
            session.mkdir(parents=True, exist_ok=True)
            path = session / "session.jsonl"
            if not path.exists():
                rows = [
                    {
                        "type": "session",
                        "id": "session-abc-1",
                        "cwd": "/app",
                    },
                    {
                        "type": "turn/start",
                        "seq": 0,
                        "time": 1,
                        "data": {"turn": 1},
                    },
                    {
                        "type": "assistant/message",
                        "seq": 1,
                        "time": 2,
                        "data": {
                            "turn": 1,
                            "step": 1,
                            "usage": {"inputTokens": 5, "outputTokens": 2},
                        },
                    },
                    {
                        "type": "tool/call",
                        "seq": 2,
                        "time": 3,
                        "data": {
                            "turn": 1,
                            "step": 1,
                            "callId": "call-1",
                            "name": "read",
                            "arguments": '{"path":"src/a.py"}',
                        },
                    },
                ]
                path.write_text(
                    "\n".join(json.dumps(row) for row in rows) + "\n",
                    encoding="utf-8",
                )
            else:
                with path.open("a", encoding="utf-8") as stream:
                    stream.write(
                        json.dumps(
                            {
                                "type": "assistant/message",
                                "seq": 3,
                                "time": 4,
                                "data": {
                                    "turn": 2,
                                    "step": 1,
                                    "usage": {"inputTokens": 7, "outputTokens": 3},
                                },
                            }
                        )
                        + "\n"
                    )
                    stream.write(
                        json.dumps(
                            {
                                "type": "tool/call",
                                "seq": 4,
                                "time": 5,
                                "data": {
                                    "turn": 2,
                                    "step": 1,
                                    "callId": "call-2",
                                    "name": "read",
                                    "arguments": '{"path":"src/b.py"}',
                                },
                            }
                        )
                        + "\n"
                    )
            return SimpleNamespace(return_code=0, stdout="", stderr="")

    agent = _agent(agent_module, tmp_path, arm="lhos")
    environment = FakeEnvironment()
    context = agent_module.AgentContext()
    awaitable = agent.run("first", environment, context)

    asyncio.run(awaitable)
    asyncio.run(agent.resume_after_verifier_rejection("second", context))

    assert len(environment.calls) == 2
    assert "--resume session-abc-1" not in environment.calls[1]
    assert "LHOS_DSH_RESUME_SESSION_ID=session-abc-1" in environment.calls[1]
    assert context.metadata["dsh_session_reused"] is True
    assert context.metadata["dsh_model_calls_cumulative"] == 2
    assert context.metadata["dsh_tool_calls_cumulative"] == 2
    assert context.metadata["dsh_resumes_cumulative"] == 1
    assert context.metadata["dsh_token_units_cumulative"] > 0
    assert context.metadata["termination_reason"] == "confirmed_task_complete"

    failed_context = agent_module.AgentContext()
    agent._populate_context(
        failed_context,
        current_home=tmp_path / "agent" / "dsh-home",
        current_is_cumulative=True,
        completed=False,
        slice_preempted=False,
    )
    assert "termination_reason" not in failed_context.metadata

    observability = json.loads(
        (tmp_path / "agent" / "dsh-observability.json").read_text(encoding="utf-8")
    )
    assert observability["session_reused"] is True
    assert observability["resume_api"] == "ctx.agents.resume"
    assert "must-not-be-read" not in json.dumps(observability)


def test_time_slice_command_and_controller_checkpoint(agent_module, tmp_path, monkeypatch):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    agent = _agent(agent_module, tmp_path, time_slice_seconds=30)
    effective = agent_module._effective_slice_seconds(
        agent.time_slice_seconds,
        (
            "## VERIFICATION FAILED — CONTINUE WORKING\n"
            "You still have approximately 18 seconds remaining in this trial."
        ),
    )
    assert effective == 3
    command = agent._build_command(
        instruction_file=agent_module.PurePosixPath("/logs/agent/instruction.txt"),
        dsh_home=agent_module.PurePosixPath("/logs/agent/dsh-home"),
        stdout_file=agent_module.PurePosixPath("/logs/agent/stdout.log"),
        stderr_file=agent_module.PurePosixPath("/logs/agent/stderr.log"),
        resume_session_id=None,
        runtime_patch=None,
        time_slice_seconds=effective,
    )
    assert "timeout --foreground --signal=TERM --kill-after=5s 3" in command
    assert 'if [ "$rc" -eq 124 ]; then exit 197' in command

    class SliceEnvironment:
        def __init__(self):
            self.calls = 0

        async def exec(self, *, command, **_kwargs):
            self.calls += 1
            home = tmp_path / "agent" / "dsh-runs" / f"invocation-{self.calls:04d}" / "dsh-home"
            _write_session(
                home,
                session_id=f"session-baseline-{self.calls}",
                turn=1,
            )
            assert "timeout --foreground" in command
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    environment = SliceEnvironment()
    first = agent_module.AgentContext()
    second = agent_module.AgentContext()
    asyncio.run(agent.run("initial task", environment, first))
    asyncio.run(
        agent.run(
            (
                "## VERIFICATION FAILED — CONTINUE WORKING\n"
                "You still have approximately 18 seconds remaining in this trial."
            ),
            environment,
            second,
        )
    )
    assert environment.calls == 2
    assert first.metadata["slice_preempted"] is True
    assert first.metadata["controller_termination_reason"] == "time_slice_preempted"
    assert first.metadata["termination_reason"] == "confirmed_task_complete"
    assert first.metadata["effective_time_slice_seconds"] == 30
    assert second.metadata["effective_time_slice_seconds"] == 3
    assert second.metadata["slice_preemptions_cumulative"] == 2
    assert len(list((tmp_path / "agent" / "dsh-runs").glob("*/dsh-home/sessions/*/*"))) == 2


def test_slice_lhos_resumes_and_real_nonzero_still_raises(agent_module, tmp_path, monkeypatch):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class LhosSliceEnvironment:
        def __init__(self):
            self.calls = 0
            self.commands: list[str] = []

        async def exec(self, *, command, **_kwargs):
            self.calls += 1
            self.commands.append(command)
            home = tmp_path / "lhos" / "agent" / "dsh-home"
            if self.calls == 1:
                _write_session(home, session_id="session-lhos-slice", turn=1)
            else:
                _write_session(
                    home,
                    session_id="session-lhos-slice",
                    turn=2,
                    append=True,
                )
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    lhos = _agent(
        agent_module,
        tmp_path / "lhos",
        arm="lhos",
        time_slice_seconds=20,
    )
    environment = LhosSliceEnvironment()
    context = agent_module.AgentContext()
    asyncio.run(lhos.run("initial task", environment, context))
    asyncio.run(
        lhos.resume_after_verifier_rejection(
            "Your submitted solution did not pass verification. Approximately 12 seconds remain.",
            context,
        )
    )
    assert environment.calls == 2
    assert "LHOS_DSH_RESUME_SESSION_ID=session-lhos-slice" in environment.commands[1]
    assert context.metadata["dsh_session_reused"] is True
    assert context.metadata["slice_preempted"] is True
    assert context.metadata["effective_time_slice_seconds"] == 1

    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)
    failed = _agent(
        agent_module,
        tmp_path / "failure",
        time_slice_seconds=20,
    )

    class FailedEnvironment:
        async def exec(self, **_kwargs):
            return SimpleNamespace(return_code=23, stdout="", stderr="real failure")

    failed_context = agent_module.AgentContext()
    with pytest.raises(agent_module.NonZeroAgentExitCodeError, match="exit 23"):
        asyncio.run(failed.run("initial task", FailedEnvironment(), failed_context))
    assert failed_context.metadata["slice_preempted"] is False
    assert "termination_reason" not in failed_context.metadata


def test_sigkill_at_slice_deadline_is_checkpoint_but_early_sigkill_fails(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)

    class DeadlineSigkillEnvironment:
        async def exec(self, **_kwargs):
            home = (
                tmp_path
                / "deadline"
                / "agent"
                / "dsh-runs"
                / "invocation-0001"
                / "dsh-home"
            )
            _write_session(
                home,
                session_id="session-deadline-sigkill",
                turn=1,
            )
            await asyncio.sleep(0.03)
            return SimpleNamespace(return_code=137, stdout="", stderr="")

    deadline = _agent(
        agent_module,
        tmp_path / "deadline",
        time_slice_seconds=0.02,
    )
    deadline_context = agent_module.AgentContext()
    asyncio.run(
        deadline.run(
            "initial task",
            DeadlineSigkillEnvironment(),
            deadline_context,
        )
    )
    assert deadline_context.metadata["slice_preempted"] is True
    assert (
        deadline_context.metadata["controller_termination_reason"]
        == "time_slice_preempted"
    )

    class EarlySigkillEnvironment:
        async def exec(self, **_kwargs):
            home = (
                tmp_path
                / "early"
                / "agent"
                / "dsh-runs"
                / "invocation-0001"
                / "dsh-home"
            )
            _write_session(
                home,
                session_id="session-early-sigkill",
                turn=1,
            )
            return SimpleNamespace(return_code=137, stdout="", stderr="oom")

    early = _agent(
        agent_module,
        tmp_path / "early",
        time_slice_seconds=10,
    )
    with pytest.raises(agent_module.NonZeroAgentExitCodeError, match="exit 137"):
        asyncio.run(
            early.run(
                "initial task",
                EarlySigkillEnvironment(),
                agent_module.AgentContext(),
            )
        )


def test_resume_patch_is_file_based_and_declares_resume_api(agent_module, tmp_path, monkeypatch):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    agent = _agent(agent_module, tmp_path, arm="lhos")
    agent._shared_host_home.mkdir(parents=True, exist_ok=True)
    agent._write_resume_runner(
        agent._shared_host_home,
        agent._shared_container_home,
    )

    patch = (tmp_path / "agent" / "dsh-home" / "lhos-resume.patch.yml").read_text(encoding="utf-8")
    runner = (tmp_path / "agent" / "dsh-home" / "lhos-resume-runner" / "index.mjs").read_text(
        encoding="utf-8"
    )
    assert "disabled: true" in patch
    assert "file:///logs/agent/dsh-home/lhos-resume-runner/index.mjs" in patch
    assert "ctx.agents.resume" in runner or "agents.resume" in runner
    assert "LHOS_DSH_RESUME_SESSION_ID" in runner

    node = (
        Path(__file__).resolve().parents[2]
        / "artifacts"
        / "tools"
        / "node-v24.19.0-win-x64"
        / "node.exe"
    )
    if node.is_file():
        for generated in (tmp_path / "agent" / "dsh-home" / "lhos-resume-runner" / "index.mjs",):
            checked = subprocess.run(
                [str(node), "--check", str(generated)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )
            assert checked.returncode == 0, checked.stderr


def test_adaptive_context_control_restarts_then_resumes_new_generation(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class AdaptiveEnvironment:
        def __init__(self):
            self.commands: list[str] = []

        async def exec(self, *, command, **_kwargs):
            self.commands.append(command)
            if "generation-0001" in command:
                home = (
                    tmp_path
                    / "agent"
                    / "dsh-generations"
                    / "generation-0001"
                    / "dsh-home"
                )
                if "LHOS_DSH_RESUME_SESSION_ID=session-generation-1" in command:
                    _write_session(
                        home,
                        session_id="session-generation-1",
                        turn=2,
                        append=True,
                    )
                else:
                    _write_session(
                        home,
                        session_id="session-generation-1",
                        turn=1,
                    )
            else:
                _write_session(
                    tmp_path / "agent" / "dsh-home",
                    session_id="session-generation-0",
                    turn=1,
                )
            return SimpleNamespace(return_code=0, stdout="", stderr="")

    decisions = iter(
        (
            SimpleNamespace(
                action=agent_module.HarnessContinuationAction.RESTART_COMPACTED,
                reason="cache_pressure",
                context_score=2.5,
                cache_tokens_per_call=30_000.0,
                cumulative_session_cache_read_tokens=130_048,
                consecutive_max_tokens=2,
                event_progress_ratio=0.059784,
                completed_without_verification=False,
                guard_triggers=(
                    "consecutive_max_tokens",
                    "semantic_progress_collapse",
                ),
                bounded_handoff_items=("workspace-state", "next-test"),
                decision_hash="a" * 64,
            ),
            SimpleNamespace(
                action=agent_module.HarnessContinuationAction.RESUME,
                reason="context_is_efficient",
                context_score=0.5,
                cache_tokens_per_call=1_000.0,
                bounded_handoff_items=(),
                decision_hash="b" * 64,
            ),
        )
    )

    observed = []

    class DeterministicPolicy:
        def decide(self, history, current, **_kwargs):
            observed.append((history, current))
            return next(decisions)

    agent = _agent(agent_module, tmp_path, arm="lhos")
    environment = AdaptiveEnvironment()
    context = agent_module.AgentContext()
    original = "PRIVATE ORIGINAL PROMPT BODY"
    asyncio.run(agent.run(original, environment, context))
    agent._semantic_policy = DeterministicPolicy()

    asyncio.run(
        agent.resume_after_verifier_rejection(
            "PRIVATE VERIFIER PROMPT BODY",
            context,
        )
    )
    assert context.metadata["dsh_session_generation"] == 1
    assert context.metadata["dsh_session_id"] == "session-generation-1"
    assert context.metadata["dsh_controlled_restarts_cumulative"] == 1
    assert "LHOS_DSH_RESUME_SESSION_ID" not in environment.commands[1]
    assert "generation-0001" in environment.commands[1]
    restart_instruction = (
        tmp_path
        / "agent"
        / "invocations"
        / "invocation-0002"
        / "instruction.txt"
    ).read_text(encoding="utf-8")
    assert "Original task (authoritative):" in restart_instruction
    assert original in restart_instruction
    assert "Latest verifier continuation:" in restart_instruction
    assert "PRIVATE VERIFIER PROMPT BODY" in restart_instruction
    assert "workspace-state" in restart_instruction

    asyncio.run(agent.resume_after_verifier_rejection("continue", context))
    assert (
        "LHOS_DSH_RESUME_SESSION_ID=session-generation-1"
        in environment.commands[2]
    )
    assert "generation-0001" in environment.commands[2]
    assert context.metadata["dsh_session_reused"] is True
    assert context.metadata["dsh_resume_evidence_mode"] == "adaptive_context_control"
    assert context.metadata["dsh_session_generation_count"] == 2
    assert context.metadata["dsh_model_calls_cumulative"] == 3
    assert observed[0][1].usage.model_calls == 1
    assert observed[1][1].usage.model_calls == 1
    assert observed[1][1].session_id == "session-generation-1"

    control_path = tmp_path / "agent" / "dsh-semantic-control.json"
    control_text = control_path.read_text(encoding="utf-8")
    control = json.loads(control_text)
    assert control["mode"] == "adaptive_context_control"
    assert control["controlled_restart_count"] == 1
    assert control["session_generation_count"] == 2
    assert [item["session_id"] for item in control["session_generations"]] == [
        "session-generation-0",
        "session-generation-1",
    ]
    assert [item["action"] for item in control["decisions"]] == [
        "restart_compacted",
        "resume",
    ]
    assert control["guard_trigger_counts"] == {
        "consecutive_max_tokens": 1,
        "semantic_progress_collapse": 1,
    }
    assert control["completed_without_verification_count"] == 0
    assert "bounded_handoff_items" not in control_text
    assert "workspace-state" not in control_text
    assert original not in control_text
    assert "PRIVATE VERIFIER PROMPT BODY" not in control_text

    observability = json.loads(
        (tmp_path / "agent" / "dsh-observability.json").read_text(encoding="utf-8")
    )
    assert observability["resume_evidence_mode"] == "adaptive_context_control"
    assert observability["controlled_restart_count"] == 1
    assert observability["session_file_count"] == 2
    assert observability["usage"]["model_calls"] == 3
    assert observability["semantic_guard_trigger_counts"] == {
        "consecutive_max_tokens": 1,
        "semantic_progress_collapse": 1,
    }


def test_max_tokens_is_checkpoint_only_with_durable_usage(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class MaxTokensEnvironment:
        def __init__(self, *, with_usage: bool):
            self.with_usage = with_usage

        async def exec(self, **_kwargs):
            home = tmp_path / ("valid" if self.with_usage else "invalid") / "agent" / "dsh-home"
            session_id = "session-max-tokens"
            session = home / "sessions" / "app" / session_id
            session.mkdir(parents=True, exist_ok=True)
            rows = [
                {"type": "session", "id": session_id, "cwd": "/app"},
                {
                    "type": "turn/start",
                    "seq": 0,
                    "time": 1,
                    "data": {"turn": 1},
                },
            ]
            if self.with_usage:
                rows.append(
                    {
                        "type": "assistant/chunk",
                        "seq": 1,
                        "time": 2,
                        "data": {
                            "turn": 1,
                            "step": 1,
                            "chunk": {
                                "type": "usage",
                                "usage": {"inputTokens": 10, "outputTokens": 2},
                            },
                        },
                    }
                )
            rows.append(
                {
                    "type": "turn/end",
                    "seq": 2,
                    "time": 3,
                    "data": {"turn": 1, "reason": {"kind": "max-tokens"}},
                }
            )
            (session / "session.jsonl").write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            return SimpleNamespace(return_code=1, stdout="", stderr="max tokens")

    valid = _agent(agent_module, tmp_path / "valid", arm="lhos")
    valid_context = agent_module.AgentContext()
    asyncio.run(
        valid.run(
            "task",
            MaxTokensEnvironment(with_usage=True),
            valid_context,
        )
    )
    assert valid_context.metadata["max_tokens_checkpoint"] is True
    assert (
        valid_context.metadata["controller_termination_reason"]
        == "max_tokens_checkpoint"
    )
    assert valid_context.metadata["dsh_max_tokens_checkpoints_cumulative"] == 1
    assert valid_context.metadata["dsh_session_id"] == "session-max-tokens"

    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)

    class BaselineMaxTokensEnvironment:
        async def exec(self, **_kwargs):
            home = (
                tmp_path
                / "baseline"
                / "agent"
                / "dsh-runs"
                / "invocation-0001"
                / "dsh-home"
            )
            session_id = "session-baseline-max-tokens"
            session = home / "sessions" / "app" / session_id
            session.mkdir(parents=True, exist_ok=True)
            rows = [
                {"type": "session", "id": session_id, "cwd": "/app"},
                {
                    "type": "turn/start",
                    "seq": 0,
                    "time": 1,
                    "data": {"turn": 1},
                },
                {
                    "type": "assistant/chunk",
                    "seq": 1,
                    "time": 2,
                    "data": {
                        "turn": 1,
                        "step": 1,
                        "chunk": {
                            "type": "usage",
                            "usage": {"inputTokens": 4, "outputTokens": 2},
                        },
                    },
                },
                {
                    "type": "turn/end",
                    "seq": 2,
                    "time": 3,
                    "data": {"turn": 1, "reason": {"kind": "max-tokens"}},
                },
            ]
            (session / "session.jsonl").write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            return SimpleNamespace(return_code=1, stdout="", stderr="max tokens")

    baseline = _agent(agent_module, tmp_path / "baseline")
    baseline_context = agent_module.AgentContext()
    asyncio.run(
        baseline.run(
            "task",
            BaselineMaxTokensEnvironment(),
            baseline_context,
        )
    )
    assert baseline_context.metadata["max_tokens_checkpoint"] is True
    assert (
        baseline_context.metadata["controller_termination_reason"]
        == "max_tokens_checkpoint"
    )

    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    invalid = _agent(agent_module, tmp_path / "invalid", arm="lhos")
    invalid_context = agent_module.AgentContext()
    with pytest.raises(agent_module.NonZeroAgentExitCodeError, match="exit 1"):
        asyncio.run(
            invalid.run(
                "task",
                MaxTokensEnvironment(with_usage=False),
                invalid_context,
            )
        )
    assert invalid_context.metadata["max_tokens_checkpoint"] is False
    assert "termination_reason" not in invalid_context.metadata


def test_compacted_restart_keeps_full_task_and_whole_artifact_uris(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    agent = _agent(
        agent_module,
        tmp_path,
        arm="lhos",
        semantic_handoff_max_chars=512,
    )
    original = "authoritative requirement " * 300
    agent._original_instruction = original
    current = agent_module.HarnessPhaseObservation(
        phase_index=3,
        session_id="session-one",
    )
    decision = SimpleNamespace(
        bounded_handoff_items=(
            "instruction_sha256:" + "a" * 64 + "; instruction:truncated copy",
            "write_uri:workspace://workspace/implementation.py",
            "read_uri:workspace://output/verification_result.json",
        )
    )
    result = agent._build_compacted_restart_instruction(
        user_prompt="latest verifier feedback " * 100,
        current=current,
        decision=decision,
    )

    assert original.strip() in result
    assert "write_uri:workspace://workspace/implementation.py" in result
    assert "read_uri:workspace://output/verification_result.json" in result
    assert "instruction_sha256:" + "a" * 64 in result
    assert "truncated copy" not in result


def test_semantic_observation_keeps_repeated_artifact_writes_per_phase(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    agent = _agent(agent_module, tmp_path, arm="lhos")
    session = (
        tmp_path
        / "agent"
        / "dsh-home"
        / "sessions"
        / "app"
        / "session-repeat-write"
    )
    session.mkdir(parents=True)
    path = session / "session.jsonl"
    rows = [
        {"type": "session", "id": "session-repeat-write", "cwd": "/app"},
        {
            "type": "assistant/message",
            "seq": 1,
            "time": 1,
            "data": {
                "turn": 1,
                "step": 1,
                "usage": {"inputTokens": 5, "outputTokens": 1},
            },
        },
        {
            "type": "tool/call",
            "seq": 2,
            "time": 2,
            "data": {
                "turn": 1,
                "step": 1,
                "callId": "call-write-1",
                "name": "write",
                "arguments": '{"path":"/app/workspace/state.py"}',
            },
        },
    ]
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    first = agent._observe_semantic_phase()

    with path.open("a", encoding="utf-8") as stream:
        for row in (
            {
                "type": "assistant/message",
                "seq": 3,
                "time": 3,
                "data": {
                    "turn": 2,
                    "step": 1,
                    "usage": {"inputTokens": 7, "outputTokens": 1},
                },
            },
            {
                "type": "tool/call",
                "seq": 4,
                "time": 4,
                "data": {
                    "turn": 2,
                    "step": 1,
                    "callId": "call-write-2",
                    "name": "write",
                    "arguments": '{"path":"/app/workspace/state.py"}',
                },
            },
        ):
            stream.write(json.dumps(row) + "\n")
    agent._invocation_count += 1
    second = agent._observe_semantic_phase()

    assert first.write_set == ("workspace://workspace/state.py",)
    assert second.write_set == ("workspace://workspace/state.py",)
    assert first.usage.model_calls == 1
    assert second.usage.model_calls == 1


def test_semantic_observation_projects_v3_turn_end_and_unknown_io_signals(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    agent = _agent(agent_module, tmp_path, arm="lhos")
    session = (
        tmp_path
        / "agent"
        / "dsh-home"
        / "sessions"
        / "app"
        / "session-v3-observation"
    )
    session.mkdir(parents=True)
    path = session / "session.jsonl"
    rows = [
        {"type": "session", "id": "session-v3-observation", "cwd": "/app"},
        {
            "type": "assistant/message",
            "seq": 1,
            "time": 1,
            "data": {
                "turn": 1,
                "step": 1,
                "usage": {
                    "inputTokens": 5,
                    "outputTokens": 1,
                    "cacheReadTokens": 91_712,
                },
            },
        },
        {
            "type": "tool/call",
            "seq": 2,
            "time": 2,
            "data": {
                "turn": 1,
                "step": 1,
                "callId": "call-shell",
                "name": "bash",
                "arguments": '{"command":"pytest -q"}',
            },
        },
        {
            "type": "turn/end",
            "seq": 3,
            "time": 3,
            "data": {"turn": 1, "reason": {"kind": "max-tokens"}},
        },
    ]
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    first = agent._observe_semantic_phase()

    assert first.max_tokens_checkpoint is True
    assert first.harness_completed is False
    assert first.unknown_io is True
    assert first.usage.cache_read_tokens == 91_712

    with path.open("a", encoding="utf-8") as stream:
        for row in (
            {
                "type": "assistant/message",
                "seq": 4,
                "time": 4,
                "data": {
                    "turn": 2,
                    "step": 1,
                    "usage": {
                        "inputTokens": 2,
                        "outputTokens": 1,
                        "cacheReadTokens": 38_336,
                    },
                },
            },
            {
                "type": "turn/end",
                "seq": 5,
                "time": 5,
                "data": {"turn": 2, "reason": {"kind": "completed"}},
            },
        ):
            stream.write(json.dumps(row) + "\n")
    agent._invocation_count += 1

    second = agent._observe_semantic_phase()

    assert second.max_tokens_checkpoint is False
    assert second.harness_completed is True
    assert second.unknown_io is False
    assert second.usage.cache_read_tokens == 38_336


def test_agent_exposes_v3_semantic_guard_defaults(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")
    agent = _agent(agent_module, tmp_path, arm="lhos")

    assert agent._semantic_config["max_consecutive_max_tokens"] == 2
    assert (
        agent._semantic_config["cumulative_cache_read_tokens_threshold"]
        == 128_000
    )
    assert agent._semantic_config["cumulative_cache_requires_max_tokens"] == 2
    assert agent._semantic_config["no_progress_phases"] == 2
    assert agent._semantic_config["no_progress_event_ratio_threshold"] == 0.10


def test_final_short_slice_without_new_events_is_a_budget_tail_checkpoint(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class TailEnvironment:
        def __init__(self):
            self.calls = 0

        async def exec(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                _write_session(
                    tmp_path / "lhos" / "agent" / "dsh-home",
                    session_id="session-tail",
                    turn=1,
                )
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    lhos = _agent(
        agent_module,
        tmp_path / "lhos",
        arm="lhos",
        time_slice_seconds=60,
    )
    environment = TailEnvironment()
    context = agent_module.AgentContext()
    asyncio.run(lhos.run("initial task", environment, context))
    asyncio.run(
        lhos.resume_after_verifier_rejection(
            (
                "Your submitted solution did not pass verification. "
                "Approximately 22 seconds remain."
            ),
            context,
        )
    )

    assert environment.calls == 2
    assert context.metadata["effective_time_slice_seconds"] == 7
    assert context.metadata["budget_tail_slice"] is True
    assert context.metadata["budget_tail_no_progress"] is True
    assert context.metadata["slice_no_progress_failure"] is False
    assert (
        context.metadata["controller_termination_reason"]
        == "budget_tail_no_progress_checkpoint"
    )
    assert context.metadata["termination_reason"] == "confirmed_task_complete"

    invocation = json.loads(
        (
            tmp_path
            / "lhos"
            / "agent"
            / "invocations"
            / "invocation-0002"
            / "invocation.json"
        ).read_text(encoding="utf-8")
    )
    assert invocation["status"] == "budget_tail_no_progress"
    assert invocation["remaining_budget_seconds"] == 22
    assert invocation["slice_checkpoint_kind"] == "budget_tail_no_progress"

    observability = json.loads(
        (
            tmp_path / "lhos" / "agent" / "dsh-observability.json"
        ).read_text(encoding="utf-8")
    )
    assert observability["last_invocation_status"] == "budget_tail_no_progress"
    assert observability["budget_tail_no_progress"] is True
    assert observability["budget_tail_no_progress_checkpoints"] == 1
    assert observability["slice_no_progress_failures"] == 0
    assert observability["event_count"] == 2

    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)

    class FreshTailEnvironment:
        async def exec(self, **_kwargs):
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    fresh = _agent(
        agent_module,
        tmp_path / "fresh",
        time_slice_seconds=60,
    )
    fresh_context = agent_module.AgentContext()
    asyncio.run(
        fresh.run(
            (
                "Your submitted solution did not pass verification. "
                "Approximately 22 seconds remain."
            ),
            FreshTailEnvironment(),
            fresh_context,
        )
    )
    assert fresh_context.metadata["budget_tail_no_progress"] is True
    assert (
        fresh_context.metadata["controller_termination_reason"]
        == "budget_tail_no_progress_checkpoint"
    )


def test_normal_slice_without_progress_fails_for_both_arms(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    class LhosNoProgressEnvironment:
        def __init__(self):
            self.calls = 0

        async def exec(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                _write_session(
                    tmp_path / "lhos" / "agent" / "dsh-home",
                    session_id="session-no-progress",
                    turn=1,
                )
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    lhos = _agent(
        agent_module,
        tmp_path / "lhos",
        arm="lhos",
        time_slice_seconds=60,
    )
    lhos_environment = LhosNoProgressEnvironment()
    asyncio.run(
        lhos.run(
            "initial task",
            lhos_environment,
            agent_module.AgentContext(),
        )
    )
    lhos_context = agent_module.AgentContext()
    with pytest.raises(
        agent_module.NonZeroAgentExitCodeError,
        match="without durable DSH progress",
    ):
        asyncio.run(
            lhos.resume_after_verifier_rejection(
                (
                    "Your submitted solution did not pass verification. "
                    "Approximately 120 seconds remain."
                ),
                lhos_context,
            )
        )
    assert lhos_context.metadata["slice_no_progress_failure"] is True
    assert "termination_reason" not in lhos_context.metadata

    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)

    class FreshNoProgressEnvironment:
        async def exec(self, **_kwargs):
            return SimpleNamespace(
                return_code=agent_module._SLICE_EXIT_CODE,
                stdout="",
                stderr="",
            )

    fresh = _agent(
        agent_module,
        tmp_path / "fresh",
        time_slice_seconds=60,
    )
    fresh_context = agent_module.AgentContext()
    with pytest.raises(
        agent_module.NonZeroAgentExitCodeError,
        match="without durable DSH progress",
    ):
        asyncio.run(
            fresh.run(
                "initial task",
                FreshNoProgressEnvironment(),
                fresh_context,
            )
        )
    assert fresh_context.metadata["slice_no_progress_failure"] is True
    assert fresh_context.metadata["budget_tail_no_progress"] is False
    assert "termination_reason" not in fresh_context.metadata


def test_structured_451_is_projected_but_never_retried(
    agent_module,
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("HB_CONTINUE_MODE", raising=False)

    class CensoredEnvironment:
        def __init__(self):
            self.calls = 0

        async def exec(self, **_kwargs):
            self.calls += 1
            home = (
                tmp_path
                / "agent"
                / "dsh-runs"
                / "invocation-0001"
                / "dsh-home"
            )
            session = home / "sessions" / "app" / "session-censored"
            session.mkdir(parents=True, exist_ok=True)
            rows = [
                {
                    "type": "session",
                    "id": "session-censored",
                    "cwd": "/app",
                },
                {
                    "type": "assistant/chunk",
                    "seq": 1,
                    "time": 1,
                    "data": {
                        "turn": 1,
                        "step": 1,
                        "chunk": {
                            "type": "usage",
                            "usage": {"inputTokens": 10, "outputTokens": 2},
                        },
                    },
                },
                {
                    "type": "turn/end",
                    "seq": 2,
                    "time": 2,
                    "data": {
                        "turn": 1,
                        "reason": {
                            "kind": "error",
                            "error": {
                                "code": "PI_AI_ERROR",
                                "message": (
                                    '451: {"type":"censorship_blocked",'
                                    '"message":"blocked"}'
                                ),
                            },
                        },
                    },
                },
            ]
            (session / "session.jsonl").write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            return SimpleNamespace(return_code=1, stdout="", stderr="")

    agent = _agent(agent_module, tmp_path)
    environment = CensoredEnvironment()
    context = agent_module.AgentContext()
    with pytest.raises(
        agent_module.NonZeroAgentExitCodeError,
        match=r"provider_censored.*451",
    ):
        asyncio.run(agent.run("task", environment, context))

    assert environment.calls == 1
    assert context.metadata["provider_censored"] is True
    assert context.metadata["provider_censored_cumulative"] == 1
    failure = context.metadata["provider_failure"]
    assert failure["outcome"] == "provider_censored"
    assert failure["failure_class"] == "content_policy"
    assert failure["status_code"] == 451
    assert failure["provider_code"] == "PI_AI_ERROR"
    assert failure["retryable"] is False
    assert failure["source"] == "structured_turn_end"
    assert len(failure["message_sha256"]) == 64
    assert "blocked" not in json.dumps(failure)
    assert "termination_reason" not in context.metadata

    invocation = json.loads(
        (
            tmp_path
            / "agent"
            / "invocations"
            / "invocation-0001"
            / "invocation.json"
        ).read_text(encoding="utf-8")
    )
    assert invocation["status"] == "provider_censored"
    assert invocation["provider_censored"] is True
    assert invocation["provider_failure"]["retryable"] is False

    observability = json.loads(
        (tmp_path / "agent" / "dsh-observability.json").read_text(
            encoding="utf-8"
        )
    )
    assert observability["provider_censored"] is True
    assert observability["provider_censored_count"] == 1
    assert observability["provider_failure"]["status_code"] == 451

    completed = agent_module.DeepSeekTraceSummary(
        turn_end_reasons=({"kind": "completed", "message": "Discuss HTTP 451"},),
    )
    assert agent_module._structured_provider_failure(completed) is None

def test_initial_run_one_shot_gets_semantic_checkpoints(
    agent_module,
    tmp_path,
    monkeypatch,
):
    """漏洞C修复：one-shot（time_slice=None）的 lhos 臂 initial run
    不再一次跑满 max-tokens，而是强制时间片循环，每个 slice 边界执行
    语义 decide()（会话内周期检查点）。"""
    monkeypatch.setenv("HB_CONTINUE_MODE", "same_conversation")

    decisions_seen: list[int] = []
    policies_seen: list[int] = []

    class OneShotSliceEnvironment:
        def __init__(self):
            self.calls = 0

        async def exec(self, *, command, **_kwargs):
            self.calls += 1
            home = tmp_path / "agent" / "dsh-home"
            _write_session(
                home,
                session_id="session-oneshot",
                turn=self.calls,
                append=self.calls > 1,
            )
            if self.calls < 3:
                return SimpleNamespace(
                    return_code=agent_module._SLICE_EXIT_CODE,
                    stdout="",
                    stderr="",
                )
            return SimpleNamespace(return_code=0, stdout="", stderr="")

    class ResumePolicy:
        def __init__(self, **kwargs):
            pass

        def decide(self, history, current, **_kwargs):
            decisions_seen.append(len(history))
            return SimpleNamespace(
                action=agent_module.HarnessContinuationAction.RESUME,
                reason="context_is_efficient",
                context_score=0.5,
                cache_tokens_per_call=1_000.0,
                bounded_handoff_items=(),
                decision_hash="b" * 64,
            )

    # run() 内部会用 SemanticContextPolicy(**config) 重建 policy，
    # 因此替换类本身，使注入的 decide 生效
    monkeypatch.setattr(agent_module, "SemanticContextPolicy", ResumePolicy)

    agent = _agent(agent_module, tmp_path, arm="lhos")
    assert agent.time_slice_seconds is None  # one-shot: 修复前语义控制 0 介入
    environment = OneShotSliceEnvironment()
    context = agent_module.AgentContext()
    asyncio.run(agent.run("initial one-shot task", environment, context))

    # 修复后：initial run 内部跑了 3 个 invocation（2 次 slice 边界 + 1 次完成），
    # 语义 decide() 在会话内被周期调用 2 次
    assert environment.calls == 3
    assert agent._invocation_count == 3
    assert len(decisions_seen) == 2
    assert len(agent._semantic_observations) == 2
    assert context.metadata["slice_preemptions_cumulative"] == 2
    assert context.metadata["slice_preempted"] is False  # 最后一次为 completed


def test_sessions_fingerprint_tracks_append_only_growth(
    agent_module, tmp_path: Path
) -> None:
    sessions = tmp_path / "sessions" / "--app--" / "session-abc"
    sessions.mkdir(parents=True)
    session_file = sessions / "session.jsonl"
    session_file.write_text('{"type":"session","id":"session-abc"}\n', encoding="utf-8")

    first = agent_module._sessions_fingerprint(sessions.parent)
    assert first is not None and len(first) == 1
    # No change -> identical fingerprint (memoization would hit).
    assert agent_module._sessions_fingerprint(sessions.parent) == first

    # Append-only growth -> fingerprint changes (memoization must miss).
    with session_file.open("a", encoding="utf-8") as handle:
        handle.write('{"type":"turn/start","data":{}}\n')
    grown = agent_module._sessions_fingerprint(sessions.parent)
    assert grown is not None and grown != first

    # A new session file also changes the fingerprint.
    other = sessions / "other.jsonl"
    other.write_text('{"type":"session","id":"session-def"}\n', encoding="utf-8")
    widened = agent_module._sessions_fingerprint(sessions.parent)
    assert widened is not None and widened != grown and len(widened) == 2

    # A missing root is cacheable-empty (parse would return an empty summary).
    assert agent_module._sessions_fingerprint(tmp_path / "absent") == ()
