"""LongHorizonOS E2 — model + tool integration tests (offline, no API key)."""

from __future__ import annotations

import tempfile

import pytest

from lhos.integrations.models.openai_compatible import (
    FakeTransport,
    ModelCallError,
    OpenAICompatibleModel,
)
from lhos.integrations.models.protocols import Message
from lhos.integrations.semantic import CommandVerifier
from lhos.integrations.tools.git import GitTool
from lhos.integrations.tools.shell import ShellTool
from lhos.integrations.tools.workspace import WorkspaceTool


# ── model adapter (offline) ─────────────────────────────────────────────────
def test_openai_compatible_complete_offline():
    transport = FakeTransport(text="hello")
    model = OpenAICompatibleModel("fake", transport=transport)
    r = model.complete([Message(role="user", content="hi")])
    assert r.ok and r.text == "hello"
    assert transport.last_request is not None
    assert transport.last_request["model"] == "fake"


def test_openai_compatible_structured():
    transport = FakeTransport(text='{"answer": 42}')
    model = OpenAICompatibleModel("fake", transport=transport)
    r = model.complete_structured([Message(role="user", content="q")], schema={"type": "object"})
    assert r.ok and '"answer"' in r.text


def test_model_failure_is_operational_not_semantic():
    transport = FakeTransport(text="x", fail=True)
    model = OpenAICompatibleModel("fake", transport=transport, fail_closed=True)
    with pytest.raises(ModelCallError):
        model.complete([Message(role="user", content="hi")])


def test_model_malformed_response_raises():
    transport = FakeTransport(text="x")  # returns dict but we override via __call__? no
    # use a raw transport returning a non-dict to force malformed
    from typing import Any

    def bad(url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
        return {}  # no choices

    model = OpenAICompatibleModel("fake", transport=bad)
    with pytest.raises(ModelCallError):
        model.complete([Message(role="user", content="hi")])


def test_model_does_not_require_api_key_by_default():
    model = OpenAICompatibleModel("fake", api_key="")
    assert not model.api_key  # no hardcoded secret


# ── shell tool ──────────────────────────────────────────────────────────────
def test_shell_tool_normal_and_nonzero():
    sh = ShellTool()
    ok = sh.run("true")
    assert ok.ok
    fail = sh.run("false")
    assert not fail.ok and fail.value["exit_code"] == 1


def test_shell_capability_denied_no_fallback():
    sh = ShellTool()
    r = sh.run("true", check_capability=lambda cap: False)
    assert not r.ok and "denied" in r.error


def test_shell_timeout():
    sh = ShellTool(timeout_s=0.2)
    r = sh.run("sleep 5")
    assert not r.ok and "timeout" in r.error


# ── workspace tool ──────────────────────────────────────────────────────────
def test_workspace_root_scoped_and_paths_confined():
    tmp = tempfile.mkdtemp(prefix="e2_ws_")
    ws = WorkspaceTool(tmp)
    ws.write("a.txt", "hi")
    assert ws.read("a.txt").value == "hi"
    assert ws.stat("a.txt").value["size"] == 2
    with pytest.raises(PermissionError):
        ws._resolve("../escape")  # escapes root


# ── git tool (minimal reads) ────────────────────────────────────────────────
def test_git_tool_reads_in_temp_repo():
    import subprocess

    tmp = tempfile.mkdtemp(prefix="e2_git_")
    subprocess.run(["git", "init", "-q", tmp], check=True)
    subprocess.run(["git", "-C", tmp, "config", "user.email", "a@b.c"], check=True)
    subprocess.run(["git", "-C", tmp, "config", "user.name", "a"], check=True)
    (__import__("pathlib").Path(tmp) / "f.txt").write_text("x")
    subprocess.run(["git", "-C", tmp, "add", "."], check=True)
    subprocess.run(["git", "-C", tmp, "commit", "-qm", "init"], check=True)
    git = GitTool(tmp)
    s = git.status()
    assert s.ok
    r = git.rev()
    assert r.ok and len(r.value.get("stdout", "")) >= 40


# ── CommandVerifier → real shell → Evidence path ───────────────────────────
def test_command_verifier_runs_real_shell_and_enters_vpg():
    from lhos.sdk import Agent, AgentOS, Goal

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a", verify=CommandVerifier("true", artifact_id="x", version=1, shell=sh))
    res = os_.run(g, max_dispatches=2)
    assert "T1" in res.verified  # real shell verifier -> VPG VERIFIED


def test_command_verifier_fail_no_verified():
    from lhos.sdk import Agent, AgentOS, Goal

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a", verify=CommandVerifier("false", artifact_id="x", version=1, shell=sh))
    res = os_.run(g, max_dispatches=2)
    assert res.task_states.get("T1") != "verified"
