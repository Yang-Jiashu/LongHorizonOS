from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from lhos.benchmarks import swe_host_native

_TEST_PATCH = """\
- id: agent-default-model
  config:
    provider: stepfun
    model: test-model
- id: llm-pi-ai
  config:
    providers:
      stepfun:
        apiKeyEnv: STEPFUN_API_KEY
        api: openai-completions
        baseURL: https://api.stepfun.com/step_plan/v1
        reasoning: low
"""


def test_credential_env_defaults_to_deepseek_and_supports_stepfun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LHOS_DSH_CREDENTIAL_ENV", raising=False)
    assert swe_host_native._credential_env_name() == "DEEPSEEK_API_KEY"

    monkeypatch.setenv("LHOS_DSH_CREDENTIAL_ENV", "STEPFUN_API_KEY")
    assert swe_host_native._credential_env_name() == "STEPFUN_API_KEY"
    assert swe_host_native._credential_env_name("CUSTOM_PROVIDER_KEY") == "CUSTOM_PROVIDER_KEY"


def test_credential_env_rejects_values_that_cannot_be_environment_names() -> None:
    with pytest.raises(ValueError, match="invalid credential environment variable"):
        swe_host_native._credential_env_name("STEPFUN_API_KEY=secret")


def test_keys_prefer_explicit_pool_then_selected_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STEPFUN_API_KEY", "stepfun-fallback")
    monkeypatch.setenv("LHOS_DSH_API_KEYS", " pool-one, pool-two ")
    assert swe_host_native._keys("STEPFUN_API_KEY") == ["pool-one", "pool-two"]

    monkeypatch.delenv("LHOS_DSH_API_KEYS")
    assert swe_host_native._keys("STEPFUN_API_KEY") == ["stepfun-fallback"]


def test_stepfun_environment_exposes_only_selected_key_to_dsh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LHOS_DSH_API_KEYS", "pool-one,pool-two")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "unrelated-deepseek-key")
    monkeypatch.setenv("STEPFUN_API_KEY", "unselected-stepfun-key")
    monkeypatch.delenv("LHOS_DSH_BASE_URL", raising=False)
    monkeypatch.delenv("STEPFUN_BASE_URL", raising=False)

    dsh_env = swe_host_native._env(tmp_path, "selected-stepfun-key", "STEPFUN_API_KEY")
    assert dsh_env["STEPFUN_API_KEY"] == "selected-stepfun-key"
    assert dsh_env["STEPFUN_BASE_URL"] == "https://api.stepfun.com/step_plan/v1"
    assert "LHOS_DSH_API_KEYS" not in dsh_env
    assert "DEEPSEEK_API_KEY" not in dsh_env

    evaluation_env = swe_host_native._env(tmp_path, None, "STEPFUN_API_KEY")
    assert "STEPFUN_API_KEY" not in evaluation_env
    assert "DEEPSEEK_API_KEY" not in evaluation_env
    assert "LHOS_DSH_API_KEYS" not in evaluation_env


def test_sensenova_environment_remains_backward_compatible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LHOS_DSH_BASE_URL", "https://example.invalid/v1")

    env = swe_host_native._env(tmp_path, "selected-deepseek-key")

    assert env["DEEPSEEK_API_KEY"] == "selected-deepseek-key"
    assert env["DEEPSEEK_BASE_URL"] == "https://example.invalid/v1"
    assert "STEPFUN_API_KEY" not in env


def test_dsh_record_redacts_selected_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected_key = "selected-provider-secret"
    marker = tmp_path / "credential-check.txt"
    dsh = tmp_path / "fake-dsh.py"
    dsh.write_text(
        "import os, pathlib, sys\n"
        f"ok = os.environ.get('STEPFUN_API_KEY') == {selected_key!r}\n"
        "ok = ok and 'LHOS_DSH_API_KEYS' not in os.environ\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(ok), encoding='utf-8')\n"
        f"print('provider response contained {selected_key}')\n"
        f"print('request failed for {selected_key}', file=sys.stderr)\n"
        "raise SystemExit(1)\n",
        encoding="utf-8",
    )
    patch = tmp_path / "provider.yml"
    patch.write_text(_TEST_PATCH, encoding="utf-8")
    monkeypatch.setattr(swe_host_native, "_dsh_home", lambda: tmp_path / "dsh-home")

    record = swe_host_native._run_dsh(
        workspace=tmp_path,
        run_root=tmp_path / "run",
        node=Path(sys.executable),
        dsh=dsh,
        patch=patch,
        key=selected_key,
        credential_env="STEPFUN_API_KEY",
        timeout_seconds=1,
        case=swe_host_native.DEFAULT_CASE,
        validate_runtime_versions=False,
    )

    assert marker.read_text(encoding="utf-8") == "True"
    assert selected_key not in json.dumps(record)
    assert "[REDACTED]" in record["stdout_tail"]
    assert "[REDACTED]" in record["stderr_tail"]


def test_dsh_success_without_usage_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsh = tmp_path / "fake-dsh.py"
    dsh.write_text("print('done')\n", encoding="utf-8")
    patch = tmp_path / "provider.yml"
    patch.write_text(_TEST_PATCH, encoding="utf-8")
    monkeypatch.setattr(swe_host_native, "_dsh_home", lambda: tmp_path / "dsh-home")

    record = swe_host_native._run_dsh(
        workspace=tmp_path,
        run_root=tmp_path / "run",
        node=Path(sys.executable),
        dsh=dsh,
        patch=patch,
        key="provider-key",
        credential_env="STEPFUN_API_KEY",
        timeout_seconds=1,
        case=swe_host_native.DEFAULT_CASE,
        validate_runtime_versions=False,
    )

    assert record["exit_code"] == 126
    assert record["failure"] == "DeepSeek Harness completed without committed provider usage"


def test_evaluate_captures_agent_patch_before_public_test_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    diffs = iter(
        [
            subprocess.CompletedProcess(["git"], 0, stdout="src/example.py\n"),
            subprocess.CompletedProcess(["git"], 0, stdout=" M src/example.py\n"),
            subprocess.CompletedProcess(
                ["git"],
                0,
                stdout="diff --git a/src/example.py b/src/example.py\n+fixed = True\n",
            ),
            subprocess.CompletedProcess(["git"], 0, stdout=""),
            subprocess.CompletedProcess(["git"], 0, stdout=""),
        ]
    )
    monkeypatch.setattr(swe_host_native, "_git", lambda *_args: next(diffs))
    monkeypatch.setattr(
        swe_host_native,
        "_apply_test_patch",
        lambda *_args: {"passed": True, "exit_code": 0, "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        swe_host_native,
        "_run_pytest",
        lambda *_args: {
            "passed": True,
            "exit_code": 0,
            "elapsed_ms": 1.0,
            "stdout_tail": "",
            "stderr_tail": "",
        },
    )

    result = swe_host_native._evaluate(
        tmp_path,
        tmp_path / "test.patch",
        swe_host_native.DEFAULT_CASE,
    )

    assert result["passed"] is True
    assert result["agent_patch"].endswith("+fixed = True\n")


def test_stepfun_cordis_patch_uses_environment_credential_only() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    patch = (
        repo_root / "benchmarks" / "real_dsh_dynamic_coding" / "stepfun-pi-ai.cordis.patch.yml"
    ).read_text(encoding="utf-8")

    assert "provider: stepfun" in patch
    assert "model: step-3.5-flash-2603" in patch
    assert "apiKeyEnv: STEPFUN_API_KEY" in patch
    assert "baseURL: https://api.stepfun.com/step_plan/v1" in patch
    assert "sk-" not in patch


def test_stepfun_37_cordis_patch_pins_model_and_reasoning() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    patch = (
        repo_root / "benchmarks" / "real_dsh_dynamic_coding" / "stepfun-3.7-pi-ai.cordis.patch.yml"
    ).read_text(encoding="utf-8")

    assert "model: step-3.7-flash" in patch
    assert "apiKeyEnv: STEPFUN_API_KEY" in patch
    assert "baseURL: https://api.stepfun.com/step_plan/v1" in patch
    assert "reasoning: medium" in patch
    assert "medium: medium" in patch
    assert "sk-" not in patch
