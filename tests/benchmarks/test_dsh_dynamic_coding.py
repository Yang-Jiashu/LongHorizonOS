from __future__ import annotations

import json
from pathlib import Path

import pytest

from lhos.benchmarks.dsh_dynamic_coding.case import (
    AFFECTED_V2,
    PRESERVED_V2,
    TASK_SPECS,
    apply_v2_mutation,
    content_hashes,
    create_workspace,
    protected_files,
    task_prompt,
)
from lhos.benchmarks.dsh_dynamic_coding.dsh_worker import (
    _credential_env_name,
    _keys,
    _usage_from_logs,
)
from lhos.benchmarks.dsh_dynamic_coding.experiment import (
    _MUTATED_INPUT_CONSUMERS,
    DshConfig,
    SharedV1Snapshot,
    _comparison,
    _failed_arm,
    _key_slot_for_attempt,
    _load_attempt_records,
    _manifest_id,
    _target_verification,
    _usage_summary,
    _workspace_manifest,
    run_experiment,
)
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome


def _relative_hashes(workspace: Path) -> dict[str, str]:
    hashes = content_hashes(protected_files(workspace))
    return {
        str(Path(path).relative_to(workspace.resolve())).replace("\\", "/"): digest
        for path, digest in hashes.items()
    }


def _write_verified_v1(workspace: Path) -> None:
    (workspace / "src/pricing_service/core.py").write_text(
        """\
from decimal import Decimal, ROUND_HALF_UP


def calculate_total(subtotal, discount_percent=Decimal("0")) -> Decimal:
    subtotal_value = Decimal(str(subtotal))
    discount_value = Decimal(str(discount_percent))
    if subtotal_value < 0:
        raise ValueError("subtotal")
    if discount_value < 0 or discount_value > 100:
        raise ValueError("discount_percent")
    total = subtotal_value * (Decimal("1") - discount_value / Decimal("100"))
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
""",
        encoding="utf-8",
    )
    (workspace / "src/pricing_service/api.py").write_text(
        """\
from .core import calculate_total


def quote(payload):
    total = calculate_total(
        payload["subtotal"],
        payload.get("discount_percent", 0),
    )
    return {"total": f"{total:.2f}"}
""",
        encoding="utf-8",
    )
    (workspace / "src/pricing_service/audit.py").write_text(
        """\
from decimal import Decimal, ROUND_HALF_UP


def build_audit_event(order_id, total):
    normalized = str(order_id).strip()
    if not normalized:
        raise ValueError("order_id")
    money = Decimal(str(total)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return {
        "event": "order_priced",
        "order_id": normalized,
        "total": f"{money:.2f}",
    }
""",
        encoding="utf-8",
    )
    (workspace / "src/pricing_service/service.py").write_text(
        """\
from .api import quote
from .audit import build_audit_event


def process_order(order_id, payload):
    quoted = quote(payload)
    return {
        "quote": quoted,
        "audit": build_audit_event(order_id, quoted["total"]),
    }
""",
        encoding="utf-8",
    )


def test_v2_mutation_changes_only_the_declared_pricing_contract_surface(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    create_workspace(workspace)
    before = _relative_hashes(workspace)

    apply_v2_mutation(workspace)

    after = _relative_hashes(workspace)
    changed = {path for path, digest in before.items() if after[path] != digest}
    assert changed == {
        "requirements/pricing_contract.json",
        "tests_public/test_api.py",
        "tests_public/test_core.py",
        "tests_public/test_integration.py",
    }
    assert before["requirements/audit_contract.json"] == after["requirements/audit_contract.json"]
    assert before["tests_public/test_audit.py"] == after["tests_public/test_audit.py"]
    assert not list(workspace.rglob("*.pyc"))
    assert not list(workspace.rglob("__pycache__"))
    assert not list(workspace.rglob(".pytest_cache"))


def test_oracle_cone_has_one_independent_preserved_branch() -> None:
    assert AFFECTED_V2 == ("pricing_core", "pricing_api", "integration")
    assert PRESERVED_V2 == ("audit",)
    assert TASK_SPECS["pricing_api"].dependencies == ("pricing_core",)
    assert TASK_SPECS["integration"].dependencies == ("pricing_api", "audit")


def test_task_prompt_fixes_tools_and_write_scope() -> None:
    prompt = task_prompt("pricing_core", 2)
    assert "Do not use web search, subagents, workflows, or external services" in prompt
    assert "edit only the target implementation file(s)" in prompt
    assert "tests_public/test_core.py" in prompt
    assert "pricing contract version 2" in prompt


def test_usage_parser_deduplicates_final_message_and_usage_chunk(tmp_path: Path) -> None:
    session_dir = tmp_path / "sessions" / "project" / "session-1"
    session_dir.mkdir(parents=True)
    events = [
        {"type": "session", "version": 0, "id": "session-1"},
        {
            "type": "assistant/chunk",
            "data": {
                "turn": 1,
                "step": 1,
                "chunk": {
                    "type": "usage",
                    "usage": {
                        "inputTokens": 10,
                        "outputTokens": 3,
                        "cacheReadTokens": 20,
                    },
                },
            },
        },
        {
            "type": "assistant/message",
            "data": {
                "turn": 1,
                "step": 1,
                "usage": {
                    "inputTokens": 10,
                    "outputTokens": 3,
                    "cacheReadTokens": 20,
                },
            },
        },
        {
            "type": "assistant/chunk",
            "data": {
                "turn": 1,
                "step": 2,
                "chunk": {
                    "type": "usage",
                    "usage": {
                        "inputTokens": 7,
                        "outputTokens": 2,
                        "cacheWriteTokens": 5,
                    },
                },
            },
        },
        {
            "type": "tool/call",
            "data": {"turn": 1, "step": 1, "name": "read", "arguments": "{}"},
        },
        {"type": "turn/end", "data": {"turn": 1, "reason": {"kind": "completed"}}},
    ]
    (session_dir / "session.jsonl").write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )

    usage = _usage_from_logs(tmp_path / "sessions")

    assert usage["model_calls"] == 2
    assert usage["uncached_input_tokens"] == 17
    assert usage["cache_read_tokens"] == 20
    assert usage["cache_write_tokens"] == 5
    assert usage["output_tokens"] == 5
    assert usage["total_token_units"] == 47
    assert usage["tool_calls"] == 1


def test_stepfun_credentials_use_named_environment(monkeypatch) -> None:
    monkeypatch.delenv("LHOS_DSH_API_KEYS", raising=False)
    monkeypatch.setenv("STEPFUN_API_KEY", "stepfun-test-key")
    assert _credential_env_name("STEPFUN_API_KEY") == "STEPFUN_API_KEY"
    assert _keys("STEPFUN_API_KEY") == ["stepfun-test-key"]


def test_comparison_reports_repair_savings_only_for_valid_arms() -> None:
    static = {
        "valid": True,
        "repair_usage": {"total_token_units": 400, "model_calls": 20},
        "repair_wall_ms": 200.0,
        "repair_tasks_executed": ["a", "b", "c", "d"],
    }
    lhos = {
        "valid": True,
        "repair_usage": {"total_token_units": 300, "model_calls": 15},
        "repair_wall_ms": 100.0,
        "repair_tasks_executed": ["a", "b", "d"],
    }

    comparison = _comparison(static, lhos)

    assert comparison["pair_valid"] is True
    assert comparison["repair_token_saving_ratio"] == 0.25
    assert comparison["repair_model_call_saving_ratio"] == 0.25
    assert comparison["repair_wall_speedup"] == 2.0


def test_attempt_loader_normalizes_adapter_records_and_deduplicates_worker_copy(
    tmp_path: Path,
) -> None:
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    adapter = {
        "schema_version": "deepseek-harness-attempt.v1",
        "attempt_record_id": "adapter-record-1",
        "phase_id": "pricing_core",
        "phase_version": 2,
        "attempt_number": 1,
        "binding": {"attempt_id": "attempt-1"},
        "process": {"exit_code": 0, "elapsed_ms": 120},
        "trace": {
            "usage": {
                "uncached_input_tokens": 10,
                "output_tokens": 3,
                "cache_read_tokens": 20,
                "cache_write_tokens": 5,
                "reasoning_tokens": 2,
                "verification_tokens": 0,
                "model_calls": 1,
                "tool_calls": 2,
            },
            "tool_calls": [{"name": "read"}, {"name": "edit"}],
            "event_count": 9,
        },
        "completed": True,
        "dsh_home": "dsh-home",
        "prompt_sha256": "a" * 64,
    }
    (attempts / "adapter.json").write_text(json.dumps(adapter), encoding="utf-8")

    records = _load_attempt_records(tmp_path)
    summary = _usage_summary(records, version=2)

    assert len(records) == 1
    assert records[0]["task_id"] == "pricing_core"
    assert records[0]["usage"]["input_token_units"] == 35
    assert records[0]["usage"]["total_token_units"] == 38
    assert summary["attempts"] == 1
    assert summary["total_token_units"] == 38
    assert summary["tool_calls"] == 2

    compatibility = {
        **records[0],
        "schema_version": "dsh-dynamic-attempt.v1",
        "elapsed_ms": 125,
    }
    (attempts / "compatibility.json").write_text(
        json.dumps(compatibility),
        encoding="utf-8",
    )

    deduplicated = _load_attempt_records(tmp_path)

    assert len(deduplicated) == 1
    assert deduplicated[0]["record_path"].endswith("compatibility.json")
    assert _usage_summary(deduplicated, version=2)["total_token_units"] == 38


def test_key_slot_assignment_is_controller_order_independent() -> None:
    static_slots = [
        _key_slot_for_attempt(
            "pricing_core",
            2,
            attempt,
            key_count=3,
            max_attempts=2,
        )
        for attempt in (1, 2)
    ]
    lhos_slots = [
        _key_slot_for_attempt(
            "pricing_core",
            2,
            attempt,
            key_count=3,
            max_attempts=2,
        )
        for attempt in (1, 2)
    ]

    assert static_slots == lhos_slots


def test_failed_arm_is_retained_in_pair_comparison() -> None:
    failed = _failed_arm("dsh_lhos", RuntimeError("provider unavailable"))
    valid = {
        "valid": True,
        "repair_usage": {"total_token_units": 10, "model_calls": 1},
        "repair_wall_ms": 10,
        "repair_tasks_executed": ["audit"],
    }

    comparison = _comparison(valid, failed)

    assert comparison["pair_valid"] is False
    assert "provider unavailable" in comparison["lhos_error"]


def test_pair_index_offset_preserves_global_alternating_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []
    snapshot = SharedV1Snapshot(
        snapshot_id="a" * 64,
        path=tmp_path,
        manifest=(),
        generation={"valid": True},
    )
    monkeypatch.setattr(
        "lhos.benchmarks.dsh_dynamic_coding.experiment._build_shared_v1_snapshot",
        lambda *_args: snapshot,
    )
    monkeypatch.setattr(
        "lhos.benchmarks.dsh_dynamic_coding.experiment._run_static_arm",
        lambda *_args: (
            seen.append("static")
            or {
                "valid": True,
                "repair_usage": {"total_token_units": 10, "model_calls": 1},
                "repair_wall_ms": 10,
                "repair_tasks_executed": ["audit"],
            }
        ),
    )
    monkeypatch.setattr(
        "lhos.benchmarks.dsh_dynamic_coding.experiment._run_lhos_arm",
        lambda *_args: (
            seen.append("lhos")
            or {
                "valid": True,
                "repair_usage": {"total_token_units": 5, "model_calls": 1},
                "repair_wall_ms": 5,
                "repair_tasks_executed": [],
            }
        ),
    )
    config = DshConfig(
        node=tmp_path / "node",
        dsh=tmp_path / "dsh",
        patch=tmp_path / "patch",
        timeout_seconds=1,
        max_concurrency=1,
        max_attempts=1,
    )

    result = run_experiment(
        output_dir=tmp_path / "run",
        config=config,
        repeat=2,
        pair_index_offset=3,
    )

    assert [pair["pair"] for pair in result["pairs"]] == [4, 5]
    assert [pair["order"] for pair in result["pairs"]] == [
        ["dsh_lhos", "dsh_static_restart"],
        ["dsh_static_restart", "dsh_lhos"],
    ]
    assert seen == ["lhos", "static", "static", "lhos"]


@pytest.mark.asyncio
async def test_shared_v1_adoption_and_exact_observation_repair_cone(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    create_workspace(workspace)
    _write_verified_v1(workspace)
    initial_manifest = _workspace_manifest(workspace)
    initial_snapshot_id = _manifest_id(initial_manifest)
    protected_hashes = content_hashes(protected_files(workspace))
    state = {"version": 1}

    async def adopt(_context, _task_id):
        assert _workspace_manifest(workspace) == initial_manifest
        assert _manifest_id(_workspace_manifest(workspace)) == initial_snapshot_id

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "adopter",
                executor=adopt,
                executor_api="context_v1",
                max_concurrency=2,
            )
        )
        goal = Goal("shared-v1-adoption", executor_api="context_v1")
        tasks = {}

        def verifier_for(task_id: str):
            def verify(_context, _dispatched_task_id):
                verification = _target_verification(
                    workspace,
                    task_id,
                    protected_hashes,
                )
                version = 1 if task_id == "audit" else int(state["version"])
                return VerificationOutcome(
                    passed=bool(verification["passed"]),
                    artifact_id=TASK_SPECS[task_id].artifact_id,
                    version=version,
                    content=(workspace / TASK_SPECS[task_id].target_files[0]).read_text(
                        encoding="utf-8"
                    ),
                    details=verification,
                )

            return verify

        for task_id in ("pricing_core", "audit", "pricing_api", "integration"):
            spec = TASK_SPECS[task_id]
            tasks[task_id] = goal.task(
                task_id,
                agent="adopter",
                depends_on=tuple(tasks[item] for item in spec.dependencies),
                verify=verifier_for(task_id),
                inputs=spec.inputs,
                outputs=spec.target_files,
                max_attempts=1,
                executor_api="context_v1",
            )

        goal.compile(os_)
        previous = {
            resource: os_.observe_artifact(
                goal,
                resource,
                1,
                (workspace / resource).read_bytes(),
            )
            for resource in _MUTATED_INPUT_CONSUMERS
        }
        adopted = await os_.run_async(
            goal,
            max_dispatches=8,
            max_steps=8,
            max_concurrency=2,
            adaptive=True,
            max_parallelism=2,
        )

        assert adopted.goal_state == "closed"
        assert set(adopted.verified) == set(TASK_SPECS)
        assert _workspace_manifest(workspace) == initial_manifest

        apply_v2_mutation(workspace)
        state["version"] = 2
        current = {
            resource: os_.observe_artifact(
                goal,
                resource,
                2,
                (workspace / resource).read_bytes(),
            )
            for resource in _MUTATED_INPUT_CONSUMERS
        }
        repaired = os_.reconcile_observations(
            goal,
            tuple(
                {
                    "previous_observation": previous[resource],
                    "observation": current[resource],
                    "affected_task_ids": consumers,
                    "resource_uri": f"workspace://{resource}",
                }
                for resource, consumers in _MUTATED_INPUT_CONSUMERS.items()
            ),
        )

        assert set(repaired.affected) == set(AFFECTED_V2)
        assert set(repaired.preserved) == set(PRESERVED_V2)
        assert set(repaired.frontier) == {"pricing_core"}
    finally:
        os_.close()
