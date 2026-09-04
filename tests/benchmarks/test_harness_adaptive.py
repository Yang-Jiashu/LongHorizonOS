"""Focused gates for the real AgentOS/Harness benchmark vertical slice."""

from __future__ import annotations

import pytest

from lhos.benchmarks.harness_adaptive import (
    DEFAULT_MAX_CONCURRENCY,
    HarnessProviderRequest,
    HarnessProviderResult,
    load_provider_factory,
    run_benchmark,
)


def test_harness_benchmark_crosses_ownership_and_vpg_path() -> None:
    report = run_benchmark(delay_seconds=0.003, max_concurrency=DEFAULT_MAX_CONCURRENCY)

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["scope"]["ownership_path"] is True
    assert report["scope"]["exact_identity_harness_control"] is True
    assert report["scope"]["policy_difference_only"] is True

    assert report["static"]["verified_task_ids"] == [
        "a-conflict",
        "b-conflict",
        "c-independent",
        "d-independent",
    ]
    assert report["adaptive"]["verified_task_ids"] == report["static"]["verified_task_ids"]
    assert report["static"]["verified_progress"] == 1.0
    assert report["adaptive"]["verified_progress"] == 1.0
    assert report["static"]["runtime_audit"]["ownership_path"] is True
    assert report["adaptive"]["runtime_audit"]["ownership_path"] is True
    assert report["static"]["runtime_audit"]["harness_control_events"] == 5
    assert report["adaptive"]["runtime_audit"]["harness_control_events"] == 4
    assert report["static"]["runtime_audit"]["pass_evidence_nodes"] == 4
    assert report["adaptive"]["runtime_audit"]["pass_evidence_nodes"] == 4
    assert report["static"]["runtime_audit"]["active_claims_after_run"] == 0
    assert report["adaptive"]["runtime_audit"]["live_kernel_leases_after_run"] == 0

    assert report["static"]["stale_attempts"] == 1
    assert report["static"]["rework_attempts"] == 1
    assert report["adaptive"]["stale_attempts"] == 0
    assert report["adaptive"]["rework_attempts"] == 0
    assert report["comparison"]["total_token_reduction"] > 0
    assert report["static"]["usage"]["synthetic_usage"] is True
    assert report["adaptive"]["usage"]["synthetic_usage"] is True


class _ReportedProvider:
    provider_id = "test-reported-provider"

    async def execute(self, request: HarnessProviderRequest) -> HarnessProviderResult:
        return HarnessProviderResult(
            content=f"{request.task_id}:reported",
            input_tokens=7,
            output_tokens=5,
            model_cost_usd=0.000123,
            usage_kind="provider_reported",
        )


def test_explicit_provider_factory_reports_usage_without_changing_path() -> None:
    report = run_benchmark(
        delay_seconds=0.003,
        max_concurrency=2,
        provider_factory=_ReportedProvider,
        provider_factory_spec="tests.provider:factory",
    )

    assert report["valid"] is True
    assert report["static"]["provider_id"] == "test-reported-provider"
    assert report["adaptive"]["provider_id"] == "test-reported-provider"
    assert report["static"]["usage"]["provider_reported_usage"] is True
    assert report["adaptive"]["usage"]["provider_reported_usage"] is True
    assert (
        report["static"]["runtime_audit"]["harness_control_events"]
        == (report["static"]["executed_attempts"])
    )


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "module:callable"),
        ("missing-separator", "module:callable"),
        ("lhos.benchmarks.harness_adaptive:missing_factory", "not callable"),
    ],
)
def test_provider_factory_loader_rejects_malformed_specs(value: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        load_provider_factory(value)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"delay_seconds": 0}, "delay_seconds"),
        ({"delay_seconds": True}, "delay_seconds"),
        ({"max_concurrency": 1}, "max_concurrency"),
        ({"max_concurrency": True}, "max_concurrency"),
        ({"verification_tokens_per_attempt": -1}, "verification_tokens"),
    ],
)
def test_harness_benchmark_rejects_invalid_parameters(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_benchmark(**kwargs)
