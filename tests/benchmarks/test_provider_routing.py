"""Regression tests for the bounded provider-routing benchmark."""

from __future__ import annotations

import pytest

from lhos.benchmarks.provider_routing import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_TASK_COUNT,
    run_provider_routing_benchmark,
)


def test_provider_routing_benchmark_is_valid_and_measures_real_dispatch() -> None:
    report = run_provider_routing_benchmark(
        task_count=DEFAULT_TASK_COUNT,
        max_concurrency=DEFAULT_MAX_CONCURRENCY,
        delay_seconds=0.001,
    )

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["callback"]["correctness"]["all_tasks_verified"] is True
    assert report["provider_route"]["correctness"]["all_tasks_verified"] is True
    assert report["provider_route"]["correctness"]["provider_route_invoked"] is True
    assert report["provider_route"]["correctness"]["base_callbacks_not_used_by_route"] is True
    assert report["provider_route"]["provider_calls"] == DEFAULT_TASK_COUNT * 2
    assert report["provider_route"]["callback_calls"] == 0
    assert report["callback"]["verified_progress"] == 1.0
    assert report["provider_route"]["verified_progress"] == 1.0
    assert report["comparison"]["token_reduction"] > 0
    assert report["comparison"]["verified_progress_delta"] == 0.0


def test_provider_routing_benchmark_has_reproducible_stable_metrics() -> None:
    first = run_provider_routing_benchmark(
        task_count=3,
        max_concurrency=2,
        delay_seconds=0.001,
    )
    second = run_provider_routing_benchmark(
        task_count=3,
        max_concurrency=2,
        delay_seconds=0.001,
    )

    stable_paths = (
        ("callback", "total_tokens"),
        ("callback", "callback_calls"),
        ("provider_route", "total_tokens"),
        ("provider_route", "provider_calls"),
        ("provider_route", "callback_calls"),
    )
    for label, key in stable_paths:
        assert first[label][key] == second[label][key]
    assert first["comparison"]["token_reduction"] == second["comparison"]["token_reduction"]
    assert first["provider_route"]["verified"] == second["provider_route"]["verified"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"task_count": 0}, "task_count"),
        ({"task_count": True}, "task_count"),
        ({"delay_seconds": 0}, "delay_seconds"),
        ({"delay_seconds": True}, "delay_seconds"),
        ({"max_concurrency": 0}, "max_concurrency"),
        ({"max_concurrency": True}, "max_concurrency"),
    ],
)
def test_provider_routing_benchmark_rejects_invalid_parameters(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        run_provider_routing_benchmark(**kwargs)
