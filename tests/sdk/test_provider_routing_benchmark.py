"""Regression gate for the deterministic bounded provider-routing benchmark."""

from __future__ import annotations

from lhos.benchmarks.provider_routing import (
    run_provider_routing_benchmark,
)


def _stable_projection(report: dict) -> dict:
    """Drop orientation-only wall-clock fields before reproducibility checks."""

    projection = {
        key: value
        for key, value in report.items()
        if key not in {"callback", "provider_route", "comparison"}
    }
    for label in ("callback", "provider_route"):
        case = {key: value for key, value in report[label].items() if key != "elapsed_ms"}
        projection[label] = case
    comparison = dict(report["comparison"])
    comparison.pop("wall_time_ratio", None)
    projection["comparison"] = comparison
    return projection


def test_provider_routing_benchmark_is_valid_and_measures_real_adapter_calls() -> None:
    report = run_provider_routing_benchmark(
        task_count=4,
        delay_seconds=0.0005,
        max_concurrency=2,
    )

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["callback"]["verified_progress"] == 1.0
    assert report["provider_route"]["verified_progress"] == 1.0

    # The callback and provider modes execute the same four tasks, but the
    # opt-in route must invoke exactly one model + verifier hook per task.
    assert report["callback"]["callback_calls"] == 8
    assert report["provider_route"]["provider_calls"] == 8
    assert report["provider_route"]["callback_calls"] == 0
    assert report["provider_route"]["provider_model_keys"] == ["cheap"] * 4
    assert report["provider_route"]["provider_verifier_keys"] == ["light"] * 4

    # Deterministic fake-provider accounting is the measured signal; this is
    # not a claim about any real provider's economics.
    assert report["comparison"]["token_reduction"] == 224
    assert report["comparison"]["verified_progress_delta"] == 0.0


def test_provider_routing_benchmark_stable_fields_are_reproducible() -> None:
    kwargs = {
        "task_count": 3,
        "delay_seconds": 0.0005,
        "max_concurrency": 2,
    }
    first = run_provider_routing_benchmark(**kwargs)
    second = run_provider_routing_benchmark(**kwargs)

    assert _stable_projection(first) == _stable_projection(second)
