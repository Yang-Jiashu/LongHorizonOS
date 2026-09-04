"""Focused tests for the bounded multi-seed online-compute benchmark."""

from __future__ import annotations

import json

import pytest

from lhos.benchmarks.adaptive_control import (
    ControlledScenario,
    default_scenario,
    run_benchmark,
    run_multi_seed_benchmark,
)


def test_multi_seed_reports_each_seed_and_numeric_extrema() -> None:
    report = run_multi_seed_benchmark(seeds=(7, 11, 19))

    assert report["valid"] is True
    assert report["seeds"] == [7, 11, 19]
    assert [item["seed"] for item in report["runs"]] == [7, 11, 19]
    assert all(item["report"]["valid"] for item in report["runs"])

    comparison = report["summary"]["comparison"]
    # The canonical scenario is seed-invariant, so all extrema coincide.
    assert comparison["token_reduction"]["mean"] == 1320
    assert comparison["token_reduction"]["min"] == 1320
    assert comparison["token_reduction"]["max"] == 1320
    assert comparison["token_reduction"]["count"] == 3
    assert report["summary"]["same_verified_task_set_all"] is True


def test_multi_seed_does_not_change_single_seed_contract() -> None:
    single = run_benchmark()
    multi = run_multi_seed_benchmark(seeds=(0,))

    assert multi["runs"][0]["report"] == single
    assert multi["scope"]["simulated_provider"] is True
    assert "not a statistically powered real-model evaluation" in multi["scope"]["interpretation"]


def test_multi_seed_accepts_mapping_scenario_and_provider() -> None:
    scenario = default_scenario().model_dump(mode="json")
    scenario["scenario_id"] = "seed-sweep"
    report = run_multi_seed_benchmark(
        scenario,
        seeds=(2, 3),
        provider={"provider_id": "sweep-sim", "latency_multiplier": 1.25},
    )

    assert report["valid"]
    assert all(
        item["report"]["scenario"]["provider"]["provider_id"] == "sweep-sim"
        for item in report["runs"]
    )
    assert report["summary"]["adaptive_metrics"]["wall_time_seconds"]["mean"] == 6.25


@pytest.mark.parametrize(
    ("seeds", "exception", "message"),
    [
        ((), ValueError, "at least one"),
        ((1, 1), ValueError, "duplicate"),
        ((-1,), ValueError, "non-negative"),
        ((True,), TypeError, "non-negative integers"),
        (("1",), TypeError, "non-negative integers"),
        ("12", TypeError, "iterable"),
    ],
)
def test_multi_seed_validates_seed_inputs(
    seeds: object, exception: type[Exception], message: str
) -> None:
    with pytest.raises(exception, match=message):
        run_multi_seed_benchmark(seeds=seeds)  # type: ignore[arg-type]


def test_multi_seed_report_is_json_serializable_and_reproducible() -> None:
    first = run_multi_seed_benchmark(seeds=(0, 1, 2))
    second = run_multi_seed_benchmark(seeds=(0, 1, 2))

    assert first == second
    assert json.loads(json.dumps(first))["summary"]["seed_count"] == 3


def test_seed_field_is_preserved_in_each_nested_scenario() -> None:
    report = run_multi_seed_benchmark(seeds=(4, 9))

    assert [item["report"]["scenario"]["seed"] for item in report["runs"]] == [4, 9]
    assert all(
        ControlledScenario.model_validate(item["report"]["scenario"]).seed == item["seed"]
        for item in report["runs"]
    )
