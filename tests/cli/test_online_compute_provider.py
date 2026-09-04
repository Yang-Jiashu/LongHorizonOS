"""CLI coverage for the deterministic online-compute provider profile."""

from __future__ import annotations

import json

from lhos.cli import core


def test_online_compute_provider_flags_are_forwarded(capsys) -> None:
    exit_code = core.main(
        [
            "benchmark",
            "online-compute",
            "--json",
            "--provider-id",
            "cheap-sim",
            "--latency-multiplier",
            "1.5",
            "--input-token-multiplier",
            "0.5",
            "--output-token-multiplier",
            "0.5",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["scope"]["provider_id"] == "cheap-sim"
    assert payload["scenario"]["provider"]["latency_multiplier"] == 1.5
    assert payload["scenario"]["provider"]["input_token_multiplier"] == 0.5
    assert payload["scenario"]["provider"]["output_token_multiplier"] == 0.5
    assert payload["comparison"]["stale_attempt_reduction"] == 3
    assert payload["comparison"]["reexecuted_task_reduction"] == 2


def test_online_compute_provider_flags_reject_invalid_multiplier(capsys) -> None:
    exit_code = core.main(
        [
            "benchmark",
            "online-compute",
            "--json",
            "--latency-multiplier",
            "0",
        ]
    )
    assert exit_code == 2
    assert "benchmark error" in capsys.readouterr().err
