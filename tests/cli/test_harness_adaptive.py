"""CLI JSON gate for the real Harness integration benchmark."""

from __future__ import annotations

import json

from lhos.cli import core


def test_harness_adaptive_cli_json_exposes_authoritative_path(capsys) -> None:
    exit_code = core.main(
        [
            "benchmark",
            "harness-adaptive",
            "--json",
            "--delay-ms",
            "3",
            "--max-concurrency",
            "2",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["scope"]["ownership_path"] is True
    assert payload["scope"]["exact_identity_harness_control"] is True
    assert payload["static"]["runtime_audit"]["harness_control_events"] == 5
    assert payload["adaptive"]["runtime_audit"]["harness_control_events"] == 4
    assert payload["static"]["verified_task_ids"] == payload["adaptive"]["verified_task_ids"]
    assert payload["static"]["wall_clock_measured"] is True
    assert payload["static"]["usage"]["synthetic_usage"] is True
