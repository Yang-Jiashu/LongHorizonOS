"""CLI gates for the real-sleep adaptive wall-clock benchmark."""

from __future__ import annotations

import json

from lhos.cli import core


def test_adaptive_wallclock_runtime_cli_json_is_machine_readable(capsys) -> None:
    assert (
        core.main(
            [
                "benchmark",
                "wallclock-adaptive-runtime",
                "--delay-ms",
                "2",
                "--json",
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)

    assert report["benchmark"] == "adaptive_wallclock_runtime"
    assert report["valid"] is True
    assert report["static"]["epochs"] == 3
    assert report["adaptive"]["epochs"] == 2
    assert report["scope"]["real_perf_counter_wall_clock"] is True
    assert report["scope"]["wall_clock_gate"] is False


def test_adaptive_wallclock_runtime_cli_human_scope_is_honest(capsys) -> None:
    assert (
        core.main(
            [
                "benchmark",
                "wallclock-adaptive-runtime",
                "--delay-ms",
                "2",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out

    assert "REAL-WALL-CLOCK ADAPTIVE RUNTIME BENCHMARK" in output
    assert "static:   epochs=3" in output
    assert "adaptive: epochs=2" in output
    assert "real asyncio.sleep/perf_counter" in output
    assert "not an LLM/GPU/production speed claim" in output
