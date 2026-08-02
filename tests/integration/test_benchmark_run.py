"""Integration tests for the benchmark runner (spec 22-25).

- completeness: every cell returns a full result row;
- mode contrast: local repair recovers from a mid-run constraint change,
  repair-disabled dynamic mode strands, transcript/static ignore it;
- crash recovery: worker crash resumes in graph modes, restarts in transcript;
- reproducibility: identical (task, mode, seed) cells produce identical rows
  modulo wall-clock fields.
"""

from __future__ import annotations

import pytest

from lhos.benchmarks.runner import run_suite

MODES = ["transcript", "dynamic_graph_fifo", "dynamic_graph_local_repair"]
PRESETS = ["serial_chain", "constraint_change", "worker_crash", "external_wait"]

# Fields that legitimately differ across identical reruns (wall clock and
# machine-local paths); everything else must be bit-identical.
NON_REPRODUCIBLE = {
    "wall_time_seconds",
    "aupbc_time",
    "scheduler_time_seconds",
    "checkpoint_time_seconds",
    "db_path",
}

REQUIRED_KEYS = {
    "task_id", "preset", "size", "seed", "mode", "success", "run_status",
    "verified_progress", "progress_ratio", "failed_nodes", "invalidated_nodes",
    "input_tokens", "output_tokens", "total_tokens", "model_calls", "tool_calls",
    "wall_time_seconds", "simulated_time_seconds", "model_cost_usd",
    "graph_maintenance_tokens", "verification_tokens", "graph_maintenance_events",
    "scheduler_time_seconds", "checkpoint_time_seconds",
    "aupbc_tokens", "aupbc_time", "aupbc_tool_calls",
    "useful_work_ratio", "replanning_amplification", "invalidated_work_rate",
    "recovery_overhead", "critical_path_stretch", "run_id",
}


@pytest.fixture(scope="module")
def rows(tmp_path_factory):
    work = tmp_path_factory.mktemp("bench")
    return run_suite(modes=MODES, presets=PRESETS, seeds=[1, 2], size="small", work_root=work)


def _row(rows, preset, mode, seed):
    matches = [
        r for r in rows
        if r["preset"] == preset and r["mode"] == mode and r["seed"] == seed
    ]
    assert len(matches) == 1, f"missing cell {preset}/{mode}/s{seed}"
    return matches[0]


class TestCompleteness:
    def test_every_cell_has_a_full_row(self, rows):
        assert len(rows) == len(PRESETS) * len(MODES) * 2
        for row in rows:
            assert REQUIRED_KEYS <= set(row), f"missing keys: {REQUIRED_KEYS - set(row)}"
            assert 0.0 <= row["progress_ratio"] <= 1.0
            assert row["total_tokens"] > 0

    def test_benign_presets_succeed_everywhere(self, rows):
        for mode in MODES:
            for seed in (1, 2):
                assert _row(rows, "serial_chain", mode, seed)["success"]
                assert _row(rows, "external_wait", mode, seed)["success"]


class TestModeContrast:
    def test_local_repair_recovers_constraint_change(self, rows):
        row = _row(rows, "constraint_change", "dynamic_graph_local_repair", 1)
        assert row["success"]
        assert row["oracle_affected_nodes"] > 0
        assert row["re_executed_nodes"] > 0
        assert 0.0 < row["replanning_amplification"] <= 1.0

    def test_repair_disabled_strands_constraint_change(self, rows):
        row = _row(rows, "constraint_change", "dynamic_graph_fifo", 1)
        assert not row["success"]
        assert row["run_status"] == "failed"
        assert row["replanning_amplification"] == 0.0

    def test_transcript_cannot_react_to_constraint_change(self, rows):
        row = _row(rows, "constraint_change", "transcript", 1)
        # Completes "successfully" while carrying silently stale work: the
        # oracle records affected nodes, the transcript replans none.
        assert row["success"]
        assert row["oracle_affected_nodes"] > 0
        assert row["replanning_amplification"] == 0.0

    def test_crash_recovery_contrast(self, rows):
        graph = _row(rows, "worker_crash", "dynamic_graph_local_repair", 1)
        baseline = _row(rows, "worker_crash", "transcript", 1)
        assert graph["success"] and graph["crashes"] == 1
        assert baseline["success"] and baseline["restarts"] == 1
        # Restart-from-scratch costs more than checkpointed resume.
        assert baseline["total_tokens"] > graph["total_tokens"]
        assert baseline["recovery_overhead"] > graph["recovery_overhead"]


class TestReproducibility:
    def test_identical_cells_are_reproducible(self, tmp_path):
        kwargs = dict(
            modes=["transcript", "dynamic_graph_local_repair"],
            presets=["serial_chain", "constraint_change"],
            seeds=[1, 2],
            size="small",
        )
        first = run_suite(work_root=tmp_path / "a", **kwargs)
        second = run_suite(work_root=tmp_path / "b", **kwargs)
        assert len(first) == len(second)
        for a, b in zip(first, second):
            assert a["run_id"] == b["run_id"]
            diff = {
                k: (a[k], b[k])
                for k in a
                if k not in NON_REPRODUCIBLE and a[k] != b[k]
            }
            assert not diff, f"non-reproducible fields: {diff}"
