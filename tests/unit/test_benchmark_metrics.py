"""Unit tests for benchmark metrics (spec 24.3) on known cases."""

from __future__ import annotations

import pytest

from lhos.benchmarks import metrics


class TestAupbc:
    def test_linear_progress_is_half(self):
        curve = [
            {"tokens": 0, "verified_progress": 0.0},
            {"tokens": 100, "verified_progress": 1.0},
        ]
        assert metrics.aupbc(curve, "tokens") == pytest.approx(0.5)

    def test_staircase_known_value(self):
        curve = [
            {"tokens": 0, "verified_progress": 0.0},
            {"tokens": 50, "verified_progress": 0.5},
            {"tokens": 100, "verified_progress": 1.0},
        ]
        # area = 50*0.25 + 50*0.75 = 50; denom = 100*1.0
        assert metrics.aupbc(curve, "tokens") == pytest.approx(0.5)

    def test_early_progress_beats_late_progress(self):
        early = [
            {"tokens": 0, "verified_progress": 0.0},
            {"tokens": 10, "verified_progress": 0.9},
            {"tokens": 100, "verified_progress": 1.0},
        ]
        late = [
            {"tokens": 0, "verified_progress": 0.0},
            {"tokens": 90, "verified_progress": 0.1},
            {"tokens": 100, "verified_progress": 1.0},
        ]
        assert metrics.aupbc(early, "tokens") > metrics.aupbc(late, "tokens")

    def test_empty_and_degenerate_curves(self):
        assert metrics.aupbc([], "tokens") == 0.0
        assert metrics.aupbc([{"tokens": 0, "verified_progress": 0.0}], "tokens") == 0.0


class TestUsefulWorkRatio:
    def test_all_useful(self):
        ex = [
            {"node_id": "a", "attempt": 1, "tokens": 100},
            {"node_id": "b", "attempt": 1, "tokens": 200},
        ]
        assert metrics.useful_work_ratio(ex, {"a", "b"}) == pytest.approx(1.0)

    def test_retried_node_counts_only_final_attempt(self):
        ex = [
            {"node_id": "a", "attempt": 1, "tokens": 100},  # superseded
            {"node_id": "a", "attempt": 2, "tokens": 100},  # final, verified
            {"node_id": "b", "attempt": 1, "tokens": 100},
        ]
        # useful = 200 (a's final + b) / 300 total
        assert metrics.useful_work_ratio(ex, {"a", "b"}) == pytest.approx(2 / 3)

    def test_failed_node_is_not_useful(self):
        ex = [{"node_id": "a", "attempt": 1, "tokens": 100}]
        assert metrics.useful_work_ratio(ex, set()) == 0.0

    def test_zero_total_guard(self):
        assert metrics.useful_work_ratio([], set()) == 0.0


class TestInvalidatedWorkRate:
    def test_no_retries_is_zero(self):
        ex = [
            {"node_id": "a", "attempt": 1, "tokens": 100},
            {"node_id": "b", "attempt": 1, "tokens": 100},
        ]
        assert metrics.invalidated_work_rate(ex) == 0.0

    def test_superseded_attempts(self):
        ex = [
            {"node_id": "a", "attempt": 1, "tokens": 100},
            {"node_id": "a", "attempt": 2, "tokens": 100},
            {"node_id": "b", "attempt": 1, "tokens": 200},
        ]
        assert metrics.invalidated_work_rate(ex) == pytest.approx(0.25)

    def test_zero_total_guard(self):
        assert metrics.invalidated_work_rate([]) == 0.0


class TestRatios:
    def test_replanning_amplification(self):
        assert metrics.replanning_amplification(0, 0) == 1.0  # neutral
        assert metrics.replanning_amplification(0, 4) == 0.0  # repair missing
        assert metrics.replanning_amplification(4, 4) == 1.0  # perfect repair
        assert metrics.replanning_amplification(8, 4) == 2.0  # over-replanning

    def test_recovery_overhead(self):
        assert metrics.recovery_overhead(0.0, 0.0) == 0.0
        assert metrics.recovery_overhead(50.0, 0.0) == 0.0  # unknown remaining
        assert metrics.recovery_overhead(50.0, 200.0) == pytest.approx(0.25)

    def test_critical_path_stretch(self):
        assert metrics.critical_path_stretch(10.0, 0.0) == 0.0
        assert metrics.critical_path_stretch(10.0, 5.0) == pytest.approx(2.0)
        assert metrics.critical_path_stretch(5.0, 5.0) == pytest.approx(1.0)
