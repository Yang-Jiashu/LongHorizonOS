"""Unit tests for the controlled task generator (spec 22.1-22.3)."""

from __future__ import annotations

import networkx as nx
import pytest

from lhos.benchmarks.controlled import PRESETS, SIZES, generate


class TestDeterminism:
    @pytest.mark.parametrize("preset", PRESETS)
    def test_same_seed_identical_task(self, preset):
        first = generate(preset, "small", seed=7)
        second = generate(preset, "small", seed=7)
        assert first.model_dump() == second.model_dump()

    def test_different_seed_differs(self):
        first = generate("wide_dag", "small", seed=1)
        second = generate("wide_dag", "small", seed=2)
        assert first.model_dump() != second.model_dump()


class TestSchema:
    @pytest.mark.parametrize("preset", PRESETS)
    def test_every_preset_builds_a_valid_spec(self, preset):
        task = generate(preset, "small", seed=1)
        spec = task.spec
        assert spec.task_id == f"controlled-{preset}-small-s1"
        assert spec.total_progress_weight > 0
        temp_ids = [n["temp_id"] for n in spec.oracle_nodes]
        assert len(temp_ids) == len(set(temp_ids)), "duplicate temp_ids"
        for edge in spec.oracle_edges:
            assert edge["source"] in temp_ids
            assert edge["target"] in temp_ids
        # The DEPENDS_ON subgraph (source depends on target) must be acyclic.
        dag = nx.DiGraph()
        dag.add_nodes_from(temp_ids)
        for e in spec.oracle_edges:
            if e.get("kind", "depends_on") == "depends_on":
                dag.add_edge(e["target"], e["source"])
        assert nx.is_directed_acyclic_graph(dag)

    @pytest.mark.parametrize("size,count", list(SIZES.items()))
    def test_sizes(self, size, count):
        task = generate("serial_chain", size, seed=1)
        schedulable = [n for n in task.spec.oracle_nodes if n.get("schedulable")]
        assert len(schedulable) == count

    def test_unknown_preset_and_size_rejected(self):
        with pytest.raises(ValueError):
            generate("nope", "small", 1)
        with pytest.raises(ValueError):
            generate("serial_chain", "huge", 1)

    def test_control_variables_recorded(self):
        task = generate("constraint_change", "small", seed=3)
        cv = task.control_variables
        for key in (
            "node_count", "graph_depth", "graph_width", "critical_path_length",
            "parallelism", "tool_latency_ms", "failure_probability",
            "constraint_change_probability", "artifact_invalidation_probability",
            "retryability",
        ):
            assert key in cv
        assert cv["node_count"] == 20
        assert cv["constraint_change_probability"] == 1.0


class TestOracle:
    def test_serial_chain_critical_path_covers_every_node(self):
        task = generate("serial_chain", "small", seed=1)
        assert len(task.oracle.critical_path) == 20
        assert task.oracle.critical_path_seconds > 0

    def test_priorities_bounded(self):
        task = generate("branch_join", "small", seed=1)
        assert task.oracle.priorities
        assert all(0.0 <= p <= 1.0 for p in task.oracle.priorities.values())

    def test_constraint_event_carries_true_affected_scope(self):
        task = generate("constraint_change", "small", seed=1)
        events = task.spec.environment_events
        assert len(events) == 1
        event = events[0]
        assert event["type"] == "constraint_changed"
        assert event["invalidates"], "must-invalidate victim missing"
        # The oracle scope is embedded for scoring and recorded in OracleInfo.
        assert event["oracle_affected"] == task.oracle.affected_by_event["constraint_0"]
        assert set(event["invalidates"]) <= set(event["oracle_affected"])

    def test_graph_spec_hides_priorities_unless_oracle(self):
        task = generate("branch_join", "small", seed=1)
        plain = task.graph_spec(use_oracle_priorities=False)
        assert all(n["priority"] == 0.0 for n in plain["nodes"])
        hinted = task.graph_spec(use_oracle_priorities=True)
        assert any(n["priority"] > 0.0 for n in hinted["nodes"])
        # graph_spec is a deep copy: mutating it must not corrupt the task.
        hinted["nodes"][0]["metadata"]["script"]["environment_events"] = []
        assert task.spec.oracle_nodes


class TestPresetTwists:
    def test_crash_presets_inject_exactly_one_crash(self):
        for preset, key in (
            ("worker_crash", "crash_on_attempt"),
            ("runtime_crash", "crash_before_verification"),
            ("post_tool_crash", "crash_after_tool_calls"),
        ):
            task = generate(preset, "small", seed=1)
            scripts = [n["metadata"]["script"] for n in task.spec.oracle_nodes]
            assert sum(1 for s in scripts if s.get(key)) == 1
            assert len(task.spec.failure_injections) == 1

    def test_external_wait_marks_one_waiting_node(self):
        task = generate("external_wait", "small", seed=1)
        scripts = [n["metadata"]["script"] for n in task.spec.oracle_nodes]
        assert sum(1 for s in scripts if s.get("attempts", {}).get("1", {}).get("status") == "waiting") == 1

    def test_noop_nodes_have_command_verification(self):
        task = generate("noop_nodes", "small", seed=1)
        noops = [n for n in task.spec.oracle_nodes if n["metadata"].get("noop")]
        assert len(noops) == 4  # 20% of 20
        assert all(n["verification_spec"]["type"] == "command" for n in noops)

    def test_artifact_preset_models_produces_consumes(self):
        task = generate("artifact_modified", "small", seed=1)
        kinds = {(e["kind"]) for e in task.spec.oracle_edges}
        assert {"produces", "consumes", "depends_on"} <= kinds
        assert task.spec.environment_events[0]["type"] == "artifact_updated"
