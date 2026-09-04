"""Patch proposal Pydantic validation tests.

Verifies that GraphPatchProposal rejects malformed operations at model
construction time (ValueError from Pydantic field validators).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.runtimes.verified_progress.patches import (
    AddNodeOp,
    GraphPatchProposal,
)


class TestAddNodeValidation:
    def test_callable_in_execution_spec_rejected(self):
        def foo():
            return 1

        with pytest.raises(ValidationError):
            AddNodeOp(
                node_id="n1",
                graph_id="G",
                node_type="task",
                created_by_pid="p1",
                execution_spec={"run": foo},
            )

    def test_host_absolute_path_rejected(self):
        with pytest.raises(ValidationError):
            AddNodeOp(
                node_id="n1",
                graph_id="G",
                node_type="task",
                created_by_pid="p1",
                execution_spec={"cmd": "/etc/passwd"},
            )

    def test_forbidden_key_callback_rejected(self):
        with pytest.raises(ValidationError):
            AddNodeOp(
                node_id="n1",
                graph_id="G",
                node_type="task",
                created_by_pid="p1",
                execution_spec={"callback": "x"},
            )

    def test_forbidden_key_fn_rejected(self):
        with pytest.raises(ValidationError):
            AddNodeOp(
                node_id="n1",
                graph_id="G",
                node_type="task",
                created_by_pid="p1",
                execution_spec={"fn": "x"},
            )

    def test_forbidden_key_function_rejected(self):
        with pytest.raises(ValidationError):
            AddNodeOp(
                node_id="n1",
                graph_id="G",
                node_type="task",
                created_by_pid="p1",
                execution_spec={"function": "x"},
            )

    def test_valid_execution_spec_accepted(self):
        op = AddNodeOp(
            node_id="n1",
            graph_id="G",
            node_type="task",
            created_by_pid="p1",
            execution_spec={"cmd": "echo", "retries": 3},
        )
        assert op.execution_spec == {"cmd": "echo", "retries": 3}


class TestPatchProposalValidation:
    def test_empty_operations_ok_at_model_level(self, graph):
        # Model construction does not validate emptiness — that happens at
        # submit time. Pydantic accepts the empty tuple.
        gid, rt = graph
        p = GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="k1",
            operations=(),
        )
        assert p.operations == ()

    def test_invalid_addnode_in_ops_rejected_at_construction(self):
        with pytest.raises(ValidationError):
            GraphPatchProposal(
                graph_id="G",
                expected_graph_version=0,
                author_pid="p1",
                idempotency_key="k1",
                operations=(
                    AddNodeOp(
                        node_id="n1",
                        graph_id="G",
                        node_type="task",
                        created_by_pid="p1",
                        execution_spec={"callback": "x"},
                    ),
                ),
            )

    def test_composite_key_shape(self, graph):
        gid, rt = graph
        p = GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="hello",
            operations=(),
        )
        assert p.composite_key == ("p1", gid, "hello")

    def test_empty_ops_rejected_at_submit(self, graph):
        from lhos.runtimes.verified_progress.errors import VPGError

        gid, rt = graph
        p = GraphPatchProposal(
            graph_id=gid,
            expected_graph_version=0,
            author_pid="p1",
            idempotency_key="k-empty",
            operations=(),
        )
        with pytest.raises(VPGError) as ei:
            rt.submit_patch(p)
        assert ei.value.code.value == "PATCH_EMPTY"
