"""TaskRequirements decoding (Section 10)."""
from __future__ import annotations

from lhos.runtimes.multi_agent.requirements import decode_task_requirements


def test_decode_from_scheduler_block():
    payload = {
        "metadata": {
            "scheduler": {
                "task_kind": "code_review",
                "required_specializations": ["python"],
                "required_tools": ["shell"],
                "priority": 100,
                "max_attempts": 3,
            }
        }
    }
    r = decode_task_requirements("t1", payload)
    assert r.task_id == "t1"
    assert r.task_kind == "code_review"
    assert r.required_specializations == ("python",)
    assert r.required_tools == ("shell",)
    assert r.priority == 100
    assert r.max_attempts == 3


def test_decode_falls_back_to_task_kind_top_level():
    payload = {"task_kind": "build", "metadata": {}}
    r = decode_task_requirements("t2", payload)
    assert r.task_kind == "build"


def test_decode_handles_missing_scheduler_block_gracefully():
    payload = {"metadata": {}}
    r = decode_task_requirements("t3", payload)
    assert r.required_specializations == ()
    assert r.required_tools == ()
    assert r.required_capabilities == ()


def test_decode_rejects_bad_metadata_shape():
    payload = {"metadata": "not-a-dict"}
    r = decode_task_requirements("t4", payload)
    assert r.task_kind == ""
    assert r.required_specializations == ()
