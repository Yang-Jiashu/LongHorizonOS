"""Smoke test: verify fixture wiring boots."""

from __future__ import annotations


def test_env_fixture_boots(env) -> None:
    assert env["pid"] == "p1"
    assert env["ctx_svc"] is not None
    assert env["ctx_sdk"] is not None
    assert env["artifact_svc"] is not None
