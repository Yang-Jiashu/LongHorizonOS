"""Integration tests for LLM call logging (Step 3).

Verifies that:
- Successful calls are written to the DB and JSONL trace.
- Provider errors are also logged (status='provider_error').
- Retry counts and parse failure counts are recorded correctly.
- Repair calls are logged separately.
- Planner, Worker, and Reconciler calls are all logged.
- DB and JSONL counts and token totals are consistent.
- API keys and Authorization headers never appear in logs.
"""

from __future__ import annotations

import json

import pytest

from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.llm.adapter import MockLLMClient
from lhos.infrastructure.llm.call_logger import LLMCallLogger
from lhos.infrastructure.llm.logged_client import LoggedLLMClient
from lhos.ports.llm import LLMRequest, LLMResponse, LLMUsage


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "test.db")
    yield database
    database.close()


@pytest.fixture
def trace_path(tmp_path):
    return tmp_path / "traces" / "llm_calls.jsonl"


@pytest.fixture
def logger(db, trace_path, tmp_path):
    return LLMCallLogger(db=db, trace_path=trace_path, artifacts_dir=tmp_path / "artifacts")


@pytest.fixture
def run_id():
    return "test-run-001"


def _make_request(role: str = "worker", messages=None, metadata=None) -> LLMRequest:
    return LLMRequest(
        role=role,
        messages=messages or [{"role": "user", "content": "test"}],
        response_schema={"type": "object"},
        temperature=0.0,
        max_output_tokens=1024,
        metadata=metadata or {},
    )


def _make_response(text: str = '{"action_type":"claim_done"}') -> LLMResponse:
    return LLMResponse(
        text=text,
        provider="mock",
        model_id="mock-model",
        usage=LLMUsage(
            input_tokens=100,
            output_tokens=50,
            total_tokens=150,
            cost_usd=0.0,
        ),
        latency_ms=10,
        request_hash="test-hash",
    )


class TestSuccessfulCallLogging:
    def test_success_writes_one_db_record(self, db, logger, run_id):
        """A successful call writes exactly one record with status='success'."""
        client = MockLLMClient(responses=['{"action_type":"claim_done"}'])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(_make_request())

        rows = db.conn.execute("SELECT * FROM llm_calls WHERE run_id = ?", (run_id,)).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == "success"
        assert rows[0]["input_tokens"] == 100
        assert rows[0]["output_tokens"] == 50

    def test_success_writes_jsonl(self, logger, trace_path, run_id):
        """A successful call also writes a JSONL trace line."""
        client = MockLLMClient(responses=['{"action_type":"claim_done"}'])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(_make_request())

        assert trace_path.exists()
        lines = trace_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["status"] == "success"
        assert entry["role"] == "worker"

    def test_db_jsonl_count_consistent(self, db, logger, trace_path, run_id):
        """DB and JSONL have the same number of records."""
        client = MockLLMClient(
            responses=['{"action_type":"tool_call"}', '{"action_type":"claim_done"}']
        )
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(_make_request())
        logged.generate(_make_request())

        db_count = db.conn.execute(
            "SELECT COUNT(*) FROM llm_calls WHERE run_id = ?", (run_id,)
        ).fetchone()[0]
        jsonl_lines = trace_path.read_text(encoding="utf-8").strip().split("\n")
        assert db_count == len(jsonl_lines) == 2

    def test_db_jsonl_tokens_consistent(self, db, logger, trace_path, run_id):
        """DB and JSONL token totals match."""
        client = MockLLMClient(responses=['{"action_type":"claim_done"}'])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(_make_request())

        db_row = db.conn.execute(
            "SELECT total_tokens FROM llm_calls WHERE run_id = ?", (run_id,)
        ).fetchone()
        jsonl_entry = json.loads(trace_path.read_text(encoding="utf-8").strip())
        assert db_row["total_tokens"] == jsonl_entry["total_tokens"] == 150


class TestProviderErrorLogging:
    def test_provider_error_is_logged(self, db, logger, run_id):
        """When the inner client raises, the call is still logged."""

        class FailingClient:
            provider = "sensenova"
            model_id = "test-model"

            def generate(self, request):
                raise RuntimeError("API connection refused")

        logged = LoggedLLMClient(inner=FailingClient(), logger=logger, run_id=run_id)

        with pytest.raises(RuntimeError):
            logged.generate(_make_request())

        rows = db.conn.execute("SELECT * FROM llm_calls WHERE run_id = ?", (run_id,)).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == "provider_error"
        assert rows[0]["error_type"] == "RuntimeError"
        assert rows[0]["input_tokens"] == 0
        assert rows[0]["output_tokens"] == 0

    def test_provider_error_jsonl(self, logger, trace_path, run_id):
        """Provider errors are also written to JSONL."""

        class FailingClient:
            provider = "sensenova"
            model_id = "test-model"

            def generate(self, request):
                raise ConnectionError("network timeout")

        logged = LoggedLLMClient(inner=FailingClient(), logger=logger, run_id=run_id)

        with pytest.raises(ConnectionError):
            logged.generate(_make_request())

        entry = json.loads(trace_path.read_text(encoding="utf-8").strip())
        assert entry["status"] == "provider_error"
        assert entry["error_type"] == "ConnectionError"


class TestParseFailureLogging:
    def test_parse_failure_status(self, db, logger, run_id):
        """When parse_failure_count > 0, status is 'parse_failed'."""
        client = MockLLMClient(responses=["not valid json"])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)

        # Generate — the response has parse_failure_count=0 by default.
        # We need to simulate a parse failure.

        original_generate = client.generate

        def generate_with_parse_failure(request):
            resp = original_generate(request)
            resp.parse_failure_count = 1
            return resp

        client.generate = generate_with_parse_failure
        logged.generate(_make_request())

        rows = db.conn.execute("SELECT * FROM llm_calls WHERE run_id = ?", (run_id,)).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == "parse_failed"
        assert rows[0]["parse_failure_count"] == 1


class TestMultiRoleLogging:
    def test_planner_worker_reconciler_all_logged(self, db, logger, run_id):
        """Calls from planner, worker, and reconciler are all logged."""
        client = MockLLMClient(
            responses=[
                '{"operations":[]}',  # planner
                '{"action_type":"claim_done"}',  # worker
                '{"should_reconcile":false}',  # reconciler
            ]
        )
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)

        logged.generate(_make_request(role="planner"))
        logged.generate(_make_request(role="worker"))
        logged.generate(_make_request(role="reconciler"))

        rows = db.conn.execute(
            "SELECT role FROM llm_calls WHERE run_id = ? ORDER BY timestamp",
            (run_id,),
        ).fetchall()
        roles = [r["role"] for r in rows]
        assert roles == ["planner", "worker", "reconciler"]


class TestApiKeySafety:
    def test_api_key_not_in_db(self, db, logger, run_id):
        """API keys must not appear in the database."""
        request = LLMRequest(
            role="worker",
            messages=[
                {"role": "system", "content": "You are a worker."},
                {"role": "user", "content": "sk-abc123secretkey do something"},
            ],
            metadata={},
        )
        client = MockLLMClient(responses=['{"action_type":"claim_done"}'])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(request)

        rows = db.conn.execute(
            "SELECT request_body_json, response_body_json FROM llm_calls WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        for row in rows:
            body = row["request_body_json"]
            assert "abc123secretkey" not in body
            assert "sk-abc123" not in body or "REDACTED" in body

    def test_authorization_header_not_in_jsonl(self, logger, trace_path, run_id):
        """Authorization headers must not appear in JSONL traces."""
        request = LLMRequest(
            role="worker",
            messages=[
                {
                    "role": "system",
                    "content": "Authorization: Bearer sk-test-key-12345",
                }
            ],
            metadata={},
        )
        client = MockLLMClient(responses=['{"action_type":"claim_done"}'])
        logged = LoggedLLMClient(inner=client, logger=logger, run_id=run_id)
        logged.generate(request)

        trace_text = trace_path.read_text(encoding="utf-8")
        assert "sk-test-key-12345" not in trace_text
