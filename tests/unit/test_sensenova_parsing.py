"""Unit tests for SenseNova response parsing (Step 7).

Tests cover all response scenarios:
- content only (normal)
- reasoning + content (thinking mode with final answer)
- reasoning only (content empty — should trigger repair, not use reasoning)
- empty content (should raise error)
- valid JSON in content
- JSON wrapped in markdown fences
- invalid JSON (should trigger repair)
- repair success
- repair failure
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lhos.infrastructure.llm.sensenova import SenseNovaClient, SenseNovaError
from lhos.ports.llm import LLMRequest


def _make_request() -> LLMRequest:
    return LLMRequest(
        role="worker",
        messages=[{"role": "user", "content": "test"}],
        response_schema={"type": "object"},
        temperature=0.0,
        max_output_tokens=1024,
        metadata={},
    )


def _mock_api_response(content: str = "", reasoning: str = "", usage: dict | None = None):
    """Create a mock API response dict."""
    return {
        "id": "test-req-123",
        "choices": [
            {
                "message": {
                    "content": content,
                    "reasoning": reasoning,
                }
            }
        ],
        "usage": usage
        or {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
        },
    }


class TestContentOnlyResponse:
    """content is present, reasoning is empty."""

    def test_content_only_parsed_correctly(self):
        """When content has valid JSON, it's used as the response text."""
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.return_value = _mock_api_response(
                content='{"action_type":"claim_done","summary":"done"}'
            )
            response = client.generate(_make_request())

        assert response.text == '{"action_type":"claim_done","summary":"done"}'
        assert response.parse_failure_count == 0


class TestReasoningAndContent:
    """Both reasoning and content are present."""

    def test_content_used_not_reasoning(self):
        """When both are present, content is used; reasoning is ignored."""
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.return_value = _mock_api_response(
                content='{"action_type":"claim_done"}',
                reasoning="I should claim done because the task is complete.",
            )
            response = client.generate(_make_request())

        assert response.text == '{"action_type":"claim_done"}'
        assert "reasoning" not in response.text.lower()


class TestReasoningOnly:
    """content is empty, reasoning is non-empty (Step 7 critical fix)."""

    def test_reasoning_not_used_as_action(self):
        """When content is empty but reasoning has text, reasoning is NOT used."""
        client = SenseNovaClient(api_key="test-key")

        repair_response = _mock_api_response(
            content='{"action_type":"claim_done","summary":"repaired"}'
        )

        with patch.object(client, "_make_request") as mock:
            # First call returns empty content + reasoning.
            # Second call (repair) returns valid content.
            mock.side_effect = [
                _mock_api_response(content="", reasoning="I think we should claim done."),
                repair_response,
            ]
            response = client.generate(_make_request())

        # The final text should be from the repair, not the reasoning.
        assert "claim_done" in response.text
        assert "I think we should claim done" not in response.text
        assert response.parse_failure_count == 1


class TestEmptyContent:
    """Both content and reasoning are empty."""

    def test_empty_content_raises_error(self):
        """When both content and reasoning are empty, an error is raised."""
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.return_value = _mock_api_response(content="", reasoning="")
            with pytest.raises(SenseNovaError, match="empty content"):
                client.generate(_make_request())


class TestValidJsonInContent:
    """Content contains valid JSON."""

    def test_valid_json_parsed(self):
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.return_value = _mock_api_response(
                content='{"action_type":"tool_call","tool_request":{"tool_name":"shell","arguments":{"command":"ls"}}}'
            )
            response = client.generate(_make_request())

        assert response.parsed_output is not None
        assert response.parsed_output["action_type"] == "tool_call"


class TestMarkdownFencedJson:
    """Content has JSON wrapped in markdown fences."""

    def test_markdown_fences_stripped(self):
        """Markdown-fenced JSON is parsed after stripping fences."""
        from lhos.infrastructure.llm.structured_output import parse_structured

        fenced = '```json\n{"action_type":"claim_done"}\n```'
        parsed = parse_structured(
            fenced, type("M", (), {"model_validate": staticmethod(lambda d: d)})()
        )
        assert parsed["action_type"] == "claim_done"


class TestInvalidJsonTriggersRepair:
    """Content has invalid JSON — repair is attempted."""

    def test_invalid_json_triggers_repair_success(self):
        """Invalid JSON triggers a repair; repair returns valid JSON."""
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.side_effect = [
                _mock_api_response(content="This is not JSON at all."),
                _mock_api_response(content='{"action_type":"claim_done"}'),
            ]
            response = client.generate(_make_request())

        assert response.parse_failure_count == 1
        assert "claim_done" in response.text

    def test_invalid_json_repair_also_fails(self):
        """When repair also returns invalid JSON, parse_failure_count stays 1."""
        client = SenseNovaClient(api_key="test-key")

        with patch.object(client, "_make_request") as mock:
            mock.side_effect = [
                _mock_api_response(content="not json"),
                _mock_api_response(content="also not json"),
            ]
            response = client.generate(_make_request())

        assert response.parse_failure_count == 1
        assert response.parsed_output is None
