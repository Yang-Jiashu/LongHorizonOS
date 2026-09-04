from __future__ import annotations

import io
import json
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from lhos.benchmarks.semantic_repair.live_model import (
    StepCodeAPIError,
    StepCodeChatClient,
    _summarize_calls,
)


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def test_client_rotates_keys_across_requests() -> None:
    authorizations: list[str] = []

    def opener(request: Request, *, timeout: float) -> FakeResponse:
        assert timeout == 3.0
        authorizations.append(request.get_header("Authorization", ""))
        return FakeResponse({"data": [{"id": "gpt-test"}]})

    client = StepCodeChatClient(
        api_keys=["secret-one", "secret-two"],
        timeout_seconds=3.0,
        opener=opener,
    )

    assert client.list_models() == ["gpt-test"]
    assert client.list_models() == ["gpt-test"]
    assert client.list_models() == ["gpt-test"]
    assert authorizations == [
        "Bearer secret-one",
        "Bearer secret-two",
        "Bearer secret-one",
    ]


def test_chat_parses_content_and_marks_reported_usage() -> None:
    def opener(request: Request, *, timeout: float) -> FakeResponse:
        assert request.full_url.endswith("/chat/completions")
        return FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": [
                                {"type": "text", "text": "PASS"},
                            ]
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 1,
                    "total_tokens": 12,
                },
            }
        )

    call = StepCodeChatClient(api_key="secret", opener=opener).chat(
        model="gpt-test",
        task_id="T0",
    )

    assert call.passed
    assert call.instruction_followed
    assert call.usage_reported
    assert call.total_tokens == 12
    assert _summarize_calls([call])["token_totals_complete"]


@pytest.mark.parametrize("usage", [None, {}, {"total_tokens": 0}])
def test_chat_marks_missing_or_zero_usage_as_partial(usage: object) -> None:
    payload: dict[str, Any] = {
        "choices": [{"message": {"content": "PASS"}}],
    }
    if usage is not None:
        payload["usage"] = usage

    def opener(request: Request, *, timeout: float) -> FakeResponse:
        return FakeResponse(payload)

    call = StepCodeChatClient(api_key="secret", opener=opener).chat(
        model="gpt-test",
        task_id="T0",
    )
    summary = _summarize_calls([call])

    assert not call.usage_reported
    assert summary["usage_reported_calls"] == 0
    assert summary["usage_missing_calls"] == 1
    assert not summary["token_totals_complete"]


def test_http_error_redacts_all_configured_keys() -> None:
    def opener(request: Request, *, timeout: float) -> FakeResponse:
        body = b'{"error":"secret-one and secret-two were rejected"}'
        raise HTTPError(request.full_url, 401, "Unauthorized", None, io.BytesIO(body))

    client = StepCodeChatClient(
        api_keys=["secret-one", "secret-two"],
        opener=opener,
    )

    with pytest.raises(StepCodeAPIError) as caught:
        client.list_models()

    message = str(caught.value)
    assert "secret-one" not in message
    assert "secret-two" not in message
    assert message.count("[REDACTED]") == 2
