"""Tests for the thin OpenAI-compatible httpx client (no network, no openai SDK)."""

from __future__ import annotations

import json

import httpx
import pytest

from hibiki.runtime.openai_client import (
    ChatMessage,
    ModelClientError,
    OpenAICompatibleClient,
)

_API_KEY = "sk-super-secret-key"


def _client(handler, *, max_retries: int = 2) -> OpenAICompatibleClient:
    client = OpenAICompatibleClient(
        base_url="http://model.test/v1",
        api_key=_API_KEY,
        model="test-model",
        max_retries=max_retries,
        transport=httpx.MockTransport(handler),
    )
    # Retry tests must not sleep wall-clock time.
    client._sleep = lambda _delay: None
    return client


def _reply(content: str = "ok", *, finish_reason: str = "stop") -> dict:
    return {
        "choices": [
            {
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason,
            }
        ],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
    }


def test_normal_reply_is_parsed():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_reply("hello"))

    client = _client(handler)
    try:
        reply = client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()

    assert reply.content == "hello"
    assert reply.tool_calls == ()
    assert reply.finish_reason == "stop"
    assert reply.usage["total_tokens"] == 5
    assert len(seen) == 1
    body = json.loads(seen[0].content)
    assert body["model"] == "test-model"
    assert body["messages"] == [{"role": "user", "content": "hi"}]
    assert body["temperature"] == 0.0
    assert "tools" not in body
    assert "stream" not in body
    assert seen[0].headers["authorization"] == f"Bearer {_API_KEY}"


def test_tool_call_reply_is_normalized():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {
                                        "name": "fs_read",
                                        "arguments": '{"path": "notes.txt"}',
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            },
        )

    client = _client(handler)
    tools = [{"type": "function", "function": {"name": "fs_read", "parameters": {}}}]
    try:
        reply = client.chat([ChatMessage(role="user", content="go")], tools=tools)
    finally:
        client.close()

    assert reply.content is None
    assert reply.finish_reason == "tool_calls"
    assert len(reply.tool_calls) == 1
    call = reply.tool_calls[0]
    assert call["id"] == "call_1"
    assert call["type"] == "function"
    assert call["function"]["name"] == "fs_read"
    assert json.loads(call["function"]["arguments"]) == {"path": "notes.txt"}
    assert json.loads(seen[0].content)["tools"] == tools


def test_legacy_function_call_is_supported():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "function_call": {"name": "fs_list", "arguments": '{"path": "."}'},
                        },
                        "finish_reason": "function_call",
                    }
                ]
            },
        )

    client = _client(handler)
    try:
        reply = client.chat([ChatMessage(role="user", content="go")])
    finally:
        client.close()
    assert len(reply.tool_calls) == 1
    assert reply.tool_calls[0]["function"]["name"] == "fs_list"


def test_rate_limit_429_is_retried_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={"error": "slow down"})
        return httpx.Response(200, json=_reply("recovered"))

    client = _client(handler)
    try:
        reply = client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert reply.content == "recovered"
    assert calls["n"] == 2


def test_server_error_retries_are_bounded():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, text="boom")

    client = _client(handler, max_retries=2)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "server"
    assert excinfo.value.retryable is True
    assert calls["n"] == 3


def test_bad_request_400_is_not_retried():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": "bad payload"})

    client = _client(handler)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "bad_request"
    assert excinfo.value.retryable is False
    assert excinfo.value.status == 400
    assert calls["n"] == 1


def test_auth_error_is_not_retried():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "nope"})

    client = _client(handler)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "auth"
    assert excinfo.value.retryable is False


def test_timeout_is_retryable():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.TimeoutException("too slow")

    client = _client(handler, max_retries=0)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "timeout"
    assert excinfo.value.retryable is True
    assert calls["n"] == 1


def test_missing_choices_is_a_protocol_error():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"id": "x", "object": "chat.completion"})

    client = _client(handler, max_retries=0)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "protocol"
    assert excinfo.value.retryable is False
    assert calls["n"] == 1


def test_non_json_tool_arguments_is_a_protocol_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {"name": "fs_read", "arguments": "not-json"},
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            },
        )

    client = _client(handler, max_retries=0)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert excinfo.value.kind == "protocol"
    assert excinfo.value.retryable is False


def test_api_key_never_appears_in_error_messages():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text=f"invalid api key {_API_KEY}")

    client = _client(handler)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert _API_KEY not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_error_message_redacts_key_even_in_server_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=f"upstream echoed {_API_KEY}")

    client = _client(handler, max_retries=0)
    try:
        with pytest.raises(ModelClientError) as excinfo:
            client.chat([ChatMessage(role="user", content="hi")])
    finally:
        client.close()
    assert _API_KEY not in str(excinfo.value)


def test_message_dump_includes_tool_metadata():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_reply())

    client = _client(handler)
    try:
        client.chat(
            [
                ChatMessage(
                    role="assistant",
                    content=None,
                    tool_calls=(
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "fs_read", "arguments": "{}"},
                        },
                    ),
                ),
                ChatMessage(role="tool", content="{}", tool_call_id="call_1", name="fs_read"),
            ]
        )
    finally:
        client.close()
    messages = json.loads(seen[0].content)["messages"]
    assert messages[0]["tool_calls"][0]["id"] == "call_1"
    assert messages[1]["tool_call_id"] == "call_1"
    assert messages[1]["name"] == "fs_read"
