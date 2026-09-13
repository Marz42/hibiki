"""Thin httpx client for an OpenAI-compatible ``/chat/completions`` endpoint.

The project deliberately does **not** use the ``openai`` SDK: its 3.x line moved to
``httpx2``, which this project does not depend on, and M1 only needs a single
non-streaming ``chat.completions`` call.  Everything here is therefore plain
``httpx`` plus explicit error classification, so the adapter can decide what is
retryable instead of guessing at SDK exception types.

The API key is injected at construction time and is *never* logged, persisted or
echoed into an exception message (:meth:`OpenAICompatibleClient._redact`).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

#: Backoff schedule for retryable failures (seconds): bounded and deterministic.
_BACKOFF_BASE_S = 0.25
_BACKOFF_MAX_S = 2.0


@dataclass(frozen=True)
class ChatMessage:
    """One message in an OpenAI-compatible conversation."""

    role: str
    content: str | None = None
    tool_call_id: str | None = None
    name: str | None = None
    tool_calls: tuple[dict, ...] = ()


@dataclass(frozen=True)
class ModelReply:
    """A normalized, provider-independent model reply."""

    content: str | None
    tool_calls: tuple[dict, ...]
    finish_reason: str | None
    usage: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)


class ModelClientError(Exception):
    """A classified model-call failure.

    ``kind`` is one of ``rate_limit``, ``timeout``, ``connection``, ``auth``,
    ``bad_request``, ``server`` or ``protocol``.  ``retryable`` says whether the
    caller may retry the identical request.
    """

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        retryable: bool,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.retryable = retryable
        self.status = status


def _classify_status(status: int) -> tuple[str, bool]:
    if status == 429:
        return "rate_limit", True
    if status in (401, 403):
        return "auth", False
    if 500 <= status <= 599:
        return "server", True
    if 400 <= status <= 499:
        return "bad_request", False
    return "protocol", False


def _normalize_tool_call(call: Any) -> dict:
    """Return ``call`` with a string ``arguments`` field, or raise ValueError."""
    if not isinstance(call, dict):
        raise ValueError("tool call must be an object")
    function = call.get("function")
    if not isinstance(function, dict):
        raise ValueError("tool call is missing its function object")
    name = function.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("tool call is missing a function name")
    arguments = function.get("arguments", "{}")
    if isinstance(arguments, dict):
        arguments = json.dumps(arguments)
    if not isinstance(arguments, str):
        raise ValueError("tool call arguments must be a JSON string")
    # A non-JSON argument payload means the provider broke the protocol; never
    # hand a half-parsed call to a tool dispatcher.
    json.loads(arguments)
    normalized = dict(call)
    normalized["type"] = call.get("type") or "function"
    normalized["function"] = {"name": name, "arguments": arguments}
    return normalized


class OpenAICompatibleClient:
    """Minimal synchronous client for an OpenAI-compatible chat endpoint."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        timeout_s: float = 60.0,
        max_retries: int = 2,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not isinstance(base_url, str) or not base_url:
            raise ValueError("base_url must be a non-empty string")
        if not isinstance(api_key, str):
            raise ValueError("api_key must be a string")
        if not isinstance(model, str) or not model:
            raise ValueError("model must be a non-empty string")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.max_retries = int(max_retries)
        self._api_key = api_key
        self._client = httpx.Client(timeout=timeout_s, transport=transport)
        #: Overridable for tests so retry tests do not sleep wall-clock time.
        self._sleep = time.sleep

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def chat(
        self,
        messages: list[ChatMessage],
        *,
        tools: list[dict] | None = None,
        temperature: float = 0.0,
    ) -> ModelReply:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [self._dump_message(message) for message in messages],
            "temperature": temperature,
        }
        if tools:
            body["tools"] = list(tools)

        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        last_error: ModelClientError | None = None
        for attempt in range(self.max_retries + 1):
            reply, error = self._attempt(url, body, headers)
            if error is None:
                assert reply is not None
                return reply
            last_error = error
            if not error.retryable or attempt >= self.max_retries:
                break
            self._sleep(min(_BACKOFF_BASE_S * (2**attempt), _BACKOFF_MAX_S))
        assert last_error is not None
        raise last_error

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _attempt(
        self, url: str, body: dict, headers: dict
    ) -> tuple[ModelReply | None, ModelClientError | None]:
        try:
            response = self._client.post(url, json=body, headers=headers)
        except httpx.TimeoutException:
            return None, ModelClientError("timeout", "model request timed out", retryable=True)
        except httpx.TransportError as exc:
            return None, ModelClientError(
                "connection",
                f"model connection failed ({type(exc).__name__})",
                retryable=True,
            )
        except httpx.HTTPError as exc:
            return None, ModelClientError(
                "connection",
                f"model transport failed ({type(exc).__name__})",
                retryable=True,
            )

        if response.status_code >= 400:
            kind, retryable = _classify_status(response.status_code)
            detail = self._redact(_short(response.text))
            return None, ModelClientError(
                kind,
                f"model request failed with status {response.status_code}: {detail}",
                retryable=retryable,
                status=response.status_code,
            )

        try:
            payload = response.json()
        except ValueError:
            return None, ModelClientError(
                "protocol", "model response body is not JSON", retryable=False
            )
        try:
            return self._parse_reply(payload), None
        except ModelClientError as exc:
            return None, exc

    def _parse_reply(self, payload: Any) -> ModelReply:
        if not isinstance(payload, dict):
            raise ModelClientError(
                "protocol", "model response is not a JSON object", retryable=False
            )
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ModelClientError(
                "protocol", "model response is missing 'choices'", retryable=False
            )
        choice = choices[0]
        if not isinstance(choice, dict):
            raise ModelClientError(
                "protocol", "model choice is not an object", retryable=False
            )
        message = choice.get("message")
        if not isinstance(message, dict):
            raise ModelClientError(
                "protocol", "model choice is missing 'message'", retryable=False
            )

        raw_calls = message.get("tool_calls")
        if raw_calls is None and message.get("function_call") is not None:
            function_call = message.get("function_call")
            raw_calls = [
                {"id": "legacy_call_1", "type": "function", "function": function_call}
            ]
        if raw_calls is None:
            raw_calls = []
        if not isinstance(raw_calls, list):
            raise ModelClientError(
                "protocol", "model 'tool_calls' is not a list", retryable=False
            )
        try:
            tool_calls = tuple(_normalize_tool_call(call) for call in raw_calls)
        except (ValueError, TypeError) as exc:
            raise ModelClientError(
                "protocol", f"malformed tool call: {exc}", retryable=False
            ) from exc

        content = message.get("content")
        if content is not None and not isinstance(content, str):
            content = str(content)
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            usage = {}
        finish_reason = choice.get("finish_reason")
        if finish_reason is not None and not isinstance(finish_reason, str):
            finish_reason = str(finish_reason)
        return ModelReply(
            content=content,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
            usage=usage,
            raw=payload,
        )

    @staticmethod
    def _dump_message(message: ChatMessage) -> dict:
        dumped: dict[str, Any] = {"role": message.role, "content": message.content}
        if message.name is not None:
            dumped["name"] = message.name
        if message.tool_call_id is not None:
            dumped["tool_call_id"] = message.tool_call_id
        if message.tool_calls:
            dumped["tool_calls"] = list(message.tool_calls)
        return dumped

    def _redact(self, text: str) -> str:
        if self._api_key and self._api_key in text:
            return text.replace(self._api_key, "***")
        return text


def _short(text: str, limit: int = 400) -> str:
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[:limit] + "..."
