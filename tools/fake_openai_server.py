"""A tiny OpenAI-compatible stand-in for the M1 runner (no network, no billing).

It is a *test double for the provider*, not for HIBIKI: the adapter still speaks real
HTTP/JSON to ``/chat/completions``, and its tool calls still go through the real Tool
Broker and the real Docker sandbox. Scripted per task so the three fixed tasks can be
exercised end to end before real credentials exist.

Usage:
    uv run --no-sync python tools/fake_openai_server.py --port 8765 --script summary
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

SCRIPTS: dict[str, list[dict[str, Any]]] = {}


def _tool(call_id: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(arguments)},
    }


def _summary_script(task_hint: str) -> list[dict[str, Any]]:
    return [
        {
            "content": None,
            "tool_calls": [
                _tool(
                    "c1",
                    "fs_read",
                    {"path": "input.txt"},
                )
            ],
        },
        {
            "content": None,
            "tool_calls": [
                _tool(
                    "c2",
                    "fs_write",
                    {
                        "path": "summary.txt",
                        "content": (
                            "- HIBIKI is a task-centered agent execution system.\n"
                            "- It freezes authorization in an immutable contract before "
                            "any worker runs.\n"
                            "- External actions are authorized and deduplicated; unknown "
                            "outcomes are never replayed.\n"
                        ),
                    },
                )
            ],
        },
        {
            "content": None,
            "tool_calls": [
                _tool("c3", "artifact_publish", {"path": "summary.txt"}),
            ],
        },
        {
            "content": "Wrote summary.txt with three bullets and published it.",
            "tool_calls": [],
            "artifact_refs_from_tool": True,
            "deliverable": "summary.txt",
        },
    ]


def _conversion_script() -> list[dict[str, Any]]:
    payload = [
        {"name": "alpha", "count": 3, "ratio": 0.5},
        {"name": "beta", "count": 10, "ratio": 1.25},
        {"name": "gamma", "count": 0, "ratio": 0.0},
    ]
    return [
        {"content": None, "tool_calls": [_tool("c1", "fs_read", {"path": "input.csv"})]},
        {
            "content": None,
            "tool_calls": [
                _tool(
                    "c2",
                    "fs_write",
                    {"path": "output.json", "content": json.dumps(payload, indent=2)},
                )
            ],
        },
        {"content": None, "tool_calls": [_tool("c3", "artifact_publish", {"path": "output.json"})]},
        {
            "content": "Converted input.csv into output.json and published it.",
            "tool_calls": [],
            "artifact_refs_from_tool": True,
            "deliverable": "output.json",
        },
    ]


def _repo_script() -> list[dict[str, Any]]:
    fixed = "def add(a, b):\n    return a + b\n"
    return [
        {"content": None, "tool_calls": [_tool("c1", "fs_read", {"path": "sample.py"})]},
        {
            "content": None,
            "tool_calls": [_tool("c2", "fs_write", {"path": "sample.py", "content": fixed})],
        },
        {
            "content": None,
            "tool_calls": [
                _tool(
                    "c3",
                    "shell_run",
                    {
                        "argv": [
                            "python",
                            "-m",
                            "pytest",
                            "-q",
                            "test_sample.py",
                        ]
                    },
                )
            ],
        },
        {"content": None, "tool_calls": [_tool("c4", "artifact_publish", {"path": "sample.py"})]},
        {
            "content": "Fixed add() and the sample tests pass.",
            "tool_calls": [],
            "artifact_refs_from_tool": True,
            "deliverable": "sample.py",
        },
    ]


def _published_hashes(messages: list[dict[str, Any]]) -> list[str]:
    """Collect ``artifact_hash`` values the Core returned from publish tool calls."""
    found: list[str] = []
    for message in messages:
        if message.get("role") != "tool":
            continue
        blob = str(message.get("content") or "")
        start = blob.find("{")
        if start < 0:
            continue
        try:
            payload = json.loads(blob[start:])
        except ValueError:
            continue
        digest = payload.get("artifact_hash")
        if isinstance(digest, str) and digest and digest not in found:
            found.append(digest)
    return found


def script_for(model: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pick a script by what the worker has already done (stateless across calls)."""
    blob = json.dumps(messages)
    if "input.csv" in blob or "output.json" in blob:
        return _conversion_script()
    if "sample.py" in blob or "test_sample" in blob:
        return _repo_script()
    return _summary_script("summary")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: FakeServer

    def log_message(self, *args: Any) -> None:  # silence the default access log
        return

    def do_POST(self) -> None:  # noqa: N802 — http.server API
        length = int(self.headers.get("content-length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        if not self.path.rstrip("/").endswith("/chat/completions"):
            self.send_error(404, "unknown endpoint")
            return
        messages = body.get("messages") or []
        model = body.get("model") or ""
        script = script_for(model, messages)
        # One counter per conversation so repeated runs each replay the whole script.
        # A fresh conversation starts with a single user message; any run whose prompt
        # already carries an assistant/tool exchange is the same conversation.
        first_turn = len(messages) <= 2
        key = model + "|" + json.dumps(messages[:1])[:200]
        with self.server.lock:
            step = 0 if first_turn else self.server.steps.get(key, 0)
            call = script[step] if step < len(script) else {"content": "done", "tool_calls": []}
            self.server.steps[key] = step + 1
        message: dict[str, Any] = {"role": "assistant", "content": call.get("content")}
        if call.get("tool_calls"):
            message["tool_calls"] = call["tool_calls"]
        if call.get("artifact_refs_from_tool"):
            # Behave like a competent model: report the hashes the publish tool really
            # returned instead of inventing references.
            message["content"] = (
                f"{call.get('content')} artifact_refs="
                f"{json.dumps(_published_hashes(messages))}"
            )
        payload = {
            "id": "chatcmpl-fake",
            "object": "chat.completion",
            "model": body.get("model") or "fake",
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": "tool_calls" if call.get("tool_calls") else "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class FakeServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, Handler)
        self.steps: dict[str, int] = {}
        self.lock = threading.Lock()


def main() -> int:
    parser = argparse.ArgumentParser(prog="fake-openai-server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    server = FakeServer((args.host, args.port))
    print(f"fake OpenAI-compatible server on http://{args.host}:{args.port}/v1", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
