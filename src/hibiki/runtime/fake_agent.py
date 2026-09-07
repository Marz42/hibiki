from __future__ import annotations

from typing import Any

from hibiki.domain.ports import AgentAdapter


class FakeAgentAdapter(AgentAdapter):
    """In-process fake agent with controllable lifecycle."""

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self.started: list[str] = []
        self.stopped: list[tuple[str, str]] = []
        self.fail_start_ids: set[str] = set()
        self.keep_alive_after_lease: set[str] = set()

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        run_id = run_spec["run_id"]
        if run_id in self._runs:
            return self._runs[run_id]
        if run_id in self.fail_start_ids:
            raise RuntimeError("fake_start_failed")
        record = {
            "run_id": run_id,
            "alive": True,
            "status": "RUNNING",
            "spec": run_spec,
            "writer_alive": True,
        }
        self._runs[run_id] = record
        self.started.append(run_id)
        return dict(record)

    def send(self, run_id: str, message: dict[str, Any]) -> None:
        if run_id not in self._runs:
            raise KeyError(run_id)
        self._runs[run_id].setdefault("messages", []).append(message)

    def stop(self, run_id: str, reason: str) -> dict[str, Any]:
        self.stopped.append((run_id, reason))
        rec = self._runs.get(run_id)
        if rec is None:
            return {"run_id": run_id, "alive": False, "status": "UNKNOWN"}
        if run_id in self.keep_alive_after_lease:
            # Simulate stubborn writer: stop command recorded but process still alive
            return {"run_id": run_id, "alive": True, "status": "RUNNING", "stop_requested": True}
        rec["alive"] = False
        rec["writer_alive"] = False
        rec["status"] = "STOPPED"
        rec["stop_reason"] = reason
        return {"run_id": run_id, "alive": False, "status": "STOPPED"}

    def inspect(self, run_id: str) -> dict[str, Any]:
        rec = self._runs.get(run_id)
        if rec is None:
            return {"run_id": run_id, "alive": False, "status": "MISSING"}
        return {
            "run_id": run_id,
            "alive": bool(rec.get("alive")),
            "writer_alive": bool(rec.get("writer_alive")),
            "status": rec.get("status"),
            "identity": f"fake:{run_id}",
        }

    def mark_dead(self, run_id: str) -> None:
        if run_id in self._runs:
            self._runs[run_id]["alive"] = False
            self._runs[run_id]["writer_alive"] = False
            self._runs[run_id]["status"] = "DEAD"

    def is_alive(self, run_id: str) -> bool:
        rec = self._runs.get(run_id)
        return bool(rec and rec.get("alive"))
