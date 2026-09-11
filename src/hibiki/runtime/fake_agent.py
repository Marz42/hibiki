from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from hibiki.domain.ports import AgentAdapter


class FakeAgentAdapter(AgentAdapter):
    """In-process fake agent with controllable lifecycle.

    Adapter revoke protocol
    -----------------------
    - ``start`` / ``stop`` share an ``RLock`` so revoke-check and process
      registration are atomic (no window for a concurrent stop between them).
    - ``revoked_ids`` / ``start_revoked=True`` means *further starts are
      forbidden*. This is independent of process liveness.
    - ``alive`` / ``writer_alive`` report whether the simulated writer is still
      running. A stubborn writer may remain alive after stop while still being
      start-revoked; replaying ``start`` must not clear those flags.
    - Core treats ``start_revoked`` on a start result as a fenced no-op, and
      only releases Workspace on stop ACK when revoked *and* not alive.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self.started: list[str] = []
        self.stopped: list[tuple[str, str]] = []
        self.fail_start_ids: set[str] = set()
        self.keep_alive_after_lease: set[str] = set()
        self.revoked_ids: set[str] = set()
        # Test hook: called under lock after revoke check passes, before register.
        # Must not release the adapter lock. Signature: () -> None
        self.start_after_revoke_check_hook: Callable[[], None] | None = None

    def _revoked_start_result(
        self, run_id: str, run_spec: dict[str, Any]
    ) -> dict[str, Any]:
        """Forbid start without erasing an existing stubborn writer record."""
        existing = self._runs.get(run_id)
        if existing is not None:
            existing["start_revoked"] = True
            return dict(existing)
        rec = {
            "run_id": run_id,
            "alive": False,
            "writer_alive": False,
            "status": "REVOKED",
            "start_revoked": True,
            "spec": run_spec,
        }
        self._runs[run_id] = rec
        return dict(rec)

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        run_id = run_spec["run_id"]
        with self._lock:
            if run_id in self.revoked_ids:
                return self._revoked_start_result(run_id, run_spec)
            if self.start_after_revoke_check_hook is not None:
                self.start_after_revoke_check_hook()
            # Re-check: stop may have been queued; under lock it only runs after us,
            # but double-check covers hook-injected revoke and future callers.
            if run_id in self.revoked_ids:
                return self._revoked_start_result(run_id, run_spec)
            if run_id in self._runs:
                return dict(self._runs[run_id])
            if run_id in self.fail_start_ids:
                raise RuntimeError("fake_start_failed")
            if run_id in self.revoked_ids:
                return self._revoked_start_result(run_id, run_spec)
            record = {
                "run_id": run_id,
                "alive": True,
                "status": "RUNNING",
                "spec": run_spec,
                "writer_alive": True,
                "start_revoked": False,
            }
            self._runs[run_id] = record
            self.started.append(run_id)
            return dict(record)

    def send(self, run_id: str, message: dict[str, Any]) -> None:
        with self._lock:
            if run_id not in self._runs:
                raise KeyError(run_id)
            self._runs[run_id].setdefault("messages", []).append(message)

    def stop(self, run_id: str, reason: str) -> dict[str, Any]:
        with self._lock:
            self.stopped.append((run_id, reason))
            self.revoked_ids.add(run_id)
            rec = self._runs.get(run_id)
            if run_id in self.keep_alive_after_lease and rec is not None:
                # Stubborn writer: forbid later starts but keep process alive.
                rec["start_revoked"] = True
                rec["stop_requested"] = True
                return {
                    "run_id": run_id,
                    "alive": True,
                    "writer_alive": True,
                    "status": "RUNNING",
                    "stop_requested": True,
                    "start_revoked": True,
                }
            if rec is None:
                return {
                    "run_id": run_id,
                    "alive": False,
                    "writer_alive": False,
                    "status": "REVOKED",
                    "start_revoked": True,
                }
            rec["alive"] = False
            rec["writer_alive"] = False
            rec["status"] = "STOPPED"
            rec["stop_reason"] = reason
            rec["start_revoked"] = True
            return {
                "run_id": run_id,
                "alive": False,
                "writer_alive": False,
                "status": "STOPPED",
                "start_revoked": True,
            }

    def inspect(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            rec = self._runs.get(run_id)
            if rec is None:
                return {"run_id": run_id, "alive": False, "status": "MISSING"}
            return {
                "run_id": run_id,
                "alive": bool(rec.get("alive")),
                "writer_alive": bool(rec.get("writer_alive")),
                "status": rec.get("status"),
                "identity": f"fake:{run_id}",
                "start_revoked": bool(rec.get("start_revoked")),
            }

    def mark_dead(self, run_id: str) -> None:
        with self._lock:
            if run_id in self._runs:
                self._runs[run_id]["alive"] = False
                self._runs[run_id]["writer_alive"] = False
                self._runs[run_id]["status"] = "DEAD"

    def is_alive(self, run_id: str) -> bool:
        with self._lock:
            rec = self._runs.get(run_id)
            return bool(rec and rec.get("alive"))
