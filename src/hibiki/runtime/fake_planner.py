"""Fake Planner adapter and barrier helper for M2 acceptance."""

from __future__ import annotations

import threading
from typing import Any

from hibiki.runtime.fake_agent import FakeAgentAdapter


class FakePlannerAdapter(FakeAgentAdapter):
    """Same lifecycle surface as FakeAgentAdapter; records PLAN starts."""

    def __init__(self) -> None:
        super().__init__()
        self.plan_started: list[str] = []

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        result = super().start(run_spec)
        if result.get("alive") and run_spec.get("assignment_kind") == "PLAN":
            self.plan_started.append(run_spec["run_id"])
        return result


class BarrierFakeAgentAdapter(FakeAgentAdapter):
    """Registers RUNNING then waits so tests can observe overlapping intervals.

    ``start`` itself must not block on the barrier (outbox drain is sequential).
    Instead, each successful start spawns a short-lived worker thread that waits
    on the barrier; the test joins ``entered_event`` then asserts both Runs are
    still RUNNING before releasing ``release_event``.
    """

    def __init__(self, parties: int = 2) -> None:
        super().__init__()
        self.parties = parties
        self.barrier = threading.Barrier(parties)
        self.entered_event = threading.Event()
        self.release_event = threading.Event()
        self.entered_ids: list[str] = []
        self._entered_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        result = super().start(run_spec)
        if not result.get("alive"):
            return result
        run_id = run_spec["run_id"]

        def _hold() -> None:
            with self._entered_lock:
                self.entered_ids.append(run_id)
                if len(self.entered_ids) >= self.parties:
                    self.entered_event.set()
            try:
                self.barrier.wait(timeout=30)
            except threading.BrokenBarrierError:
                return
            self.release_event.wait(timeout=60)

        t = threading.Thread(target=_hold, name=f"barrier-{run_id}", daemon=True)
        self._threads.append(t)
        t.start()
        return result
