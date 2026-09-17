"""Fake Planner adapter and barrier helper for M2 acceptance."""

from __future__ import annotations

import threading
from typing import Any

from hibiki.runtime.fake_agent import FakeAgentAdapter


class FakePlannerAdapter(FakeAgentAdapter):
    """Same lifecycle surface as FakeAgentAdapter; records PLAN starts and message drains.

    Real proposal loops still belong to the Harness/Core in Fake mode, but the
    adapter now surfaces RunInput/checkpoint fields and can advance the message
    cursor so Planner recovery is exercisable without a live model.
    """

    def __init__(self) -> None:
        super().__init__()
        self.plan_started: list[str] = []
        self.consumed_sequences: dict[str, int] = {}
        self._core: Any | None = None

    def bind_core(self, core: Any) -> None:
        """Optional Core handle for cursor consumption during PLAN start."""
        self._core = core

    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        result = super().start(run_spec)
        if result.get("alive") and run_spec.get("assignment_kind") == "PLAN":
            run_id = run_spec["run_id"]
            self.plan_started.append(run_id)
            self._maybe_consume_messages(run_spec)
        return result

    def _maybe_consume_messages(self, run_spec: dict[str, Any]) -> None:
        core = self._core
        if core is None:
            return
        task_id = run_spec.get("task_id")
        run_id = run_spec.get("run_id")
        generation = run_spec.get("generation")
        if not task_id or not run_id:
            return
        try:
            from hibiki.domain.enums import ActorType
            from hibiki.domain.types import AuthContext

            auth = AuthContext(
                principal_id=str(run_spec.get("principal_id") or ""),
                actor_id=str(run_spec.get("agent_instance_id") or f"planner:{run_id}"),
                actor_type=ActorType.INTERNAL,
                auth_context_id=f"run:{run_id}",
                bound_task_id=str(task_id),
                bound_run_id=str(run_id),
                bound_fencing_epoch=int(run_spec.get("fencing_epoch") or 1),
                bound_grant_epoch=int(run_spec.get("grant_epoch") or 0),
            )
            # Drain up to the current max sequence into a checkpoint advance.
            messages = []
            if hasattr(core, "list_task_messages"):
                messages = list(core.list_task_messages(task_id) or [])
            last_seq = max((int(m.get("sequence_no") or 0) for m in messages), default=0)
            cursor = int(run_spec.get("last_consumed_message_seq") or 0)
            if last_seq >= cursor:
                advanced = core.execute(
                    "advance_planner_checkpoint",
                    auth,
                    {
                        "task_id": task_id,
                        "run_id": run_id,
                        "generation": generation,
                        "fencing_epoch": run_spec.get("fencing_epoch"),
                        "last_consumed_message_seq": last_seq,
                        "checkpoint_ref": f"fake-ckpt:{run_id}:{last_seq}",
                    },
                )
                if getattr(advanced, "ok", False):
                    self.consumed_sequences[run_id] = last_seq
        except Exception:  # noqa: BLE001 — Fake recovery assist must not break start
            return


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
