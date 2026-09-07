"""Suggested defaults from SPEC §27 — readable and snapshotable."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class SystemDefaults:
    core_instances: int = 1
    global_run_concurrency: int = 4
    per_task_run_concurrency: int = 2
    active_planner_sessions_per_task: int = 1
    intake_concurrency: int = 1
    max_work_unit_attempts: int = 3
    max_run_model_turns: int = 30
    max_task_model_calls: int = 200
    max_task_intake_calls: int = 10
    max_plan_revisions: int = 10
    max_work_units_per_task: int = 30
    heartbeat_seconds: int = 5
    lease_seconds: int = 30
    run_wall_timeout_seconds: int = 1800
    cooperative_stop_window_seconds: int = 10
    retry_backoff_seconds: tuple[int, ...] = (2, 5)
    scheduler_scan_seconds: int = 1
    decision_ttl_hours: int = 24
    side_effect_approval_ttl_minutes: int = 15
    message_page_size: int = 50
    message_page_max: int = 200

    def as_dict(self) -> dict:
        return asdict(self)


DEFAULTS = SystemDefaults()
