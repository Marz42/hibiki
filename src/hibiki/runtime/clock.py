from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from hibiki.domain.ports import Clock


def as_utc_naive(dt: datetime) -> datetime:
    """SQLite drops tzinfo; normalize all comparisons to naive UTC."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(UTC).replace(tzinfo=None)


class FakeClock(Clock):
    def __init__(self, start: datetime | None = None) -> None:
        self._now = as_utc_naive(start or datetime(2026, 9, 7, 0, 0, 0, tzinfo=UTC))

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs: float) -> datetime:
        self._now = self._now + timedelta(**kwargs)
        return self._now

    def set(self, when: datetime) -> None:
        self._now = as_utc_naive(when)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"
