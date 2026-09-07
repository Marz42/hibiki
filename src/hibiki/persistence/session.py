from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

T = TypeVar("T")


class SerialSessionExecutor:
    """All authoritative writes go through one lock + short sync Session."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._sf = session_factory
        self._lock = threading.RLock()
        self._crash_before_commit = False
        self._crash_after_commit = False
        self._after_commit_hooks: list[Callable[[], None]] = []

    def set_crash_before_commit(self, value: bool = True) -> None:
        self._crash_before_commit = value

    def set_crash_after_commit(self, value: bool = True) -> None:
        self._crash_after_commit = value

    def on_after_commit(self, hook: Callable[[], None]) -> None:
        self._after_commit_hooks.append(hook)

    def clear_hooks(self) -> None:
        self._after_commit_hooks.clear()

    @contextmanager
    def transaction(self) -> Iterator[Session]:
        with self._lock:
            session = self._sf()
            try:
                yield session
                if self._crash_before_commit:
                    self._crash_before_commit = False
                    session.rollback()
                    raise RuntimeError("injected_crash_before_commit")
                session.commit()
                hooks = list(self._after_commit_hooks)
                self._after_commit_hooks.clear()
                if self._crash_after_commit:
                    self._crash_after_commit = False
                    raise RuntimeError("injected_crash_after_commit")
                for hook in hooks:
                    hook()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    def run(self, fn: Callable[[Session], T]) -> T:
        with self.transaction() as session:
            return fn(session)


class InstanceLock:
    """Single Core instance lock via SQLite-backed lock file semantics."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._conn = None

    def acquire(self) -> None:
        # Hold a reserved connection with BEGIN IMMEDIATE on a lock table row.
        from sqlalchemy import text

        self._conn = self._engine.connect()
        self._conn.execute(text("BEGIN IMMEDIATE"))
        self._conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS core_instance_lock ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), holder TEXT NOT NULL)"
            )
        )
        row = self._conn.execute(text("SELECT holder FROM core_instance_lock WHERE id=1")).fetchone()
        if row is None:
            self._conn.execute(
                text("INSERT INTO core_instance_lock (id, holder) VALUES (1, 'core')")
            )
        self._conn.execute(text("UPDATE core_instance_lock SET holder='core' WHERE id=1"))
        self._conn.commit()

    def release(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
