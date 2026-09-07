from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TypeVar

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
    """Process-lifetime exclusive file lock for a single Core data directory."""

    def __init__(self, data_dir: Path, *, holder: str = "core") -> None:
        self._path = Path(data_dir) / "core.lock"
        self._holder = holder
        self._fh = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "a+", encoding="utf-8")
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                if self._fh.read(1) == "":
                    self._fh.write("0")
                    self._fh.flush()
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._fh.close()
            self._fh = None
            raise RuntimeError(
                f"another Core instance holds the lock on {self._path}"
            ) from exc
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(self._holder)
        self._fh.flush()

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
