"""Safe path handling inside a workspace root (M1 Task C).

Decided design (``docs/M1-CHECKLIST.md`` §3): a component-by-component ``dir_fd``
walk — ``O_DIRECTORY | O_NOFOLLOW`` for every intermediate component and
``O_NOFOLLOW`` for the final one.  CPython does not expose ``openat2(2)``, so the
kernel is asked to refuse symlink traversal at each step instead of validating a
string with ``startswith``/``realpath`` heuristics, which race.  Containment is
structural rather than textual: ``..`` and absolute forms are rejected before the
walk starts, and no component is ever followed, so the path cannot leave the root.
"""

from __future__ import annotations

import errno
import hashlib
import ntpath
import os
import secrets
import stat
from typing import BinaryIO

#: Symlinks are the traversal vector this module exists to close; report them as a
#: safety refusal rather than a generic filesystem error.
_SYMLINK_REASON = "refusing to follow a symlink"


class PathSafetyError(ValueError):
    """A requested relative path violates the workspace containment rules."""


def _is_symlink(parent_fd: int, name: str) -> bool:
    """True when ``name`` inside ``parent_fd`` is itself a symlink (never followed)."""
    try:
        st = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISLNK(st.st_mode)


class WorkspacePaths:
    """Open and create files strictly inside one workspace root.

    The root is opened once as a directory file descriptor; every later operation
    walks from that descriptor, so renaming the root out from under the object cannot
    redirect an operation (the descriptor keeps pointing at the original directory).
    """

    def __init__(self, root: str | os.PathLike[str]) -> None:
        raw = os.fspath(root)
        if isinstance(raw, bytes):
            raw = os.fsdecode(raw)
        if not raw:
            raise PathSafetyError("workspace root must not be empty")
        resolved = os.path.realpath(os.path.abspath(raw))
        st = os.stat(resolved)
        if not stat.S_ISDIR(st.st_mode):
            raise NotADirectoryError(errno.ENOTDIR, "workspace root is not a directory", raw)
        self._root = resolved
        self._root_fd: int | None = os.open(
            resolved, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )

    @property
    def root(self) -> str:
        """Absolute, symlink-resolved workspace root."""
        return self._root

    def close(self) -> None:
        if self._root_fd is not None:
            os.close(self._root_fd)
            self._root_fd = None

    def __enter__(self) -> WorkspacePaths:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    def resolve_for_read(self, relative: str) -> str:
        """Return the absolute real path of a readable regular file inside root."""
        parts = self._validate(relative)
        parent_fd = self._walk(parts[:-1], create=False)
        try:
            fd = self._open_final(parent_fd, parts[-1], os.O_RDONLY)
            try:
                self._require_regular(fd, parts[-1])
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)
        return self._absolute(parts)

    def open_for_read(self, relative: str) -> BinaryIO:
        parts = self._validate(relative)
        parent_fd = self._walk(parts[:-1], create=False)
        try:
            fd = self._open_final(parent_fd, parts[-1], os.O_RDONLY)
        finally:
            os.close(parent_fd)
        try:
            self._require_regular(fd, parts[-1])
            return os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise

    def open_for_write(self, relative: str, *, mode: int = 0o644) -> BinaryIO:
        parts = self._validate(relative)
        parent_fd = self._walk(parts[:-1], create=False)
        try:
            fd = self._open_final(
                parent_fd, parts[-1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode
            )
        finally:
            os.close(parent_fd)
        return os.fdopen(fd, "wb")

    def list_dir(self, relative: str) -> list[str]:
        parts = self._validate(relative)
        parent_fd = self._walk(parts[:-1], create=False)
        try:
            fd = self._open_final(parent_fd, parts[-1], os.O_RDONLY | os.O_DIRECTORY)
            try:
                return sorted(os.listdir(fd))
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)

    def mkdir(self, relative: str) -> None:
        """Create ``relative`` as a directory, including missing parents."""
        parts = self._validate(relative)
        fd = self._walk(parts, create=True)
        os.close(fd)

    def exists(self, relative: str) -> bool:
        parts = self._validate(relative)
        try:
            parent_fd = self._walk(parts[:-1], create=False)
        except (FileNotFoundError, NotADirectoryError):
            return False
        try:
            try:
                st = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if stat.S_ISLNK(st.st_mode):
                raise PathSafetyError(
                    f"{_SYMLINK_REASON} at {parts[-1]!r}; a symlink is never a valid target"
                )
            return True
        finally:
            os.close(parent_fd)

    def atomic_write(self, relative: str, content: bytes, *, mode: int = 0o644) -> str:
        """Write ``content`` atomically inside the walked directory; return its sha256.

        The default mode is world-readable because the sandbox runs as uid 65534 and
        must be able to read the files the broker wrote into the workspace.
        """
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("content must be bytes-like")
        data = bytes(content)
        parts = self._validate(relative)
        name = parts[-1]
        parent_fd = self._walk(parts[:-1], create=False)
        tmp_name = f".hibiki-tmp-{os.getpid()}-{secrets.token_hex(8)}"
        try:
            if _is_symlink(parent_fd, name):
                raise PathSafetyError(
                    f"{_SYMLINK_REASON} at {name!r}; refusing to replace a symlink"
                )
            fd = os.open(
                tmp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                mode,
                dir_fd=parent_fd,
            )
            try:
                view = memoryview(data)
                while view:
                    view = view[os.write(fd, view) :]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp_name, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            os.fsync(parent_fd)
        except BaseException:
            try:
                os.unlink(tmp_name, dir_fd=parent_fd)
            except OSError:
                pass
            raise
        finally:
            os.close(parent_fd)
        return hashlib.sha256(data).hexdigest()

    # ------------------------------------------------------------------
    # Validation and the dir_fd walk
    # ------------------------------------------------------------------

    def _validate(self, relative: str) -> list[str]:
        if not isinstance(relative, str):
            raise PathSafetyError(f"path must be a string, got {type(relative).__name__}")
        if "\x00" in relative:
            raise PathSafetyError("path must not contain a NUL byte")
        if not relative:
            raise PathSafetyError("path must not be empty")
        if os.path.isabs(relative) or ntpath.splitdrive(relative)[0]:
            raise PathSafetyError(f"absolute paths and drive letters are not allowed: {relative!r}")
        if relative[0] in "/\\":
            raise PathSafetyError(f"absolute paths are not allowed: {relative!r}")
        parts = [part for part in relative.split("/") if part not in ("", ".")]
        if any(part == ".." for part in parts):
            raise PathSafetyError(f"'..' components are not allowed: {relative!r}")
        if not parts:
            raise PathSafetyError(f"path does not name a file or directory: {relative!r}")
        return parts

    def _absolute(self, parts: list[str]) -> str:
        candidate = os.path.join(self._root, *parts)
        # Not a string-prefix test: proving containment by construction, then
        # double-checking with commonpath keeps the invariant explicit.
        if os.path.commonpath([self._root, candidate]) != self._root:
            raise PathSafetyError(f"path leaves the workspace root: {'/'.join(parts)!r}")
        return candidate

    def _dup_root(self) -> int:
        if self._root_fd is None:
            raise ValueError("WorkspacePaths is closed")
        return os.dup(self._root_fd)

    def _walk(self, parts: list[str], *, create: bool) -> int:
        """Open every component of ``parts`` as a directory; return the final fd."""
        fd = self._dup_root()
        try:
            for name in parts:
                nxt = self._open_dir(fd, name, create=create)
                os.close(fd)
                fd = nxt
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _open_dir(self, parent_fd: int, name: str, *, create: bool) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            return os.open(name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            if not create:
                raise
            try:
                os.mkdir(name, dir_fd=parent_fd)
            except FileExistsError:
                pass
            try:
                return os.open(name, flags, dir_fd=parent_fd)
            except OSError as exc:
                raise self._dir_error(parent_fd, exc, name) from exc
        except OSError as exc:
            raise self._dir_error(parent_fd, exc, name) from exc

    def _dir_error(self, parent_fd: int, exc: OSError, name: str) -> OSError:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            if _is_symlink(parent_fd, name):
                return PathSafetyError(
                    f"{_SYMLINK_REASON} in path component {name!r}"
                )
            return NotADirectoryError(errno.ENOTDIR, "not a directory", name)
        return exc

    def _open_final(self, parent_fd: int, name: str, flags: int, mode: int = 0o644) -> int:
        try:
            return os.open(
                name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=parent_fd
            )
        except OSError as exc:
            # O_NOFOLLOW on a symlink is ELOOP for a file target and ENOTDIR when
            # O_DIRECTORY is also set; both mean "a symlink was not followed".
            if exc.errno == errno.ELOOP or (
                exc.errno == errno.ENOTDIR and _is_symlink(parent_fd, name)
            ):
                raise PathSafetyError(f"{_SYMLINK_REASON} at {name!r}") from exc
            raise

    @staticmethod
    def _require_regular(fd: int, name: str) -> None:
        mode = os.fstat(fd).st_mode
        if stat.S_ISDIR(mode):
            raise IsADirectoryError(errno.EISDIR, "is a directory", name)
        if not stat.S_ISREG(mode):
            raise OSError(errno.EINVAL, "not a regular file", name)
