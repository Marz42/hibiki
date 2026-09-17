"""Safe path handling inside a workspace root (M1 Task C).

Decided design (``docs/M1-CHECKLIST.md`` §3): a component-by-component ``dir_fd``
walk — ``O_DIRECTORY | O_NOFOLLOW`` for every intermediate component and
``O_NOFOLLOW`` for the final one.  CPython does not expose ``openat2(2)``, so the
kernel is asked to refuse symlink traversal at each step instead of validating a
string with ``startswith``/``realpath`` heuristics, which race.  Containment is
structural rather than textual: ``..`` and absolute forms are rejected before the
walk starts, and no component is ever followed, so the path cannot leave the root.

On Windows, directory FDs and ``dir_fd`` are unavailable (``os.open`` on a directory
raises ``PermissionError``). A path-based walk with the same validation and symlink
refusal is used there; TOCTOU hardening remains Unix/dir_fd-only.
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

#: Linux/macOS expose these open(2) flags; Windows does not. Fall back to 0 and
#: compensate with an explicit symlink check before each open on those hosts.
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)

#: Windows cannot open directories with ``os.open(O_RDONLY)`` and does not support
#: ``dir_fd`` walks. Path-based containment is used there.
_PATH_MODE = os.name == "nt"


class PathSafetyError(ValueError):
    """A requested relative path violates the workspace containment rules."""


def _is_symlink(parent_fd: int, name: str) -> bool:
    """True when ``name`` inside ``parent_fd`` is itself a symlink (never followed)."""
    try:
        st = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISLNK(st.st_mode)


def _is_symlink_path(path: str) -> bool:
    try:
        return stat.S_ISLNK(os.lstat(path).st_mode)
    except OSError:
        return False


class WorkspacePaths:
    """Open and create files strictly inside one workspace root.

    On Unix the root is opened once as a directory file descriptor; every later
    operation walks from that descriptor. On Windows a validated path walk is used
    instead because directory FDs / ``dir_fd`` are unavailable.
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
        self._path_mode = _PATH_MODE
        self._closed = False
        self._root_fd: int | None
        if self._path_mode:
            self._root_fd = None
        else:
            self._root_fd = os.open(resolved, os.O_RDONLY | _O_DIRECTORY | _O_CLOEXEC)

    @property
    def root(self) -> str:
        """Absolute, symlink-resolved workspace root."""
        return self._root

    def close(self) -> None:
        self._closed = True
        if self._root_fd is not None:
            os.close(self._root_fd)
            self._root_fd = None

    def __enter__(self) -> WorkspacePaths:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("WorkspacePaths is closed")

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    def resolve_for_read(self, relative: str) -> str:
        """Return the absolute real path of a readable regular file inside root."""
        self._ensure_open()
        if self._path_mode:
            path = self._path_resolve(relative, expect_dir=False)
            st = os.lstat(path)
            if stat.S_ISLNK(st.st_mode):
                raise PathSafetyError(f"{_SYMLINK_REASON} at {relative!r}")
            if not stat.S_ISREG(st.st_mode):
                raise OSError(errno.EINVAL, "not a regular file", relative)
            return path
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
        self._ensure_open()
        if self._path_mode:
            path = self._path_resolve(relative, expect_dir=False)
            if _is_symlink_path(path):
                raise PathSafetyError(f"{_SYMLINK_REASON} at {relative!r}")
            fd = os.open(path, os.O_RDONLY | _O_CLOEXEC)
            try:
                self._require_regular(fd, relative)
                return os.fdopen(fd, "rb")
            except BaseException:
                os.close(fd)
                raise
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
        self._ensure_open()
        if self._path_mode:
            path = self._path_resolve(relative, expect_dir=False, create_parents=True)
            if _is_symlink_path(path):
                raise PathSafetyError(f"{_SYMLINK_REASON} at {relative!r}")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _O_CLOEXEC, mode)
            return os.fdopen(fd, "wb")
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
        self._ensure_open()
        if self._path_mode:
            path = self._path_resolve(relative, expect_dir=True)
            return sorted(os.listdir(path))
        parts = self._validate(relative)
        parent_fd = self._walk(parts[:-1], create=False)
        try:
            fd = self._open_final(parent_fd, parts[-1], os.O_RDONLY | _O_DIRECTORY)
            try:
                return sorted(os.listdir(fd))
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)

    def mkdir(self, relative: str) -> None:
        """Create ``relative`` as a directory, including missing parents."""
        self._ensure_open()
        if self._path_mode:
            self._path_resolve(
                relative, expect_dir=True, create_parents=True, create_leaf_dir=True
            )
            return
        parts = self._validate(relative)
        fd = self._walk(parts, create=True)
        os.close(fd)

    def exists(self, relative: str) -> bool:
        self._ensure_open()
        if self._path_mode:
            try:
                path = self._path_resolve(relative, expect_dir=False)
            except (FileNotFoundError, NotADirectoryError):
                return False
            if _is_symlink_path(path):
                raise PathSafetyError(
                    f"{_SYMLINK_REASON} at {relative!r}; a symlink is never a valid target"
                )
            return os.path.lexists(path)
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

    def read_bytes(self, relative: str, *, max_bytes: int | None = None) -> tuple[bytes, str]:
        """Read a file and hash the *same* descriptor (no check-then-open race).

        Callers must use these bytes rather than re-opening ``resolve_for_read``: the
        path could be swapped for a symlink between validation and use.
        """
        with self.open_for_read(relative) as handle:
            data = handle.read() if max_bytes is None else handle.read(max_bytes)
        return data, hashlib.sha256(data).hexdigest()

    def write_bytes(self, relative: str, data: bytes, *, mode: int = 0o644) -> str:
        """Write bytes through the safe walk; returns the sha256 of ``data``."""
        return self.atomic_write(relative, data, mode=mode)

    def atomic_write(self, relative: str, content: bytes, *, mode: int = 0o644) -> str:
        """Write ``content`` atomically inside the walked directory; return its sha256.

        The default mode is world-readable because the sandbox runs as uid 65534 and
        must be able to read the files the broker wrote into the workspace.
        """
        self._ensure_open()
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("content must be bytes-like")
        data = bytes(content)
        if self._path_mode:
            parts = self._validate(relative)
            parent = self._path_resolve(
                "/".join(parts[:-1]) if len(parts) > 1 else ".",
                expect_dir=True,
                create_parents=True,
                allow_dot=True,
            )
            final = os.path.join(parent, parts[-1])
            if _is_symlink_path(final):
                raise PathSafetyError(
                    f"{_SYMLINK_REASON} at {parts[-1]!r}; refusing to replace a symlink"
                )
            tmp_name = f".hibiki-tmp-{os.getpid()}-{secrets.token_hex(8)}"
            tmp_path = os.path.join(parent, tmp_name)
            try:
                with open(tmp_path, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, final)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            return hashlib.sha256(data).hexdigest()
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
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC,
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
    # Validation and the dir_fd / path walk
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
        if os.path.commonpath([self._root, candidate]) != self._root:
            raise PathSafetyError(f"path leaves the workspace root: {'/'.join(parts)!r}")
        return candidate

    def _path_resolve(
        self,
        relative: str,
        *,
        expect_dir: bool,
        create_parents: bool = False,
        create_leaf_dir: bool = False,
        allow_dot: bool = False,
    ) -> str:
        """Windows path-mode walk: refuse ``..`` / abs / symlink components."""
        if allow_dot and relative in {".", ""}:
            return self._root
        parts = self._validate(relative)
        cur = self._root
        for i, name in enumerate(parts):
            nxt = os.path.join(cur, name)
            if os.path.commonpath([self._root, os.path.abspath(nxt)]) != self._root:
                raise PathSafetyError(f"path leaves the workspace root: {relative!r}")
            is_last = i == len(parts) - 1
            if _is_symlink_path(nxt):
                raise PathSafetyError(f"{_SYMLINK_REASON} in path component {name!r}")
            if not os.path.lexists(nxt):
                if is_last and (create_leaf_dir or (create_parents and expect_dir)):
                    os.makedirs(nxt, exist_ok=True)
                elif not is_last and create_parents:
                    os.makedirs(nxt, exist_ok=True)
                elif is_last and create_parents and not expect_dir:
                    os.makedirs(cur, exist_ok=True)
                    return nxt
                else:
                    raise FileNotFoundError(errno.ENOENT, "No such file or directory", nxt)
            if not is_last or expect_dir:
                if os.path.lexists(nxt):
                    st = os.lstat(nxt)
                    if not stat.S_ISDIR(st.st_mode):
                        raise NotADirectoryError(errno.ENOTDIR, "not a directory", nxt)
            cur = nxt
        return cur

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
        if _O_NOFOLLOW == 0 and _is_symlink(parent_fd, name):
            raise PathSafetyError(f"{_SYMLINK_REASON} in path component {name!r}")
        flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
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
        if _O_NOFOLLOW == 0 and _is_symlink(parent_fd, name):
            raise PathSafetyError(f"{_SYMLINK_REASON} at {name!r}")
        try:
            return os.open(
                name, flags | _O_NOFOLLOW | _O_CLOEXEC, mode, dir_fd=parent_fd
            )
        except OSError as exc:
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
