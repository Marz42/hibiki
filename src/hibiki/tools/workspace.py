"""On-disk Workspace materialization and inspection (M1 Task E, SPEC §11.1 / §11.3).

A Workspace is a real directory under one root, one directory per ``workspace_id``.
The manager is intentionally conservative:

* materialization is idempotent — a second call never deletes existing content and
  never rewrites a file a writer already produced;
* every path is resolved through :class:`hibiki.tools.paths.WorkspacePaths`, which
  refuses ``..``, absolute paths and symlink traversal at the kernel level, so this
  module does not re-implement containment;
* ``release`` keeps the files; ``quarantine`` only drops a marker, because the
  decision to release a Workspace belongs to the Core, not to this helper.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from hibiki.domain.hashing import canonical_json
from hibiki.tools.paths import PathSafetyError, WorkspacePaths

#: Manager-owned metadata files: never part of the user content view.
_MANAGED_FILES = frozenset({"baseline.json", "QUARANTINE.json"})

_READ_CHUNK = 64 * 1024
_GIT_TIMEOUT_S = 30


class WorkspaceManager:
    """Materialize and inspect Workspaces under one root directory."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    def materialize(
        self,
        workspace_id: str,
        *,
        base_ref: str | None = None,
        seed_files: Mapping[str, bytes] | None = None,
    ) -> str:
        """Create ``root/<workspace_id>`` and return its absolute path.

        Idempotent: an existing directory keeps all of its content, seed files are
        written only when absent, and a recorded baseline is not rewritten unless
        ``base_ref`` is supplied again.
        """
        self._validate_id(workspace_id)
        self.root.mkdir(parents=True, exist_ok=True)
        workspace = self.root / workspace_id
        workspace.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(workspace, 0o700)
        except OSError:
            # Best effort: the directory already exists with acceptable mode.
            pass

        if seed_files:
            self._write_seeds(workspace, seed_files)

        if base_ref is not None:
            baseline = self.git_snapshot(base_ref)
            baseline["workspace_id"] = workspace_id
            baseline["created_at"] = self._now_iso()
            with WorkspacePaths(workspace) as paths:
                paths.atomic_write("baseline.json", canonical_json(baseline).encode("utf-8"))
        return str(workspace)

    def _write_seeds(self, workspace: Path, seed_files: Mapping[str, bytes]) -> None:
        with WorkspacePaths(workspace) as paths:
            for relative, content in seed_files.items():
                raw = content if isinstance(content, (bytes, bytearray, memoryview)) else str(content).encode()
                parent = os.path.dirname(str(relative))
                if parent:
                    paths.mkdir(parent)
                if paths.exists(relative):
                    # Never clobber content that is already there.
                    continue
                paths.atomic_write(relative, bytes(raw))

    @staticmethod
    def _validate_id(workspace_id: str) -> None:
        if not isinstance(workspace_id, str) or not workspace_id:
            raise ValueError("workspace_id must be a non-empty string")
        if "\x00" in workspace_id:
            raise ValueError("workspace_id must not contain a NUL byte")
        if workspace_id in {".", ".."}:
            raise ValueError(f"workspace_id must not be {workspace_id!r}")
        if (
            "/" in workspace_id
            or "\\" in workspace_id
            or os.sep in workspace_id
            or (os.altsep is not None and os.altsep in workspace_id)
            or workspace_id != os.path.basename(workspace_id)
        ):
            raise ValueError(f"workspace_id must not contain a path separator: {workspace_id!r}")

    # ------------------------------------------------------------------
    # Baseline / diff
    # ------------------------------------------------------------------

    def git_snapshot(self, base_ref: str) -> dict:
        """Record the resolved commit and worktree diff of a git ``base_ref``."""
        path = Path(base_ref)
        commit = self._git(path, "rev-parse", "HEAD")
        if commit is None:
            return {
                "base_ref": str(path),
                "base_commit": None,
                "git_repo": False,
                "worktree_diff": "",
            }
        return {
            "base_ref": str(path.resolve()),
            "base_commit": commit,
            "git_repo": True,
            "worktree_diff": self._git(path, "diff") or "",
        }

    def baseline(self, workspace_id: str) -> dict:
        """Read the recorded ``baseline.json``; ``{}`` when there is none."""
        workspace = self._require_dir(workspace_id)
        try:
            with WorkspacePaths(workspace) as paths:
                with paths.open_for_read("baseline.json") as handle:
                    return json.loads(handle.read().decode("utf-8"))
        except (FileNotFoundError, IsADirectoryError, OSError, ValueError):
            return {}

    def diff(self, workspace_id: str) -> str:
        """A deterministic description of the Workspace's current content.

        Git-based for a Workspace that is itself a repository, otherwise a sorted
        listing with sizes and hashes.
        """
        workspace = self._require_dir(workspace_id)
        if (workspace / ".git").is_dir():
            return self._git_diff(workspace)
        return self._listing_diff(workspace)

    def _git_diff(self, workspace: Path) -> str:
        lines: list[str] = []
        recorded = self.baseline(workspace.name)
        if recorded.get("base_commit"):
            lines.append(f"# base_commit {recorded['base_commit']}")
        status = self._git(workspace, "status", "--porcelain=v1") or ""
        diff = self._git(workspace, "diff") or ""
        staged = self._git(workspace, "diff", "--cached") or ""
        for block in (status, diff, staged):
            for line in block.splitlines():
                if line.strip() and not any(name in line for name in _MANAGED_FILES):
                    lines.append(line)
        return "\n".join(lines) + ("\n" if lines else "")

    def _listing_diff(self, workspace: Path) -> str:
        entries: list[tuple[str, str, int]] = []
        with WorkspacePaths(workspace) as paths:
            for relative in self.list_files(workspace.name):
                try:
                    with paths.open_for_read(relative) as handle:
                        digest = _hash_stream(handle)
                except (PathSafetyError, OSError):
                    continue
                size = (workspace / relative).stat().st_size
                entries.append((relative, digest, size))
        lines = [
            f"{digest}  {size:>10d}  {relative}"
            for relative, digest, size in sorted(entries)
        ]
        return "\n".join(lines) + ("\n" if lines else "")

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def list_files(self, workspace_id: str) -> list[str]:
        """Relative paths of every regular file, never following a symlink."""
        workspace = self._require_dir(workspace_id)
        found: list[str] = []
        with WorkspacePaths(workspace) as paths:
            for dirpath, dirnames, filenames in os.walk(workspace, followlinks=False):
                dirnames[:] = sorted(
                    name
                    for name in dirnames
                    if not os.path.islink(os.path.join(dirpath, name))
                )
                for name in sorted(filenames):
                    full = os.path.join(dirpath, name)
                    if os.path.islink(full):
                        continue
                    relative = os.path.relpath(full, workspace).replace(os.sep, "/")
                    if relative in _MANAGED_FILES:
                        continue
                    try:
                        # Resolving through WorkspacePaths proves containment and
                        # regular-file-ness without following a symlink.
                        paths.resolve_for_read(relative)
                    except (PathSafetyError, OSError):
                        continue
                    found.append(relative)
        return sorted(found)

    def archive_manifest(self, workspace_id: str) -> dict:
        """Map every regular file to its sha256, excluding manager metadata."""
        workspace = self._require_dir(workspace_id)
        manifest: dict[str, str] = {}
        with WorkspacePaths(workspace) as paths:
            for relative in self.list_files(workspace_id):
                try:
                    with paths.open_for_read(relative) as handle:
                        manifest[relative] = _hash_stream(handle)
                except (PathSafetyError, OSError):
                    continue
        return dict(sorted(manifest.items()))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def release(self, workspace_id: str) -> None:
        """Validate and keep the Workspace; releasing is a Core decision."""
        self._require_dir(workspace_id)

    def quarantine(self, workspace_id: str, reason: str) -> None:
        """Write a ``QUARANTINE.json`` marker; the files are left untouched."""
        workspace = self._require_dir(workspace_id)
        marker = {
            "workspace_id": workspace_id,
            "reason": str(reason),
            "quarantined_at": self._now_iso(),
        }
        with WorkspacePaths(workspace) as paths:
            paths.atomic_write("QUARANTINE.json", canonical_json(marker).encode("utf-8"))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _require_dir(self, workspace_id: str) -> Path:
        self._validate_id(workspace_id)
        workspace = self.root / workspace_id
        if not workspace.is_dir():
            raise FileNotFoundError(f"workspace {workspace_id!r} is not materialized")
        return workspace

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _git(path: Path, *args: str) -> str | None:
        """Run a read-only git command; return stripped stdout or None on failure."""
        try:
            completed = subprocess.run(
                ["git", "-C", str(path), *args],
                capture_output=True,
                text=True,
                timeout=_GIT_TIMEOUT_S,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if completed.returncode != 0:
            return None
        return completed.stdout.strip()


def _hash_stream(handle) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(_READ_CHUNK), b""):
        digest.update(chunk)
    return digest.hexdigest()
