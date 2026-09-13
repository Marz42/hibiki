"""Tests for safe workspace path handling (M1 Task C path decision)."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from hibiki.tools.paths import PathSafetyError, WorkspacePaths


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    workspace = tmp_path / "root"
    workspace.mkdir()
    return workspace


def test_successful_read_list_nested_mkdir_and_atomic_write(root: Path) -> None:
    with WorkspacePaths(root) as paths:
        assert paths.root == os.path.realpath(root)
        paths.mkdir("a/b/c")
        assert (root / "a" / "b" / "c").is_dir()
        assert paths.exists("a/b/c") is True
        assert paths.exists("a/nope") is False

        digest = paths.atomic_write("a/b/c/file.txt", b"hello")
        assert digest == hashlib.sha256(b"hello").hexdigest()
        assert (root / "a" / "b" / "c" / "file.txt").read_bytes() == b"hello"
        # No temporary file survives the atomic write.
        assert paths.list_dir("a/b/c") == ["file.txt"]

        expected = os.path.join(paths.root, "a", "b", "c", "file.txt")
        assert paths.resolve_for_read("a/b/c/file.txt") == expected
        with paths.open_for_read("a/b/c/file.txt") as handle:
            assert handle.read() == b"hello"
        assert paths.list_dir("a") == ["b"]


def test_open_for_write_writes_a_regular_file(root: Path) -> None:
    with WorkspacePaths(root) as paths:
        with paths.open_for_write("plain.txt", mode=0o600) as handle:
            handle.write(b"data")
        assert (root / "plain.txt").read_bytes() == b"data"

        (root / "dangling").symlink_to(root.parent / "outside.txt")
        with pytest.raises(PathSafetyError):
            paths.open_for_write("dangling")


def test_missing_file_for_read_is_a_normal_filesystem_error(root: Path) -> None:
    with WorkspacePaths(root) as paths:
        with pytest.raises(FileNotFoundError):
            paths.resolve_for_read("missing.txt")
        with pytest.raises(FileNotFoundError):
            paths.open_for_read("missing.txt")


@pytest.mark.parametrize(
    "bad",
    [
        "../x",
        "a/../../x",
        "a/b/../../../../etc/passwd",
        "/etc/passwd",
        "C:\\x",
        "C:/x",
        "\\\\server\\share\\x",
        "a\x00b",
        "",
        ".",
        "./",
    ],
)
def test_unsafe_paths_are_rejected_by_every_operation(root: Path, bad: str) -> None:
    with WorkspacePaths(root) as paths:
        operations = (
            lambda: paths.resolve_for_read(bad),
            lambda: paths.open_for_read(bad),
            lambda: paths.open_for_write(bad),
            lambda: paths.list_dir(bad),
            lambda: paths.mkdir(bad),
            lambda: paths.exists(bad),
            lambda: paths.atomic_write(bad, b"x"),
        )
        for operation in operations:
            with pytest.raises(PathSafetyError):
                operation()


def test_symlinked_directory_inside_root_pointing_outside_is_refused(
    root: Path, tmp_path: Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    (root / "linkdir").symlink_to(outside, target_is_directory=True)

    with WorkspacePaths(root) as paths:
        with pytest.raises(PathSafetyError):
            paths.list_dir("linkdir")
        with pytest.raises(PathSafetyError):
            paths.resolve_for_read("linkdir/secret.txt")
        with pytest.raises(PathSafetyError):
            paths.open_for_read("linkdir/secret.txt")
        with pytest.raises(PathSafetyError):
            paths.exists("linkdir")
        with pytest.raises(PathSafetyError):
            paths.mkdir("linkdir/sub")
        with pytest.raises(PathSafetyError):
            paths.atomic_write("linkdir/secret.txt", b"overwritten")

    assert (outside / "secret.txt").read_text() == "secret"


def test_symlink_at_the_final_component_is_refused(root: Path, tmp_path: Path) -> None:
    target = tmp_path / "outside.txt"
    target.write_text("secret")
    (root / "final.txt").symlink_to(target)

    with WorkspacePaths(root) as paths:
        with pytest.raises(PathSafetyError):
            paths.resolve_for_read("final.txt")
        with pytest.raises(PathSafetyError):
            paths.open_for_read("final.txt")
        with pytest.raises(PathSafetyError):
            paths.open_for_write("final.txt")
        with pytest.raises(PathSafetyError):
            paths.atomic_write("final.txt", b"x")
        with pytest.raises(PathSafetyError):
            paths.exists("final.txt")

    assert target.read_text() == "secret"


def test_intermediate_regular_file_is_not_a_directory(root: Path) -> None:
    (root / "afile").write_text("x")
    with WorkspacePaths(root) as paths:
        with pytest.raises(NotADirectoryError):
            paths.resolve_for_read("afile/child.txt")
        assert paths.exists("afile/child.txt") is False


def test_root_must_exist_and_be_a_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        WorkspacePaths(tmp_path / "missing")

    a_file = tmp_path / "file"
    a_file.write_text("x")
    with pytest.raises(NotADirectoryError):
        WorkspacePaths(a_file)


def test_closed_workspace_refuses_further_operations(root: Path) -> None:
    paths = WorkspacePaths(root)
    paths.close()
    paths.close()  # idempotent
    with pytest.raises(ValueError):
        paths.exists("anything")
