"""Tests for on-disk Workspace materialization and inspection (M1 Task E)."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess

import pytest

from hibiki.tools.workspace import WorkspaceManager


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.strip()


def _init_repo(path, files: dict[str, str]) -> str:
    os.makedirs(path, exist_ok=True)
    _git(path, "init")
    for name, content in files.items():
        with open(os.path.join(path, name), "w", encoding="utf-8") as handle:
            handle.write(content)
    _git(path, "add", "-A")
    _git(
        path,
        "-c",
        "user.email=test@example.com",
        "-c",
        "user.name=test",
        "commit",
        "-m",
        "initial",
    )
    return _git(path, "rev-parse", "HEAD")


def test_materialize_creates_private_dir_and_is_idempotent(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_1", seed_files={"notes.txt": b"hello"})

    assert os.path.isdir(path)
    if os.name != "nt":
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o700
    assert open(os.path.join(path, "notes.txt"), "rb").read() == b"hello"

    # A second call must not delete content a writer produced.
    with open(os.path.join(path, "writer.txt"), "w", encoding="utf-8") as handle:
        handle.write("kept")
    again = manager.materialize("ws_1", seed_files={"notes.txt": b"replaced?"})

    assert again == path
    assert open(os.path.join(path, "writer.txt"), encoding="utf-8").read() == "kept"
    assert open(os.path.join(path, "notes.txt"), "rb").read() == b"hello"


def test_seed_files_include_nested_directories(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_nested", seed_files={"a/b/c.txt": b"deep"})
    assert open(os.path.join(path, "a", "b", "c.txt"), "rb").read() == b"deep"


def test_baseline_records_the_resolved_git_commit(tmp_path):
    source = tmp_path / "source"
    commit = _init_repo(source, {"main.py": "print('hi')\n"})
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_git", base_ref=str(source))

    recorded = manager.baseline("ws_git")
    assert recorded["base_commit"] == commit
    assert recorded["git_repo"] is True
    assert recorded["workspace_id"] == "ws_git"
    assert os.path.isfile(os.path.join(path, "baseline.json"))


def test_non_repo_base_ref_is_recorded_as_such(tmp_path):
    plain = tmp_path / "plain"
    plain.mkdir()
    manager = WorkspaceManager(tmp_path / "workspaces")
    manager.materialize("ws_plain", base_ref=str(plain))
    recorded = manager.baseline("ws_plain")
    assert recorded["git_repo"] is False
    assert recorded["base_commit"] is None


def test_diff_falls_back_to_listing_and_hashes(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    manager.materialize("ws_diff", seed_files={"b.txt": b"second", "a.txt": b"first"})
    diff = manager.diff("ws_diff")
    lines = [line for line in diff.splitlines() if line]
    assert len(lines) == 2
    assert lines[0].endswith("a.txt")
    assert _sha256(b"first") in lines[0]
    assert lines[1].endswith("b.txt")


def test_diff_uses_git_when_the_workspace_is_a_repo(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_repo")
    _init_repo(path, {"tracked.txt": "old\n"})
    with open(os.path.join(path, "tracked.txt"), "w", encoding="utf-8") as handle:
        handle.write("changed\n")

    diff = manager.diff("ws_repo")
    assert "tracked.txt" in diff
    assert "changed" in diff


def test_archive_manifest_hashes_every_regular_file(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    manager.materialize(
        "ws_manifest",
        seed_files={"one.txt": b"1", "dir/two.txt": b"22"},
    )
    manifest = manager.archive_manifest("ws_manifest")
    assert manifest == {"dir/two.txt": _sha256(b"22"), "one.txt": _sha256(b"1")}

    manager.quarantine("ws_manifest", "test")
    assert "QUARANTINE.json" not in manager.archive_manifest("ws_manifest")


def test_list_files_never_follows_a_symlink_out_of_root(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_link", seed_files={"inside.txt": b"ok"})
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    os.symlink(outside, os.path.join(path, "escape.txt"))

    files = manager.list_files("ws_link")
    assert files == ["inside.txt"]


@pytest.mark.parametrize("bad_id", ["../x", "a/b", "..", "a\x00b", ""])
def test_materialize_refuses_unsafe_workspace_ids(tmp_path, bad_id):
    manager = WorkspaceManager(tmp_path / "workspaces")
    with pytest.raises(ValueError):
        manager.materialize(bad_id)


def test_quarantine_writes_a_marker_and_keeps_files(tmp_path):
    manager = WorkspaceManager(tmp_path / "workspaces")
    path = manager.materialize("ws_q", seed_files={"work.txt": b"payload"})
    manager.quarantine("ws_q", "stubborn writer")

    marker = json.loads(open(os.path.join(path, "QUARANTINE.json"), encoding="utf-8").read())
    assert marker["workspace_id"] == "ws_q"
    assert marker["reason"] == "stubborn writer"
    assert open(os.path.join(path, "work.txt"), "rb").read() == b"payload"

    # release is a validated no-op that keeps the files
    manager.release("ws_q")
    assert open(os.path.join(path, "work.txt"), "rb").read() == b"payload"
