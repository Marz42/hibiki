"""Tests for the Docker per-Run sandbox driver (M1 Task D)."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from hibiki.tools.sandbox import DockerSandboxAdapter, SandboxLimits, SandboxSpec

IMAGE = "python:3.12-slim"

# Probe once at import: container tests only, so the rest of the suite stays
# green (and fast) on hosts without Docker.
DOCKER_AVAILABLE = DockerSandboxAdapter().available()
docker_required = pytest.mark.skipif(not DOCKER_AVAILABLE, reason="docker unavailable")

_READONLY_PROBE = """
import json
import os

out = {}
for path in ("/x", "/tmp/x"):
    try:
        with open(path, "w") as handle:
            handle.write("x")
        out[path] = "allowed"
    except OSError:
        out[path] = "denied"
out["uid"] = os.getuid()
caps = {}
with open("/proc/self/status") as handle:
    for line in handle:
        key, _, value = line.partition(":")
        if key in ("CapEff", "NoNewPrivs"):
            caps[key] = value.strip()
out["caps"] = caps
print(json.dumps(out))
"""


def _spec(workspace: Path, **overrides: object) -> SandboxSpec:
    return SandboxSpec(
        image=IMAGE,
        workspace_host_path=str(workspace),
        **overrides,  # type: ignore[arg-type]
    )


def _running_containers() -> int:
    proc = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"ancestor={IMAGE}"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return len(proc.stdout.split())


def _no_lingering_containers(baseline: int, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _running_containers() <= baseline:
            return True
        time.sleep(0.2)
    return False


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    # The container runs as uid 65534, so the bind-mounted workspace must be
    # world-writable (pytest's tmp dirs are 0o700 by default).
    os.chmod(tmp_path, 0o777)
    return tmp_path


@docker_required
def test_execute_runs_command_and_captures_stdout(workspace: Path) -> None:
    adapter = DockerSandboxAdapter(_spec(workspace))

    result = adapter.execute({"argv": ["python", "-c", "print('hello-sandbox')"]})

    assert result["status"] == "ok"
    assert result["exit_code"] == 0
    assert result["stdout"].strip() == "hello-sandbox"
    assert result["stderr"] == ""
    assert result["container_id"]
    assert result["duration_ms"] >= 0
    assert result["oom_killed"] is False


@docker_required
def test_no_network(workspace: Path) -> None:
    adapter = DockerSandboxAdapter(_spec(workspace))
    code = "import socket; socket.create_connection(('1.1.1.1', 443), 3)"

    result = adapter.execute({"argv": ["python", "-c", code]})

    assert result["status"] == "error"
    assert result["exit_code"] not in (0, None)
    assert "unreachable" in result["stderr"].lower() or "network" in result["stderr"].lower()


@docker_required
def test_read_only_root_non_root_user_and_no_capabilities(workspace: Path) -> None:
    adapter = DockerSandboxAdapter(_spec(workspace))

    result = adapter.execute({"argv": ["python", "-c", _READONLY_PROBE]})

    assert result["status"] == "ok", result["stderr"]
    probe = json.loads(result["stdout"])
    assert probe["/x"] == "denied"  # read-only root filesystem
    assert probe["/tmp/x"] == "allowed"  # writable tmpfs
    assert probe["uid"] == 65534
    assert int(probe["caps"]["CapEff"], 16) == 0
    assert probe["caps"]["NoNewPrivs"] == "1"


@docker_required
def test_workspace_is_writable_and_readonly_mount_is_enforced(
    workspace: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    readonly = tmp_path_factory.mktemp("readonly-inputs")
    (readonly / "keep.txt").write_text("keep")
    os.chmod(readonly, 0o755)
    spec = _spec(workspace, readonly_host_paths=((str(readonly), "/inputs"),))
    adapter = DockerSandboxAdapter(spec)

    result = adapter.execute(
        {
            "argv": [
                "python",
                "-c",
                "open('/workspace/out.txt','w').write('from-sandbox');"
                "print(open('/inputs/keep.txt').read(), end='')",
            ]
        }
    )

    assert result["status"] == "ok", result["stderr"]
    assert result["stdout"] == "keep"
    assert (workspace / "out.txt").read_text() == "from-sandbox"

    denied = adapter.execute({"argv": ["python", "-c", "open('/inputs/no.txt','w').write('x')"]})
    assert denied["status"] == "error"
    assert denied["exit_code"] not in (0, None)
    assert not (readonly / "no.txt").exists()


@docker_required
def test_wall_clock_timeout_kills_the_container(workspace: Path) -> None:
    spec = _spec(workspace, limits=SandboxLimits(wall_timeout_s=2))
    adapter = DockerSandboxAdapter(spec)
    baseline = _running_containers()

    started = time.monotonic()
    result = adapter.execute({"argv": ["sleep", "30"]})
    elapsed = time.monotonic() - started

    assert result["status"] == "timeout"
    assert result["exit_code"] not in (0, None)
    assert result["container_id"]
    assert elapsed < 15
    assert _no_lingering_containers(baseline), "docker kill left the sandbox running"


@docker_required
def test_memory_limit_kill_is_not_reported_as_success(workspace: Path) -> None:
    spec = _spec(workspace, limits=SandboxLimits(memory_mb=64, memory_swap_mb=64))
    adapter = DockerSandboxAdapter(spec)

    result = adapter.execute(
        {"argv": ["python", "-c", "a = bytearray(256 * 1024 * 1024); print(len(a))"]}
    )

    assert result["status"] == "error"
    assert result["exit_code"] == 137
    assert result["oom_killed"] is True


@docker_required
def test_container_env_is_explicit_and_never_inherits_host(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "leak-me")
    monkeypatch.setenv("HIBIKI_HOST_MARKER", "host-only")
    adapter = DockerSandboxAdapter(_spec(workspace, env={"HIBIKI_EXPLICIT": "yes"}))

    result = adapter.execute(
        {
            "argv": [
                "python",
                "-c",
                "import os; print(os.environ.get('HIBIKI_EXPLICIT'),"
                " os.environ.get('OPENAI_API_KEY'), os.environ.get('HIBIKI_HOST_MARKER'))",
            ]
        }
    )

    assert result["status"] == "ok", result["stderr"]
    assert result["stdout"].strip() == "yes None None"


def test_build_run_args_applies_the_hardening_profile(tmp_path: Path) -> None:
    spec = SandboxSpec(
        image=IMAGE,
        workspace_host_path=str(tmp_path),
        readonly_host_paths=((str(tmp_path), "/inputs"),),
        limits=SandboxLimits(
            memory_mb=256,
            memory_swap_mb=256,
            cpus=0.5,
            pids=64,
            wall_timeout_s=7,
            stop_grace_s=3,
            nofile=128,
        ),
        env={"HIBIKI_MODE": "test"},
    )

    args = DockerSandboxAdapter(spec).build_run_args(
        {"argv": ["python", "-c", "print(1)"], "cwd": "/workspace"}, "/tmp/cid"
    )
    joined = " ".join(args)

    assert args[:2] == ["docker", "run"]
    assert "--cidfile /tmp/cid" in joined
    for flag in (
        "--rm",
        "--init",
        "--read-only",
        "--tmpfs /tmp",
        "--user 65534:65534",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--network=none",
        "--pids-limit=64",
        "--memory=256m",
        "--memory-swap=256m",
        "--cpus=0.5",
        "--ulimit nofile=128",
        "--stop-timeout=3",
        "--workdir /workspace",
        f"-v {tmp_path}:/workspace:rw",
        f"-v {tmp_path}:/inputs:ro",
        "--env HIBIKI_MODE=test",
    ):
        assert flag in joined, flag
    assert args[-4:] == [IMAGE, "python", "-c", "print(1)"]
    assert args.count("--env") == 1  # only the explicit spec env is forwarded


def test_unavailable_when_docker_cli_is_missing(tmp_path: Path) -> None:
    adapter = DockerSandboxAdapter(
        SandboxSpec(workspace_host_path=str(tmp_path)), docker_bin="definitely-not-docker"
    )

    assert adapter.available() is False
    result = adapter.execute({"argv": ["echo", "hi"]})

    assert result["status"] == "unavailable"
    assert result["exit_code"] is None
    assert result["stdout"] == ""
    assert result["container_id"] is None
    assert result["oom_killed"] is False


def test_credential_like_env_keys_are_rejected() -> None:
    with pytest.raises(ValueError):
        SandboxSpec(env={"OPENAI_API_KEY": "x"})
    for key in ("GITHUB_TOKEN", "DB_PASSWORD", "MY_SECRET", "AWS_CREDENTIAL"):
        with pytest.raises(ValueError):
            SandboxSpec(workspace_host_path="/tmp/workspace", env={key: "x"})


def test_malformed_command_and_spec_raise_value_error(tmp_path: Path) -> None:
    adapter = DockerSandboxAdapter(SandboxSpec(workspace_host_path=str(tmp_path)))

    for bad in (
        {},
        {"argv": []},
        {"argv": "echo hi"},
        {"argv": [1]},
        {"argv": ["echo"], "cwd": "relative"},
        {"argv": ["echo"], "timeout_s": 0},
        {"argv": ["echo"], "stdin": 5},
    ):
        with pytest.raises(ValueError):
            adapter.execute(bad)

    with pytest.raises(ValueError):
        SandboxSpec(workspace_host_path="relative/workspace")
    with pytest.raises(ValueError):
        SandboxSpec(
            workspace_host_path=str(tmp_path),
            readonly_host_paths=(("relative/readonly", "/inputs"),),
        )
    with pytest.raises(ValueError):
        SandboxSpec(
            workspace_host_path=str(tmp_path),
            readonly_host_paths=((str(tmp_path), "/workspace"),),
        )
    with pytest.raises(ValueError):
        SandboxSpec(
            workspace_host_path=str(tmp_path),
            readonly_host_paths=(("/var/run/docker.sock", "/docker.sock"),),
        )
    with pytest.raises(ValueError):
        DockerSandboxAdapter().execute({"argv": ["echo"]})
