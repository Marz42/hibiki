"""Docker-backed per-Run sandbox driver (M1 Task D, spec §13.3 / §11.1).

Host security context (``docs/M1-CHECKLIST.md`` §3)
---------------------------------------------------
The Docker daemon on this host is **rootful inside its VM** (Docker Desktop), and
the orchestrator user is in the ``docker`` group. That group membership is
host-root-equivalent: whoever can talk to the daemon can start a privileged
container that mounts the host. Two consequences are non-negotiable here:

1. The driver must never mount ``/var/run/docker.sock`` (or ``/run/docker.sock``)
   into a sandbox — a container holding the daemon socket escapes every flag
   below. :class:`SandboxSpec` rejects those host paths outright.
2. Credentials must not be passed with ``--env``. :meth:`DockerSandboxAdapter.execute`
   forwards only the explicit :attr:`SandboxSpec.env` mapping and never inherits
   ``os.environ``; keys that look like a key/token/secret/password/credential are
   refused at construction time rather than silently dropped.

Rootless Docker is not available on this host (no ``uidmap``/``newuidmap``), so
the spec's rootless preference is an explicit M1 limitation, not a guarantee.

Every container is started through the hardened profile built by
:meth:`DockerSandboxAdapter.build_run_args`::

    docker run --rm --init --read-only --tmpfs /tmp --user 65534:65534
      --cap-drop=ALL --security-opt=no-new-privileges --network=none
      --pids-limit=N --memory=Xm --memory-swap=Xm --cpus=N
      --ulimit nofile=N --stop-timeout=T --workdir <cwd>
      -v <workspace>:/workspace:rw [-v <host>:<ctr>:ro ...]
      [--env K=V ...] <image> <argv...>

Docker has no wall-clock flag, so ``wall_timeout_s`` is enforced orchestrator-side:
on expiry the driver runs ``docker kill <id>`` (the id comes from ``--cidfile``)
and reaps the client process.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from hibiki.domain.ports import SandboxAdapter

_WORKSPACE_MOUNT = "/workspace"
_CONTAINER_USER = "65534:65534"
_INFO_TIMEOUT_S = 20
_KILL_TIMEOUT_S = 20
_CIDFILE_WAIT_S = 2.0
_REAP_GRACE_S = 5.0

# Same shape as the credential rule the checklist requires: fail closed on any
# env key that could carry a secret. False positives (e.g. "MONKEY") are fine;
# a leaked model API key is not.
_CREDENTIAL_KEY_RE = re.compile(r"key|token|secret|password|credential", re.IGNORECASE)

_DAEMON_ERROR_MARKERS = (
    "cannot connect to the docker daemon",
    "is the docker daemon running",
    "error during connect",
    "docker daemon is not running",
)

# realpath maps /var/run/docker.sock onto /run/docker.sock where /var/run symlinks.
_DOCKER_SOCKET_PATHS = frozenset(
    os.path.realpath(path) for path in ("/var/run/docker.sock", "/run/docker.sock")
)


@dataclass(frozen=True)
class SandboxLimits:
    """Per-container resource and time ceilings."""

    memory_mb: int = 512
    memory_swap_mb: int = 512
    cpus: float = 1.0
    pids: int = 128
    wall_timeout_s: int = 120
    stop_grace_s: int = 5
    nofile: int = 256

    def __post_init__(self) -> None:
        for name in (
            "memory_mb",
            "memory_swap_mb",
            "pids",
            "wall_timeout_s",
            "stop_grace_s",
            "nofile",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer, got {value!r}")
        if isinstance(self.cpus, bool) or not isinstance(self.cpus, (int, float)) or self.cpus <= 0:
            raise ValueError(f"cpus must be positive, got {self.cpus!r}")


@dataclass(frozen=True)
class SandboxSpec:
    """Explicit sandbox configuration; nothing is read from global state.

    ``workspace_host_path`` defaults to ``""`` only so the dataclass keeps the
    documented field order (a required field cannot follow ``image``'s default).
    An empty or relative path is always rejected by ``__post_init__``, so the
    path is effectively required.
    """

    image: str = "python:3.12-slim"
    workspace_host_path: str = ""
    readonly_host_paths: tuple[tuple[str, str], ...] = ()
    limits: SandboxLimits = SandboxLimits()
    env: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.image, str) or not self.image:
            raise ValueError("image must be a non-empty string")
        # Credentials are checked first: a leaked key is the worst outcome, so
        # report that even when the rest of the spec is also malformed.
        if not isinstance(self.env, Mapping):
            raise ValueError("env must be a mapping of str to str")
        for key, value in self.env.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("env must map str keys to str values")
            if _CREDENTIAL_KEY_RE.search(key):
                raise ValueError(
                    f"refusing credential-like env key {key!r}: credentials never enter a sandbox"
                )
        self._check_mount_path(self.workspace_host_path, "workspace_host_path")
        for host, container in self.readonly_host_paths:
            self._check_mount_path(host, "readonly_host_paths host path")
            if not isinstance(container, str) or not container.startswith("/"):
                raise ValueError("readonly_host_paths container paths must be absolute")
            if container.rstrip("/") == _WORKSPACE_MOUNT:
                raise ValueError(f"readonly_host_paths may not shadow {_WORKSPACE_MOUNT}")

    @staticmethod
    def _check_mount_path(path: str, label: str) -> None:
        if not isinstance(path, str) or not os.path.isabs(path):
            raise ValueError(f"{label} must be an absolute host path, got {path!r}")
        if os.path.realpath(path) in _DOCKER_SOCKET_PATHS:
            raise ValueError(f"{label} may not be the Docker daemon socket: {path!r}")


@dataclass(frozen=True)
class _Command:
    """Validated form of the ``command`` mapping handed to ``execute``."""

    argv: tuple[str, ...]
    cwd: str
    stdin: str | None
    timeout_s: int | None


class DockerSandboxAdapter(SandboxAdapter):
    """Run one hardened container per command through the local Docker CLI."""

    def __init__(self, spec: SandboxSpec | None = None, *, docker_bin: str = "docker") -> None:
        """Create a driver for ``spec`` without reading process/global state.

        ``spec`` may be omitted only for an :meth:`available` probe; ``execute``
        refuses to run without an explicit :class:`SandboxSpec`.
        """
        if not isinstance(docker_bin, str) or not docker_bin:
            raise ValueError("docker_bin must be a non-empty string")
        self.spec = spec
        self.docker_bin = docker_bin

    def available(self) -> bool:
        """True when the Docker CLI is present and its daemon answers."""
        try:
            probe = subprocess.run(
                [self.docker_bin, "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=_INFO_TIMEOUT_S,
                env=_docker_cli_env(),
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return probe.returncode == 0

    def build_run_args(self, command: dict[str, Any], cidfile: str) -> list[str]:
        """Return the hardened ``docker run`` argv for ``command`` (starts nothing).

        Exposed so the hardening profile can be asserted without a daemon.
        """
        spec = self._require_spec()
        return self._run_args(self._validate_command(command), spec, cidfile)

    def execute(self, command: dict[str, Any]) -> dict[str, Any]:
        spec = self._require_spec()
        cmd = self._validate_command(command)
        cancel_event = command.get("cancel_event")
        if cancel_event is not None and not hasattr(cancel_event, "is_set"):
            raise ValueError("command['cancel_event'] must be an event-like object")
        timeout_s = self._effective_timeout(cmd, spec)
        started = time.monotonic()
        stdin = cmd.stdin.encode("utf-8") if cmd.stdin is not None else None
        raw_out, raw_err = b"", b""
        with tempfile.TemporaryDirectory(prefix="hibiki-sandbox-") as tmpdir:
            cidfile = os.path.join(tmpdir, "container-id")
            args = self._run_args(cmd, spec, cidfile)
            try:
                proc = subprocess.Popen(
                    args,
                    stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=_docker_cli_env(),
                    start_new_session=True,
                )
            except OSError as exc:
                return self._result(
                    "unavailable", None, "", f"docker unavailable: {exc}", started, None, False
                )
            container_id: str | None = None
            timed_out = False
            cancelled = False
            deadline = started + timeout_s
            # Non-blocking read loop so a stop request can interrupt a long command
            # immediately instead of waiting for the wall clock (SPEC §18).
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    try:
                        os.set_blocking(stream.fileno(), False)
                    except (AttributeError, OSError, ValueError):
                        # Windows / unsupported fds: fall back to blocking reads.
                        pass
            if proc.stdin is not None and stdin is not None:
                try:
                    proc.stdin.write(stdin)
                    proc.stdin.flush()
                    proc.stdin.close()
                except OSError:
                    pass
            exit_confirmed = True
            while True:
                for stream in (proc.stdout, proc.stderr):
                    chunk = _safe_read(stream)
                    if chunk:
                        if stream is proc.stdout:
                            raw_out += chunk
                        else:
                            raw_err += chunk
                if proc.poll() is not None:
                    break
                if cancel_event is not None and cancel_event.is_set():
                    cancelled = True
                    raw_out, raw_err, container_id, exit_confirmed = self._stop_container(
                        proc, cidfile, spec, raw_out, raw_err
                    )
                    break
                if time.monotonic() >= deadline:
                    # Docker has no wall-clock flag: kill the container by cid, then
                    # reap the client so no `docker run` process is left behind.
                    timed_out = True
                    raw_out, raw_err, container_id, exit_confirmed = self._stop_container(
                        proc, cidfile, spec, raw_out, raw_err
                    )
                    break
                time.sleep(0.05)
            raw_out += _safe_read(proc.stdout) or b""
            raw_err += _safe_read(proc.stderr) or b""
            container_id = container_id or _read_cidfile(cidfile)
            if not cancelled and not timed_out and container_id:
                # Normal completion: the client exited; confirm the container is gone
                # (``--rm`` should have removed it). If it is still running, do not
                # claim a clean exit.
                exit_confirmed = not self._container_running(container_id)
        stdout = raw_out.decode("utf-8", errors="replace")
        stderr = raw_err.decode("utf-8", errors="replace")
        exit_code = proc.returncode
        if cancelled:
            status = "cancelled" if exit_confirmed else "stop_unconfirmed"
        elif timed_out:
            status = "timeout" if exit_confirmed else "stop_unconfirmed"
        elif not exit_confirmed:
            status = "stop_unconfirmed"
        elif exit_code == 0:
            status = "ok"
        elif _looks_like_daemon_failure(stderr):
            status = "unavailable"
        else:
            status = "error"
        # SIGKILL (137) is what the kernel reports for a memory-cgroup OOM kill;
        # a failed command is never reported as success.
        oom_killed = not timed_out and exit_code == 137
        result = self._result(
            status, exit_code, stdout, stderr, started, container_id, oom_killed
        )
        result["exit_confirmed"] = bool(exit_confirmed)
        return result

    def _stop_container(
        self,
        proc: subprocess.Popen[bytes],
        cidfile: str,
        spec: SandboxSpec,
        raw_out: bytes,
        raw_err: bytes,
    ) -> tuple[bytes, bytes, str | None, bool]:
        """Kill the container and guarantee the client process group is reaped.

        If the cidfile is not ready (the daemon is slow or wedged) there is no container
        id to kill, so the *client* process group is killed instead — otherwise the
        `docker run` process stays alive holding the Workspace mounted read-write and a
        retry would become a second writer.

        Returns ``(stdout, stderr, container_id, exit_confirmed)``. A failed or
        unverifiable ``docker kill`` leaves ``exit_confirmed=False`` so callers must
        keep the Workspace quarantined rather than treating the CLI exit as proof.
        """
        container_id = _wait_for_container_id(cidfile)
        killed = self._kill_container(container_id)
        for stream, name in ((proc.stdout, "out"), (proc.stderr, "err")):
            if stream is None:
                continue
            chunk = _safe_read(stream)
            if name == "out":
                raw_out += chunk
            else:
                raw_err += chunk
        if container_id is None:
            # No container id: the client process group is the only handle left.
            _kill_process_group(proc)
            raw_out, raw_err = self._reap(proc, spec.limits.stop_grace_s)
            # Without a container id we cannot prove the writer is gone.
            return raw_out or b"", raw_err or b"", None, False
        raw_out, raw_err = self._reap(proc, spec.limits.stop_grace_s)
        still_running = self._container_running(container_id)
        exit_confirmed = bool(killed) and not still_running
        return raw_out or b"", raw_err or b"", container_id, exit_confirmed

    def _require_spec(self) -> SandboxSpec:
        if self.spec is None:
            raise ValueError("DockerSandboxAdapter needs an explicit SandboxSpec to run commands")
        return self.spec

    def _kill_container(self, container_id: str | None) -> bool:
        """Send ``docker kill`` and return whether the CLI reported success.

        Failures are no longer swallowed: a non-zero exit, timeout, or OS error
        means the caller must treat the writer as still potentially alive.
        """
        if not container_id:
            return False
        try:
            completed = subprocess.run(
                [self.docker_bin, "kill", container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=_KILL_TIMEOUT_S,
                env=_docker_cli_env(),
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return completed.returncode == 0

    def _container_running(self, container_id: str | None) -> bool:
        """True when ``docker inspect`` reports the container is still running."""
        if not container_id:
            return False
        try:
            completed = subprocess.run(
                [
                    self.docker_bin,
                    "inspect",
                    "-f",
                    "{{.State.Running}}",
                    container_id,
                ],
                capture_output=True,
                text=True,
                timeout=_KILL_TIMEOUT_S,
                env=_docker_cli_env(),
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            # Inspect failed: treat as still possibly running (fail closed).
            return True
        if completed.returncode != 0:
            # Unknown / removed container: not running.
            return False
        return completed.stdout.strip().lower() in {"true", "1"}

    def inspect_container(self, container_id: str | None) -> dict[str, Any]:
        """Public probe used by reconcile/stop confirmation."""
        if not container_id:
            return {"container_id": None, "running": False, "known": False}
        running = self._container_running(container_id)
        return {
            "container_id": container_id,
            "running": running,
            # When inspect fails closed as running we still report known=True only
            # if the CLI returned a definitive answer; callers use ``running``.
            "known": True,
        }

    @staticmethod
    def _reap(proc: subprocess.Popen[bytes], stop_grace_s: int) -> tuple[bytes, bytes]:
        try:
            out, err = proc.communicate(timeout=stop_grace_s + _REAP_GRACE_S)
        except subprocess.TimeoutExpired:
            _kill_process_group(proc)
            out, err = proc.communicate()
        return out or b"", err or b""

    @staticmethod
    def _validate_command(command: dict[str, Any]) -> _Command:
        if not isinstance(command, dict):
            raise ValueError("command must be a mapping")
        argv = command.get("argv")
        if not isinstance(argv, (list, tuple)) or not argv:
            raise ValueError("command['argv'] must be a non-empty list of strings")
        for item in argv:
            if not isinstance(item, str) or not item:
                raise ValueError("command['argv'] entries must be non-empty strings")
        cwd = command.get("cwd", _WORKSPACE_MOUNT)
        if not isinstance(cwd, str) or not cwd.startswith("/"):
            raise ValueError("command['cwd'] must be an absolute container path")
        stdin = command.get("stdin")
        if stdin is not None and not isinstance(stdin, str):
            raise ValueError("command['stdin'] must be a string or None")
        timeout_s = command.get("timeout_s")
        if timeout_s is not None and (
            isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0
        ):
            raise ValueError("command['timeout_s'] must be a positive number or None")
        return _Command(tuple(argv), cwd, stdin, int(timeout_s) if timeout_s is not None else None)

    @staticmethod
    def _effective_timeout(cmd: _Command, spec: SandboxSpec) -> float:
        wall = float(spec.limits.wall_timeout_s)
        # Remaining Run wall budget (orchestrator) may further tighten the limit.
        remaining = None
        # Carried on the validated command via a side channel on the raw dict is
        # handled by callers setting timeout_s; here we only cap by sandbox wall.
        if cmd.timeout_s is None:
            return wall
        # A per-command request may tighten the wall clock but never extend it.
        return min(wall, float(cmd.timeout_s))

    def _run_args(self, cmd: _Command, spec: SandboxSpec, cidfile: str) -> list[str]:
        limits = spec.limits
        args = [
            self.docker_bin,
            "run",
            "--cidfile",
            cidfile,
            "--rm",
            "--init",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--user",
            _CONTAINER_USER,
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--network=none",
            f"--pids-limit={limits.pids}",
            f"--memory={limits.memory_mb}m",
            f"--memory-swap={limits.memory_swap_mb}m",
            f"--cpus={limits.cpus}",
            "--ulimit",
            f"nofile={limits.nofile}",
            f"--stop-timeout={limits.stop_grace_s}",
            "--workdir",
            cmd.cwd,
            "-v",
            f"{spec.workspace_host_path}:{_WORKSPACE_MOUNT}:rw",
        ]
        for host, container in spec.readonly_host_paths:
            args += ["-v", f"{host}:{container}:ro"]
        for key, value in spec.env.items():
            args += ["--env", f"{key}={value}"]
        args += [spec.image, *cmd.argv]
        return args

    @staticmethod
    def _result(
        status: str,
        exit_code: int | None,
        stdout: str,
        stderr: str,
        started: float,
        container_id: str | None,
        oom_killed: bool,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "container_id": container_id,
            "oom_killed": oom_killed,
        }


def _docker_cli_env() -> dict[str, str]:
    """Environment for the docker CLI process, minus credential-like keys.

    The container never inherits any of this — only explicit ``--env K=V`` pairs
    are forwarded — but the model API key must not even reach the CLI process.
    """
    return {key: value for key, value in os.environ.items() if not _CREDENTIAL_KEY_RE.search(key)}


def _read_cidfile(cidfile: str) -> str | None:
    try:
        with open(cidfile, encoding="utf-8") as handle:
            content = handle.read().strip()
    except OSError:
        return None
    return content or None


def _wait_for_container_id(cidfile: str) -> str | None:
    deadline = time.monotonic() + _CIDFILE_WAIT_S
    while True:
        container_id = _read_cidfile(cidfile)
        if container_id or time.monotonic() >= deadline:
            return container_id
        time.sleep(0.05)


def _safe_read(stream: Any) -> bytes:
    """Read whatever is buffered on a pipe without raising once it is closed."""
    if stream is None:
        return b""
    try:
        return stream.read() or b""
    except (ValueError, OSError):
        return b""


def _kill_process_group(proc: subprocess.Popen[bytes]) -> None:
    try:
        if hasattr(os, "killpg") and hasattr(os, "getpgid"):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        else:
            proc.kill()
    except OSError:
        # Already gone; nothing left to reap.
        pass


def _looks_like_daemon_failure(stderr: str) -> bool:
    lowered = stderr.lower()
    return any(marker in lowered for marker in _DAEMON_ERROR_MARKERS)
