"""Ports for replaceable infrastructure boundaries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime:
        raise NotImplementedError


class AgentAdapter(ABC):
    @abstractmethod
    def start(self, run_spec: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def send(self, run_id: str, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self, run_id: str, reason: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def inspect(self, run_id: str) -> dict[str, Any]:
        raise NotImplementedError


class SandboxAdapter(ABC):
    @abstractmethod
    def execute(self, command: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class ExternalAdapter(ABC):
    @abstractmethod
    def dispatch(self, effect: dict[str, Any]) -> dict[str, Any]:
        """Return receipt with status: succeeded|failed|unknown."""

    @abstractmethod
    def query(self, effect_id: str, external_idempotency_key: str) -> dict[str, Any] | None:
        raise NotImplementedError


class ArtifactStore(ABC):
    @abstractmethod
    def put(self, content: bytes, *, content_hash: str | None = None) -> str:
        raise NotImplementedError

    @abstractmethod
    def get(self, uri: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def exists(self, uri: str) -> bool:
        raise NotImplementedError


@dataclass
class RunSpec:
    run_id: str
    task_id: str
    assignment_kind: str
    profile_id: str = "fake"
    tools: list[str] = field(default_factory=list)
    context_manifest_id: str | None = None
    fencing_epoch: int = 1
    grant_epoch: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
