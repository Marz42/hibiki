from __future__ import annotations

import hashlib
from pathlib import Path

from hibiki.domain.ports import ArtifactStore, SandboxAdapter


class LocalArtifactStore(ArtifactStore):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, content: bytes, *, content_hash: str | None = None) -> str:
        digest = content_hash or hashlib.sha256(content).hexdigest()
        path = self.root / digest
        if not path.exists():
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(content)
            tmp.replace(path)
        return f"artifact://{digest}"

    def get(self, uri: str) -> bytes:
        digest = uri.removeprefix("artifact://")
        return (self.root / digest).read_bytes()

    def exists(self, uri: str) -> bool:
        digest = uri.removeprefix("artifact://")
        return (self.root / digest).exists()


class FakeSandboxAdapter(SandboxAdapter):
    def execute(self, command: dict) -> dict:
        return {"status": "ok", "echo": command}
