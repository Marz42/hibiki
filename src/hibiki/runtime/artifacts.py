from __future__ import annotations

import hashlib
import re
from pathlib import Path

from hibiki.domain.ports import ArtifactStore, SandboxAdapter

#: Content-addressed URIs are only a SHA-256 hex digest — never a relative path.
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def artifact_digest(uri: str) -> str:
    """Return the SHA-256 digest from an ``artifact://`` URI, or raise ``ValueError``."""
    if not isinstance(uri, str) or not uri.startswith("artifact://"):
        raise ValueError("artifact uri must start with artifact://")
    digest = uri.removeprefix("artifact://")
    if not _SHA256_HEX.fullmatch(digest):
        raise ValueError(
            "artifact uri must be artifact://<sha256-hex>; "
            "path traversal and non-digest suffixes are refused"
        )
    return digest


class LocalArtifactStore(ArtifactStore):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, content: bytes, *, content_hash: str | None = None) -> str:
        digest = content_hash or hashlib.sha256(content).hexdigest()
        if not _SHA256_HEX.fullmatch(str(digest)):
            raise ValueError("content_hash must be a sha256 hex digest")
        path = self.root / digest
        if not path.exists():
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(content)
            tmp.replace(path)
        return f"artifact://{digest}"

    def get(self, uri: str) -> bytes:
        digest = artifact_digest(uri)
        path = self.root / digest
        # Refuse anything that escapes the store root even if the digest check
        # somehow changed — the path must resolve to a direct child of root.
        resolved = path.resolve(strict=False)
        if resolved.parent != self.root.resolve():
            raise ValueError("artifact path escaped store root")
        return resolved.read_bytes()

    def exists(self, uri: str) -> bool:
        try:
            digest = artifact_digest(uri)
        except ValueError:
            return False
        path = self.root / digest
        resolved = path.resolve(strict=False)
        if resolved.parent != self.root.resolve():
            return False
        return resolved.is_file()


class FakeSandboxAdapter(SandboxAdapter):
    def execute(self, command: dict) -> dict:
        return {"status": "ok", "echo": command}
