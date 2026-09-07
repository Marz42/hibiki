"""Minimal ContextManifest helpers (M0 stub materialization)."""

from __future__ import annotations

from typing import Any

from hibiki.domain.hashing import content_hash


def build_manifest(
    *,
    context_manifest_id: str,
    task_id: str,
    run_id: str,
    contract_version: int,
    plan_version: int | None,
    mandatory_refs: list[str] | None = None,
    optional_refs: list[str] | None = None,
) -> dict[str, Any]:
    body = {
        "context_manifest_id": context_manifest_id,
        "task_id": task_id,
        "run_id": run_id,
        "contract_version": contract_version,
        "plan_version": plan_version,
        "context_policy": "FRESH",
        "mandatory_refs": mandatory_refs or [],
        "optional_refs": optional_refs or [],
        "excluded_categories": [],
    }
    body["manifest_hash"] = content_hash(body)
    return body
