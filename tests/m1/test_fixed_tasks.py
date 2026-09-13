"""The fixed M1 acceptance tasks must stay well-formed and inside system limits.

SPEC §24.4 G3 fixes the task set before the runs; these checks keep the fixtures from
drifting (for example by granting a tool the system catalog does not know).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hibiki.domain.defaults import DEFAULTS
from hibiki.domain.execution import M1_TOOL_CATALOG, normalize_ceiling_tools
from hibiki.interfaces.m1_runner import _load_task

TASKS_DIR = Path("docs/m1/tasks")


def _task_files() -> list[Path]:
    return sorted(TASKS_DIR.glob("*.json"))


def test_exactly_three_fixed_tasks_exist():
    files = _task_files()
    assert [p.name for p in files] == [
        "k1_attachment_summary.json",
        "k2_file_conversion.json",
        "k3_repo_change.json",
    ]
    assert len({json.loads(p.read_text(encoding="utf-8"))["task_id"] for p in files}) == 3


@pytest.mark.parametrize("path", _task_files(), ids=lambda p: p.stem)
def test_task_spec_is_loadable_and_within_system_limits(path: Path):
    spec = _load_task(path)

    tools = spec["permission_ceiling"]["tools"]
    granted, unknown = normalize_ceiling_tools({"tools": tools})
    assert unknown == [], f"{path.name} asks for unknown tools {unknown}"
    assert set(granted) <= set(M1_TOOL_CATALOG)

    limits = spec["resource_limits"]
    assert limits["wall_timeout_seconds"] <= DEFAULTS.run_wall_timeout_seconds
    assert limits["max_turns"] <= DEFAULTS.max_run_model_turns
    assert limits["max_model_calls"] <= DEFAULTS.max_task_model_calls
    assert limits["context_max_materialized_bytes"] <= DEFAULTS.context_max_materialized_bytes
    assert spec["seed_files"], "a fixed task must seed deterministic inputs"
    assert spec["deliverables"] and spec["acceptance_criteria"]
    assert spec["expected_artifacts"], "each fixed task must publish an artifact"


def test_every_fixed_task_is_offline_and_local():
    """No fixed task may depend on the network or an external business action."""
    for path in _task_files():
        spec = _load_task(path)
        blob = json.dumps(spec).lower()
        for forbidden in ("http://", "https://", "smtp", "webhook"):
            assert forbidden not in blob, f"{path.name} references {forbidden}"
        assert "shell.run" not in spec["permission_ceiling"]["tools"] or spec["task_id"] == "k3"


def test_k3_seed_repo_actually_fails_before_the_fix():
    """The sample repo must be genuinely broken, otherwise the task proves nothing."""
    spec = _load_task(TASKS_DIR / "k3_repo_change.json")
    namespace: dict = {}
    exec(spec["seed_files"]["sample.py"], namespace)  # noqa: S102 — fixture under test
    add = namespace["add"]
    assert add(2, 3) != 5, "the seeded sample must fail its own test"
    assert add(-1, 1) != 0


def test_k1_and_k2_seeds_are_deterministic_and_small():
    k1 = _load_task(TASKS_DIR / "k1_attachment_summary.json")
    k2 = _load_task(TASKS_DIR / "k2_file_conversion.json")
    assert len(k1["seed_files"]["input.txt"].splitlines()) == 3
    rows = [r for r in k2["seed_files"]["input.csv"].strip().splitlines()[1:]]
    assert len(rows) == 3


def test_runner_dry_run_validates_the_path_without_a_provider(tmp_path):
    """`--dry-run` proves the harness end to end and never reports a pass."""
    import json as _json

    from hibiki.interfaces.m1_runner import main as runner_main

    out = tmp_path / "out"
    data = tmp_path / "data"
    code = runner_main(
        [
            "--dry-run",
            "--data-dir",
            str(data),
            "--out",
            str(out),
            "--tasks",
            str(TASKS_DIR),
            "--repeats",
            "1",
        ]
    )
    summary = _json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert summary["dry_run"] is True
    assert summary["runs"] == 3
    assert summary["passed"] == 0, "a provider-less run must never be counted as a pass"
    assert code == 1, "the gate is not met without a real provider"

    # Every run still produced the full record the live gate requires.
    for record in sorted(out.glob("*-run1.json")):
        payload = _json.loads(record.read_text(encoding="utf-8"))
        assert payload["spec_hash"]
        assert payload["context_manifest_id"]
        assert payload["granted_tools"]
        assert payload["result"]["outcome"] == "BLOCKED"
        assert payload["result"]["verdict"] == "FAIL"
        assert "empty_model_completion" in payload["result"]["error_class"]
        assert payload["seed_files"]
