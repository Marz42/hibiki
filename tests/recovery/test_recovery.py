"""Recovery / migration checks."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from hibiki.application.bootstrap import bootstrap_core, run_migrations
from hibiki.persistence.models import ensure_schema_version


def test_migrate_from_empty_dir(tmp_path: Path):
    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    run_migrations(url)
    engine = create_engine(url)
    ensure_schema_version(engine, "m0")
    with engine.connect() as conn:
        row = conn.execute(text("SELECT value FROM schema_meta WHERE key='schema_version'")).one()
        assert row[0] == "m0"


def test_refuse_start_without_schema(tmp_path: Path):
    db = tmp_path / "empty.db"
    engine = create_engine(f"sqlite:///{db.as_posix()}")
    # create empty file / connection without schema_meta
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE dummy (id INTEGER)"))
        conn.commit()
    with pytest.raises(RuntimeError, match="schema version"):
        ensure_schema_version(engine, "m0")


def test_reconcile_after_restart(tmp_path: Path):
    svc, ctx = bootstrap_core(tmp_path / "d1")
    from tests.helpers import approve_flow, human_auth

    auth = human_auth()
    task_id, _ = approve_flow(svc, auth)
    svc.execute("pause_task", auth, {"task_id": task_id})
    svc.execute("runtime_quiescent", auth, {"task_id": task_id})
    ctx["lock"].release()

    svc2, ctx2 = bootstrap_core(tmp_path / "d1", run_migrate=True)
    notes = svc2.reconcile()
    assert any("PAUSED" in n for n in notes["notes"])
    assert svc2.get_task(task_id)["state"] == "PAUSED"
    ctx2["lock"].release()
