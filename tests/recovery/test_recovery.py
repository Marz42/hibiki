"""Recovery / migration checks."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from hibiki.application.bootstrap import SCHEMA_VERSION, bootstrap_core, run_migrations
from hibiki.domain.errors import SchemaStartupError
from hibiki.persistence.models import ensure_schema_version


def test_migrate_from_empty_dir(tmp_path: Path):
    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    run_migrations(url)
    engine = create_engine(url)
    ensure_schema_version(engine, SCHEMA_VERSION)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT value FROM schema_meta WHERE key='schema_version'")).one()
        assert row[0] == SCHEMA_VERSION


def test_refuse_start_without_schema(tmp_path: Path):
    db = tmp_path / "empty.db"
    engine = create_engine(f"sqlite:///{db.as_posix()}")
    # create empty file / connection without schema_meta
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE dummy (id INTEGER)"))
        conn.commit()
    with pytest.raises(RuntimeError, match="schema version"):
        ensure_schema_version(engine, SCHEMA_VERSION)


def test_startup_refuses_unknown_schema_on_existing_db(tmp_path: Path):
    """The real startup path must refuse before migrations, with a diagnostic."""
    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    run_migrations(url)
    run_migrations(url)  # second start on a healthy database stays a no-op
    con = sqlite3.connect(db)
    con.execute("UPDATE schema_meta SET value='m99_future' WHERE key='schema_version'")
    con.commit()
    con.close()

    with pytest.raises(SchemaStartupError, match="unknown"):
        run_migrations(url)


def test_startup_refuses_partially_migrated_db(tmp_path: Path):
    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    run_migrations(url)
    run_migrations(url)
    con = sqlite3.connect(db)
    con.execute("UPDATE schema_meta SET value='m99_future' WHERE key='schema_version'")
    con.commit()
    con.close()

    with pytest.raises(SchemaStartupError, match="schema version unknown"):
        bootstrap_core(tmp_path)


def test_startup_refuses_unknown_alembic_revision(tmp_path: Path):
    db = tmp_path / "hibiki.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
    con.execute("INSERT INTO alembic_version VALUES ('9999_from_the_future')")
    con.commit()
    con.close()

    with pytest.raises(SchemaStartupError, match="revision"):
        run_migrations(f"sqlite:///{db.as_posix()}")


def test_startup_upgrades_known_older_revision(tmp_path: Path):
    from alembic.config import Config

    from alembic import command
    from hibiki.application.bootstrap import project_root

    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    cfg = Config(str(project_root() / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "0002_artifacts")

    run_migrations(url)
    engine = create_engine(url)
    ensure_schema_version(engine, SCHEMA_VERSION)


def test_startup_upgrades_m0_generation_to_m1(tmp_path: Path):
    """A database left at the frozen M0 generation is migrated, not refused."""
    from alembic.config import Config

    from alembic import command
    from hibiki.application.bootstrap import project_root

    db = tmp_path / "hibiki.db"
    url = f"sqlite:///{db.as_posix()}"
    cfg = Config(str(project_root() / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "0004_evidence_sequence")
    con = sqlite3.connect(db)
    assert con.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()[0] == "m0"
    con.close()

    run_migrations(url)
    engine = create_engine(url)
    ensure_schema_version(engine, SCHEMA_VERSION)
    con = sqlite3.connect(db)
    assert con.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()[0] == "m1"
    con.close()


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
