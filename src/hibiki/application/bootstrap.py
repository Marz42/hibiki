from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from alembic import command
from hibiki.application.service import ApplicationService
from hibiki.domain.errors import SchemaStartupError
from hibiki.domain.ports import Clock
from hibiki.persistence.models import (
    create_sqlite_engine,
    ensure_schema_version,
    make_session_factory,
)
from hibiki.persistence.session import InstanceLock, SerialSessionExecutor
from hibiki.runtime.artifacts import LocalArtifactStore
from hibiki.runtime.clock import FakeClock, SystemClock
from hibiki.runtime.fake_agent import FakeAgentAdapter
from hibiki.runtime.fake_external import FakeExternalAdapter


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _alembic_revision(engine: Engine) -> str | None:
    """Current Alembic revision stored in the database, if any."""
    if "alembic_version" not in inspect(engine).get_table_names():
        return None
    with engine.connect() as conn:
        return conn.execute(text("SELECT version_num FROM alembic_version")).scalar()


def inspect_existing_schema(engine: Engine, expected: str = "m0") -> None:
    """Refuse to start on a database whose schema state is not the expected one.

    Runs *before* migrations so an unknown or newer schema produces a diagnosable
    refusal instead of a raw DDL error. A fresh database (or one with unrelated
    application tables) has no recorded schema state and is migrated normally.
    """
    tables = set(inspect(engine).get_table_names())
    if "schema_meta" in tables:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM schema_meta WHERE key='schema_version'")
            ).fetchone()
        if row is None or row[0] != expected:
            raise SchemaStartupError(
                "schema version unknown; refuse to start: "
                f"schema_meta.schema_version={None if row is None else row[0]!r}, "
                f"expected {expected!r}. Restore a backup or run "
                "'alembic upgrade head' against the expected schema."
            )

    current = _alembic_revision(engine)
    if current is not None:
        cfg = Config(str(project_root() / "alembic.ini"))
        script = ScriptDirectory.from_config(cfg)
        heads = set(script.get_heads())
        if current not in heads:
            known = {rev.revision for rev in script.walk_revisions()}
            if current not in known:
                raise SchemaStartupError(
                    f"schema version unknown; refuse to start: database reports Alembic "
                    f"revision {current!r}, which this code does not know "
                    f"(code heads: {sorted(heads)}). Restore a backup or run the "
                    "matching code version."
                )


def run_migrations(db_url: str) -> None:
    engine = create_engine(db_url)
    try:
        inspect_existing_schema(engine)
    finally:
        engine.dispose()
    cfg = Config(str(project_root() / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    # Ensure src is importable for alembic env
    command.upgrade(cfg, "head")


def bootstrap_core(
    data_dir: Path,
    *,
    clock: Clock | None = None,
    agent: FakeAgentAdapter | None = None,
    external: FakeExternalAdapter | None = None,
    fake_time: bool = True,
    run_migrate: bool = True,
    acquire_lock: bool = True,
    dispatch_enabled: bool = True,
) -> tuple[ApplicationService, dict]:
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "hibiki.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    if run_migrate:
        run_migrations(db_url)
    engine = create_sqlite_engine(db_url)
    ensure_schema_version(engine, "m0")
    lock = InstanceLock(data_dir)
    if acquire_lock:
        lock.acquire()
    sf = make_session_factory(engine)
    executor = SerialSessionExecutor(sf)
    clk: Clock = clock or (FakeClock() if fake_time else SystemClock())
    agent_adapter = agent or FakeAgentAdapter()
    external_adapter = external or FakeExternalAdapter()
    artifacts = LocalArtifactStore(data_dir / "artifacts")
    svc = ApplicationService(
        executor,
        clk,
        agent_adapter,
        external_adapter,
        dispatch_enabled=dispatch_enabled,
    )
    ctx = {
        "engine": engine,
        "lock": lock,
        "clock": clk,
        "agent": agent_adapter,
        "external": external_adapter,
        "artifacts": artifacts,
        "db_url": db_url,
        "data_dir": data_dir,
    }
    return svc, ctx
