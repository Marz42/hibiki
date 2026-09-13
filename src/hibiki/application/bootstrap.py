from __future__ import annotations

from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from alembic import command
from hibiki.application.service import ApplicationService
from hibiki.domain.errors import SchemaStartupError
from hibiki.domain.ports import AgentAdapter, Clock, SandboxAdapter
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


#: Schema generation this code writes and requires. M1 adds execution-boundary tables
#: (run_inputs / tool_invocations / context_appends) on top of the frozen M0 schema.
SCHEMA_VERSION = "m1"

#: Schema generations this code can migrate from. A database recording one of these is
#: upgraded; anything else is an unknown schema and startup is refused.
KNOWN_SCHEMA_VERSIONS: frozenset[str] = frozenset({"m0", SCHEMA_VERSION})


def _alembic_revision(engine: Engine) -> str | None:
    """Current Alembic revision stored in the database, if any."""
    if "alembic_version" not in inspect(engine).get_table_names():
        return None
    with engine.connect() as conn:
        return conn.execute(text("SELECT version_num FROM alembic_version")).scalar()


def inspect_existing_schema(engine: Engine, expected: str = SCHEMA_VERSION) -> None:
    """Refuse to start on a database whose schema state this code cannot migrate.

    Runs *before* migrations so an unknown or newer schema produces a diagnosable
    refusal instead of a raw DDL error. A fresh database, a database with unrelated
    application tables, or a database at a *known older* generation has a recognizable
    state and is migrated normally; an unrecognised ``schema_version`` or an Alembic
    revision this code does not know is refused.
    """
    tables = set(inspect(engine).get_table_names())
    if "schema_meta" in tables:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM schema_meta WHERE key='schema_version'")
            ).fetchone()
        recorded = None if row is None else row[0]
        if recorded not in KNOWN_SCHEMA_VERSIONS:
            raise SchemaStartupError(
                "schema version unknown; refuse to start: "
                f"schema_meta.schema_version={recorded!r}, expected one of "
                f"{sorted(KNOWN_SCHEMA_VERSIONS)}. Restore a backup or run "
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
    agent: AgentAdapter | None = None,
    external: FakeExternalAdapter | None = None,
    fake_time: bool = True,
    run_migrate: bool = True,
    acquire_lock: bool = True,
    dispatch_enabled: bool = True,
    tool_broker: Any | None = None,
    sandbox: SandboxAdapter | None = None,
) -> tuple[ApplicationService, dict]:
    """Build the Core with its collaborators.

    ``agent`` accepts any :class:`AgentAdapter`, so a caller can run the real
    ``ApiAgentAdapter`` in place of the Fake without touching the service. When a
    ``tool_broker`` is supplied it is exposed to the adapter through ``ctx`` only —
    the Core never executes tools itself.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "hibiki.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    if run_migrate:
        run_migrations(db_url)
    engine = create_sqlite_engine(db_url)
    ensure_schema_version(engine, SCHEMA_VERSION)
    lock = InstanceLock(data_dir)
    if acquire_lock:
        lock.acquire()
    sf = make_session_factory(engine)
    executor = SerialSessionExecutor(sf)
    clk: Clock = clock or (FakeClock() if fake_time else SystemClock())
    agent_adapter = agent or FakeAgentAdapter()
    external_adapter = external or FakeExternalAdapter()
    workspace_root = data_dir / "workspaces"
    workspace_root.mkdir(parents=True, exist_ok=True)
    artifacts = LocalArtifactStore(data_dir / "artifacts")
    svc = ApplicationService(
        executor,
        clk,
        agent_adapter,
        external_adapter,
        dispatch_enabled=dispatch_enabled,
        workspace_root=str(workspace_root),
        artifacts=artifacts,
    )
    ctx = {
        "engine": engine,
        "lock": lock,
        "clock": clk,
        "agent": agent_adapter,
        "external": external_adapter,
        "artifacts": artifacts,
        "tool_broker": tool_broker,
        "sandbox": sandbox,
        "db_url": db_url,
        "data_dir": data_dir,
    }
    return svc, ctx
