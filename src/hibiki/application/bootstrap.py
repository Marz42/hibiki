from __future__ import annotations

from pathlib import Path

from alembic.config import Config

from alembic import command
from hibiki.application.service import ApplicationService
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


def run_migrations(db_url: str) -> None:
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
