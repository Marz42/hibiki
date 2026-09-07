from __future__ import annotations

from hibiki.persistence.models import Base, create_sqlite_engine, make_session_factory


def test_engine_pragmas(tmp_path):
    db = tmp_path / "t.db"
    engine = create_sqlite_engine(f"sqlite:///{db.as_posix()}")
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        fk = conn.exec_driver_sql("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1
    sf = make_session_factory(engine)
    with sf() as s:
        assert s.bind is not None
