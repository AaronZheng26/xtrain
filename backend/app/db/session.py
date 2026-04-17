from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from app.core.config import get_settings
from app.db.base import Base


settings = get_settings()
settings.storage_root_path.mkdir(parents=True, exist_ok=True)
settings.sqlite_path_resolved.parent.mkdir(parents=True, exist_ok=True)

engine_kwargs = {
    "connect_args": {"check_same_thread": False},
}

# For local SQLite on Windows, avoid QueuePool exhaustion under concurrent requests
# by opening connections on demand and closing them immediately after use.
if settings.sqlite_url.startswith("sqlite:///"):
    engine_kwargs["poolclass"] = NullPool

engine = create_engine(settings.sqlite_url, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def initialize_database() -> None:
    import app.models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _ensure_feature_pipeline_columns()


def _ensure_feature_pipeline_columns() -> None:
    inspector = inspect(engine)
    if "feature_pipelines" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("feature_pipelines")}
    alter_statements: list[str] = []
    if "training_candidate_columns" not in existing_columns:
        alter_statements.append("ALTER TABLE feature_pipelines ADD COLUMN training_candidate_columns JSON NOT NULL DEFAULT '[]'")
    if "analysis_retained_columns" not in existing_columns:
        alter_statements.append("ALTER TABLE feature_pipelines ADD COLUMN analysis_retained_columns JSON NOT NULL DEFAULT '[]'")

    if not alter_statements:
        return

    with engine.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))


# Some call paths (for example direct TestClient usage without lifespan startup)
# hit the ORM before initialize_database() runs. Patch legacy SQLite schemas early
# so feature pipeline queries do not fail on missing metadata columns.
_ensure_feature_pipeline_columns()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
