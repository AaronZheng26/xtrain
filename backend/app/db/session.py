from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine
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


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
