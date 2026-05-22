from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import Engine, inspect, text


MigrationFn = Callable[[Engine], None]


MIGRATIONS: list[tuple[str, str, MigrationFn]] = [
    (
        "20260417_feature_pipeline_training_metadata",
        "Add feature pipeline training, context, retained, and lineage metadata columns",
        lambda engine: _add_missing_json_columns(
            engine,
            "feature_pipelines",
            {
                "training_candidate_columns": "[]",
                "business_context_columns": "[]",
                "analysis_retained_columns": "[]",
                "feature_lineage": "{}",
            },
        ),
    ),
]


def run_sqlite_migrations(engine: Engine) -> None:
    if not str(engine.url).startswith("sqlite"):
        return

    _ensure_schema_migrations_table(engine)
    applied = _read_applied_migrations(engine)
    for migration_id, description, migration in MIGRATIONS:
        if migration_id in applied:
            continue
        migration(engine)
        _record_migration(engine, migration_id, description)


def _ensure_schema_migrations_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id TEXT PRIMARY KEY,
                    description TEXT NOT NULL,
                    applied_at DATETIME NOT NULL
                )
                """
            )
        )


def _read_applied_migrations(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT id FROM schema_migrations")).fetchall()
    return {str(row[0]) for row in rows}


def _record_migration(engine: Engine, migration_id: str, description: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO schema_migrations (id, description, applied_at)
                VALUES (:id, :description, :applied_at)
                """
            ),
            {
                "id": migration_id,
                "description": description,
                "applied_at": datetime.now(UTC).isoformat(),
            },
        )


def _add_missing_json_columns(engine: Engine, table_name: str, columns: dict[str, str]) -> None:
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
    alter_statements = [
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} JSON NOT NULL DEFAULT '{default_value}'"
        for column_name, default_value in columns.items()
        if column_name not in existing_columns
    ]
    if not alter_statements:
        return

    with engine.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))
