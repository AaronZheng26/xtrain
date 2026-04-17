import sys
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import NullPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import session as session_module


class SessionMigrationTests(unittest.TestCase):
    def test_feature_pipeline_columns_are_added_for_legacy_sqlite_schema(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "legacy.db"
            temp_engine = create_engine(
                f"sqlite:///{db_path}",
                connect_args={"check_same_thread": False},
                poolclass=NullPool,
            )

            with temp_engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        CREATE TABLE feature_pipelines (
                            id INTEGER PRIMARY KEY,
                            project_id INTEGER NOT NULL,
                            dataset_version_id INTEGER NOT NULL,
                            preprocess_pipeline_id INTEGER,
                            name VARCHAR(120) NOT NULL,
                            status VARCHAR(32) NOT NULL,
                            steps JSON NOT NULL DEFAULT '[]',
                            output_path TEXT,
                            output_row_count INTEGER NOT NULL DEFAULT 0,
                            output_schema JSON NOT NULL DEFAULT '[]',
                            created_at DATETIME NOT NULL,
                            updated_at DATETIME NOT NULL
                        )
                        """
                    )
                )

            original_engine = session_module.engine
            try:
                session_module.engine = temp_engine
                session_module._ensure_feature_pipeline_columns()
                existing_columns = {column["name"] for column in inspect(temp_engine).get_columns("feature_pipelines")}
            finally:
                session_module.engine = original_engine
                temp_engine.dispose()

        self.assertIn("training_candidate_columns", existing_columns)
        self.assertIn("analysis_retained_columns", existing_columns)


if __name__ == "__main__":
    unittest.main()
