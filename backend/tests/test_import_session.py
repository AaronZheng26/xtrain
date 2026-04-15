import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.dataset_import import _write_parquet
from app.services.import_session import (
    _build_effective_schema_snapshot,
    _build_staging_select_sql,
    _load_preview_rows_from_staging,
)


class ImportSessionOptimizationTests(unittest.TestCase):
    def test_effective_schema_projection_updates_detected_fields_and_renames(self):
        base_schema = [
            {"name": "event_time", "dtype": "object", "null_count": 0, "non_null_count": 2, "sample_values": [], "candidate_roles": ["timestamp"]},
            {"name": "source_ip", "dtype": "object", "null_count": 0, "non_null_count": 2, "sample_values": [], "candidate_roles": ["categorical"]},
            {"name": "raw_message", "dtype": "object", "null_count": 0, "non_null_count": 2, "sample_values": [], "candidate_roles": ["text"]},
        ]
        detected = {
            "timestamp_candidates": ["event_time"],
            "label_candidates": [],
            "numeric_fields": [],
            "categorical_fields": ["source_ip"],
            "text_fields": ["raw_message"],
        }

        schema, next_detected = _build_effective_schema_snapshot(
            base_schema,
            detected,
            {
                "exclude_columns": ["source_ip"],
                "rename_columns": {"raw_message": "message_text"},
            },
        )

        self.assertEqual([field["name"] for field in schema], ["event_time", "message_text"])
        self.assertEqual(next_detected["timestamp_candidates"], ["event_time"])
        self.assertEqual(next_detected["categorical_fields"], [])
        self.assertEqual(next_detected["text_fields"], ["message_text"])

    def test_staging_preview_only_projects_selected_columns(self):
        frame = pd.DataFrame(
            {
                "event_time": ["2026-04-15 10:00:00", "2026-04-15 10:00:01"],
                "source_ip": ["10.0.0.1", "10.0.0.2"],
                "raw_message": ["timeout", "ok"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_path = Path(temp_dir) / "staging.parquet"
            _write_parquet(frame, staging_path)

            preview_rows = _load_preview_rows_from_staging(
                staging_path,
                {
                    "exclude_columns": ["source_ip"],
                    "rename_columns": {"raw_message": "message_text"},
                },
                limit=5,
            )

            self.assertEqual(list(preview_rows[0].keys()), ["event_time", "message_text"])
            self.assertNotIn("source_ip", preview_rows[0])
            self.assertEqual(preview_rows[0]["message_text"], "timeout")

    def test_staging_select_sql_can_materialize_large_projection_without_pandas_reload(self):
        frame = pd.DataFrame(
            {
                "event_time": ["2026-04-15 10:00:00", "2026-04-15 10:00:01"],
                "source_ip": ["10.0.0.1", "10.0.0.2"],
                "dest_ip": ["1.1.1.1", "1.1.1.2"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            staging_path = Path(temp_dir) / "staging.parquet"
            _write_parquet(frame, staging_path)
            query = _build_staging_select_sql(
                staging_path,
                {
                    "include_columns": ["event_time", "dest_ip"],
                    "rename_columns": {"dest_ip": "destination_ip"},
                },
            )

            connection = duckdb.connect()
            try:
                materialized = connection.execute(query).fetch_df()
            finally:
                connection.close()

            self.assertEqual(list(materialized.columns), ["event_time", "destination_ip"])
            self.assertEqual(materialized.loc[1, "destination_ip"], "1.1.1.2")


if __name__ == "__main__":
    unittest.main()
