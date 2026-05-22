import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import readiness as readiness_module
from app.services.readiness import analyze_dataset_readiness


class ReadinessDataset:
    id = 1
    project_id = 1
    source_id = 1
    version_name = "test"
    parquet_path = "memory"
    created_at = None

    def __init__(self, *, parser_profile: str, row_count: int, schema_columns: list[str], detected_fields: dict | None = None, label_column: str | None = None):
        self.parser_profile = parser_profile
        self.row_count = row_count
        self.schema_snapshot = [{"name": column, "dtype": "string", "null_count": 0, "non_null_count": row_count} for column in schema_columns]
        self.detected_fields = detected_fields or {}
        self.label_column = label_column


class ReadinessMapping:
    def __init__(self, mappings: dict[str, str | None] | None = None):
        self.mappings = mappings or {}


class DatasetReadinessTests(unittest.TestCase):
    def _analyze(self, dataset: ReadinessDataset, frame: pd.DataFrame | None = None):
        with (
            patch.object(readiness_module, "get_dataset", return_value=dataset),
            patch.object(readiness_module, "get_or_create_field_mapping", return_value=ReadinessMapping()),
            patch.object(readiness_module, "load_parquet_frame", return_value=frame if frame is not None else pd.DataFrame()),
        ):
            return analyze_dataset_readiness(db=None, dataset_id=dataset.id)

    def test_nginx_like_dataset_recommends_web_analysis(self):
        dataset = ReadinessDataset(
            parser_profile="nginx_access",
            row_count=2000,
            schema_columns=["event_time", "client_ip", "path", "method", "status_code", "bytes_sent"],
            detected_fields={"timestamp_candidates": ["event_time"]},
        )
        frame = pd.DataFrame({
            "event_time": ["2026-01-01T00:00:00", "2026-01-01T00:05:00"],
            "client_ip": ["10.0.0.1", "10.0.0.2"],
            "path": ["/login", "/admin"],
            "status_code": [200, 500],
        })

        result = self._analyze(dataset, frame)

        self.assertGreaterEqual(result["score"], 80)
        self.assertEqual(result["level"], "excellent")
        self.assertEqual(result["recommended_goals"], ["Web 访问异常"])
        self.assertEqual(result["recommended_templates"], ["nginx_access"])
        self.assertIn("client_ip", result["detected_fields"]["source_ip"])
        self.assertIn("path", result["detected_fields"]["url_path"])

    def test_program_runtime_dataset_recommends_runtime_analysis(self):
        dataset = ReadinessDataset(
            parser_profile="generic_log",
            row_count=800,
            schema_columns=["timestamp", "host", "process_name", "severity", "raw_message"],
            detected_fields={"timestamp_candidates": ["timestamp"]},
        )

        result = self._analyze(dataset)

        self.assertIn("程序运行异常", result["recommended_goals"])
        self.assertIn("program_runtime", result["recommended_templates"])
        self.assertIn("host", result["detected_fields"]["host"])
        self.assertIn("process_name", result["detected_fields"]["process"])

    def test_missing_key_fields_produces_limited_recommendation(self):
        dataset = ReadinessDataset(
            parser_profile="generic_log",
            row_count=80,
            schema_columns=["message"],
            detected_fields={},
        )

        result = self._analyze(dataset)

        self.assertLess(result["score"], 60)
        self.assertIn(result["level"], {"limited", "poor"})
        self.assertTrue(result["missing_fields"])
        self.assertIn("只做数据清洗与字段探测", result["recommended_mode"])

    def test_low_quality_sample_warns_about_missing_values(self):
        dataset = ReadinessDataset(
            parser_profile="generic_log",
            row_count=1000,
            schema_columns=["event_time", "user", "action", "host"],
            detected_fields={"timestamp_candidates": ["event_time"]},
        )
        frame = pd.DataFrame({
            "event_time": ["2026-01-01", None, None],
            "user": [None, None, None],
            "action": ["login", None, None],
            "host": ["host-a", None, None],
        })

        result = self._analyze(dataset, frame)

        self.assertTrue(any("缺失" in warning for warning in result["warnings"]))


if __name__ == "__main__":
    unittest.main()
