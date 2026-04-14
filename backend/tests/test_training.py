import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.schemas.training import TrainingRequest
from app.services.training import _select_training_feature_columns


class TrainingFeatureSelectionTests(unittest.TestCase):
    def test_default_selection_excludes_target_alias_and_raw_text(self):
        frame = pd.DataFrame(
            {
                "is_anomaly": ["normal", "anomaly", "normal"],
                "label": ["normal", "anomaly", "normal"],
                "raw_message": ["timeout on db", "login failed", "ok"],
                "event_hour": [9, 12, 18],
                "status_code": [200, 500, 200],
            }
        )
        payload = TrainingRequest(
            project_id=1,
            dataset_version_id=1,
            name="selection-test",
            mode="supervised",
            algorithm="random_forest",
            target_column="is_anomaly",
            feature_columns=[],
        )

        result = _select_training_feature_columns(
            frame,
            payload,
            dataset_label_column="is_anomaly",
            target_column="is_anomaly",
            preprocess_pipeline=None,
            feature_pipeline=None,
        )

        self.assertEqual(result["used_feature_columns"], ["event_hour", "status_code"])
        self.assertEqual(result["exclusion_reasons"]["label"], "label_like_column")
        self.assertEqual(result["exclusion_reasons"]["raw_message"], "raw_text_column")

    def test_explicit_high_cardinality_text_is_rejected(self):
        frame = pd.DataFrame(
            {
                "status_code": [200, 500, 404, 200, 500],
                "session_token": [f"token-{index}" for index in range(5)],
                "category": ["a", "b", "a", "b", "a"],
            }
        )
        payload = TrainingRequest(
            project_id=1,
            dataset_version_id=1,
            name="selection-test-2",
            mode="unsupervised",
            algorithm="isolation_forest",
            feature_columns=["status_code", "session_token", "category"],
            training_params={"max_categorical_cardinality": 3},
        )

        result = _select_training_feature_columns(
            frame,
            payload,
            dataset_label_column=None,
            target_column=None,
            preprocess_pipeline=None,
            feature_pipeline=None,
        )

        self.assertEqual(result["used_feature_columns"], ["status_code", "category"])
        self.assertTrue(result["exclusion_reasons"]["session_token"].startswith("identifier_column"))

    def test_numeric_complexity_features_are_not_excluded_by_source_field_name(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["timeout on db", "login failed", "ok"],
                "raw_message_entropy": [3.1, 3.4, 1.2],
                "raw_message_length": [13, 12, 2],
                "status_code": [500, 401, 200],
            }
        )
        payload = TrainingRequest(
            project_id=1,
            dataset_version_id=1,
            name="selection-test-3",
            mode="unsupervised",
            algorithm="isolation_forest",
            feature_columns=[],
        )

        result = _select_training_feature_columns(
            frame,
            payload,
            dataset_label_column=None,
            target_column=None,
            preprocess_pipeline=None,
            feature_pipeline=None,
        )

        self.assertIn("raw_message_entropy", result["used_feature_columns"])
        self.assertIn("raw_message_length", result["used_feature_columns"])
        self.assertEqual(result["exclusion_reasons"]["raw_message"], "raw_text_column")


if __name__ == "__main__":
    unittest.main()
