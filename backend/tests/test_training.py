import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.schemas.training import TrainingRequest
from app.services.training import _build_signal_summaries, _select_training_feature_columns


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

    def test_signal_summaries_highlight_spike_and_count_features(self):
        frame = pd.DataFrame(
            {
                "predicted_label": ["anomaly", "anomaly", "normal", "normal"],
                "source_15m_spike": [1, 1, 0, 0],
                "source_15m_count": [6, 4, 1, 2],
                "pair_count": [3, 2, 1, 1],
                "anomaly_score": [0.9, 0.8, 0.2, 0.1],
            }
        )

        spike_summaries = _build_signal_summaries(frame, signal_type="spike_flag")
        count_summaries = _build_signal_summaries(frame, signal_type="count_metric")

        self.assertEqual(spike_summaries[0]["column"], "source_15m_spike")
        self.assertEqual(spike_summaries[0]["anomaly_active_count"], 2)
        self.assertEqual(spike_summaries[0]["normal_active_count"], 0)
        self.assertEqual(count_summaries[0]["column"], "source_15m_count")
        self.assertGreater(count_summaries[0]["anomaly_mean"], count_summaries[0]["normal_mean"])


if __name__ == "__main__":
    unittest.main()
