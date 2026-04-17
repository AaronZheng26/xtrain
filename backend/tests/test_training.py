import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.schemas.training import TrainingRequest
from app.services.training import (
    _build_prediction_output_frame,
    _build_signal_summaries,
    _calculate_stratified_sample_targets,
    _sample_unsupervised_frame,
    _select_training_feature_columns,
)
from app.models.feature_pipeline import FeaturePipeline


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
            dataset_schema_columns=list(frame.columns),
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
            dataset_schema_columns=list(frame.columns),
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
            dataset_schema_columns=list(frame.columns),
            preprocess_pipeline=None,
            feature_pipeline=None,
        )

        self.assertIn("raw_message_entropy", result["used_feature_columns"])
        self.assertIn("raw_message_length", result["used_feature_columns"])
        self.assertEqual(result["exclusion_reasons"]["raw_message"], "raw_text_column")

    def test_feature_pipeline_defaults_only_use_training_candidates(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["timeout on db", "login failed", "ok"],
                "session_id": ["session-a", "session-b", "session-c"],
                "raw_message_entropy": [3.1, 3.4, 1.2],
                "source_ip_15m_count": [4, 8, 1],
            }
        )
        feature_pipeline = FeaturePipeline(
            project_id=1,
            dataset_version_id=1,
            name="feature-defaults",
            steps=[],
            output_schema=[{"name": column} for column in frame.columns],
            training_candidate_columns=["raw_message_entropy", "source_ip_15m_count"],
            business_context_columns=["raw_message", "session_id"],
            analysis_retained_columns=["raw_message", "session_id"],
            feature_lineage={
                "source_ip_15m_count": {
                    "source_columns": ["session_id"],
                    "step_type": "time_window_count",
                    "task_category": "behavior_tracking",
                    "recipe_id": "behavior_tracking_window",
                    "description": "time_window_count 基于 session_id 生成",
                    "business_meaning": "会话时间窗事件数",
                    "used_for_training": True,
                }
            },
        )
        payload = TrainingRequest(
            project_id=1,
            dataset_version_id=1,
            name="selection-test-4",
            mode="unsupervised",
            algorithm="isolation_forest",
            feature_columns=[],
        )

        result = _select_training_feature_columns(
            frame,
            payload,
            dataset_label_column=None,
            target_column=None,
            dataset_schema_columns=["raw_message", "session_id"],
            preprocess_pipeline=None,
            feature_pipeline=feature_pipeline,
        )

        self.assertEqual(result["selection_source"], "feature_pipeline_candidates")
        self.assertEqual(result["used_feature_columns"], ["raw_message_entropy", "source_ip_15m_count"])
        self.assertEqual(result["business_context_columns"], ["raw_message", "session_id"])
        self.assertIn("source_ip_15m_count", result["feature_lineage_snapshot"])
        self.assertEqual(result["exclusion_reasons"]["raw_message"], "not_in_training_candidates")
        self.assertEqual(result["exclusion_reasons"]["session_id"], "not_in_training_candidates")

    def test_feature_pipeline_fallback_prefers_generated_numeric_features(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["timeout on db", "login failed", "ok"],
                "session_id": ["session-a", "session-b", "session-c"],
                "raw_message_entropy": [3.1, 3.4, 1.2],
                "source_ip_15m_spike": [0, 1, 0],
            }
        )
        feature_pipeline = FeaturePipeline(
            project_id=1,
            dataset_version_id=1,
            name="feature-fallback",
            steps=[],
            output_schema=[{"name": column} for column in frame.columns],
            training_candidate_columns=[],
            analysis_retained_columns=[],
        )
        payload = TrainingRequest(
            project_id=1,
            dataset_version_id=1,
            name="selection-test-5",
            mode="unsupervised",
            algorithm="isolation_forest",
            feature_columns=[],
        )

        result = _select_training_feature_columns(
            frame,
            payload,
            dataset_label_column=None,
            target_column=None,
            dataset_schema_columns=["raw_message", "session_id"],
            preprocess_pipeline=None,
            feature_pipeline=feature_pipeline,
        )

        self.assertEqual(result["used_feature_columns"], ["raw_message_entropy", "source_ip_15m_spike"])
        self.assertEqual(result["exclusion_reasons"]["raw_message"], "not_in_training_candidates")
        self.assertEqual(result["exclusion_reasons"]["session_id"], "not_in_training_candidates")
        self.assertEqual(result["business_context_columns"], ["raw_message", "session_id"])

    def test_prediction_output_frame_keeps_business_context_columns(self):
        frame = pd.DataFrame(
            {
                "event_time": ["2026-04-01 00:00:00", "2026-04-01 00:01:00"],
                "session_id": ["session-a", "session-b"],
                "raw_message": ["timeout on db", "login failed"],
                "raw_message_entropy": [3.1, 3.4],
                "source_ip_15m_count": [4, 8],
            }
        )

        result_frame = _build_prediction_output_frame(
            base_frame=frame,
            row_index=frame.index,
            feature_columns=["raw_message_entropy", "source_ip_15m_count"],
            business_context_columns=["event_time", "session_id", "raw_message"],
        )

        self.assertEqual(
            list(result_frame.columns),
            [
                "event_time",
                "session_id",
                "raw_message",
                "raw_message_entropy",
                "source_ip_15m_count",
                "source_row_id",
                "sample_index",
            ],
        )

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


class TrainingSamplingTests(unittest.TestCase):
    def test_sampling_keeps_both_classes_when_anomalies_exceed_limit(self):
        frame = pd.DataFrame(
            {
                "predicted_label": ["anomaly"] * 190 + ["normal"] * 10,
                "anomaly_score": [float(1000 - index) for index in range(200)],
                "sample_index": list(range(200)),
            }
        )

        sampled = _sample_unsupervised_frame(frame, point_limit=100)

        label_counts = sampled["predicted_label"].value_counts().to_dict()
        self.assertEqual(len(sampled.index), 100)
        self.assertGreater(label_counts.get("anomaly", 0), 0)
        self.assertGreater(label_counts.get("normal", 0), 0)

    def test_sampling_preserves_minimum_presence_for_extreme_ratios(self):
        frame = pd.DataFrame(
            {
                "predicted_label": ["anomaly"] * 999 + ["normal"],
                "anomaly_score": [float(2000 - index) for index in range(1000)],
                "sample_index": list(range(1000)),
            }
        )

        sampled = _sample_unsupervised_frame(frame, point_limit=100)

        label_counts = sampled["predicted_label"].value_counts().to_dict()
        self.assertEqual(label_counts.get("normal", 0), 1)
        self.assertEqual(label_counts.get("anomaly", 0), 99)

    def test_target_calculation_stays_close_to_source_ratio(self):
        anomaly_target, normal_target = _calculate_stratified_sample_targets(
            total_limit=100,
            anomaly_count=30,
            normal_count=70,
        )

        self.assertEqual((anomaly_target, normal_target), (30, 70))

    def test_target_calculation_preserves_minimum_presence_for_small_class(self):
        anomaly_target, normal_target = _calculate_stratified_sample_targets(
            total_limit=100,
            anomaly_count=5,
            normal_count=500,
        )

        self.assertEqual(anomaly_target, 1)
        self.assertEqual(normal_target, 99)
        self.assertEqual(anomaly_target + normal_target, 100)


if __name__ == "__main__":
    unittest.main()
