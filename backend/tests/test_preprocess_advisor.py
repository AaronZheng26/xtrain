import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.preprocess_advisor import _build_field_advice, _build_recommended_steps


class PreprocessAdvisorTests(unittest.TestCase):
    def test_field_advice_marks_training_risks_and_conversion_candidates(self):
        frame = pd.DataFrame(
            {
                "label": ["normal", "anomaly", "normal", "anomaly"],
                "label_copy": ["normal", "anomaly", "normal", "anomaly"],
                "request_id": ["req-1", "req-2", "req-3", "req-4"],
                "raw_message": ["timeout on db", "login failed", "all good", "panic stack"],
                "metric_text": ["1.0", "2.5", "3.1", "4.9"],
                "event_time_text": [
                    "2026-04-15 09:10:00",
                    "2026-04-15 09:12:00",
                    "2026-04-15 09:14:00",
                    "2026-04-15 09:16:00",
                ],
                "sparse_numeric": [1.0, None, None, None],
                "constant_field": ["steady", "steady", "steady", "steady"],
            }
        )

        exclusion_reasons = {
            "label_copy": "duplicates_target_column",
            "request_id": "identifier_column",
            "raw_message": "raw_text_column",
            "constant_field": "constant_column",
        }

        advice = _build_field_advice(frame, "label", exclusion_reasons)
        advice_by_field = {item["field"]: item for item in advice}

        self.assertEqual(advice_by_field["label_copy"]["status"], "suspected_label_leak")
        self.assertEqual(advice_by_field["label_copy"]["recommended_action"], "drop_from_training")
        self.assertEqual(advice_by_field["request_id"]["status"], "suspected_id")
        self.assertEqual(advice_by_field["raw_message"]["recommended_action"], "move_to_feature_engineering")
        self.assertEqual(advice_by_field["metric_text"]["recommended_action"], "cast_numeric")
        self.assertEqual(advice_by_field["event_time_text"]["recommended_action"], "cast_datetime")
        self.assertEqual(advice_by_field["sparse_numeric"]["recommended_action"], "fill_null")
        self.assertEqual(advice_by_field["constant_field"]["recommended_action"], "exclude_column")

    def test_recommended_steps_group_fill_cast_and_select_actions(self):
        frame = pd.DataFrame(
            {
                "metric_text": ["1.0", "2.5", "3.1"],
                "event_time_text": ["2026-04-15 09:10:00", "2026-04-15 09:12:00", "2026-04-15 09:14:00"],
                "sparse_numeric": [1.0, None, None],
                "sparse_text": ["ok", None, None],
                "request_id": ["req-1", "req-2", "req-3"],
                "constant_field": ["steady", "steady", "steady"],
            }
        )
        field_advice = [
            {
                "field": "metric_text",
                "status": "suggest_convert",
                "reason_code": "cast_numeric",
                "reason_text": "",
                "recommended_action": "cast_numeric",
                "confidence": "medium",
            },
            {
                "field": "event_time_text",
                "status": "suggest_convert",
                "reason_code": "cast_datetime",
                "reason_text": "",
                "recommended_action": "cast_datetime",
                "confidence": "medium",
            },
            {
                "field": "sparse_numeric",
                "status": "high_missing",
                "reason_code": "high_missing",
                "reason_text": "",
                "recommended_action": "fill_null",
                "confidence": "medium",
            },
            {
                "field": "sparse_text",
                "status": "high_missing",
                "reason_code": "high_missing",
                "reason_text": "",
                "recommended_action": "fill_null",
                "confidence": "medium",
            },
            {
                "field": "request_id",
                "status": "suspected_id",
                "reason_code": "identifier_column",
                "reason_text": "",
                "recommended_action": "exclude_column",
                "confidence": "high",
            },
            {
                "field": "constant_field",
                "status": "suggest_delete",
                "reason_code": "constant_column",
                "reason_text": "",
                "recommended_action": "exclude_column",
                "confidence": "high",
            },
        ]

        recommendations = _build_recommended_steps(frame, field_advice)
        recommendations_by_id = {item["recommendation_id"]: item for item in recommendations}

        self.assertIn("fill-null-numeric", recommendations_by_id)
        self.assertIn("fill-null-text", recommendations_by_id)
        self.assertIn("cast-numeric", recommendations_by_id)
        self.assertIn("cast-datetime", recommendations_by_id)
        self.assertIn("exclude-columns", recommendations_by_id)
        self.assertIn("metric_text", recommendations_by_id["cast-numeric"]["step"]["input_selector"]["columns"])
        self.assertIn("event_time_text", recommendations_by_id["cast-datetime"]["step"]["input_selector"]["columns"])
        self.assertNotIn("request_id", recommendations_by_id["exclude-columns"]["step"]["input_selector"]["columns"])
        self.assertNotIn("constant_field", recommendations_by_id["exclude-columns"]["step"]["input_selector"]["columns"])


if __name__ == "__main__":
    unittest.main()
