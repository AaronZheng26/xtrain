import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.feature import _apply_single_step as apply_feature_step
from app.services.preprocess import _apply_steps as apply_preprocess_steps, apply_field_mapping


class PipelineStepTests(unittest.TestCase):
    def test_field_mapping_and_preprocess_steps(self):
        frame = pd.DataFrame(
            {
                "timestamp": ["2026-04-10 10:00:00", None],
                "message": [" Error ", "Ok "],
            }
        )
        mapped = apply_field_mapping(frame, {"event_time": "timestamp", "raw_message": "message"})
        processed = apply_preprocess_steps(
            mapped,
            [
                {
                    "step_type": "fill_null",
                    "input_selector": {"mode": "explicit", "columns": ["event_time"]},
                    "params": {"value": "1970-01-01 00:00:00"},
                    "output_mode": {"mode": "inplace"},
                },
                {
                    "step_type": "trim_text",
                    "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                    "params": {},
                    "output_mode": {"mode": "new_column", "suffix": "_trimmed"},
                },
            ],
        )

        self.assertIn("event_time", processed.columns)
        self.assertIn("raw_message_trimmed", processed.columns)
        self.assertEqual(processed.loc[1, "event_time"], "1970-01-01 00:00:00")
        self.assertEqual(processed.loc[0, "raw_message_trimmed"], "Error")

    def test_feature_step_appends_new_columns(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["error timeout", "login ok"],
                "source_ip": ["10.0.0.1", "8.8.8.8"],
            }
        )

        with_length = apply_feature_step(
            frame,
            {
                "step_type": "text_length",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_length"},
            },
        )
        with_ip = apply_feature_step(
            with_length,
            {
                "step_type": "ip_features",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
        )

        self.assertIn("raw_message_length", with_ip.columns)
        self.assertIn("source_ip_is_private", with_ip.columns)
        self.assertEqual(int(with_ip.loc[0, "source_ip_is_private"]), 1)
        self.assertEqual(int(with_ip.loc[1, "source_ip_is_private"]), 0)

    def test_complexity_feature_steps_generate_expected_columns(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["error timeout 500", "login ok"],
                "user_agent": ["curl/8.0", "Mozilla/5.0"],
            }
        )

        with_entropy = apply_feature_step(
            frame,
            {
                "step_type": "shannon_entropy",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_entropy"},
            },
        )
        with_composition = apply_feature_step(
            with_entropy,
            {
                "step_type": "char_composition",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["user_agent"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
        )
        with_patterns = apply_feature_step(
            with_composition,
            {
                "step_type": "pattern_flags",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {"patterns": ["ip", "hash", "base64_like"]},
                "output_mode": {"mode": "append_new_columns"},
            },
        )

        self.assertIn("raw_message_entropy", with_patterns.columns)
        self.assertIn("user_agent_digit_ratio", with_patterns.columns)
        self.assertIn("user_agent_special_ratio", with_patterns.columns)
        self.assertIn("raw_message_has_ip", with_patterns.columns)
        self.assertGreater(float(with_patterns.loc[0, "raw_message_entropy"]), 0.0)

    def test_selector_modes_can_target_columns_without_explicit_names(self):
        frame = pd.DataFrame(
            {
                "raw_message": ["error on 10.0.0.1", "ok"],
                "payload_text": ["abc123", "XYZ"],
                "event_time": pd.to_datetime(["2026-04-10 10:00:00", "2026-04-10 10:05:00"]),
            }
        )

        with_dtype = apply_feature_step(
            frame,
            {
                "step_type": "token_count",
                "enabled": True,
                "input_selector": {"mode": "dtype", "dtype": "string"},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_tokens"},
            },
        )
        with_role_tag = apply_feature_step(
            with_dtype,
            {
                "step_type": "pattern_flags",
                "enabled": True,
                "input_selector": {"mode": "role_tag", "role_tag": "text"},
                "params": {"patterns": ["ip"]},
                "output_mode": {"mode": "append_new_columns"},
            },
        )
        with_pattern = apply_feature_step(
            with_role_tag,
            {
                "step_type": "derive_time_parts",
                "enabled": True,
                "input_selector": {"mode": "name_pattern", "name_pattern": "^event_"},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
        )

        self.assertIn("raw_message_tokens", with_pattern.columns)
        self.assertIn("payload_text_tokens", with_pattern.columns)
        self.assertIn("raw_message_has_ip", with_pattern.columns)
        self.assertIn("event_time_hour", with_pattern.columns)

    def test_multi_column_feature_steps_generate_combined_columns(self):
        frame = pd.DataFrame(
            {
                "bytes_sent": [100.0, 45.0],
                "request_duration": [10.0, 5.0],
                "source_ip": ["10.0.0.1", "8.8.8.8"],
                "dest_ip": ["10.0.0.1", "1.1.1.1"],
                "method": ["GET", "POST"],
                "path": ["/admin", "/health"],
            }
        )

        with_ratio = apply_feature_step(
            frame,
            {
                "step_type": "ratio_feature",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["bytes_sent", "request_duration"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "output_column": "bytes_per_second"},
            },
        )
        with_diff = apply_feature_step(
            with_ratio,
            {
                "step_type": "difference_feature",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["bytes_sent", "request_duration"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_delta"},
            },
        )
        with_concat = apply_feature_step(
            with_diff,
            {
                "step_type": "concat_fields",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["method", "path"]},
                "params": {"separator": " "},
                "output_mode": {"mode": "append_new_columns", "output_column": "request_signature"},
            },
        )
        with_equality = apply_feature_step(
            with_concat,
            {
                "step_type": "equality_flag",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip", "dest_ip"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "output_column": "same_ip_flag"},
            },
        )

        self.assertIn("bytes_per_second", with_equality.columns)
        self.assertIn("bytes_sent_request_duration_delta", with_equality.columns)
        self.assertIn("request_signature", with_equality.columns)
        self.assertIn("same_ip_flag", with_equality.columns)
        self.assertEqual(float(with_equality.loc[0, "bytes_per_second"]), 10.0)
        self.assertEqual(float(with_equality.loc[1, "bytes_sent_request_duration_delta"]), 40.0)
        self.assertEqual(with_equality.loc[0, "request_signature"], "GET /admin")
        self.assertEqual(int(with_equality.loc[0, "same_ip_flag"]), 1)
        self.assertEqual(int(with_equality.loc[1, "same_ip_flag"]), 0)

    def test_aggregate_and_window_feature_steps_generate_expected_columns(self):
        frame = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2026-04-10 10:01:00",
                        "2026-04-10 10:03:00",
                        "2026-04-10 10:17:00",
                        "2026-04-10 10:18:00",
                    ]
                ),
                "source_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.1", "8.8.8.8"],
                "dest_ip": ["1.1.1.1", "1.1.1.1", "2.2.2.2", "1.1.1.1"],
                "process_name": ["nginx", "python", "python", "python"],
            }
        )

        with_frequency = apply_feature_step(
            frame,
            {
                "step_type": "group_frequency",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip", "dest_ip"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "output_column": "pair_count"},
            },
        )
        with_unique = apply_feature_step(
            with_frequency,
            {
                "step_type": "group_unique_count",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip"]},
                "params": {"target_column": "dest_ip"},
                "output_mode": {"mode": "append_new_columns", "output_column": "source_unique_dest_count"},
            },
        )
        with_window = apply_feature_step(
            with_unique,
            {
                "step_type": "time_window_count",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip"]},
                "params": {"time_column": "event_time", "window_minutes": 15},
                "output_mode": {"mode": "append_new_columns", "output_column": "source_15m_count"},
            },
        )

        self.assertIn("pair_count", with_window.columns)
        self.assertIn("source_unique_dest_count", with_window.columns)
        self.assertIn("source_15m_count", with_window.columns)
        self.assertEqual(int(with_window.loc[0, "pair_count"]), 2)
        self.assertEqual(int(with_window.loc[2, "source_unique_dest_count"]), 2)
        self.assertEqual(int(with_window.loc[0, "source_15m_count"]), 2)
        self.assertEqual(int(with_window.loc[2, "source_15m_count"]), 1)

    def test_window_unique_and_spike_feature_steps_generate_expected_columns(self):
        frame = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2026-04-10 10:01:00",
                        "2026-04-10 10:02:00",
                        "2026-04-10 10:04:00",
                        "2026-04-10 10:16:00",
                    ]
                ),
                "source_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1"],
                "dest_ip": ["1.1.1.1", "2.2.2.2", "2.2.2.2", "3.3.3.3"],
            }
        )

        with_unique = apply_feature_step(
            frame,
            {
                "step_type": "window_unique_count",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip"]},
                "params": {"time_column": "event_time", "target_column": "dest_ip", "window_minutes": 15},
                "output_mode": {"mode": "append_new_columns", "output_column": "source_dest_unique_15m"},
            },
        )
        with_spike = apply_feature_step(
            with_unique,
            {
                "step_type": "window_spike_flag",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip"]},
                "params": {"time_column": "event_time", "window_minutes": 15, "threshold": 3},
                "output_mode": {"mode": "append_new_columns", "output_column": "source_spike_15m"},
            },
        )

        self.assertIn("source_dest_unique_15m", with_spike.columns)
        self.assertIn("source_spike_15m", with_spike.columns)
        self.assertEqual(int(with_spike.loc[0, "source_dest_unique_15m"]), 2)
        self.assertEqual(int(with_spike.loc[3, "source_dest_unique_15m"]), 1)
        self.assertEqual(int(with_spike.loc[0, "source_spike_15m"]), 1)
        self.assertEqual(int(with_spike.loc[3, "source_spike_15m"]), 0)


if __name__ == "__main__":
    unittest.main()
