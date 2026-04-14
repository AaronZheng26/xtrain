from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.main import app
from scripts.generate_sample_logs import generate_dataset, write_csv


def ensure_sample_file(path: Path, rows: int) -> Path:
    if path.exists():
        return path
    write_csv(path, generate_dataset(rows))
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the current workflow with sample data.")
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of sample rows to generate when the sample file does not exist.",
    )
    parser.add_argument(
        "--sample",
        type=Path,
        default=Path("storage") / "samples" / "security_logs_1000.csv",
        help="Sample CSV file path.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("storage") / "reports" / "workflow_validation_latest.json",
        help="Validation report output path.",
    )
    args = parser.parse_args()

    sample_path = ensure_sample_file(args.sample, args.rows)
    project_name = f"workflow-validation-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    with TestClient(app) as client:
        project_response = client.post(
            "/api/v1/projects",
            json={"name": project_name, "description": "Automated workflow validation with sample data"},
        )
        project_response.raise_for_status()
        project = project_response.json()

        with sample_path.open("rb") as file_handle:
            import_response = client.post(
                "/api/v1/datasets/import",
                data={"project_id": str(project["id"])},
                files={"file": (sample_path.name, file_handle, "text/csv")},
            )
        import_response.raise_for_status()
        imported = import_response.json()
        dataset = imported["dataset_version"]
        dataset_id = dataset["id"]

        workspace_before = client.get(f"/api/v1/datasets/{dataset_id}/workspace", params={"preview_limit": 5})
        workspace_before.raise_for_status()

        mapping_payload = {
            "mappings": {
                "event_time": "timestamp",
                "source_ip": "source_ip",
                "dest_ip": "dest_ip",
                "status_code": "status_code",
                "label": "label",
                "raw_message": "message",
            }
        }
        mapping_response = client.put(f"/api/v1/datasets/{dataset_id}/field-mapping", json=mapping_payload)
        mapping_response.raise_for_status()

        preprocess_response = client.post(
            "/api/v1/pipelines/preprocess",
            json={
                "project_id": project["id"],
                "dataset_version_id": dataset_id,
                "name": "validation-preprocess",
                "steps": [
                    {"type": "fill_null", "params": {"columns": ["dest_ip"], "value": "0.0.0.0"}},
                    {"type": "cast_type", "params": {"column": "status_code", "target_type": "int"}},
                    {
                        "type": "select_columns",
                        "params": {
                            "columns": [
                                "event_time",
                                "source_ip",
                                "dest_ip",
                                "status_code",
                                "bytes_sent",
                                "request_duration",
                                "error_count",
                                "auth_failures",
                                "severity",
                                "raw_message",
                                "label",
                            ]
                        },
                    },
                ],
            },
        )
        preprocess_response.raise_for_status()
        preprocess = preprocess_response.json()

        preprocess_preview = client.get(f"/api/v1/pipelines/preprocess/{preprocess['id']}/preview", params={"limit": 5})
        preprocess_preview.raise_for_status()

        feature_response = client.post(
            "/api/v1/pipelines/features",
            json={
                "project_id": project["id"],
                "dataset_version_id": dataset_id,
                "preprocess_pipeline_id": preprocess["id"],
                "name": "validation-features",
                "steps": [
                    {
                        "type": "select_features",
                        "params": {
                            "columns": [
                                "event_time",
                                "source_ip",
                                "dest_ip",
                                "status_code",
                                "bytes_sent",
                                "request_duration",
                                "error_count",
                                "auth_failures",
                                "severity",
                                "raw_message",
                                "label",
                            ]
                        },
                    },
                    {"type": "derive_time_parts", "params": {"column": "event_time", "prefix": "event"}},
                    {"type": "text_length", "params": {"column": "raw_message", "output_column": "message_length"}},
                    {"type": "frequency_encode", "params": {"column": "source_ip", "output_column": "source_ip_freq"}},
                ],
            },
        )
        feature_response.raise_for_status()
        feature = feature_response.json()

        feature_preview = client.get(f"/api/v1/pipelines/features/{feature['id']}/preview", params={"limit": 5})
        feature_preview.raise_for_status()

        supervised_response = client.post(
            "/api/v1/training/models",
            json={
                "project_id": project["id"],
                "dataset_version_id": dataset_id,
                "preprocess_pipeline_id": preprocess["id"],
                "feature_pipeline_id": feature["id"],
                "name": "validation-supervised",
                "mode": "supervised",
                "algorithm": "random_forest",
                "target_column": "label",
                "feature_columns": [
                    "source_ip",
                    "dest_ip",
                    "status_code",
                    "bytes_sent",
                    "request_duration",
                    "error_count",
                    "auth_failures",
                    "severity",
                    "raw_message",
                    "event_hour",
                    "event_dayofweek",
                    "message_length",
                    "source_ip_freq",
                ],
                "training_params": {"test_size": 0.25, "n_estimators": 120},
            },
        )
        supervised_response.raise_for_status()
        supervised_model = supervised_response.json()

        supervised_preview = client.get(
            f"/api/v1/training/models/{supervised_model['id']}/preview",
            params={"limit": 5},
        )
        supervised_preview.raise_for_status()

        unsupervised_response = client.post(
            "/api/v1/training/models",
            json={
                "project_id": project["id"],
                "dataset_version_id": dataset_id,
                "preprocess_pipeline_id": preprocess["id"],
                "feature_pipeline_id": feature["id"],
                "name": "validation-unsupervised",
                "mode": "unsupervised",
                "algorithm": "isolation_forest",
                "target_column": "label",
                "feature_columns": [
                    "source_ip",
                    "dest_ip",
                    "status_code",
                    "bytes_sent",
                    "request_duration",
                    "error_count",
                    "auth_failures",
                    "severity",
                    "raw_message",
                    "event_hour",
                    "event_dayofweek",
                    "message_length",
                    "source_ip_freq",
                ],
                "training_params": {"contamination": 0.1},
            },
        )
        unsupervised_response.raise_for_status()
        unsupervised_model = unsupervised_response.json()

        unsupervised_preview = client.get(
            f"/api/v1/training/models/{unsupervised_model['id']}/preview",
            params={"limit": 5},
        )
        unsupervised_preview.raise_for_status()

        workspace_after = client.get(f"/api/v1/datasets/{dataset_id}/workspace", params={"preview_limit": 5})
        workspace_after.raise_for_status()

    report = {
        "project": {"id": project["id"], "name": project["name"]},
        "sample_file": str(sample_path),
        "dataset": {
            "id": dataset_id,
            "version_name": dataset["version_name"],
            "row_count": dataset["row_count"],
            "label_column": dataset["label_column"],
        },
        "workspace_before_counts": {
            "preprocess_pipelines": len(workspace_before.json()["preprocess_pipelines"]),
            "feature_pipelines": len(workspace_before.json()["feature_pipelines"]),
            "models": len(workspace_before.json()["models"]),
        },
        "preprocess": {
            "id": preprocess["id"],
            "output_row_count": preprocess["output_row_count"],
            "preview_columns": preprocess_preview.json()["columns"],
        },
        "feature": {
            "id": feature["id"],
            "output_row_count": feature["output_row_count"],
            "preview_columns": feature_preview.json()["columns"],
        },
        "supervised_model": {
            "id": supervised_model["id"],
            "algorithm": supervised_model["algorithm"],
            "metrics": supervised_model["metrics"],
            "preview_columns": supervised_preview.json()["columns"],
        },
        "unsupervised_model": {
            "id": unsupervised_model["id"],
            "algorithm": unsupervised_model["algorithm"],
            "metrics": unsupervised_model["metrics"],
            "preview_columns": unsupervised_preview.json()["columns"],
        },
        "workspace_after_counts": {
            "preprocess_pipelines": len(workspace_after.json()["preprocess_pipelines"]),
            "feature_pipelines": len(workspace_after.json()["feature_pipelines"]),
            "models": len(workspace_after.json()["models"]),
        },
        "status": "passed",
    }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
