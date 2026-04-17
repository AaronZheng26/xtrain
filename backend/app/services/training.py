import pickle
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import HTTPException
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.svm import OneClassSVM, SVC
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models.feature_pipeline import FeaturePipeline
from app.models.job import Job
from app.models.model_version import ModelVersion
from app.models.preprocess_pipeline import PreprocessPipeline
from app.models.project import Project
from app.schemas.training import TrainingRequest
from app.services.dataset_import import get_dataset, json_safe_records, load_parquet_frame, write_parquet
from app.services.field_mapping import get_or_create_field_mapping
from app.services.preprocess import apply_field_mapping

DEFAULT_MAX_CATEGORICAL_CARDINALITY = 50
LABEL_LIKE_COLUMNS = {
    "label",
    "labels",
    "target",
    "targetlabel",
    "groundtruth",
    "actuallabel",
    "isanomaly",
    "anomalylabel",
    "attacklabel",
}
RAW_TEXT_HINTS = ("message", "rawmessage", "useragent", "path", "url", "query", "payload", "stacktrace")
IDENTIFIER_HINTS = ("uuid", "guid", "trace", "session", "request", "eventid")
RESERVED_TRAINING_COLUMNS = {"predicted_label", "prediction_proba", "anomaly_score", "actual_label", "sample_index"}
RESERVED_ANALYSIS_SIGNAL_COLUMNS = RESERVED_TRAINING_COLUMNS


def create_model_version(db: Session, payload: TrainingRequest) -> ModelVersion:
    model_version, _job = create_training_job(db, payload)
    return model_version


def create_training_job(db: Session, payload: TrainingRequest) -> tuple[ModelVersion, Job]:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    dataset = get_dataset(db, payload.dataset_version_id)
    if dataset.project_id != payload.project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")

    preprocess_pipeline = _validate_preprocess_pipeline(db, payload)
    feature_pipeline = _validate_feature_pipeline(db, payload)

    job = Job(
        name=payload.name,
        job_type="training",
        status="queued",
        progress=0,
        message="Waiting to start training",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    model_version = ModelVersion(
        project_id=payload.project_id,
        dataset_version_id=payload.dataset_version_id,
        preprocess_pipeline_id=payload.preprocess_pipeline_id,
        feature_pipeline_id=payload.feature_pipeline_id,
        job_id=job.id,
        name=payload.name,
        mode=payload.mode,
        algorithm=payload.algorithm,
        status="queued",
        target_column=payload.target_column,
        feature_columns=payload.feature_columns,
        training_params=payload.training_params,
    )
    db.add(model_version)
    db.commit()
    db.refresh(model_version)
    return model_version, job


def run_training_job(job_id: int, model_id: int) -> None:
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        model_version = db.get(ModelVersion, model_id)
        if not job or not model_version:
            return

        payload = TrainingRequest(
            project_id=model_version.project_id,
            dataset_version_id=model_version.dataset_version_id,
            preprocess_pipeline_id=model_version.preprocess_pipeline_id,
            feature_pipeline_id=model_version.feature_pipeline_id,
            name=model_version.name,
            mode=model_version.mode,
            algorithm=model_version.algorithm,
            target_column=model_version.target_column,
            feature_columns=model_version.feature_columns or [],
            training_params=model_version.training_params or {},
        )

        try:
            dataset = get_dataset(db, payload.dataset_version_id)
            preprocess_pipeline = _validate_preprocess_pipeline(db, payload)
            feature_pipeline = _validate_feature_pipeline(db, payload)

            _set_job_state(db, job, status="running", progress=5, message="Preparing training input")
            model_version.status = "running"
            db.add(model_version)
            db.commit()

            frame = _load_training_input_frame(db, dataset.id, preprocess_pipeline, feature_pipeline)
            _set_job_state(db, job, status="running", progress=25, message="Preparing training matrix")

            trained = _train_model(
                frame,
                dataset.label_column,
                payload,
                dataset_schema_columns=[field.get("name") for field in dataset.schema_snapshot if field.get("name")],
                preprocess_pipeline=preprocess_pipeline,
                feature_pipeline=feature_pipeline,
            )

            output_dir = get_settings().storage_root_path / "models" / f"project_{payload.project_id}"
            output_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = output_dir / f"model_{model_version.id}_{uuid.uuid4().hex[:8]}.pkl"
            prediction_path = output_dir / f"predictions_{model_version.id}_{uuid.uuid4().hex[:8]}.parquet"

            _set_job_state(db, job, status="running", progress=75, message="Persisting model artifacts")
            _try_dump_model(trained["model"], artifact_path)
            write_parquet(trained["prediction_frame"], prediction_path)

            model_version.status = "completed"
            model_version.target_column = trained["target_column"]
            model_version.feature_columns = trained["feature_columns"]
            model_version.metrics = trained["metrics"]
            model_version.report_json = trained["report_json"]
            model_version.artifact_path = str(artifact_path) if artifact_path.exists() else None
            model_version.prediction_path = str(prediction_path)
            db.add(model_version)
            db.commit()
            db.refresh(model_version)

            _set_job_state(db, job, status="completed", progress=100, message="Training finished")
        except HTTPException as exc:
            model_version.status = "failed"
            db.add(model_version)
            db.commit()
            _set_job_state(db, job, status="failed", progress=100, message=str(exc.detail))
        except Exception as exc:
            model_version.status = "failed"
            db.add(model_version)
            db.commit()
            _set_job_state(db, job, status="failed", progress=100, message=f"Training failed: {exc}")


def list_model_versions(
    db: Session,
    project_id: int,
    dataset_version_id: int | None = None,
) -> list[ModelVersion]:
    query = select(ModelVersion).where(ModelVersion.project_id == project_id)
    if dataset_version_id is not None:
        query = query.where(ModelVersion.dataset_version_id == dataset_version_id)
    query = query.order_by(ModelVersion.created_at.desc())
    return list(db.scalars(query))


def get_model_version(db: Session, model_id: int) -> ModelVersion:
    model_version = db.get(ModelVersion, model_id)
    if not model_version:
        raise HTTPException(status_code=404, detail="Model version not found")
    return model_version


def preview_model_version(model_version: ModelVersion, limit: int = 20) -> dict[str, Any]:
    if not model_version.prediction_path:
        raise HTTPException(status_code=400, detail="Model version has no prediction preview yet")
    frame = load_parquet_frame(model_version.prediction_path)
    sort_column = "anomaly_score" if "anomaly_score" in frame.columns else "prediction_proba" if "prediction_proba" in frame.columns else None
    if sort_column:
        frame = frame.sort_values(by=sort_column, ascending=False)
    frame = frame.head(min(max(limit, 1), 50))
    return {
        "model_id": model_version.id,
        "metrics": model_version.metrics,
        "columns": list(frame.columns),
        "rows": json_safe_records(frame),
    }


def get_model_analysis(
    model_version: ModelVersion,
    point_limit: int = 600,
    histogram_bins: int = 16,
) -> dict[str, Any]:
    if model_version.mode.lower() != "unsupervised":
        raise HTTPException(status_code=400, detail="Visualization analysis is only available for unsupervised models")
    if not model_version.prediction_path:
        raise HTTPException(status_code=400, detail="Model version has no prediction output")

    frame = load_parquet_frame(model_version.prediction_path).copy()
    if "anomaly_score" not in frame.columns or "predicted_label" not in frame.columns:
        raise HTTPException(status_code=400, detail="Prediction output does not contain anomaly analysis fields")

    frame = frame.reset_index(drop=True)
    frame["sample_index"] = np.arange(len(frame))
    sampled = _sample_unsupervised_frame(frame, point_limit)

    return {
        "model_id": model_version.id,
        "mode": model_version.mode,
        "metrics": model_version.metrics,
        "sample_size": int(len(sampled)),
        "anomaly_count": int((sampled["predicted_label"] == "anomaly").sum()),
        "score_points": _build_score_points(sampled),
        "score_histogram": _build_score_histogram(sampled, histogram_bins),
        "embedding_points": _build_embedding_points(sampled, model_version.feature_columns),
        "spike_signal_summaries": _build_signal_summaries(frame, signal_type="spike_flag"),
        "count_signal_summaries": _build_signal_summaries(frame, signal_type="count_metric"),
    }


def _validate_preprocess_pipeline(db: Session, payload: TrainingRequest) -> PreprocessPipeline | None:
    if payload.preprocess_pipeline_id is None:
        return None
    pipeline = db.get(PreprocessPipeline, payload.preprocess_pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Preprocess pipeline not found")
    if pipeline.project_id != payload.project_id or pipeline.dataset_version_id != payload.dataset_version_id:
        raise HTTPException(status_code=400, detail="Preprocess pipeline does not match the selected dataset")
    if not pipeline.output_path:
        raise HTTPException(status_code=400, detail="Preprocess pipeline has no output")
    return pipeline


def _validate_feature_pipeline(db: Session, payload: TrainingRequest) -> FeaturePipeline | None:
    if payload.feature_pipeline_id is None:
        return None
    pipeline = db.get(FeaturePipeline, payload.feature_pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Feature pipeline not found")
    if pipeline.project_id != payload.project_id or pipeline.dataset_version_id != payload.dataset_version_id:
        raise HTTPException(status_code=400, detail="Feature pipeline does not match the selected dataset")
    if not pipeline.output_path:
        raise HTTPException(status_code=400, detail="Feature pipeline has no output")
    return pipeline


def _load_training_input_frame(
    db: Session,
    dataset_id: int,
    preprocess_pipeline: PreprocessPipeline | None,
    feature_pipeline: FeaturePipeline | None,
) -> pd.DataFrame:
    if feature_pipeline and feature_pipeline.output_path:
        return load_parquet_frame(feature_pipeline.output_path)
    if preprocess_pipeline and preprocess_pipeline.output_path:
        return load_parquet_frame(preprocess_pipeline.output_path)

    dataset = get_dataset(db, dataset_id)
    frame = load_parquet_frame(dataset.parquet_path)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    return apply_field_mapping(frame, mapping)


def _train_model(
    frame: pd.DataFrame,
    dataset_label_column: str | None,
    payload: TrainingRequest,
    dataset_schema_columns: list[str] | None = None,
    preprocess_pipeline: PreprocessPipeline | None = None,
    feature_pipeline: FeaturePipeline | None = None,
) -> dict[str, Any]:
    mode = payload.mode.lower()
    if mode not in {"supervised", "unsupervised"}:
        raise HTTPException(status_code=400, detail="mode must be supervised or unsupervised")

    target_column = payload.target_column or dataset_label_column
    feature_selection = _select_training_feature_columns(
        frame,
        payload,
        dataset_label_column=dataset_label_column,
        target_column=target_column,
        dataset_schema_columns=dataset_schema_columns or [],
        preprocess_pipeline=preprocess_pipeline,
        feature_pipeline=feature_pipeline,
    )
    feature_columns = feature_selection["used_feature_columns"]
    if not feature_columns:
        exclusion_summary = ", ".join(
            f"{column}: {reason}" for column, reason in feature_selection["exclusion_reasons"].items()
        )
        detail = "No valid feature columns selected"
        if exclusion_summary:
            detail = f"{detail}. Exclusions: {exclusion_summary}"
        raise HTTPException(status_code=400, detail=detail)

    prediction_base = frame.copy()

    if mode == "supervised":
        if not target_column or target_column not in frame.columns:
            raise HTTPException(status_code=400, detail="Supervised training requires a valid target column")

        train_frame = frame.dropna(subset=[target_column]).copy()
        if train_frame.empty:
            raise HTTPException(status_code=400, detail="Target column has no usable rows")

        y = train_frame[target_column].astype(str)
        X = _prepare_feature_matrix(train_frame, feature_columns)
        if len(np.unique(y)) < 2:
            raise HTTPException(status_code=400, detail="Supervised training needs at least two target classes")

        stratify = y if y.nunique() > 1 and y.value_counts().min() >= 2 else None
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=float(payload.training_params.get("test_size", 0.3)),
            random_state=42,
            stratify=stratify,
        )

        model = _build_supervised_model(payload.algorithm, payload.training_params)
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        probability = _supervised_probability(model, X_test)

        metrics = {
            "accuracy": round(float(accuracy_score(y_test, predictions)), 4),
            "precision_macro": round(float(precision_score(y_test, predictions, average="macro", zero_division=0)), 4),
            "recall_macro": round(float(recall_score(y_test, predictions, average="macro", zero_division=0)), 4),
            "f1_macro": round(float(f1_score(y_test, predictions, average="macro", zero_division=0)), 4),
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
        }
        result_frame = train_frame.loc[X_test.index, feature_columns].copy()
        result_frame["actual_label"] = y_test.values
        result_frame["predicted_label"] = predictions
        if probability is not None:
            result_frame["prediction_proba"] = probability

        return {
            "model": model,
            "target_column": target_column,
            "feature_columns": feature_columns,
            "metrics": metrics,
            "report_json": {
                "classes": sorted(y.astype(str).unique().tolist()),
                **feature_selection,
            },
            "prediction_frame": result_frame.reset_index(drop=True),
        }

    X = _prepare_feature_matrix(frame, feature_columns)
    model = _build_unsupervised_model(payload.algorithm, payload.training_params)
    labels, scores = _fit_predict_unsupervised(model, X, payload.algorithm)

    metrics: dict[str, Any] = {
        "rows": int(len(X)),
        "anomaly_count": int((labels == 1).sum()),
        "anomaly_ratio": round(float((labels == 1).mean()), 4),
    }

    report_json: dict[str, Any] = {
        "score_summary": {
            "min": round(float(np.nanmin(scores)), 4),
            "max": round(float(np.nanmax(scores)), 4),
            "mean": round(float(np.nanmean(scores)), 4),
        },
        **feature_selection,
    }

    result_frame = prediction_base.loc[X.index, feature_columns].copy()
    result_frame["predicted_label"] = np.where(labels == 1, "anomaly", "normal")
    result_frame["anomaly_score"] = scores

    if target_column and target_column in prediction_base.columns:
        binary_target = _to_binary_anomaly_labels(prediction_base.loc[X.index, target_column])
        metrics.update(
            {
                "precision": round(float(precision_score(binary_target, labels, zero_division=0)), 4),
                "recall": round(float(recall_score(binary_target, labels, zero_division=0)), 4),
                "f1": round(float(f1_score(binary_target, labels, zero_division=0)), 4),
            }
        )
        result_frame["actual_label"] = prediction_base.loc[X.index, target_column].astype(str).values

    return {
        "model": model,
        "target_column": target_column if target_column in prediction_base.columns else None,
        "feature_columns": feature_columns,
        "metrics": metrics,
        "report_json": report_json,
        "prediction_frame": result_frame.reset_index(drop=True),
    }


def _prepare_feature_matrix(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    X = frame.loc[:, feature_columns].copy()
    for column in X.columns:
        if pd.api.types.is_numeric_dtype(X[column]):
            X[column] = X[column].fillna(0)
        else:
            X[column] = X[column].astype("string").fillna("missing")
    X = pd.get_dummies(X, dummy_na=True)
    if X.empty:
        raise HTTPException(status_code=400, detail="Feature matrix is empty after preprocessing")
    return X


def _select_training_feature_columns(
    frame: pd.DataFrame,
    payload: TrainingRequest,
    dataset_label_column: str | None,
    target_column: str | None,
    dataset_schema_columns: list[str],
    preprocess_pipeline: PreprocessPipeline | None,
    feature_pipeline: FeaturePipeline | None,
) -> dict[str, Any]:
    available_columns = list(frame.columns)
    max_categorical_cardinality = max(int(payload.training_params.get("max_categorical_cardinality", DEFAULT_MAX_CATEGORICAL_CARDINALITY)), 5)

    if feature_pipeline:
        feature_metadata = _resolve_feature_pipeline_selection_metadata(
            frame,
            feature_pipeline,
            preprocess_pipeline=preprocess_pipeline,
            dataset_schema_columns=dataset_schema_columns,
        )
        if payload.feature_columns:
            requested_feature_columns = [column for column in payload.feature_columns if column in available_columns]
            selection_source = "explicit_request"
            default_exclusion_reasons = {}
        else:
            requested_feature_columns = [column for column in feature_metadata["training_candidate_columns"] if column in available_columns]
            selection_source = "feature_pipeline_candidates"
            default_exclusion_reasons = {
                column: "not_in_training_candidates"
                for column in feature_metadata["analysis_retained_columns"]
                if column in available_columns
            }

        used_feature_columns, hard_exclusion_reasons = _select_feature_output_columns(
            frame,
            requested_feature_columns,
            target_column=target_column,
        )
        exclusion_reasons = {**default_exclusion_reasons, **hard_exclusion_reasons}
        excluded_feature_columns = list(exclusion_reasons)
    else:
        if payload.feature_columns:
            requested_feature_columns = [column for column in payload.feature_columns if column in available_columns]
            selection_source = "explicit_request"
        else:
            requested_feature_columns = _default_requested_feature_columns(
                available_columns,
                preprocess_pipeline=preprocess_pipeline,
                feature_pipeline=feature_pipeline,
            )
            selection_source = "preprocess_pipeline_defaults" if preprocess_pipeline else "dataset_safe_defaults"

        used_feature_columns, exclusion_reasons = _select_safe_default_columns(
            frame,
            requested_feature_columns,
            target_column=target_column,
            dataset_label_column=dataset_label_column,
            max_categorical_cardinality=max_categorical_cardinality,
        )
        excluded_feature_columns = list(exclusion_reasons)

    return {
        "selection_source": selection_source,
        "requested_feature_columns": requested_feature_columns,
        "used_feature_columns": used_feature_columns,
        "excluded_feature_columns": excluded_feature_columns,
        "exclusion_reasons": exclusion_reasons,
        "max_categorical_cardinality": max_categorical_cardinality,
    }


def _default_requested_feature_columns(
    available_columns: list[str],
    preprocess_pipeline: PreprocessPipeline | None,
    feature_pipeline: FeaturePipeline | None,
) -> list[str]:
    if feature_pipeline and feature_pipeline.output_schema:
        pipeline_columns = [field.get("name") for field in feature_pipeline.output_schema if field.get("name")]
        return [column for column in pipeline_columns if column in available_columns]
    if preprocess_pipeline and preprocess_pipeline.output_schema:
        pipeline_columns = [field.get("name") for field in preprocess_pipeline.output_schema if field.get("name")]
        return [column for column in pipeline_columns if column in available_columns]
    return list(available_columns)


def _resolve_feature_pipeline_selection_metadata(
    frame: pd.DataFrame,
    feature_pipeline: FeaturePipeline,
    *,
    preprocess_pipeline: PreprocessPipeline | None,
    dataset_schema_columns: list[str],
) -> dict[str, list[str]]:
    stored_training_candidates = [column for column in (feature_pipeline.training_candidate_columns or []) if column in frame.columns]
    stored_analysis_columns = [column for column in (feature_pipeline.analysis_retained_columns or []) if column in frame.columns]
    if stored_training_candidates or stored_analysis_columns:
        accounted = set(stored_training_candidates) | set(stored_analysis_columns)
        remaining = [column for column in frame.columns if column not in accounted]
        return {
            "training_candidate_columns": stored_training_candidates,
            "analysis_retained_columns": stored_analysis_columns + remaining,
        }

    upstream_columns = _resolve_feature_pipeline_input_columns(
        preprocess_pipeline=preprocess_pipeline,
        dataset_schema_columns=dataset_schema_columns,
    )
    training_candidate_columns: list[str] = []
    analysis_retained_columns: list[str] = []

    for column in frame.columns:
        series = frame[column]
        is_generated = column not in upstream_columns
        if is_generated and pd.api.types.is_numeric_dtype(series):
            training_candidate_columns.append(column)
        else:
            analysis_retained_columns.append(column)

    return {
        "training_candidate_columns": training_candidate_columns,
        "analysis_retained_columns": analysis_retained_columns,
    }


def _resolve_feature_pipeline_input_columns(
    *,
    preprocess_pipeline: PreprocessPipeline | None,
    dataset_schema_columns: list[str],
) -> list[str]:
    if preprocess_pipeline and preprocess_pipeline.output_schema:
        return [field.get("name") for field in preprocess_pipeline.output_schema if field.get("name")]
    return list(dataset_schema_columns)


def _select_feature_output_columns(
    frame: pd.DataFrame,
    requested_feature_columns: list[str],
    *,
    target_column: str | None,
    used_feature_columns: list[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    selected_columns = list(used_feature_columns or [])
    exclusion_reasons: dict[str, str] = {}

    for column in requested_feature_columns:
        reason = _get_feature_output_exclusion_reason(
            frame,
            column,
            target_column=target_column,
            used_feature_columns=selected_columns,
        )
        if reason:
            exclusion_reasons[column] = reason
            continue
        selected_columns.append(column)

    return selected_columns, exclusion_reasons


def _select_safe_default_columns(
    frame: pd.DataFrame,
    requested_feature_columns: list[str],
    *,
    target_column: str | None,
    dataset_label_column: str | None,
    max_categorical_cardinality: int,
) -> tuple[list[str], dict[str, str]]:
    used_feature_columns: list[str] = []
    exclusion_reasons: dict[str, str] = {}

    for column in requested_feature_columns:
        reason = _get_safe_default_exclusion_reason(
            frame,
            column,
            target_column=target_column,
            dataset_label_column=dataset_label_column,
            used_feature_columns=used_feature_columns,
            max_categorical_cardinality=max_categorical_cardinality,
        )
        if reason:
            exclusion_reasons[column] = reason
            continue
        used_feature_columns.append(column)

    return used_feature_columns, exclusion_reasons


def _get_feature_output_exclusion_reason(
    frame: pd.DataFrame,
    column: str,
    *,
    target_column: str | None,
    used_feature_columns: list[str],
) -> str | None:
    if column not in frame.columns:
        return "missing_from_input"
    if column in RESERVED_TRAINING_COLUMNS:
        return "reserved_training_column"
    if target_column and column == target_column:
        return "target_column"

    series = frame[column]
    non_null = series.dropna()
    if non_null.empty:
        return "empty_after_generation"
    if int(non_null.nunique(dropna=True)) <= 1:
        return "constant_after_generation"

    if target_column and target_column in frame.columns and _series_equal(series, frame[target_column]):
        return "duplicates_target_column"

    for existing_column in used_feature_columns:
        if existing_column in frame.columns and _series_equal(series, frame[existing_column]):
            return f"duplicates_feature:{existing_column}"

    return None


def _get_safe_default_exclusion_reason(
    frame: pd.DataFrame,
    column: str,
    *,
    target_column: str | None,
    dataset_label_column: str | None,
    used_feature_columns: list[str],
    max_categorical_cardinality: int,
) -> str | None:
    if column not in frame.columns:
        return "missing_from_input"
    if column in RESERVED_TRAINING_COLUMNS:
        return "reserved_training_column"
    if target_column and column == target_column:
        return "target_column"

    normalized_name = _normalize_column_name(column)
    if _is_label_like_column(normalized_name) and column not in {target_column, dataset_label_column}:
        return "label_like_column"

    series = frame[column]
    if _is_identifier_column(normalized_name) and not pd.api.types.is_numeric_dtype(series):
        return "identifier_column"
    if _is_raw_text_column(normalized_name) and not pd.api.types.is_numeric_dtype(series):
        return "raw_text_column"

    non_null = series.dropna()
    if non_null.empty:
        return "empty_column"
    if int(non_null.nunique(dropna=True)) <= 1:
        return "constant_column"

    if target_column and target_column in frame.columns and _series_equal(series, frame[target_column]):
        return "duplicates_target_column"

    for existing_column in used_feature_columns:
        if existing_column in frame.columns and _series_equal(series, frame[existing_column]):
            return f"duplicates_feature:{existing_column}"

    if not pd.api.types.is_numeric_dtype(series):
        cardinality = int(non_null.astype("string").nunique(dropna=True))
        if cardinality > max_categorical_cardinality:
            return f"high_cardinality:{cardinality}>{max_categorical_cardinality}"

    return None


def _normalize_column_name(column: str) -> str:
    return "".join(character for character in str(column).strip().lower() if character.isalnum())


def _is_label_like_column(normalized_name: str) -> bool:
    return normalized_name in LABEL_LIKE_COLUMNS


def _is_identifier_column(normalized_name: str) -> bool:
    if normalized_name.endswith("id") or normalized_name.startswith("id"):
        return True
    return any(hint in normalized_name for hint in IDENTIFIER_HINTS)


def _is_raw_text_column(normalized_name: str) -> bool:
    return any(hint in normalized_name for hint in RAW_TEXT_HINTS)


def _series_equal(left: pd.Series, right: pd.Series) -> bool:
    left_normalized = left.astype("string").fillna("__missing__")
    right_normalized = right.astype("string").fillna("__missing__")
    return left_normalized.equals(right_normalized)


def _build_supervised_model(algorithm: str, params: dict[str, Any]):
    algorithm = algorithm.lower()
    if algorithm == "logistic_regression":
        return LogisticRegression(max_iter=int(params.get("max_iter", 500)))
    if algorithm == "random_forest":
        return RandomForestClassifier(
            n_estimators=int(params.get("n_estimators", 200)),
            random_state=42,
        )
    if algorithm == "svm":
        return SVC(
            probability=True,
            kernel=str(params.get("kernel", "rbf")),
            C=float(params.get("C", 1.0)),
        )
    raise HTTPException(status_code=400, detail=f"Unsupported supervised algorithm: {algorithm}")


def _build_unsupervised_model(algorithm: str, params: dict[str, Any]):
    algorithm = algorithm.lower()
    if algorithm == "isolation_forest":
        return IsolationForest(
            contamination=float(params.get("contamination", 0.1)),
            random_state=42,
        )
    if algorithm == "one_class_svm":
        return OneClassSVM(
            nu=float(params.get("nu", 0.1)),
            kernel=str(params.get("kernel", "rbf")),
        )
    if algorithm == "local_outlier_factor":
        from sklearn.neighbors import LocalOutlierFactor

        return LocalOutlierFactor(
            contamination=float(params.get("contamination", 0.1)),
            n_neighbors=int(params.get("n_neighbors", 20)),
        )
    raise HTTPException(status_code=400, detail=f"Unsupported unsupervised algorithm: {algorithm}")


def _fit_predict_unsupervised(model, X: pd.DataFrame, algorithm: str) -> tuple[np.ndarray, np.ndarray]:
    algorithm = algorithm.lower()
    if algorithm == "local_outlier_factor":
        raw_predictions = model.fit_predict(X)
        scores = -model.negative_outlier_factor_
    else:
        model.fit(X)
        raw_predictions = model.predict(X)
        if hasattr(model, "score_samples"):
            scores = -np.asarray(model.score_samples(X))
        elif hasattr(model, "decision_function"):
            scores = -np.asarray(model.decision_function(X))
        else:
            scores = np.zeros(len(X))
    labels = np.where(raw_predictions == -1, 1, 0)
    return labels.astype(int), scores.astype(float)


def _to_binary_anomaly_labels(series: pd.Series) -> np.ndarray:
    lowered = series.astype(str).str.strip().str.lower()
    positive = lowered.isin({"1", "true", "yes", "anomaly", "attack", "malicious", "abnormal"})
    return positive.astype(int).to_numpy()


def _supervised_probability(model, X_test: pd.DataFrame) -> np.ndarray | None:
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X_test)
        if probabilities.shape[1] > 1:
            return probabilities.max(axis=1)
        return probabilities[:, 0]
    return None


def _try_dump_model(model, artifact_path: Path) -> None:
    try:
        with artifact_path.open("wb") as file_handle:
            pickle.dump(model, file_handle)
    except Exception:
        if artifact_path.exists():
            artifact_path.unlink(missing_ok=True)


def _sample_unsupervised_frame(frame: pd.DataFrame, point_limit: int) -> pd.DataFrame:
    point_limit = min(max(int(point_limit), 100), 2000)
    if len(frame) <= point_limit:
        return frame.copy()

    anomaly_frame = frame.loc[frame["predicted_label"] == "anomaly"].copy()
    normal_frame = frame.loc[frame["predicted_label"] != "anomaly"].copy()

    if len(anomaly_frame) >= point_limit:
        return anomaly_frame.nlargest(point_limit, "anomaly_score").sort_values("sample_index").reset_index(drop=True)

    normal_limit = max(point_limit - len(anomaly_frame), 0)
    if len(normal_frame) > normal_limit:
        normal_frame = normal_frame.sample(n=normal_limit, random_state=42)

    sampled = pd.concat([anomaly_frame, normal_frame], ignore_index=True)
    return sampled.sort_values("sample_index").reset_index(drop=True)


def _build_score_points(frame: pd.DataFrame) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    has_actual_label = "actual_label" in frame.columns
    for row in frame.itertuples(index=False):
        points.append(
            {
                "sample_index": int(row.sample_index),
                "anomaly_score": round(float(row.anomaly_score), 6),
                "predicted_label": str(row.predicted_label),
                "actual_label": str(row.actual_label) if has_actual_label and row.actual_label is not None else None,
            }
        )
    return points


def _set_job_state(db: Session, job: Job, *, status: str, progress: int, message: str) -> None:
    job.status = status
    job.progress = progress
    job.message = message
    db.add(job)
    db.commit()
    db.refresh(job)


def _build_score_histogram(frame: pd.DataFrame, histogram_bins: int) -> list[dict[str, Any]]:
    scores = frame["anomaly_score"].astype(float).to_numpy()
    if len(scores) == 0:
        return []

    score_min = float(np.nanmin(scores))
    score_max = float(np.nanmax(scores))
    if np.isclose(score_min, score_max):
        score_max = score_min + 1.0

    bins = np.linspace(score_min, score_max, histogram_bins + 1)
    histogram: list[dict[str, Any]] = []

    for index in range(histogram_bins):
        left = float(bins[index])
        right = float(bins[index + 1])
        if index == histogram_bins - 1:
            bucket = frame.loc[(frame["anomaly_score"] >= left) & (frame["anomaly_score"] <= right)]
        else:
            bucket = frame.loc[(frame["anomaly_score"] >= left) & (frame["anomaly_score"] < right)]
        histogram.append(
            {
                "bucket_label": f"{left:.2f} - {right:.2f}",
                "range_start": round(left, 6),
                "range_end": round(right, 6),
                "normal_count": int((bucket["predicted_label"] != "anomaly").sum()),
                "anomaly_count": int((bucket["predicted_label"] == "anomaly").sum()),
            }
        )

    return histogram


def _build_embedding_points(frame: pd.DataFrame, feature_columns: list[str]) -> list[dict[str, Any]]:
    usable_columns = [column for column in feature_columns if column in frame.columns]
    if not usable_columns:
        return []

    X = _prepare_feature_matrix(frame, usable_columns)
    if X.empty:
        return []

    if X.shape[1] == 1:
        coordinates = np.column_stack([X.iloc[:, 0].to_numpy(dtype=float), np.zeros(len(X))])
    else:
        pca = PCA(n_components=2, random_state=42)
        coordinates = pca.fit_transform(X.to_numpy(dtype=float))

    points: list[dict[str, Any]] = []
    has_actual_label = "actual_label" in frame.columns
    for row_index, row in enumerate(frame.itertuples(index=False)):
        points.append(
            {
                "x": round(float(coordinates[row_index][0]), 6),
                "y": round(float(coordinates[row_index][1]), 6),
                "predicted_label": str(row.predicted_label),
                "anomaly_score": round(float(row.anomaly_score), 6),
                "actual_label": str(row.actual_label) if has_actual_label and row.actual_label is not None else None,
            }
        )

    return points


def _build_signal_summaries(
    frame: pd.DataFrame,
    signal_type: str,
    limit: int = 8,
) -> list[dict[str, Any]]:
    if "predicted_label" not in frame.columns:
        return []

    anomaly_mask = frame["predicted_label"].astype(str) == "anomaly"
    normal_mask = ~anomaly_mask
    summaries: list[tuple[float, dict[str, Any]]] = []

    for column in frame.columns:
        classified_signal_type = _classify_signal_column(column)
        if classified_signal_type != signal_type:
            continue

        series = pd.to_numeric(frame[column], errors="coerce")
        if series.isna().all():
            continue

        anomaly_values = series.loc[anomaly_mask]
        normal_values = series.loc[normal_mask]
        if signal_type == "spike_flag":
            summary = _build_spike_signal_summary(column, anomaly_values, normal_values)
            score = summary["anomaly_active_rate"] or 0.0
        else:
            summary = _build_count_signal_summary(column, anomaly_values, normal_values)
            score = summary["anomaly_mean"] or 0.0

        summaries.append((float(score), summary))

    summaries.sort(key=lambda item: item[0], reverse=True)
    return [summary for _, summary in summaries[:limit]]


def _classify_signal_column(column: str) -> str | None:
    if column in RESERVED_ANALYSIS_SIGNAL_COLUMNS:
        return None

    normalized_name = _normalize_column_name(column)
    if "spike" in normalized_name:
        return "spike_flag"
    if "count" in normalized_name:
        return "count_metric"
    return None


def _build_spike_signal_summary(column: str, anomaly_values: pd.Series, normal_values: pd.Series) -> dict[str, Any]:
    anomaly_active = anomaly_values.fillna(0) > 0
    normal_active = normal_values.fillna(0) > 0
    return {
        "column": column,
        "signal_type": "spike_flag",
        "anomaly_mean": _rounded_or_none(anomaly_values.mean(skipna=True)),
        "normal_mean": _rounded_or_none(normal_values.mean(skipna=True)),
        "anomaly_max": _rounded_or_none(anomaly_values.max(skipna=True)),
        "normal_max": _rounded_or_none(normal_values.max(skipna=True)),
        "anomaly_active_count": int(anomaly_active.sum()),
        "normal_active_count": int(normal_active.sum()),
        "anomaly_active_rate": _safe_rate(anomaly_active),
        "normal_active_rate": _safe_rate(normal_active),
    }


def _build_count_signal_summary(column: str, anomaly_values: pd.Series, normal_values: pd.Series) -> dict[str, Any]:
    anomaly_active = anomaly_values.fillna(0) > 0
    normal_active = normal_values.fillna(0) > 0
    return {
        "column": column,
        "signal_type": "count_metric",
        "anomaly_mean": _rounded_or_none(anomaly_values.mean(skipna=True)),
        "normal_mean": _rounded_or_none(normal_values.mean(skipna=True)),
        "anomaly_max": _rounded_or_none(anomaly_values.max(skipna=True)),
        "normal_max": _rounded_or_none(normal_values.max(skipna=True)),
        "anomaly_active_count": int(anomaly_active.sum()),
        "normal_active_count": int(normal_active.sum()),
        "anomaly_active_rate": _safe_rate(anomaly_active),
        "normal_active_rate": _safe_rate(normal_active),
    }


def _rounded_or_none(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return round(float(value), 4)


def _safe_rate(series: pd.Series) -> float | None:
    if len(series.index) == 0:
        return None
    return round(float(series.mean()), 4)
