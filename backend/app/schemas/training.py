from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TrainingRequest(BaseModel):
    project_id: int
    dataset_version_id: int
    preprocess_pipeline_id: int | None = None
    feature_pipeline_id: int | None = None
    name: str = Field(min_length=1, max_length=120)
    mode: str
    algorithm: str
    target_column: str | None = None
    feature_columns: list[str] = Field(default_factory=list)
    training_params: dict[str, Any] = Field(default_factory=dict)


class ModelVersionRead(BaseModel):
    id: int
    project_id: int
    dataset_version_id: int
    preprocess_pipeline_id: int | None
    feature_pipeline_id: int | None
    job_id: int | None
    name: str
    mode: str
    algorithm: str
    status: str
    target_column: str | None
    feature_columns: list[str]
    used_feature_columns: list[str] = Field(default_factory=list)
    excluded_feature_columns: list[str] = Field(default_factory=list)
    exclusion_reasons: dict[str, str] = Field(default_factory=dict)
    training_params: dict
    metrics: dict
    report_json: dict
    artifact_path: str | None
    prediction_path: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ModelPreviewRead(BaseModel):
    model_id: int
    metrics: dict
    columns: list[str]
    rows: list[dict]


class ModelAnalysisScorePointRead(BaseModel):
    sample_index: int
    anomaly_score: float
    predicted_label: str
    actual_label: str | None = None


class ModelAnalysisHistogramBucketRead(BaseModel):
    bucket_label: str
    range_start: float
    range_end: float
    normal_count: int
    anomaly_count: int


class ModelAnalysisEmbeddingPointRead(BaseModel):
    x: float
    y: float
    predicted_label: str
    anomaly_score: float
    actual_label: str | None = None


class ModelAnalysisSignalSummaryRead(BaseModel):
    column: str
    signal_type: str
    anomaly_mean: float | None = None
    normal_mean: float | None = None
    anomaly_max: float | None = None
    normal_max: float | None = None
    anomaly_active_count: int | None = None
    normal_active_count: int | None = None
    anomaly_active_rate: float | None = None
    normal_active_rate: float | None = None


class ModelAnalysisRead(BaseModel):
    model_id: int
    mode: str
    metrics: dict
    sample_size: int
    anomaly_count: int
    score_points: list[ModelAnalysisScorePointRead]
    score_histogram: list[ModelAnalysisHistogramBucketRead]
    embedding_points: list[ModelAnalysisEmbeddingPointRead]
    spike_signal_summaries: list[ModelAnalysisSignalSummaryRead] = Field(default_factory=list)
    count_signal_summaries: list[ModelAnalysisSignalSummaryRead] = Field(default_factory=list)
