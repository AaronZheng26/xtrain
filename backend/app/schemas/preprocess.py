from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class PreprocessStep(BaseModel):
    step_id: str | None = None
    step_type: str | None = None
    type: str | None = None
    enabled: bool = True
    input_selector: dict[str, Any] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    output_mode: dict[str, Any] = Field(default_factory=dict)


class PreprocessPipelineCreate(BaseModel):
    project_id: int
    dataset_version_id: int
    name: str = Field(min_length=1, max_length=120)
    steps: list[PreprocessStep] = Field(default_factory=list)


class PreprocessPipelineRead(BaseModel):
    id: int
    project_id: int
    dataset_version_id: int
    name: str
    status: str
    steps: list[dict]
    output_path: str | None
    output_row_count: int
    output_schema: list[dict]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class PreprocessPreviewRead(BaseModel):
    pipeline_id: int
    columns: list[str]
    rows: list[dict]


class PreprocessStepPreviewRequest(BaseModel):
    project_id: int
    dataset_version_id: int
    steps: list[PreprocessStep] = Field(default_factory=list)
    preview_step_index: int = Field(ge=0)
    limit: int = Field(default=8, ge=1, le=20)


class PreprocessStepPreviewRead(BaseModel):
    preview_step_index: int
    step: dict[str, Any]
    before_row_count: int
    after_row_count: int
    before_columns: list[str]
    after_columns: list[str]
    added_columns: list[str]
    removed_columns: list[str]
    before_rows: list[dict]
    after_rows: list[dict]


class PreprocessTrainingAdvisorRequest(BaseModel):
    project_id: int
    dataset_version_id: int
    steps: list[PreprocessStep] = Field(default_factory=list)
    target_column: str | None = None
    sample_limit: int = Field(default=1500, ge=100, le=10000)


class FieldAdviceRead(BaseModel):
    field: str
    status: str
    reason_code: str
    reason_text: str
    recommended_action: str
    confidence: str
    feature_handoff: dict[str, Any] | None = None


class RecommendedPreprocessStepDraftRead(BaseModel):
    recommendation_id: str
    title: str
    description: str
    step: dict[str, Any]


class PreprocessTrainingAdvisorSummaryRead(BaseModel):
    direct_trainable_fields: int
    high_risk_fields: int
    pending_fields: int
    total_fields: int
    target_column: str | None = None
    suggested_training_columns: list[str] = Field(default_factory=list)
    excluded_training_columns: list[str] = Field(default_factory=list)
    analysis_basis: str


class PreprocessTrainingAdvisorRead(BaseModel):
    summary: PreprocessTrainingAdvisorSummaryRead
    field_advice: list[FieldAdviceRead]
    recommended_steps: list[RecommendedPreprocessStepDraftRead] = Field(default_factory=list)
    analysis_mode: str
    sample_size: int
    generated_at: datetime


class PreprocessTrainingAdvisorRunRead(BaseModel):
    id: int
    project_id: int
    dataset_version_id: int
    job_id: int | None
    status: str
    analysis_mode: str
    sample_size: int
    result: PreprocessTrainingAdvisorRead | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
