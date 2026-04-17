from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class FeatureStep(BaseModel):
    step_id: str | None = None
    step_type: str | None = None
    type: str | None = None
    enabled: bool = True
    input_selector: dict[str, Any] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    output_mode: dict[str, Any] = Field(default_factory=dict)


class FeaturePipelineCreate(BaseModel):
    project_id: int
    dataset_version_id: int
    preprocess_pipeline_id: int | None = None
    name: str = Field(min_length=1, max_length=120)
    mode: str | None = None
    template_id: str | None = None
    steps: list[FeatureStep] = Field(default_factory=list)


class FeaturePipelineRead(BaseModel):
    id: int
    project_id: int
    dataset_version_id: int
    preprocess_pipeline_id: int | None
    name: str
    status: str
    steps: list[dict]
    output_path: str | None
    output_row_count: int
    output_schema: list[dict]
    training_candidate_columns: list[str] = Field(default_factory=list)
    business_context_columns: list[str] = Field(default_factory=list)
    analysis_retained_columns: list[str] = Field(default_factory=list)
    feature_lineage: dict[str, dict[str, Any]] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class FeaturePreviewRead(BaseModel):
    pipeline_id: int
    columns: list[str]
    rows: list[dict]
    training_candidate_columns: list[str] = Field(default_factory=list)
    business_context_columns: list[str] = Field(default_factory=list)
    analysis_retained_columns: list[str] = Field(default_factory=list)
    feature_lineage: dict[str, dict[str, Any]] = Field(default_factory=dict)


class FeatureTemplateRead(BaseModel):
    id: str
    project_id: int | None = None
    scope: str
    name: str
    log_type: str
    description: str
    steps: list[dict]
    field_hints: dict[str, list[str]] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FeatureTemplateCreate(BaseModel):
    project_id: int
    name: str = Field(min_length=1, max_length=120)
    log_type: str = Field(min_length=1, max_length=64)
    description: str = Field(default="", max_length=500)
    steps: list[FeatureStep] = Field(default_factory=list)
    field_hints: dict[str, list[str]] = Field(default_factory=dict)


class FeatureStepPreviewRequest(BaseModel):
    project_id: int
    dataset_version_id: int
    preprocess_pipeline_id: int | None = None
    steps: list[FeatureStep] = Field(default_factory=list)
    preview_step_index: int = Field(ge=0)
    limit: int = Field(default=8, ge=1, le=20)


class FeatureStepPreviewRead(BaseModel):
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
