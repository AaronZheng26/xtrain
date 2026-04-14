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
