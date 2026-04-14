from datetime import datetime

from pydantic import BaseModel

from app.schemas.feature import FeaturePipelineRead
from app.schemas.field_mapping import FieldMappingRead
from app.schemas.preprocess import PreprocessPipelineRead
from app.schemas.training import ModelVersionRead


class DataSourceRead(BaseModel):
    id: int
    project_id: int
    file_name: str
    file_type: str
    parser_profile: str
    storage_path: str
    status: str
    row_count: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DatasetVersionRead(BaseModel):
    id: int
    project_id: int
    source_id: int
    version_name: str
    parser_profile: str
    parquet_path: str
    row_count: int
    label_column: str | None
    schema_snapshot: list[dict]
    detected_fields: dict
    created_at: datetime

    model_config = {"from_attributes": True}


class DatasetImportRead(BaseModel):
    data_source: DataSourceRead
    dataset_version: DatasetVersionRead


class DatasetPreviewRead(BaseModel):
    dataset_id: int
    columns: list[str]
    rows: list[dict]


class DatasetWorkspaceRead(BaseModel):
    dataset: DatasetVersionRead
    preview: DatasetPreviewRead
    field_mapping: FieldMappingRead
    preprocess_pipelines: list[PreprocessPipelineRead]
    feature_pipelines: list[FeaturePipelineRead]
    models: list[ModelVersionRead]
