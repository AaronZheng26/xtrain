from datetime import datetime

from pydantic import BaseModel, Field

from app.schemas.dataset import DatasetImportRead


class LogTemplateRead(BaseModel):
    id: str
    name: str
    log_type: str
    description: str
    parser_profile: str


class ImportSessionRead(BaseModel):
    id: int
    project_id: int
    file_name: str
    file_type: str
    raw_file_path: str
    status: str
    selected_template_id: str
    parser_profile: str
    parse_options: dict
    cleaning_options: dict
    field_mapping: dict
    preview_schema: list[dict]
    detected_fields: dict
    preview_rows: list[dict]
    error_rows: list[dict]
    row_count: int
    confirmed_dataset_version_id: int | None
    template_suggestions: list[LogTemplateRead] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ImportSessionTemplateUpdate(BaseModel):
    template_id: str


class ImportSessionParseOptionsUpdate(BaseModel):
    parse_options: dict = Field(default_factory=dict)


class ImportSessionCleaningOptionsUpdate(BaseModel):
    cleaning_options: dict = Field(default_factory=dict)


class ImportSessionFieldMappingUpdate(BaseModel):
    field_mapping: dict[str, str | None] = Field(default_factory=dict)


class ImportSessionConfirmRead(BaseModel):
    import_session: ImportSessionRead
    import_result: DatasetImportRead
