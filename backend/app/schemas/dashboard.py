from datetime import datetime

from pydantic import BaseModel

from app.schemas.job import JobRead
from app.schemas.project import ProjectRead


class DatasetSummaryRead(BaseModel):
    id: int
    project_id: int
    version_name: str
    parser_profile: str
    row_count: int
    label_column: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class ModelSummaryRead(BaseModel):
    id: int
    project_id: int
    dataset_version_id: int
    name: str
    mode: str
    algorithm: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class DashboardSummaryRead(BaseModel):
    project_count: int
    dataset_count: int
    model_count: int
    job_count: int
    recent_projects: list[ProjectRead]
    recent_jobs: list[JobRead]
    recent_datasets: list[DatasetSummaryRead]
    recent_models: list[ModelSummaryRead]
