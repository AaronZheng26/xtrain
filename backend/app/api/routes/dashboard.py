from sqlalchemy import func, select
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends

from app.db.session import get_db
from app.models.dataset_version import DatasetVersion
from app.models.job import Job
from app.models.model_version import ModelVersion
from app.models.project import Project
from app.schemas.dashboard import DashboardSummaryRead


router = APIRouter()


@router.get("/summary", response_model=DashboardSummaryRead)
def read_dashboard_summary(db: Session = Depends(get_db)) -> DashboardSummaryRead:
    return DashboardSummaryRead(
        project_count=int(db.scalar(select(func.count()).select_from(Project)) or 0),
        dataset_count=int(db.scalar(select(func.count()).select_from(DatasetVersion)) or 0),
        model_count=int(db.scalar(select(func.count()).select_from(ModelVersion)) or 0),
        job_count=int(db.scalar(select(func.count()).select_from(Job)) or 0),
        recent_projects=list(db.scalars(select(Project).order_by(Project.created_at.desc()).limit(6))),
        recent_jobs=list(db.scalars(select(Job).order_by(Job.created_at.desc()).limit(8))),
        recent_datasets=list(
            db.scalars(select(DatasetVersion).order_by(DatasetVersion.created_at.desc()).limit(8))
        ),
        recent_models=list(db.scalars(select(ModelVersion).order_by(ModelVersion.created_at.desc()).limit(8))),
    )
