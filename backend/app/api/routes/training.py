from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.job import JobSubmissionRead
from app.schemas.training import ModelAnalysisRead, ModelPreviewRead, ModelVersionRead, TrainingRequest
from app.services.training import (
    create_training_job,
    get_model_analysis,
    get_model_version,
    list_model_versions,
    preview_model_version,
    run_training_job,
)
from app.services.job_manager import job_manager


router = APIRouter()


@router.get("/models", response_model=list[ModelVersionRead])
def read_model_versions(
    project_id: int = Query(...),
    dataset_version_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[ModelVersionRead]:
    return list_model_versions(db, project_id, dataset_version_id)


@router.post("/models", response_model=JobSubmissionRead, status_code=status.HTTP_202_ACCEPTED)
def create_model(payload: TrainingRequest, db: Session = Depends(get_db)) -> JobSubmissionRead:
    model_version, job = create_training_job(db, payload)
    job_manager.submit_task(run_training_job, job.id, model_version.id)
    return JobSubmissionRead(job=job, resource_id=model_version.id, resource_type="model_version")


@router.get("/models/{model_id}", response_model=ModelVersionRead)
def read_model_version(model_id: int, db: Session = Depends(get_db)) -> ModelVersionRead:
    return get_model_version(db, model_id)


@router.get("/models/{model_id}/preview", response_model=ModelPreviewRead)
def read_model_preview(model_id: int, limit: int = 20, db: Session = Depends(get_db)) -> ModelPreviewRead:
    model_version = get_model_version(db, model_id)
    return ModelPreviewRead(**preview_model_version(model_version, limit=limit))


@router.get("/models/{model_id}/analysis", response_model=ModelAnalysisRead)
def read_model_analysis(
    model_id: int,
    point_limit: int = Query(default=600, ge=100, le=2000),
    histogram_bins: int = Query(default=16, ge=6, le=40),
    db: Session = Depends(get_db),
) -> ModelAnalysisRead:
    model_version = get_model_version(db, model_id)
    return ModelAnalysisRead(**get_model_analysis(model_version, point_limit=point_limit, histogram_bins=histogram_bins))
