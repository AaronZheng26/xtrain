from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.job import JobSubmissionRead
from app.schemas.preprocess import (
    PreprocessPipelineCreate,
    PreprocessPipelineRead,
    PreprocessPreviewRead,
    PreprocessStepPreviewRead,
    PreprocessStepPreviewRequest,
)
from app.services.preprocess import (
    create_preprocess_job,
    get_preprocess_pipeline,
    list_preprocess_pipelines,
    preview_preprocess_pipeline,
    preview_preprocess_step,
    run_preprocess_pipeline_job,
)
from app.services.job_manager import job_manager


router = APIRouter()


@router.get("/preprocess", response_model=list[PreprocessPipelineRead])
def read_preprocess_pipelines(
    project_id: int = Query(...),
    dataset_version_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PreprocessPipelineRead]:
    return list_preprocess_pipelines(db, project_id, dataset_version_id)


@router.post("/preprocess", response_model=JobSubmissionRead, status_code=status.HTTP_202_ACCEPTED)
def create_preprocess(payload: PreprocessPipelineCreate, db: Session = Depends(get_db)) -> JobSubmissionRead:
    job, pipeline = create_preprocess_job(db, payload)
    job_manager.submit_task(run_preprocess_pipeline_job, job.id, pipeline.id)
    return JobSubmissionRead(job=job, resource_id=pipeline.id, resource_type="preprocess_pipeline")


@router.get("/preprocess/{pipeline_id}", response_model=PreprocessPipelineRead)
def read_preprocess_pipeline(pipeline_id: int, db: Session = Depends(get_db)) -> PreprocessPipelineRead:
    return get_preprocess_pipeline(db, pipeline_id)


@router.get("/preprocess/{pipeline_id}/preview", response_model=PreprocessPreviewRead)
def read_preprocess_preview(pipeline_id: int, limit: int = 20, db: Session = Depends(get_db)) -> PreprocessPreviewRead:
    pipeline = get_preprocess_pipeline(db, pipeline_id)
    return PreprocessPreviewRead(**preview_preprocess_pipeline(pipeline, limit=limit))


@router.post("/preprocess/step-preview", response_model=PreprocessStepPreviewRead)
def create_preprocess_step_preview(
    payload: PreprocessStepPreviewRequest,
    db: Session = Depends(get_db),
) -> PreprocessStepPreviewRead:
    return PreprocessStepPreviewRead(
        **preview_preprocess_step(
            db,
            project_id=payload.project_id,
            dataset_version_id=payload.dataset_version_id,
            steps=[step.model_dump() for step in payload.steps],
            preview_step_index=payload.preview_step_index,
            limit=payload.limit,
        )
    )
