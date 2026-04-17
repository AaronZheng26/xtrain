from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.job import JobSubmissionRead
from app.schemas.feature import (
    FeaturePipelineCreate,
    FeaturePipelineRead,
    FeaturePreviewRead,
    FeatureStepPreviewRead,
    FeatureStepPreviewRequest,
)
from app.services.feature import (
    create_feature_job,
    get_feature_pipeline,
    list_feature_pipelines,
    preview_feature_pipeline,
    preview_feature_step,
    run_feature_pipeline_job,
)
from app.services.job_manager import job_manager


router = APIRouter()


@router.get("/features", response_model=list[FeaturePipelineRead])
def read_feature_pipelines(
    project_id: int = Query(...),
    dataset_version_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[FeaturePipelineRead]:
    return list_feature_pipelines(db, project_id, dataset_version_id)


@router.post("/features", response_model=JobSubmissionRead, status_code=status.HTTP_202_ACCEPTED)
def create_feature(payload: FeaturePipelineCreate, db: Session = Depends(get_db)) -> JobSubmissionRead:
    job, pipeline = create_feature_job(db, payload)
    job_manager.submit_task(run_feature_pipeline_job, job.id, pipeline.id)
    return JobSubmissionRead(job=job, resource_id=pipeline.id, resource_type="feature_pipeline")


@router.get("/features/{pipeline_id}", response_model=FeaturePipelineRead)
def read_feature_pipeline(pipeline_id: int, db: Session = Depends(get_db)) -> FeaturePipelineRead:
    return get_feature_pipeline(db, pipeline_id)


@router.get("/features/{pipeline_id}/preview", response_model=FeaturePreviewRead)
def read_feature_preview(pipeline_id: int, limit: int = 20, db: Session = Depends(get_db)) -> FeaturePreviewRead:
    pipeline = get_feature_pipeline(db, pipeline_id)
    return FeaturePreviewRead(**preview_feature_pipeline(db, pipeline, limit=limit))


@router.post("/features/step-preview", response_model=FeatureStepPreviewRead)
def create_feature_step_preview(
    payload: FeatureStepPreviewRequest,
    db: Session = Depends(get_db),
) -> FeatureStepPreviewRead:
    return FeatureStepPreviewRead(
        **preview_feature_step(
            db,
            project_id=payload.project_id,
            dataset_version_id=payload.dataset_version_id,
            preprocess_pipeline_id=payload.preprocess_pipeline_id,
            steps=[step.model_dump() for step in payload.steps],
            preview_step_index=payload.preview_step_index,
            limit=payload.limit,
        )
    )
