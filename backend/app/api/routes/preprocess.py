from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.preprocess import (
    PreprocessPipelineCreate,
    PreprocessPipelineRead,
    PreprocessPreviewRead,
    PreprocessStepPreviewRead,
    PreprocessStepPreviewRequest,
)
from app.services.preprocess import (
    create_preprocess_pipeline,
    get_preprocess_pipeline,
    list_preprocess_pipelines,
    preview_preprocess_pipeline,
    preview_preprocess_step,
)


router = APIRouter()


@router.get("/preprocess", response_model=list[PreprocessPipelineRead])
def read_preprocess_pipelines(
    project_id: int = Query(...),
    dataset_version_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[PreprocessPipelineRead]:
    return list_preprocess_pipelines(db, project_id, dataset_version_id)


@router.post("/preprocess", response_model=PreprocessPipelineRead, status_code=status.HTTP_201_CREATED)
def create_preprocess(payload: PreprocessPipelineCreate, db: Session = Depends(get_db)) -> PreprocessPipelineRead:
    return create_preprocess_pipeline(db, payload)


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
