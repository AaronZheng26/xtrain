from fastapi import APIRouter, Depends, File, Form, Response, UploadFile, status
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.dataset import DatasetImportRead, DatasetPreviewRead, DatasetVersionRead, DatasetWorkspaceRead
from app.schemas.field_mapping import FieldMappingRead, FieldMappingUpdate
from app.services.cleanup import delete_dataset_with_assets
from app.services.dataset_import import get_dataset, import_dataset, list_datasets, preview_dataset
from app.services.feature import list_feature_pipelines
from app.services.field_mapping import get_or_create_field_mapping, update_field_mapping
from app.services.preprocess import list_preprocess_pipelines
from app.services.training import list_model_versions


router = APIRouter()


@router.get("", response_model=list[DatasetVersionRead])
def read_datasets(project_id: int, db: Session = Depends(get_db)) -> list[DatasetVersionRead]:
    return list_datasets(db, project_id)


@router.post("/import", response_model=DatasetImportRead, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    project_id: int = Form(...),
    file: UploadFile = File(...),
    parser_profile: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> DatasetImportRead:
    data_source, dataset_version = import_dataset(
        db,
        project_id=project_id,
        upload_file=file,
        parser_profile=parser_profile,
    )
    return DatasetImportRead(data_source=data_source, dataset_version=dataset_version)


@router.get("/{dataset_id}", response_model=DatasetVersionRead)
def read_dataset(dataset_id: int, db: Session = Depends(get_db)) -> DatasetVersionRead:
    return get_dataset(db, dataset_id)


@router.get("/{dataset_id}/detect-schema", response_model=DatasetVersionRead)
def detect_dataset_schema(dataset_id: int, db: Session = Depends(get_db)) -> DatasetVersionRead:
    return get_dataset(db, dataset_id)


@router.get("/{dataset_id}/preview", response_model=DatasetPreviewRead)
def read_dataset_preview(dataset_id: int, limit: int = 20, db: Session = Depends(get_db)) -> DatasetPreviewRead:
    dataset = get_dataset(db, dataset_id)
    return DatasetPreviewRead(**preview_dataset(dataset, limit=min(max(limit, 1), 50)))


@router.get("/{dataset_id}/workspace", response_model=DatasetWorkspaceRead)
def read_dataset_workspace(dataset_id: int, preview_limit: int = 12, db: Session = Depends(get_db)) -> DatasetWorkspaceRead:
    dataset = get_dataset(db, dataset_id)
    return DatasetWorkspaceRead(
        dataset=dataset,
        preview=DatasetPreviewRead(**preview_dataset(dataset, limit=min(max(preview_limit, 1), 50))),
        field_mapping=get_or_create_field_mapping(db, dataset_id),
        preprocess_pipelines=list_preprocess_pipelines(db, dataset.project_id, dataset_version_id=dataset_id),
        feature_pipelines=list_feature_pipelines(db, dataset.project_id, dataset_version_id=dataset_id),
        models=list_model_versions(db, dataset.project_id, dataset_version_id=dataset_id),
    )


@router.get("/{dataset_id}/field-mapping", response_model=FieldMappingRead)
def read_field_mapping(dataset_id: int, db: Session = Depends(get_db)) -> FieldMappingRead:
    return get_or_create_field_mapping(db, dataset_id)


@router.put("/{dataset_id}/field-mapping", response_model=FieldMappingRead)
def save_field_mapping(
    dataset_id: int,
    payload: FieldMappingUpdate,
    db: Session = Depends(get_db),
) -> FieldMappingRead:
    try:
        return update_field_mapping(db, dataset_id, payload.mappings)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(dataset_id: int, db: Session = Depends(get_db)) -> Response:
    delete_dataset_with_assets(db, dataset_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
