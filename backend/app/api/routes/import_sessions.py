from fastapi import APIRouter, Depends, File, Form, UploadFile, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.dataset import DatasetImportRead
from app.schemas.import_session import (
    ImportSessionCleaningOptionsUpdate,
    ImportSessionConfirmRead,
    ImportSessionFieldMappingUpdate,
    ImportSessionParseOptionsUpdate,
    ImportSessionRead,
    ImportSessionTemplateUpdate,
    LogTemplateRead,
)
from app.services.import_session import (
    confirm_import_session,
    create_import_session,
    get_import_session,
    list_log_templates,
    serialize_import_session,
    update_session_cleaning_options,
    update_session_field_mapping,
    update_session_parse_options,
    update_session_template,
)


router = APIRouter()


@router.get("/log-templates", response_model=list[LogTemplateRead])
def read_log_templates() -> list[LogTemplateRead]:
    return [LogTemplateRead(**template) for template in list_log_templates()]


@router.post("", response_model=ImportSessionRead, status_code=status.HTTP_201_CREATED)
async def create_session(
    project_id: int = Form(...),
    file: UploadFile = File(...),
    parser_profile: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> ImportSessionRead:
    import_session = create_import_session(
        db,
        project_id=project_id,
        upload_file=file,
        parser_profile=parser_profile,
    )
    return ImportSessionRead(**serialize_import_session(import_session))


@router.get("/{session_id}", response_model=ImportSessionRead)
def read_session(session_id: int, db: Session = Depends(get_db)) -> ImportSessionRead:
    return ImportSessionRead(**serialize_import_session(get_import_session(db, session_id)))


@router.put("/{session_id}/template", response_model=ImportSessionRead)
def update_template(
    session_id: int,
    payload: ImportSessionTemplateUpdate,
    db: Session = Depends(get_db),
) -> ImportSessionRead:
    return ImportSessionRead(**serialize_import_session(update_session_template(db, session_id, payload.template_id)))


@router.put("/{session_id}/parse-options", response_model=ImportSessionRead)
def update_parse_options(
    session_id: int,
    payload: ImportSessionParseOptionsUpdate,
    db: Session = Depends(get_db),
) -> ImportSessionRead:
    return ImportSessionRead(**serialize_import_session(update_session_parse_options(db, session_id, payload.parse_options)))


@router.put("/{session_id}/cleaning-options", response_model=ImportSessionRead)
def update_cleaning_options(
    session_id: int,
    payload: ImportSessionCleaningOptionsUpdate,
    db: Session = Depends(get_db),
) -> ImportSessionRead:
    return ImportSessionRead(**serialize_import_session(update_session_cleaning_options(db, session_id, payload.cleaning_options)))


@router.put("/{session_id}/field-mapping", response_model=ImportSessionRead)
def update_field_mapping(
    session_id: int,
    payload: ImportSessionFieldMappingUpdate,
    db: Session = Depends(get_db),
) -> ImportSessionRead:
    return ImportSessionRead(**serialize_import_session(update_session_field_mapping(db, session_id, payload.field_mapping)))


@router.post("/{session_id}/confirm", response_model=ImportSessionConfirmRead)
def confirm_session(session_id: int, db: Session = Depends(get_db)) -> ImportSessionConfirmRead:
    import_session, data_source, dataset_version = confirm_import_session(db, session_id)
    return ImportSessionConfirmRead(
        import_session=ImportSessionRead(**serialize_import_session(import_session)),
        import_result=DatasetImportRead(data_source=data_source, dataset_version=dataset_version),
    )
