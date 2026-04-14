from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.feature import FeatureTemplateCreate, FeatureTemplateRead
from app.services.feature import create_feature_template, list_feature_templates


router = APIRouter()


@router.get("/templates", response_model=list[FeatureTemplateRead])
def read_feature_templates(
    project_id: int = Query(...),
    db: Session = Depends(get_db),
) -> list[FeatureTemplateRead]:
    return [FeatureTemplateRead(**template) for template in list_feature_templates(db, project_id)]


@router.post("/templates", response_model=FeatureTemplateRead, status_code=status.HTTP_201_CREATED)
def create_template(
    payload: FeatureTemplateCreate,
    db: Session = Depends(get_db),
) -> FeatureTemplateRead:
    return FeatureTemplateRead(**create_feature_template(db, payload))
