from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.analysis import (
    LlmProviderConfigRead,
    LlmProviderTestRead,
    LlmProviderConfigUpdate,
    ModelLlmExplanationRead,
    ModelLlmExplanationRequest,
)
from app.services.llm_analysis import (
    explain_model_anomalies,
    get_or_create_llm_config,
    serialize_llm_config,
    test_llm_config_for_project,
    update_llm_config,
)


router = APIRouter()


@router.get("/projects/{project_id}/llm-config", response_model=LlmProviderConfigRead)
def read_llm_config(project_id: int, db: Session = Depends(get_db)) -> LlmProviderConfigRead:
    config = get_or_create_llm_config(db, project_id)
    return LlmProviderConfigRead(**serialize_llm_config(config))


@router.put("/projects/{project_id}/llm-config", response_model=LlmProviderConfigRead)
def write_llm_config(project_id: int, payload: LlmProviderConfigUpdate, db: Session = Depends(get_db)) -> LlmProviderConfigRead:
    config = update_llm_config(db, project_id, payload)
    return LlmProviderConfigRead(**serialize_llm_config(config))


@router.post("/projects/{project_id}/llm-config/test", response_model=LlmProviderTestRead)
async def test_llm_config(
    project_id: int,
    payload: LlmProviderConfigUpdate,
    db: Session = Depends(get_db),
) -> LlmProviderTestRead:
    result = await test_llm_config_for_project(db, project_id, payload)
    return LlmProviderTestRead(**result)


@router.post("/models/{model_id}/llm-explanation", response_model=ModelLlmExplanationRead)
async def create_llm_explanation(
    model_id: int,
    payload: ModelLlmExplanationRequest,
    db: Session = Depends(get_db),
) -> ModelLlmExplanationRead:
    return ModelLlmExplanationRead(**(await explain_model_anomalies(db, model_id, top_k=payload.top_k)))
