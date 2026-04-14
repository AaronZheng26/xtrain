from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class LlmProviderConfigUpdate(BaseModel):
    provider: str = Field(default="ollama")
    enabled: bool = True
    base_url: str = Field(default="", max_length=1000)
    model_name: str = Field(default="", max_length=120)
    api_key: str | None = Field(default=None, max_length=4000)
    clear_api_key: bool = False


class LlmProviderConfigRead(BaseModel):
    id: int
    project_id: int
    provider: str
    enabled: bool
    base_url: str
    model_name: str
    has_api_key: bool
    api_key_hint: str | None = None
    created_at: datetime
    updated_at: datetime


class ModelLlmExplanationRequest(BaseModel):
    top_k: int = Field(default=5, ge=1, le=15)


class ModelLlmExplanationRead(BaseModel):
    model_id: int
    provider: str
    model_name: str
    analyzed_rows: int
    explanation: str
    final_content: str
    reasoning_content: str | None = None
    source_columns: list[str]
    source_rows: list[dict[str, Any]]
    generated_at: datetime


class LlmProviderTestRead(BaseModel):
    provider: str
    model_name: str
    base_url: str
    success: bool
    detail: str
