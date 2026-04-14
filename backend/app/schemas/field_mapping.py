from datetime import datetime

from pydantic import BaseModel


class FieldMappingUpdate(BaseModel):
    mappings: dict[str, str | None]


class FieldMappingRead(BaseModel):
    id: int
    dataset_version_id: int
    mappings: dict[str, str | None]
    confirmed: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
