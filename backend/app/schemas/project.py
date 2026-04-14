from datetime import datetime

from pydantic import BaseModel, Field


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=2000)


class ProjectRead(BaseModel):
    id: int
    name: str
    description: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}
