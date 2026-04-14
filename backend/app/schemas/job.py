from datetime import datetime

from pydantic import BaseModel


class JobRead(BaseModel):
    id: int
    name: str
    job_type: str
    status: str
    progress: int
    message: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class DemoJobRequest(BaseModel):
    name: str = "demo-training"
    duration_seconds: int = 8
