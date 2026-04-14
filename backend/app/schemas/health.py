from pydantic import BaseModel


class HealthComponent(BaseModel):
    status: str
    detail: str


class HealthRead(BaseModel):
    api: HealthComponent
    sqlite: HealthComponent
    storage: HealthComponent
    ollama: HealthComponent
