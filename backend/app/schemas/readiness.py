from typing import Literal

from pydantic import BaseModel


ReadinessLevel = Literal["excellent", "good", "limited", "poor"]


class ReadinessTimeRange(BaseModel):
    start: str | None = None
    end: str | None = None


class DatasetReadinessRead(BaseModel):
    score: int
    level: ReadinessLevel
    row_count: int
    column_count: int
    time_range: ReadinessTimeRange
    detected_fields: dict[str, list[str]]
    missing_fields: list[str]
    recommended_goals: list[str]
    recommended_mode: str
    recommended_templates: list[str]
    warnings: list[str]
    next_steps: list[str]
