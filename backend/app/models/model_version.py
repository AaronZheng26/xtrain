from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ModelVersion(Base):
    __tablename__ = "model_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), nullable=False, index=True)
    dataset_version_id: Mapped[int] = mapped_column(ForeignKey("dataset_versions.id"), nullable=False, index=True)
    preprocess_pipeline_id: Mapped[int | None] = mapped_column(
        ForeignKey("preprocess_pipelines.id"),
        nullable=True,
        index=True,
    )
    feature_pipeline_id: Mapped[int | None] = mapped_column(
        ForeignKey("feature_pipelines.id"),
        nullable=True,
        index=True,
    )
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="draft", nullable=False)
    target_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    feature_columns: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    training_params: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    report_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    artifact_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        nullable=False,
    )

    @property
    def used_feature_columns(self) -> list[str]:
        value = (self.report_json or {}).get("used_feature_columns", self.feature_columns)
        return list(value) if isinstance(value, list) else list(self.feature_columns or [])

    @property
    def excluded_feature_columns(self) -> list[str]:
        value = (self.report_json or {}).get("excluded_feature_columns", [])
        return list(value) if isinstance(value, list) else []

    @property
    def exclusion_reasons(self) -> dict[str, str]:
        value = (self.report_json or {}).get("exclusion_reasons", {})
        return dict(value) if isinstance(value, dict) else {}
