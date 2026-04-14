from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), nullable=False, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("data_sources.id"), nullable=False, index=True)
    version_name: Mapped[str] = mapped_column(String(120), nullable=False)
    parser_profile: Mapped[str] = mapped_column(String(64), nullable=False)
    parquet_path: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    label_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    schema_snapshot: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    detected_fields: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )
