from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ImportSession(Base):
    __tablename__ = "import_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), nullable=False, index=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_type: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_file_path: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="preview_ready", nullable=False)
    selected_template_id: Mapped[str] = mapped_column(String(64), default="auto", nullable=False)
    parser_profile: Mapped[str] = mapped_column(String(64), default="auto", nullable=False)
    parse_options: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    cleaning_options: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    field_mapping: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    preview_schema: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    detected_fields: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    preview_rows: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    error_rows: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    confirmed_dataset_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
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
