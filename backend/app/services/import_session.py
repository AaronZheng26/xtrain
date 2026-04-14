from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import HTTPException, UploadFile
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.data_source import DataSource
from app.models.dataset_version import DatasetVersion
from app.models.field_mapping import FieldMapping
from app.models.import_session import ImportSession
from app.models.project import Project
from app.services.dataset_import import (
    _build_schema_snapshot,
    _load_dataframe,
    _persist_upload,
    _write_parquet,
    json_safe_records,
)


LOG_TEMPLATES = [
    {
        "id": "nginx_access",
        "name": "Nginx Access",
        "log_type": "web_access",
        "description": "适用于 Nginx/Apache 风格访问日志。",
        "parser_profile": "nginx_access",
    },
    {
        "id": "program_runtime",
        "name": "程序运行日志",
        "log_type": "application",
        "description": "适用于包含时间、级别、模块和消息的程序日志。",
        "parser_profile": "generic_log",
    },
    {
        "id": "nta_flow",
        "name": "NTA 流量日志",
        "log_type": "network_flow",
        "description": "适用于源/目的 IP、端口、协议、流量统计类日志。",
        "parser_profile": "generic_csv",
    },
    {
        "id": "generic_csv",
        "name": "通用 CSV",
        "log_type": "table",
        "description": "适用于字段已经结构化的 CSV 文件。",
        "parser_profile": "generic_csv",
    },
    {
        "id": "generic_xlsx",
        "name": "通用 Excel",
        "log_type": "table",
        "description": "适用于字段已经结构化的 Excel 文件。",
        "parser_profile": "generic_xlsx",
    },
    {
        "id": "generic_log",
        "name": "通用文本日志",
        "log_type": "text_log",
        "description": "适用于无法匹配专用模板的通用文本日志。",
        "parser_profile": "generic_log",
    },
]

STANDARD_FIELD_HINTS = {
    "event_time": ["event_time", "timestamp", "time", "datetime", "date"],
    "source_ip": ["source_ip", "src_ip", "remote_addr", "client_ip"],
    "dest_ip": ["dest_ip", "dst_ip", "destination_ip", "server_ip"],
    "src_port": ["src_port", "source_port", "sport"],
    "dest_port": ["dest_port", "dst_port", "destination_port", "dport"],
    "protocol": ["protocol", "proto"],
    "status_code": ["status_code", "status", "code"],
    "username": ["username", "user", "account"],
    "host": ["host", "hostname", "server", "source"],
    "process_name": ["process_name", "process", "program", "app"],
    "severity": ["severity", "level", "log_level"],
    "label": ["label", "is_anomaly", "anomaly", "target", "class"],
    "raw_message": ["raw_message", "raw_line", "message", "msg"],
}


def list_log_templates() -> list[dict[str, str]]:
    return LOG_TEMPLATES


def create_import_session(
    db: Session,
    *,
    project_id: int,
    upload_file: UploadFile,
    parser_profile: str | None = None,
) -> ImportSession:
    project = db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    suffix = Path(upload_file.filename or "").suffix.lower()
    if suffix not in {".log", ".csv", ".xlsx"}:
        raise HTTPException(status_code=400, detail="Only .log, .csv, and .xlsx files are supported")

    raw_path = _persist_upload(project_id, upload_file, suffix)
    frame, resolved_profile = _load_dataframe(raw_path, suffix, parser_profile)
    schema, detected = _build_schema_snapshot(frame)
    template_id = _suggest_template_id(suffix, resolved_profile, schema)

    import_session = ImportSession(
        project_id=project_id,
        file_name=upload_file.filename or raw_path.name,
        file_type=suffix.lstrip("."),
        raw_file_path=str(raw_path),
        status="preview_ready",
        selected_template_id=template_id,
        parser_profile=resolved_profile,
        parse_options={},
        cleaning_options={},
        field_mapping=_suggest_field_mapping(schema, detected),
        preview_schema=schema,
        detected_fields=detected,
        preview_rows=json_safe_records(frame.head(20)),
        error_rows=[],
        row_count=int(len(frame.index)),
    )
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def get_import_session(db: Session, session_id: int) -> ImportSession:
    import_session = db.get(ImportSession, session_id)
    if not import_session:
        raise HTTPException(status_code=404, detail="Import session not found")
    return import_session


def update_session_template(db: Session, session_id: int, template_id: str) -> ImportSession:
    import_session = get_import_session(db, session_id)
    template = _get_template(template_id)
    import_session.selected_template_id = template["id"]
    import_session.parser_profile = template["parser_profile"]
    _refresh_session_preview(import_session)
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def update_session_parse_options(db: Session, session_id: int, parse_options: dict[str, Any]) -> ImportSession:
    import_session = get_import_session(db, session_id)
    import_session.parse_options = parse_options
    parser_profile = parse_options.get("parser_profile")
    if isinstance(parser_profile, str) and parser_profile:
        import_session.parser_profile = parser_profile
    _refresh_session_preview(import_session)
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def update_session_cleaning_options(db: Session, session_id: int, cleaning_options: dict[str, Any]) -> ImportSession:
    import_session = get_import_session(db, session_id)
    import_session.cleaning_options = cleaning_options
    _refresh_session_preview(import_session)
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def update_session_field_mapping(db: Session, session_id: int, field_mapping: dict[str, str | None]) -> ImportSession:
    import_session = get_import_session(db, session_id)
    valid_columns = {field["name"] for field in import_session.preview_schema}
    sanitized: dict[str, str | None] = {}
    for standard_field, source_field in field_mapping.items():
        if source_field and source_field not in valid_columns:
            raise HTTPException(status_code=400, detail=f"Column '{source_field}' does not exist in import preview")
        sanitized[standard_field] = source_field
    import_session.field_mapping = sanitized
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def confirm_import_session(db: Session, session_id: int) -> tuple[ImportSession, DataSource, DatasetVersion]:
    import_session = get_import_session(db, session_id)
    if import_session.confirmed_dataset_version_id is not None:
        dataset = db.get(DatasetVersion, import_session.confirmed_dataset_version_id)
        if dataset:
            data_source = db.get(DataSource, dataset.source_id)
            if data_source:
                return import_session, data_source, dataset

    frame = _load_session_frame(import_session)
    frame = _apply_import_cleaning(frame, import_session.cleaning_options)
    schema, detected = _build_schema_snapshot(frame)

    data_source = DataSource(
        project_id=import_session.project_id,
        file_name=import_session.file_name,
        file_type=import_session.file_type,
        parser_profile=import_session.parser_profile,
        storage_path=import_session.raw_file_path,
        row_count=int(len(frame.index)),
        status="ready",
    )
    db.add(data_source)
    db.commit()
    db.refresh(data_source)

    dataset_dir = get_settings().storage_root_path / "processed" / f"project_{import_session.project_id}"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = dataset_dir / f"dataset_{data_source.id}.parquet"
    _write_parquet(frame, parquet_path)

    version_index = db.scalar(
        select(func.count()).select_from(DatasetVersion).where(DatasetVersion.project_id == import_session.project_id)
    )
    label_column = import_session.field_mapping.get("label") or (detected.get("label_candidates") or [None])[0]
    dataset_version = DatasetVersion(
        project_id=import_session.project_id,
        source_id=data_source.id,
        version_name=f"dataset-v{(version_index or 0) + 1}",
        parser_profile=import_session.parser_profile,
        parquet_path=str(parquet_path),
        row_count=int(len(frame.index)),
        label_column=label_column,
        schema_snapshot=schema,
        detected_fields=detected,
    )
    db.add(dataset_version)
    db.commit()
    db.refresh(dataset_version)

    if import_session.field_mapping:
        db.add(
            FieldMapping(
                dataset_version_id=dataset_version.id,
                mappings=import_session.field_mapping,
                confirmed=True,
            )
        )

    import_session.status = "confirmed"
    import_session.confirmed_dataset_version_id = dataset_version.id
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session, data_source, dataset_version


def serialize_import_session(import_session: ImportSession) -> dict[str, Any]:
    return {
        "id": import_session.id,
        "project_id": import_session.project_id,
        "file_name": import_session.file_name,
        "file_type": import_session.file_type,
        "raw_file_path": import_session.raw_file_path,
        "status": import_session.status,
        "selected_template_id": import_session.selected_template_id,
        "parser_profile": import_session.parser_profile,
        "parse_options": import_session.parse_options,
        "cleaning_options": import_session.cleaning_options,
        "field_mapping": import_session.field_mapping,
        "preview_schema": import_session.preview_schema,
        "detected_fields": import_session.detected_fields,
        "preview_rows": import_session.preview_rows,
        "error_rows": import_session.error_rows,
        "row_count": import_session.row_count,
        "confirmed_dataset_version_id": import_session.confirmed_dataset_version_id,
        "template_suggestions": _suggest_templates(import_session),
        "created_at": import_session.created_at,
        "updated_at": import_session.updated_at,
    }


def _refresh_session_preview(import_session: ImportSession) -> None:
    frame = _load_session_frame(import_session)
    frame = _apply_import_cleaning(frame, import_session.cleaning_options)
    schema, detected = _build_schema_snapshot(frame)
    import_session.preview_schema = schema
    import_session.detected_fields = detected
    import_session.preview_rows = json_safe_records(frame.head(20))
    import_session.error_rows = []
    import_session.row_count = int(len(frame.index))
    if not import_session.field_mapping:
        import_session.field_mapping = _suggest_field_mapping(schema, detected)


def _load_session_frame(import_session: ImportSession) -> pd.DataFrame:
    raw_path = get_settings().resolve_storage_path(import_session.raw_file_path)
    suffix = f".{import_session.file_type}"
    frame, resolved_profile = _load_dataframe(raw_path, suffix, import_session.parser_profile)
    import_session.parser_profile = resolved_profile
    return frame


def _apply_import_cleaning(frame: pd.DataFrame, options: dict[str, Any] | None) -> pd.DataFrame:
    current = frame.copy()
    options = options or {}

    include_columns = [column for column in options.get("include_columns", []) if column in current.columns]
    exclude_columns = [column for column in options.get("exclude_columns", []) if column in current.columns]
    rename_columns = options.get("rename_columns", {})

    if include_columns:
        current = current.loc[:, include_columns]
    if exclude_columns:
        current = current.drop(columns=exclude_columns)
    if isinstance(rename_columns, dict) and rename_columns:
        current = current.rename(columns={key: value for key, value in rename_columns.items() if key in current.columns and value})

    return current.reset_index(drop=True)


def _suggest_field_mapping(schema: list[dict], detected: dict) -> dict[str, str | None]:
    columns = [field["name"] for field in schema]
    lowered = {column.lower(): column for column in columns}

    def pick(hints: list[str]) -> str | None:
        for hint in hints:
            if hint in lowered:
                return lowered[hint]
        for column in columns:
            column_lower = column.lower()
            if any(hint in column_lower for hint in hints):
                return column
        return None

    mapping = {field: pick(hints) for field, hints in STANDARD_FIELD_HINTS.items()}
    if detected.get("timestamp_candidates"):
        mapping["event_time"] = detected["timestamp_candidates"][0]
    if detected.get("label_candidates"):
        mapping["label"] = detected["label_candidates"][0]
    return mapping


def _suggest_template_id(suffix: str, parser_profile: str, schema: list[dict]) -> str:
    if parser_profile == "nginx_access":
        return "nginx_access"
    if suffix == ".xlsx":
        return "generic_xlsx"
    if suffix == ".log":
        return "generic_log"

    columns = {field["name"].lower() for field in schema}
    if {"source_ip", "dest_ip"} & columns and {"protocol", "src_port", "dest_port"} & columns:
        return "nta_flow"
    return "generic_csv"


def _suggest_templates(import_session: ImportSession) -> list[dict[str, str]]:
    selected = import_session.selected_template_id
    ordered = sorted(LOG_TEMPLATES, key=lambda template: 0 if template["id"] == selected else 1)
    return ordered


def _get_template(template_id: str) -> dict[str, str]:
    for template in LOG_TEMPLATES:
        if template["id"] == template_id:
            return template
    raise HTTPException(status_code=400, detail=f"Unsupported log template: {template_id}")
