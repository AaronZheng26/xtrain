from __future__ import annotations

from pathlib import Path
from typing import Any
import uuid

import duckdb
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

STAGING_PATH_KEY = "_staging_path"
BASE_SCHEMA_KEY = "_base_schema"
BASE_DETECTED_FIELDS_KEY = "_base_detected_fields"
PREVIEW_LIMIT = 20


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
    staging_path = _build_import_session_staging_path(project_id)
    _write_parquet(frame, staging_path)
    template_id = _suggest_template_id(suffix, resolved_profile, schema)
    preview_schema, preview_detected = _build_effective_schema_snapshot(schema, detected, {})

    import_session = ImportSession(
        project_id=project_id,
        file_name=upload_file.filename or raw_path.name,
        file_type=suffix.lstrip("."),
        raw_file_path=str(raw_path),
        status="preview_ready",
        selected_template_id=template_id,
        parser_profile=resolved_profile,
        parse_options={
            STAGING_PATH_KEY: str(staging_path),
            BASE_SCHEMA_KEY: schema,
            BASE_DETECTED_FIELDS_KEY: detected,
        },
        cleaning_options={},
        field_mapping=_suggest_field_mapping(schema, detected),
        preview_schema=preview_schema,
        detected_fields=preview_detected,
        preview_rows=_load_preview_rows_from_staging(staging_path, {}, limit=PREVIEW_LIMIT),
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
    _rebuild_session_staging(import_session)
    _refresh_session_preview(import_session)
    db.add(import_session)
    db.commit()
    db.refresh(import_session)
    return import_session


def update_session_parse_options(db: Session, session_id: int, parse_options: dict[str, Any]) -> ImportSession:
    import_session = get_import_session(db, session_id)
    current_options = dict(import_session.parse_options or {})
    current_options.update(parse_options)
    import_session.parse_options = current_options
    parser_profile = parse_options.get("parser_profile")
    if isinstance(parser_profile, str) and parser_profile:
        import_session.parser_profile = parser_profile
    _rebuild_session_staging(import_session)
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

    data_source = DataSource(
        project_id=import_session.project_id,
        file_name=import_session.file_name,
        file_type=import_session.file_type,
        parser_profile=import_session.parser_profile,
        storage_path=import_session.raw_file_path,
        row_count=int(import_session.row_count),
        status="ready",
    )
    db.add(data_source)
    db.commit()
    db.refresh(data_source)

    dataset_dir = get_settings().storage_root_path / "processed" / f"project_{import_session.project_id}"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = dataset_dir / f"dataset_{data_source.id}.parquet"
    if _get_staging_path(import_session):
        _materialize_cleaned_staging_to_parquet(import_session, parquet_path)
        schema, detected = _get_effective_schema_and_detected(import_session)
    else:
        frame = _load_session_frame(import_session)
        frame = _apply_import_cleaning(frame, import_session.cleaning_options)
        schema, detected = _build_schema_snapshot(frame)
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
        row_count=int(import_session.row_count),
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
    import_session.parse_options = _clear_import_session_artifacts(import_session.parse_options)
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
        "parse_options": _serialize_parse_options(import_session.parse_options),
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
    if _get_staging_path(import_session):
        schema, detected = _get_effective_schema_and_detected(import_session)
        import_session.preview_schema = schema
        import_session.detected_fields = detected
        import_session.preview_rows = _load_preview_rows_from_staging(
            _get_staging_path(import_session),
            import_session.cleaning_options,
            limit=PREVIEW_LIMIT,
        )
        import_session.error_rows = []
        import_session.row_count = import_session.row_count or _get_base_row_count(import_session)
        import_session.field_mapping = _refresh_field_mapping(import_session.field_mapping, schema, detected)
        return

    frame = _load_session_frame(import_session)
    frame = _apply_import_cleaning(frame, import_session.cleaning_options)
    schema, detected = _build_schema_snapshot(frame)
    import_session.preview_schema = schema
    import_session.detected_fields = detected
    import_session.preview_rows = json_safe_records(frame.head(PREVIEW_LIMIT))
    import_session.error_rows = []
    import_session.row_count = int(len(frame.index))
    import_session.field_mapping = _refresh_field_mapping(import_session.field_mapping, schema, detected)


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


def _build_import_session_staging_path(project_id: int) -> Path:
    staging_dir = get_settings().storage_root_path / "import_sessions" / f"project_{project_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / f"staging_{uuid.uuid4().hex}.parquet"


def _get_staging_path(import_session: ImportSession) -> Path | None:
    staging_path = import_session.parse_options.get(STAGING_PATH_KEY) if isinstance(import_session.parse_options, dict) else None
    if not staging_path:
        return None
    return get_settings().resolve_storage_path(str(staging_path))


def _get_base_schema(import_session: ImportSession) -> list[dict[str, Any]]:
    base_schema = import_session.parse_options.get(BASE_SCHEMA_KEY) if isinstance(import_session.parse_options, dict) else None
    if isinstance(base_schema, list) and base_schema:
        return [dict(field) for field in base_schema if isinstance(field, dict)]
    return [dict(field) for field in import_session.preview_schema]


def _get_base_detected_fields(import_session: ImportSession) -> dict[str, Any]:
    base_detected = import_session.parse_options.get(BASE_DETECTED_FIELDS_KEY) if isinstance(import_session.parse_options, dict) else None
    if isinstance(base_detected, dict):
        return dict(base_detected)
    return dict(import_session.detected_fields or {})


def _get_base_row_count(import_session: ImportSession) -> int:
    return int(import_session.row_count or 0)


def _rebuild_session_staging(import_session: ImportSession) -> None:
    frame = _load_session_frame(import_session)
    schema, detected = _build_schema_snapshot(frame)
    staging_path = _get_staging_path(import_session) or _build_import_session_staging_path(import_session.project_id)
    _write_parquet(frame, staging_path)

    parse_options = dict(import_session.parse_options or {})
    parse_options[STAGING_PATH_KEY] = str(staging_path)
    parse_options[BASE_SCHEMA_KEY] = schema
    parse_options[BASE_DETECTED_FIELDS_KEY] = detected
    import_session.parse_options = parse_options
    import_session.row_count = int(len(frame.index))


def _get_effective_schema_and_detected(import_session: ImportSession) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return _build_effective_schema_snapshot(
        _get_base_schema(import_session),
        _get_base_detected_fields(import_session),
        import_session.cleaning_options,
    )


def _build_effective_schema_snapshot(
    base_schema: list[dict[str, Any]],
    base_detected_fields: dict[str, Any],
    cleaning_options: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cleaning_options = cleaning_options or {}
    include_columns = [column for column in cleaning_options.get("include_columns", []) if isinstance(column, str)]
    exclude_columns = {column for column in cleaning_options.get("exclude_columns", []) if isinstance(column, str)}
    rename_columns = {
        str(key): str(value)
        for key, value in (cleaning_options.get("rename_columns", {}) or {}).items()
        if isinstance(key, str) and isinstance(value, str) and value.strip()
    }

    available_columns = [str(field.get("name")) for field in base_schema if field.get("name")]
    if include_columns:
        selected_columns = [column for column in include_columns if column in available_columns and column not in exclude_columns]
    else:
        selected_columns = [column for column in available_columns if column not in exclude_columns]

    selected_set = set(selected_columns)
    effective_schema: list[dict[str, Any]] = []
    name_mapping: dict[str, str] = {}

    for field in base_schema:
        source_name = str(field.get("name", ""))
        if source_name not in selected_set:
            continue
        renamed_name = rename_columns.get(source_name, source_name)
        name_mapping[source_name] = renamed_name
        updated_field = dict(field)
        updated_field["name"] = renamed_name
        effective_schema.append(updated_field)

    effective_detected: dict[str, Any] = {}
    for key, values in (base_detected_fields or {}).items():
        if isinstance(values, list):
            renamed_values = [name_mapping[value] for value in values if value in name_mapping]
            effective_detected[key] = renamed_values
        else:
            effective_detected[key] = values

    return effective_schema, effective_detected


def _load_preview_rows_from_staging(
    staging_path: Path,
    cleaning_options: dict[str, Any] | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    query = f"SELECT * FROM ({_build_staging_select_sql(staging_path, cleaning_options)}) LIMIT ?"
    connection = duckdb.connect()
    try:
        frame = connection.execute(query, [limit]).fetch_df()
    finally:
        connection.close()
    return json_safe_records(frame)


def _materialize_cleaned_staging_to_parquet(import_session: ImportSession, parquet_path: Path) -> None:
    staging_path = _get_staging_path(import_session)
    if not staging_path:
        raise HTTPException(status_code=400, detail="Import session staging data is not available")

    query = (
        f"COPY ({_build_staging_select_sql(staging_path, import_session.cleaning_options)}) "
        f"TO {_sql_string_literal(str(parquet_path))} (FORMAT PARQUET)"
    )
    connection = duckdb.connect()
    try:
        connection.execute(query)
    finally:
        connection.close()


def _build_staging_select_sql(staging_path: Path, cleaning_options: dict[str, Any] | None) -> str:
    cleaning_options = cleaning_options or {}
    include_columns = [column for column in cleaning_options.get("include_columns", []) if isinstance(column, str)]
    exclude_columns = {column for column in cleaning_options.get("exclude_columns", []) if isinstance(column, str)}
    rename_columns = {
        str(key): str(value)
        for key, value in (cleaning_options.get("rename_columns", {}) or {}).items()
        if isinstance(key, str) and isinstance(value, str) and value.strip()
    }

    available_columns = _list_staging_columns(staging_path)
    if include_columns:
        selected_columns = [column for column in include_columns if column in available_columns and column not in exclude_columns]
    else:
        selected_columns = [column for column in available_columns if column not in exclude_columns]

    if not selected_columns:
        raise HTTPException(status_code=400, detail="Import cleaning cannot remove all columns")

    projection = ", ".join(
        _build_projected_column_expression(column, rename_columns.get(column))
        for column in selected_columns
    )
    return f"SELECT {projection} FROM read_parquet({_sql_string_literal(str(staging_path))})"


def _list_staging_columns(staging_path: Path) -> list[str]:
    connection = duckdb.connect()
    try:
        description = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_sql_string_literal(str(staging_path))})"
        ).fetchall()
    finally:
        connection.close()
    return [str(row[0]) for row in description]


def _build_projected_column_expression(source_name: str, alias_name: str | None) -> str:
    source_sql = _sql_identifier(source_name)
    if alias_name and alias_name != source_name:
        return f"{source_sql} AS {_sql_identifier(alias_name)}"
    return source_sql


def _sql_identifier(name: str) -> str:
    escaped = str(name).replace('"', '""')
    return f'"{escaped}"'


def _sql_string_literal(value: str) -> str:
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _refresh_field_mapping(
    current_mapping: dict[str, str | None] | None,
    schema: list[dict[str, Any]],
    detected: dict[str, Any],
) -> dict[str, str | None]:
    valid_columns = {str(field.get("name")) for field in schema if field.get("name")}
    suggested = _suggest_field_mapping(schema, detected)
    refreshed: dict[str, str | None] = {}
    for standard_field in STANDARD_FIELD_HINTS:
        existing_value = (current_mapping or {}).get(standard_field)
        if existing_value in valid_columns:
            refreshed[standard_field] = existing_value
        else:
            refreshed[standard_field] = suggested.get(standard_field)
    return refreshed


def _serialize_parse_options(parse_options: dict[str, Any] | None) -> dict[str, Any]:
    parse_options = dict(parse_options or {})
    for key in (STAGING_PATH_KEY, BASE_SCHEMA_KEY, BASE_DETECTED_FIELDS_KEY):
        parse_options.pop(key, None)
    return parse_options


def _clear_import_session_artifacts(parse_options: dict[str, Any] | None) -> dict[str, Any]:
    next_options = dict(parse_options or {})
    staging_path = next_options.get(STAGING_PATH_KEY)
    if staging_path:
        resolved = get_settings().resolve_storage_path(str(staging_path))
        resolved.unlink(missing_ok=True)
    for key in (STAGING_PATH_KEY, BASE_SCHEMA_KEY, BASE_DETECTED_FIELDS_KEY):
        next_options.pop(key, None)
    return next_options


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
