import json
import re
import uuid
import warnings
from collections import Counter
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import HTTPException, UploadFile
from pandas.api.types import is_numeric_dtype
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.data_source import DataSource
from app.models.dataset_version import DatasetVersion
from app.models.project import Project

NGINX_LOG_PATTERN = re.compile(
    r'(?P<remote_addr>\S+)\s+\S+\s+\S+\s+\[(?P<event_time>[^\]]+)\]\s+"(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<protocol>[^"]+)"\s+(?P<status_code>\d{3})\s+(?P<bytes_sent>\S+)(?:\s+"(?P<referer>[^"]*)"\s+"(?P<user_agent>[^"]*)")?'
)
GENERIC_LOG_PATTERN = re.compile(
    r'^(?P<event_time>\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s+(?P<level>[A-Z]+)\s+(?:(?P<source>[\w.\-]+)\s+)?(?P<message>.*)$'
)
LEVEL_PATTERN = re.compile(r"\b(INFO|WARN|WARNING|ERROR|DEBUG|TRACE|FATAL|CRITICAL)\b")
LABEL_HINTS = {"label", "labels", "is_anomaly", "anomaly", "target", "class", "y"}
TIMESTAMP_HINTS = {"time", "timestamp", "date", "datetime", "event_time"}


def import_dataset(
    db: Session,
    *,
    project_id: int,
    upload_file: UploadFile,
    parser_profile: str | None = None,
) -> tuple[DataSource, DatasetVersion]:
    project = db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    suffix = Path(upload_file.filename or "").suffix.lower()
    file_type = suffix.lstrip(".")
    if suffix not in {".log", ".csv", ".xlsx"}:
        raise HTTPException(status_code=400, detail="Only .log, .csv, and .xlsx files are supported")

    raw_path = _persist_upload(project_id, upload_file, suffix)
    dataframe, resolved_profile = _load_dataframe(raw_path, suffix, parser_profile)
    schema_snapshot, detected_fields = _build_schema_snapshot(dataframe)

    data_source = DataSource(
        project_id=project_id,
        file_name=upload_file.filename or raw_path.name,
        file_type=file_type,
        parser_profile=resolved_profile,
        storage_path=str(raw_path),
        row_count=int(len(dataframe.index)),
        status="ready",
    )
    db.add(data_source)
    db.commit()
    db.refresh(data_source)

    dataset_dir = get_settings().storage_root_path / "processed" / f"project_{project_id}"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = dataset_dir / f"dataset_{data_source.id}.parquet"
    _write_parquet(dataframe, parquet_path)

    version_index = db.scalar(
        select(func.count()).select_from(DatasetVersion).where(DatasetVersion.project_id == project_id)
    )
    label_candidates = detected_fields.get("label_candidates") or []
    dataset_version = DatasetVersion(
        project_id=project_id,
        source_id=data_source.id,
        version_name=f"dataset-v{(version_index or 0) + 1}",
        parser_profile=resolved_profile,
        parquet_path=str(parquet_path),
        row_count=int(len(dataframe.index)),
        label_column=label_candidates[0] if label_candidates else None,
        schema_snapshot=schema_snapshot,
        detected_fields=detected_fields,
    )
    db.add(dataset_version)
    db.commit()
    db.refresh(dataset_version)
    return data_source, dataset_version


def list_datasets(db: Session, project_id: int) -> list[DatasetVersion]:
    return list(
        db.scalars(
            select(DatasetVersion)
            .where(DatasetVersion.project_id == project_id)
            .order_by(DatasetVersion.created_at.desc())
        )
    )


def get_dataset(db: Session, dataset_id: int) -> DatasetVersion:
    dataset = db.get(DatasetVersion, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


def preview_dataset(dataset: DatasetVersion, limit: int = 20) -> dict:
    query = "SELECT * FROM read_parquet(?) LIMIT ?"
    connection = duckdb.connect()
    try:
        parquet_path = str(get_settings().resolve_storage_path(dataset.parquet_path))
        frame = connection.execute(query, [parquet_path, limit]).fetch_df()
    finally:
        connection.close()

    return {
        "dataset_id": dataset.id,
        "columns": list(frame.columns),
        "rows": json_safe_records(frame),
    }


def load_parquet_frame(parquet_path: str, limit: int | None = None) -> pd.DataFrame:
    connection = duckdb.connect()
    try:
        resolved_path = str(get_settings().resolve_storage_path(parquet_path))
        if limit is None:
            query = "SELECT * FROM read_parquet(?)"
            return connection.execute(query, [resolved_path]).fetch_df()
        query = "SELECT * FROM read_parquet(?) LIMIT ?"
        return connection.execute(query, [resolved_path, limit]).fetch_df()
    finally:
        connection.close()


def build_schema_snapshot(frame: pd.DataFrame) -> tuple[list[dict], dict]:
    return _build_schema_snapshot(frame)


def write_parquet(frame: pd.DataFrame, parquet_path: Path) -> None:
    _write_parquet(frame, parquet_path)


def json_safe_records(frame: pd.DataFrame) -> list[dict]:
    return _to_json_safe_records(frame)


def _persist_upload(project_id: int, upload_file: UploadFile, suffix: str) -> Path:
    settings = get_settings()
    raw_dir = settings.storage_root_path / "raw" / f"project_{project_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4().hex}{suffix}"
    raw_path = raw_dir / file_name
    payload = upload_file.file.read()
    raw_path.write_bytes(payload)
    return raw_path


def _load_dataframe(raw_path: Path, suffix: str, parser_profile: str | None) -> tuple[pd.DataFrame, str]:
    if suffix == ".csv":
        frame = pd.read_csv(raw_path)
        return _normalize_dataframe(frame), parser_profile or "generic_csv"

    if suffix == ".xlsx":
        frame = pd.read_excel(raw_path)
        return _normalize_dataframe(frame), parser_profile or "generic_xlsx"

    text = raw_path.read_text(encoding="utf-8", errors="ignore")
    rows, resolved_profile = _parse_log_rows(text, parser_profile)
    frame = pd.DataFrame(rows)
    return _normalize_dataframe(frame), resolved_profile


def _parse_log_rows(content: str, parser_profile: str | None) -> tuple[list[dict], str]:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="The uploaded file is empty")

    if parser_profile == "nginx_access" or _is_nginx_log(lines):
        return [_parse_nginx_line(line) for line in lines], "nginx_access"

    return [_parse_generic_log_line(line) for line in lines], parser_profile or "generic_log"


def _is_nginx_log(lines: list[str]) -> bool:
    sample = lines[:10]
    matches = sum(1 for line in sample if NGINX_LOG_PATTERN.match(line))
    return matches >= max(2, len(sample) // 2)


def _parse_nginx_line(line: str) -> dict:
    match = NGINX_LOG_PATTERN.match(line)
    if not match:
        return {"raw_line": line, "log_type": "generic_log"}

    parsed = match.groupdict()
    parsed["bytes_sent"] = _normalize_scalar(parsed.get("bytes_sent"))
    parsed["status_code"] = _normalize_scalar(parsed.get("status_code"))
    parsed["log_type"] = "nginx_access"
    parsed["raw_line"] = line
    return parsed


def _parse_generic_log_line(line: str) -> dict:
    match = GENERIC_LOG_PATTERN.match(line)
    if match:
        parsed = match.groupdict()
    else:
        level_match = LEVEL_PATTERN.search(line)
        parsed = {
            "event_time": None,
            "level": level_match.group(1) if level_match else None,
            "source": None,
            "message": line,
        }
    parsed["log_type"] = "generic_log"
    parsed["raw_line"] = line
    return parsed


def _normalize_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = _make_unique_columns([str(column).strip() or "unnamed" for column in frame.columns])
    frame = frame.where(pd.notnull(frame), None)
    return frame


def _make_unique_columns(columns: list[str]) -> list[str]:
    seen: Counter[str] = Counter()
    result: list[str] = []
    for column in columns:
        normalized = re.sub(r"\s+", "_", column)
        seen[normalized] += 1
        if seen[normalized] == 1:
            result.append(normalized)
        else:
            result.append(f"{normalized}_{seen[normalized]}")
    return result


def _build_schema_snapshot(frame: pd.DataFrame) -> tuple[list[dict], dict]:
    schema_snapshot: list[dict] = []
    timestamp_candidates: list[str] = []
    label_candidates: list[str] = []
    numeric_fields: list[str] = []
    categorical_fields: list[str] = []
    text_fields: list[str] = []

    for column in frame.columns:
        series = frame[column]
        sample_values = [str(value) for value in series.dropna().astype(str).head(5).tolist()]
        null_count = int(series.isna().sum())
        dtype_name = str(series.dtype)
        candidate_roles: list[str] = []
        column_name = column.lower()

        if _is_timestamp_candidate(column_name, series):
            timestamp_candidates.append(column)
            candidate_roles.append("timestamp")

        if column_name in LABEL_HINTS or column_name.endswith("_label"):
            label_candidates.append(column)
            candidate_roles.append("label")

        if is_numeric_dtype(series):
            numeric_fields.append(column)
            candidate_roles.append("numeric")
        else:
            unique_count = int(series.nunique(dropna=True))
            average_length = float(series.dropna().astype(str).str.len().mean() or 0)
            if unique_count <= 30 or unique_count <= max(5, len(series.index) * 0.2):
                categorical_fields.append(column)
                candidate_roles.append("categorical")
            if average_length >= 24 or "message" in column_name or "raw" in column_name:
                text_fields.append(column)
                candidate_roles.append("text")

        schema_snapshot.append(
            {
                "name": column,
                "dtype": dtype_name,
                "null_count": null_count,
                "non_null_count": int(len(series.index) - null_count),
                "sample_values": sample_values,
                "candidate_roles": candidate_roles,
            }
        )

    detected_fields = {
        "timestamp_candidates": timestamp_candidates,
        "label_candidates": label_candidates,
        "numeric_fields": numeric_fields,
        "categorical_fields": categorical_fields,
        "text_fields": text_fields,
    }
    return schema_snapshot, detected_fields


def _is_timestamp_candidate(column_name: str, series: pd.Series) -> bool:
    if any(hint in column_name for hint in TIMESTAMP_HINTS):
        return True
    if series.dropna().empty:
        return False

    sample = series.dropna().astype(str).head(20)
    if not sample.str.contains(r"\d").all():
        return False
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed = pd.to_datetime(sample, errors="coerce")
    success_ratio = parsed.notna().sum() / max(len(sample), 1)
    return success_ratio >= 0.8


def _write_parquet(frame: pd.DataFrame, parquet_path: Path) -> None:
    connection = duckdb.connect()
    try:
        relation = connection.from_df(frame)
        relation.write_parquet(str(parquet_path))
    finally:
        connection.close()


def _normalize_scalar(value: str | None) -> int | None | str:
    if value is None or value == "-":
        return None
    if value.isdigit():
        return int(value)
    return value


def _to_json_safe_records(frame: pd.DataFrame) -> list[dict]:
    records = json.loads(frame.to_json(orient="records", force_ascii=False))
    return records
