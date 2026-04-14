import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_string_dtype,
)
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.preprocess_pipeline import PreprocessPipeline
from app.models.project import Project
from app.schemas.preprocess import PreprocessPipelineCreate
from app.services.dataset_import import (
    build_schema_snapshot,
    get_dataset,
    json_safe_records,
    load_parquet_frame,
    write_parquet,
)
from app.services.field_mapping import get_or_create_field_mapping


def create_preprocess_pipeline(db: Session, payload: PreprocessPipelineCreate) -> PreprocessPipeline:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    dataset = get_dataset(db, payload.dataset_version_id)
    if dataset.project_id != payload.project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")

    pipeline = PreprocessPipeline(
        project_id=payload.project_id,
        dataset_version_id=payload.dataset_version_id,
        name=payload.name,
        status="running",
        steps=[_normalize_step(step.model_dump()) for step in payload.steps],
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)

    frame = load_parquet_frame(dataset.parquet_path)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    frame = apply_field_mapping(frame, mapping)
    frame = _apply_steps(frame, pipeline.steps)

    output_dir = get_settings().storage_root_path / "preprocessed" / f"project_{payload.project_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"preprocess_{pipeline.id}_{uuid.uuid4().hex[:8]}.parquet"
    write_parquet(frame, output_path)
    output_schema, _ = build_schema_snapshot(frame)

    pipeline.status = "completed"
    pipeline.output_path = str(output_path)
    pipeline.output_row_count = int(len(frame.index))
    pipeline.output_schema = output_schema
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


def list_preprocess_pipelines(db: Session, project_id: int, dataset_version_id: int | None = None) -> list[PreprocessPipeline]:
    query = select(PreprocessPipeline).where(PreprocessPipeline.project_id == project_id)
    if dataset_version_id is not None:
        query = query.where(PreprocessPipeline.dataset_version_id == dataset_version_id)
    query = query.order_by(PreprocessPipeline.created_at.desc())
    return list(db.scalars(query))


def get_preprocess_pipeline(db: Session, pipeline_id: int) -> PreprocessPipeline:
    pipeline = db.get(PreprocessPipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Preprocess pipeline not found")
    return pipeline


def preview_preprocess_pipeline(pipeline: PreprocessPipeline, limit: int = 20) -> dict[str, Any]:
    if not pipeline.output_path:
        raise HTTPException(status_code=400, detail="Pipeline has no output preview yet")
    frame = load_parquet_frame(pipeline.output_path, limit=min(max(limit, 1), 50))
    return {
        "pipeline_id": pipeline.id,
        "columns": list(frame.columns),
        "rows": json_safe_records(frame),
    }


def preview_preprocess_step(
    db: Session,
    project_id: int,
    dataset_version_id: int,
    steps: list[dict[str, Any]],
    preview_step_index: int,
    limit: int = 8,
) -> dict[str, Any]:
    dataset = get_dataset(db, dataset_version_id)
    if dataset.project_id != project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")
    if preview_step_index >= len(steps):
        raise HTTPException(status_code=400, detail="preview_step_index is out of range")

    frame = load_parquet_frame(dataset.parquet_path)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    current = apply_field_mapping(frame, mapping)
    normalized_steps = [_normalize_step(step) for step in steps]

    for step in normalized_steps[:preview_step_index]:
        current = _apply_single_step(current, step)

    before_frame = current.copy()
    target_step = normalized_steps[preview_step_index]
    after_frame = _apply_single_step(current, target_step)

    before_columns = list(before_frame.columns)
    after_columns = list(after_frame.columns)

    return {
        "preview_step_index": preview_step_index,
        "step": target_step,
        "before_row_count": int(len(before_frame.index)),
        "after_row_count": int(len(after_frame.index)),
        "before_columns": before_columns,
        "after_columns": after_columns,
        "added_columns": [column for column in after_columns if column not in before_columns],
        "removed_columns": [column for column in before_columns if column not in after_columns],
        "before_rows": json_safe_records(before_frame.head(limit)),
        "after_rows": json_safe_records(after_frame.head(limit)),
    }


def apply_field_mapping(frame: pd.DataFrame, mappings: dict[str, str | None]) -> pd.DataFrame:
    frame = frame.copy()
    for target, source in mappings.items():
        if source and source in frame.columns:
            frame[target] = frame[source]
    return frame


def _apply_steps(frame: pd.DataFrame, steps: list[dict]) -> pd.DataFrame:
    current = frame.copy()
    for raw_step in steps:
        step = _normalize_step(raw_step)
        current = _apply_single_step(current, step)

    return current.reset_index(drop=True)


def _apply_single_step(frame: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
    current = frame.copy()
    if not step["enabled"]:
        return current

    step_type = step["step_type"]
    params = step["params"]
    columns = _resolve_selector_columns(current, step)

    if step_type == "fill_null":
        value = params.get("value")
        for column in columns:
            transformed = current[column].astype("object").where(current[column].notna(), value)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns))
    elif step_type == "cast_type":
        target_type = params.get("target_type")
        for column in columns:
            transformed = _cast_series(current[column], target_type)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns))
    elif step_type == "trim_text":
        for column in columns:
            transformed = current[column].astype("string").str.strip()
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns))
    elif step_type == "lowercase":
        for column in columns:
            transformed = current[column].astype("string").str.lower()
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns))
    elif step_type == "normalize_datetime":
        input_format = params.get("input_format")
        output_format = params.get("output_format") or "%Y-%m-%d %H:%M:%S"
        for column in columns:
            parsed = pd.to_datetime(current[column], errors="coerce", format=input_format or None)
            transformed = parsed.dt.strftime(output_format).where(parsed.notna(), None)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns))
    elif step_type == "rename_columns":
        rename_map = params.get("rename_map", {})
        if not isinstance(rename_map, dict):
            raise HTTPException(status_code=400, detail="rename_map must be an object")
        valid_map = {
            str(source): str(target)
            for source, target in rename_map.items()
            if source in current.columns and isinstance(target, str) and target.strip()
        }
        if valid_map:
            current = current.rename(columns=valid_map)
    elif step_type == "filter_rows":
        current = _filter_rows(current, columns, params)
    elif step_type == "drop_duplicates":
        subset = columns or [column for column in params.get("subset", []) if column in current.columns]
        current = current.drop_duplicates(subset=subset or None)
    elif step_type == "select_columns":
        selected_columns = columns or [column for column in params.get("columns", []) if column in current.columns]
        if selected_columns:
            current = current.loc[:, selected_columns]
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported preprocess step: {step_type}")

    return current.reset_index(drop=True)


def _normalize_step(step: dict[str, Any]) -> dict[str, Any]:
    step_type = step.get("step_type") or step.get("type")
    if not step_type:
        raise HTTPException(status_code=400, detail="Preprocess step is missing step_type")

    return {
        "step_id": step.get("step_id") or step_type,
        "step_type": step_type,
        "type": step_type,
        "enabled": bool(step.get("enabled", True)),
        "input_selector": step.get("input_selector") or {},
        "params": step.get("params") or {},
        "output_mode": step.get("output_mode") or {},
    }


def _resolve_selector_columns(frame: pd.DataFrame, step: dict[str, Any]) -> list[str]:
    selector = step.get("input_selector") or {}
    params = step.get("params") or {}
    mode = selector.get("mode")

    if mode == "explicit":
        raw_columns = selector.get("columns", [])
    elif mode == "dtype":
        raw_columns = _select_columns_by_dtype(frame, selector.get("dtype"))
    elif params.get("columns"):
        raw_columns = params.get("columns", [])
    elif params.get("column"):
        raw_columns = [params.get("column")]
    else:
        raw_columns = []

    return [column for column in raw_columns if column in frame.columns]


def _select_columns_by_dtype(frame: pd.DataFrame, dtype: str | None) -> list[str]:
    if dtype == "numeric":
        return [column for column in frame.columns if is_numeric_dtype(frame[column])]
    if dtype == "datetime":
        return [column for column in frame.columns if is_datetime64_any_dtype(frame[column])]
    if dtype == "string":
        return [column for column in frame.columns if is_string_dtype(frame[column]) or frame[column].dtype == "object"]
    return []


def _write_output_series(
    frame: pd.DataFrame,
    source_column: str,
    series: pd.Series,
    output_mode: dict[str, Any],
    selected_column_count: int,
) -> pd.DataFrame:
    mode = output_mode.get("mode") or "inplace"
    if mode == "inplace":
        frame[source_column] = series
        return frame

    if mode == "new_column":
        output_column = output_mode.get("output_column")
        suffix = output_mode.get("suffix") or "_processed"

        if selected_column_count > 1 and output_column:
            raise HTTPException(
                status_code=400,
                detail="output_column can only be used when a preprocess step targets a single field",
            )

        target_column = output_column or f"{source_column}{suffix}"
        frame[target_column] = series
        return frame

    raise HTTPException(status_code=400, detail=f"Unsupported preprocess output mode: {mode}")


def _filter_rows(frame: pd.DataFrame, columns: list[str], params: dict[str, Any]) -> pd.DataFrame:
    if not columns:
        return frame

    column = columns[0]
    operator = params.get("operator") or "eq"
    value = params.get("value")
    series = frame[column]

    if operator == "eq":
        mask = series.astype("string") == str(value)
    elif operator == "ne":
        mask = series.astype("string") != str(value)
    elif operator == "contains":
        mask = series.astype("string").str.contains(str(value), case=False, na=False)
    elif operator == "gt":
        mask = pd.to_numeric(series, errors="coerce") > pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    elif operator == "gte":
        mask = pd.to_numeric(series, errors="coerce") >= pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    elif operator == "lt":
        mask = pd.to_numeric(series, errors="coerce") < pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    elif operator == "lte":
        mask = pd.to_numeric(series, errors="coerce") <= pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    elif operator == "is_null":
        mask = series.isna()
    elif operator == "not_null":
        mask = series.notna()
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported filter operator: {operator}")

    return frame.loc[mask].copy()


def _cast_series(series: pd.Series, target_type: str | None) -> pd.Series:
    if target_type == "string":
        return series.astype("string")
    if target_type == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if target_type == "float":
        return pd.to_numeric(series, errors="coerce")
    if target_type == "datetime":
        return pd.to_datetime(series, errors="coerce")
    return series
