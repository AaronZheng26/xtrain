import ipaddress
import math
import re
import uuid
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from fastapi import HTTPException
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.feature_pipeline import FeaturePipeline
from app.models.feature_template import FeatureTemplate
from app.models.preprocess_pipeline import PreprocessPipeline
from app.models.project import Project
from app.schemas.feature import FeaturePipelineCreate, FeatureTemplateCreate
from app.services.dataset_import import (
    build_schema_snapshot,
    get_dataset,
    json_safe_records,
    load_parquet_frame,
    write_parquet,
)
from app.services.field_mapping import get_or_create_field_mapping
from app.services.preprocess import apply_field_mapping

TOKEN_PATTERN = re.compile(r"[\w\-.:/]+", re.UNICODE)
ROLE_TAG_PATTERNS: dict[str, tuple[str, ...]] = {
    "text": ("message", "payload", "body", "content", "query"),
    "path": ("path", "uri", "url", "endpoint"),
    "user_agent": ("useragent", "ua", "browser"),
    "domain": ("domain", "host", "hostname", "fqdn"),
    "ip": ("ip", "sourceip", "destip", "srcip", "dstip"),
}
PATTERN_LIBRARY: dict[str, re.Pattern[str]] = {
    "ip": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "url": re.compile(r"https?://[^\s]+", re.IGNORECASE),
    "hash": re.compile(r"\b[a-fA-F0-9]{32,64}\b"),
    "hex_like": re.compile(r"\b(?:0x)?[a-fA-F0-9]{8,}\b"),
    "base64_like": re.compile(r"\b[A-Za-z0-9+/]{16,}={0,2}\b"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
}

BUILTIN_FEATURE_TEMPLATES: list[dict[str, Any]] = [
    {
        "id": "builtin:nginx_access",
        "scope": "builtin",
        "name": "Nginx 访问日志特征模板",
        "log_type": "nginx_access",
        "description": "适合 Nginx 访问日志，保留原字段并追加路径、状态码、UA 和流量相关特征。",
        "field_hints": {
            "required_columns": ["path", "status_code"],
            "optional_columns": ["event_time", "method", "user_agent", "bytes_sent", "request_duration"],
        },
        "steps": [
            {
                "step_id": "nginx_path_features",
                "step_type": "path_features",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["path"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "nginx_status_category",
                "step_type": "status_category",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["status_code"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "nginx_method_encode",
                "step_type": "category_encode",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["method"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_code"},
            },
            {
                "step_id": "nginx_ua_length",
                "step_type": "text_length",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["user_agent"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_length"},
            },
            {
                "step_id": "nginx_path_entropy",
                "step_type": "shannon_entropy",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["path"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_entropy"},
            },
            {
                "step_id": "nginx_ua_complexity",
                "step_type": "char_composition",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["user_agent"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "nginx_admin_flag",
                "step_type": "boolean_flag",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["path"]},
                "params": {"operator": "contains", "value": "admin"},
                "output_mode": {"mode": "append_new_columns", "output_column": "path_has_admin"},
            },
            {
                "step_id": "nginx_bytes_bucket",
                "step_type": "numeric_bucket",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["bytes_sent"]},
                "params": {"bins": 5},
                "output_mode": {"mode": "append_new_columns", "suffix": "_bucket"},
            },
            {
                "step_id": "nginx_duration_bucket",
                "step_type": "numeric_bucket",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["request_duration"]},
                "params": {"bins": 5},
                "output_mode": {"mode": "append_new_columns", "suffix": "_bucket"},
            },
        ],
    },
    {
        "id": "builtin:program_runtime",
        "scope": "builtin",
        "name": "程序运行日志特征模板",
        "log_type": "program_runtime",
        "description": "适合程序运行和应用日志，突出 severity、文本长度、关键词和主机/进程频次。",
        "field_hints": {
            "required_columns": ["raw_message"],
            "optional_columns": ["severity", "host", "process_name", "event_time"],
        },
        "steps": [
            {
                "step_id": "runtime_severity_map",
                "step_type": "value_map",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["severity"]},
                "params": {
                    "mapping": {"low": 1, "medium": 2, "high": 3, "critical": 4, "error": 4, "warn": 2},
                    "default_value": 0,
                },
                "output_mode": {"mode": "append_new_columns", "suffix": "_score"},
            },
            {
                "step_id": "runtime_message_length",
                "step_type": "text_length",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_length"},
            },
            {
                "step_id": "runtime_message_entropy",
                "step_type": "shannon_entropy",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_entropy"},
            },
            {
                "step_id": "runtime_keyword_count",
                "step_type": "keyword_count",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {"keywords": ["error", "exception", "failed", "timeout", "denied"]},
                "output_mode": {"mode": "append_new_columns", "output_column": "raw_message_keyword_hits"},
            },
            {
                "step_id": "runtime_pattern_flags",
                "step_type": "pattern_flags",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["raw_message"]},
                "params": {"patterns": ["ip", "url", "hash", "base64_like", "email"]},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "runtime_host_frequency",
                "step_type": "frequency_encode",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["host"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_freq"},
            },
            {
                "step_id": "runtime_process_frequency",
                "step_type": "frequency_encode",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["process_name"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_freq"},
            },
        ],
    },
    {
        "id": "builtin:nta_flow",
        "scope": "builtin",
        "name": "NTA 流量日志特征模板",
        "log_type": "nta_flow",
        "description": "适合网络流量和会话日志，突出 IP、端口、协议和流量大小相关特征。",
        "field_hints": {
            "required_columns": ["source_ip", "dest_ip"],
            "optional_columns": ["src_port", "dest_port", "protocol", "bytes", "duration", "host", "domain", "source_host", "dest_host"],
        },
        "steps": [
            {
                "step_id": "nta_source_ip",
                "step_type": "ip_features",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip", "dest_ip"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "nta_port_features",
                "step_type": "port_features",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["src_port", "dest_port"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns"},
            },
            {
                "step_id": "nta_protocol_map",
                "step_type": "value_map",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["protocol"]},
                "params": {"mapping": {"tcp": 1, "udp": 2, "icmp": 3}, "default_value": 0},
                "output_mode": {"mode": "append_new_columns", "suffix": "_code"},
            },
            {
                "step_id": "nta_domain_entropy",
                "step_type": "shannon_entropy",
                "enabled": True,
                "input_selector": {"mode": "role_tag", "role_tag": "domain"},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "suffix": "_entropy"},
            },
            {
                "step_id": "nta_bytes_bucket",
                "step_type": "numeric_bucket",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["bytes"]},
                "params": {"bins": 5},
                "output_mode": {"mode": "append_new_columns", "suffix": "_bucket"},
            },
            {
                "step_id": "nta_duration_bucket",
                "step_type": "numeric_bucket",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["duration"]},
                "params": {"bins": 5},
                "output_mode": {"mode": "append_new_columns", "suffix": "_bucket"},
            },
            {
                "step_id": "nta_flow_pair_count",
                "step_type": "group_frequency",
                "enabled": True,
                "input_selector": {"mode": "explicit", "columns": ["source_ip", "dest_ip"]},
                "params": {},
                "output_mode": {"mode": "append_new_columns", "output_column": "flow_pair_count"},
            },
        ],
    },
]


def create_feature_pipeline(db: Session, payload: FeaturePipelineCreate) -> FeaturePipeline:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    dataset = get_dataset(db, payload.dataset_version_id)
    if dataset.project_id != payload.project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")

    preprocess_pipeline = _get_preprocess_pipeline(db, payload.project_id, payload.dataset_version_id, payload.preprocess_pipeline_id)

    pipeline = FeaturePipeline(
        project_id=payload.project_id,
        dataset_version_id=payload.dataset_version_id,
        preprocess_pipeline_id=payload.preprocess_pipeline_id,
        name=payload.name,
        status="running",
        steps=[_normalize_step(step.model_dump()) for step in payload.steps],
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)

    frame = _load_feature_input_frame(db, dataset.id, preprocess_pipeline)
    frame = _apply_steps(frame, pipeline.steps)

    output_dir = get_settings().storage_root_path / "features" / f"project_{payload.project_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"feature_{pipeline.id}_{uuid.uuid4().hex[:8]}.parquet"
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


def list_feature_pipelines(db: Session, project_id: int, dataset_version_id: int | None = None) -> list[FeaturePipeline]:
    query = select(FeaturePipeline).where(FeaturePipeline.project_id == project_id)
    if dataset_version_id is not None:
        query = query.where(FeaturePipeline.dataset_version_id == dataset_version_id)
    query = query.order_by(FeaturePipeline.created_at.desc())
    return list(db.scalars(query))


def get_feature_pipeline(db: Session, pipeline_id: int) -> FeaturePipeline:
    pipeline = db.get(FeaturePipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Feature pipeline not found")
    return pipeline


def preview_feature_pipeline(pipeline: FeaturePipeline, limit: int = 20) -> dict[str, Any]:
    if not pipeline.output_path:
        raise HTTPException(status_code=400, detail="Feature pipeline has no output preview yet")
    frame = load_parquet_frame(pipeline.output_path, limit=min(max(limit, 1), 50))
    return {
        "pipeline_id": pipeline.id,
        "columns": list(frame.columns),
        "rows": json_safe_records(frame),
    }


def preview_feature_step(
    db: Session,
    project_id: int,
    dataset_version_id: int,
    preprocess_pipeline_id: int | None,
    steps: list[dict[str, Any]],
    preview_step_index: int,
    limit: int = 8,
) -> dict[str, Any]:
    dataset = get_dataset(db, dataset_version_id)
    if dataset.project_id != project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")
    if preview_step_index >= len(steps):
        raise HTTPException(status_code=400, detail="preview_step_index is out of range")

    preprocess_pipeline = _get_preprocess_pipeline(db, project_id, dataset_version_id, preprocess_pipeline_id)
    current = _load_feature_input_frame(db, dataset.id, preprocess_pipeline)
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


def list_feature_templates(db: Session, project_id: int) -> list[dict[str, Any]]:
    templates = [template.copy() for template in BUILTIN_FEATURE_TEMPLATES]
    project_templates = list(
        db.scalars(
            select(FeatureTemplate)
            .where(FeatureTemplate.project_id == project_id)
            .order_by(FeatureTemplate.updated_at.desc())
        )
    )
    templates.extend(_serialize_project_template(template) for template in project_templates)
    return templates


def create_feature_template(db: Session, payload: FeatureTemplateCreate) -> dict[str, Any]:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    template = FeatureTemplate(
        project_id=payload.project_id,
        name=payload.name,
        log_type=payload.log_type,
        description=payload.description,
        steps=[_normalize_step(step.model_dump()) for step in payload.steps],
        field_hints=payload.field_hints,
    )
    db.add(template)
    db.commit()
    db.refresh(template)
    return _serialize_project_template(template)


def _serialize_project_template(template: FeatureTemplate) -> dict[str, Any]:
    return {
        "id": f"project:{template.id}",
        "project_id": template.project_id,
        "scope": "project",
        "name": template.name,
        "log_type": template.log_type,
        "description": template.description,
        "steps": template.steps,
        "field_hints": template.field_hints,
        "created_at": template.created_at,
        "updated_at": template.updated_at,
    }


def _get_preprocess_pipeline(
    db: Session,
    project_id: int,
    dataset_version_id: int,
    preprocess_pipeline_id: int | None,
) -> PreprocessPipeline | None:
    if preprocess_pipeline_id is None:
        return None

    preprocess_pipeline = db.get(PreprocessPipeline, preprocess_pipeline_id)
    if not preprocess_pipeline:
        raise HTTPException(status_code=404, detail="Preprocess pipeline not found")
    if preprocess_pipeline.project_id != project_id:
        raise HTTPException(status_code=400, detail="Preprocess pipeline does not belong to the selected project")
    if preprocess_pipeline.dataset_version_id != dataset_version_id:
        raise HTTPException(status_code=400, detail="Preprocess pipeline does not belong to the selected dataset")
    if not preprocess_pipeline.output_path:
        raise HTTPException(status_code=400, detail="Preprocess pipeline has no output")
    return preprocess_pipeline


def _load_feature_input_frame(
    db: Session,
    dataset_id: int,
    preprocess_pipeline: PreprocessPipeline | None,
) -> pd.DataFrame:
    if preprocess_pipeline and preprocess_pipeline.output_path:
        return load_parquet_frame(preprocess_pipeline.output_path)

    dataset = get_dataset(db, dataset_id)
    frame = load_parquet_frame(dataset.parquet_path)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    return apply_field_mapping(frame, mapping)


def _apply_steps(frame: pd.DataFrame, steps: list[dict]) -> pd.DataFrame:
    current = frame.copy()
    for raw_step in steps:
        current = _apply_single_step(current, _normalize_step(raw_step))
    return current.reset_index(drop=True)


def _apply_single_step(frame: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
    current = frame.copy()
    if not step["enabled"]:
        return current

    step_type = step["step_type"]
    params = step["params"]
    columns = _resolve_selector_columns(current, step)

    if step_type == "select_features":
        if columns:
            current = current.loc[:, columns]
    elif step_type == "derive_time_parts":
        for column in columns:
            parsed = pd.to_datetime(current[column], errors="coerce")
            prefix = params.get("prefix") or column
            current[f"{prefix}_hour"] = parsed.dt.hour
            current[f"{prefix}_dayofweek"] = parsed.dt.dayofweek
            current[f"{prefix}_is_weekend"] = parsed.dt.dayofweek.isin([5, 6]).astype("Int64")
    elif step_type == "text_length":
        for column in _resolve_string_columns(current, columns):
            transformed = current[column].astype("string").str.len()
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_length")
    elif step_type == "byte_length":
        for column in _resolve_string_columns(current, columns):
            transformed = current[column].map(_byte_length)
            current = _write_output_series(current, column, transformed.astype("Int64"), step["output_mode"], len(columns), "_bytes")
    elif step_type == "token_count":
        for column in _resolve_string_columns(current, columns):
            transformed = current[column].map(_token_count)
            current = _write_output_series(current, column, transformed.astype("Int64"), step["output_mode"], len(columns), "_tokens")
    elif step_type == "shannon_entropy":
        for column in _resolve_string_columns(current, columns):
            transformed = current[column].map(_shannon_entropy)
            current = _write_output_series(current, column, transformed.astype("Float64"), step["output_mode"], len(columns), "_entropy")
    elif step_type == "char_composition":
        for column in _resolve_string_columns(current, columns):
            current = _write_multi_output_features(
                current,
                column,
                _char_composition_features(current[column]),
                step["output_mode"],
                len(columns),
            )
    elif step_type == "unique_char_ratio":
        for column in _resolve_string_columns(current, columns):
            current = _write_multi_output_features(
                current,
                column,
                _unique_char_ratio_features(current[column]),
                step["output_mode"],
                len(columns),
            )
    elif step_type == "keyword_count":
        keywords = [str(keyword).lower() for keyword in params.get("keywords", []) if str(keyword).strip()]
        for column in _resolve_string_columns(current, columns):
            transformed = current[column].astype("string").fillna("").str.lower().map(
                lambda value: sum(value.count(keyword) for keyword in keywords)
            )
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_keyword_hits")
    elif step_type == "regex_match_count":
        patterns = _normalize_regex_patterns(params)
        for column in _resolve_string_columns(current, columns):
            if len(patterns) == 1:
                transformed = current[column].map(lambda value: _regex_match_count(value, patterns[0][1]))
                current = _write_output_series(current, column, transformed.astype("Int64"), step["output_mode"], len(columns), "_regex_hits")
            else:
                feature_map = {
                    f"{pattern_name}_hits": current[column].map(lambda value, compiled=compiled: _regex_match_count(value, compiled)).astype("Int64")
                    for pattern_name, compiled in patterns
                }
                current = _write_multi_output_features(current, column, feature_map, step["output_mode"], len(columns))
    elif step_type == "pattern_flags":
        pattern_names = _normalize_pattern_names(params)
        for column in _resolve_string_columns(current, columns):
            current = _write_multi_output_features(
                current,
                column,
                _pattern_flag_features(current[column], pattern_names),
                step["output_mode"],
                len(columns),
            )
    elif step_type == "frequency_encode":
        for column in columns:
            frequencies = current[column].value_counts(dropna=False)
            transformed = current[column].map(frequencies)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_freq")
    elif step_type == "category_encode":
        for column in columns:
            transformed = current[column].astype("category").cat.codes.astype("Int64")
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_code")
    elif step_type == "numeric_bucket":
        bins = max(int(params.get("bins") or 5), 2)
        for column in columns:
            numeric_series = pd.to_numeric(current[column], errors="coerce")
            transformed = pd.cut(numeric_series, bins=bins, labels=False, duplicates="drop")
            current = _write_output_series(current, column, transformed.astype("Int64"), step["output_mode"], len(columns), "_bucket")
    elif step_type == "numeric_scale":
        method = str(params.get("method") or "zscore")
        for column in columns:
            numeric_series = pd.to_numeric(current[column], errors="coerce")
            transformed = _scale_numeric_series(numeric_series, method)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_scaled")
    elif step_type == "ratio_feature":
        left_column, right_column = _require_minimum_columns(columns, 2, step_type)[:2]
        numerator = pd.to_numeric(current[left_column], errors="coerce")
        denominator = pd.to_numeric(current[right_column], errors="coerce")
        denominator = denominator.where(denominator != 0)
        transformed = (numerator / denominator).astype("Float64")
        current = _write_combined_output_series(current, [left_column, right_column], transformed, step["output_mode"], "_ratio")
    elif step_type == "difference_feature":
        left_column, right_column = _require_minimum_columns(columns, 2, step_type)[:2]
        left_numeric = pd.to_numeric(current[left_column], errors="coerce")
        right_numeric = pd.to_numeric(current[right_column], errors="coerce")
        transformed = (left_numeric - right_numeric).astype("Float64")
        current = _write_combined_output_series(current, [left_column, right_column], transformed, step["output_mode"], "_diff")
    elif step_type == "concat_fields":
        source_columns = _require_minimum_columns(columns, 2, step_type)
        separator = str(params.get("separator") or "|")
        transformed = current[source_columns].apply(
            lambda row: separator.join(_stringify_feature_value(row[column]) for column in source_columns),
            axis=1,
        ).astype("string")
        current = _write_combined_output_series(current, source_columns, transformed, step["output_mode"], "_concat")
    elif step_type == "equality_flag":
        left_column, right_column = _require_minimum_columns(columns, 2, step_type)[:2]
        left_series = current[left_column].astype("string")
        right_series = current[right_column].astype("string")
        left_missing = left_series.isna()
        right_missing = right_series.isna()
        transformed = (left_missing & right_missing) | ((~left_missing & ~right_missing) & (left_series == right_series))
        current = _write_combined_output_series(
            current,
            [left_column, right_column],
            transformed.astype("Int64"),
            step["output_mode"],
            "_equal",
        )
    elif step_type == "group_frequency":
        group_columns = _require_minimum_columns(columns, 1, step_type)
        transformed = current.groupby(group_columns, dropna=False)[group_columns[0]].transform("size").astype("Int64")
        current = _write_combined_output_series(current, group_columns, transformed, step["output_mode"], "_group_count")
    elif step_type == "group_unique_count":
        group_columns = _require_minimum_columns(columns, 1, step_type)
        target_column = _require_step_column(current, params.get("target_column"), "group_unique_count", "target_column")
        transformed = current.groupby(group_columns, dropna=False)[target_column].transform(lambda series: series.nunique(dropna=True)).astype("Int64")
        current = _write_combined_output_series(
            current,
            [*group_columns, target_column],
            transformed,
            step["output_mode"],
            "_unique_count",
        )
    elif step_type == "time_window_count":
        time_column = _require_step_column(current, params.get("time_column"), "time_window_count", "time_column")
        window_minutes = max(int(params.get("window_minutes") or 15), 1)
        group_columns = columns
        parsed_time = pd.to_datetime(current[time_column], errors="coerce")
        bucket = parsed_time.dt.floor(f"{window_minutes}min")
        working = current[group_columns].copy() if group_columns else pd.DataFrame(index=current.index)
        working["__window_bucket"] = bucket
        transformed = working.groupby([*group_columns, "__window_bucket"], dropna=False)["__window_bucket"].transform("size").astype("Int64")
        current = _write_combined_output_series(
            current,
            [*group_columns, time_column] if group_columns else [time_column],
            transformed,
            step["output_mode"],
            f"_{window_minutes}m_count",
        )
    elif step_type == "window_unique_count":
        time_column = _require_step_column(current, params.get("time_column"), "window_unique_count", "time_column")
        target_column = _require_step_column(current, params.get("target_column"), "window_unique_count", "target_column")
        window_minutes = max(int(params.get("window_minutes") or 15), 1)
        group_columns = columns
        parsed_time = pd.to_datetime(current[time_column], errors="coerce")
        bucket = parsed_time.dt.floor(f"{window_minutes}min")
        working = current[group_columns].copy() if group_columns else pd.DataFrame(index=current.index)
        working["__window_bucket"] = bucket
        working[target_column] = current[target_column]
        transformed = working.groupby([*group_columns, "__window_bucket"], dropna=False)[target_column].transform(
            lambda series: series.nunique(dropna=True)
        ).astype("Int64")
        current = _write_combined_output_series(
            current,
            [*group_columns, target_column, time_column] if group_columns else [target_column, time_column],
            transformed,
            step["output_mode"],
            f"_{window_minutes}m_unique_count",
        )
    elif step_type == "window_spike_flag":
        time_column = _require_step_column(current, params.get("time_column"), "window_spike_flag", "time_column")
        window_minutes = max(int(params.get("window_minutes") or 15), 1)
        threshold = max(int(params.get("threshold") or 10), 1)
        group_columns = columns
        parsed_time = pd.to_datetime(current[time_column], errors="coerce")
        bucket = parsed_time.dt.floor(f"{window_minutes}min")
        working = current[group_columns].copy() if group_columns else pd.DataFrame(index=current.index)
        working["__window_bucket"] = bucket
        window_counts = working.groupby([*group_columns, "__window_bucket"], dropna=False)["__window_bucket"].transform("size")
        transformed = (window_counts >= threshold).astype("Int64")
        current = _write_combined_output_series(
            current,
            [*group_columns, time_column] if group_columns else [time_column],
            transformed,
            step["output_mode"],
            f"_{window_minutes}m_spike",
        )
    elif step_type == "ip_features":
        for column in columns:
            current = _append_ip_features(current, column)
    elif step_type == "port_features":
        for column in columns:
            current = _append_port_features(current, column)
    elif step_type == "path_features":
        for column in columns:
            current = _append_path_features(current, column)
    elif step_type == "status_category":
        for column in columns:
            numeric_series = pd.to_numeric(current[column], errors="coerce")
            transformed = numeric_series.map(_status_category)
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_category")
    elif step_type == "value_map":
        mapping = {str(key).lower(): value for key, value in (params.get("mapping") or {}).items()}
        default_value = params.get("default_value")
        for column in columns:
            transformed = current[column].astype("string").str.lower().map(lambda value: mapping.get(str(value), default_value))
            current = _write_output_series(current, column, transformed, step["output_mode"], len(columns), "_mapped")
    elif step_type == "boolean_flag":
        operator = params.get("operator") or "contains"
        value = params.get("value")
        for column in columns:
            transformed = _build_boolean_flag(current[column], operator, value)
            current = _write_output_series(current, column, transformed.astype("Int64"), step["output_mode"], len(columns), "_flag")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported feature step: {step_type}")

    return current.reset_index(drop=True)


def _normalize_step(step: dict[str, Any]) -> dict[str, Any]:
    step_type = step.get("step_type") or step.get("type")
    if not step_type:
        raise HTTPException(status_code=400, detail="Feature step is missing step_type")
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
    elif mode == "role_tag":
        raw_columns = _select_columns_by_role_tag(frame.columns, selector.get("role_tag"))
    elif mode == "name_pattern":
        raw_columns = _select_columns_by_name_pattern(frame.columns, selector.get("name_pattern"))
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


def _select_columns_by_role_tag(columns: pd.Index, role_tag: str | None) -> list[str]:
    if not role_tag:
        return []
    patterns = ROLE_TAG_PATTERNS.get(str(role_tag).strip().lower(), ())
    if not patterns:
        return []
    return [column for column in columns if any(pattern in _normalize_column_name(column) for pattern in patterns)]


def _select_columns_by_name_pattern(columns: pd.Index, pattern: str | None) -> list[str]:
    if not pattern or not str(pattern).strip():
        return []
    try:
        compiled = re.compile(str(pattern), re.IGNORECASE)
    except re.error as exc:
        raise HTTPException(status_code=400, detail=f"Invalid name_pattern regex: {exc}") from exc
    return [column for column in columns if compiled.search(str(column))]


def _resolve_string_columns(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if is_string_dtype(frame[column]) or frame[column].dtype == "object"]


def _require_minimum_columns(columns: list[str], minimum: int, step_type: str) -> list[str]:
    if len(columns) < minimum:
        raise HTTPException(status_code=400, detail=f"{step_type} requires at least {minimum} input columns")
    return columns


def _require_step_column(frame: pd.DataFrame, column: Any, step_type: str, parameter_name: str) -> str:
    value = str(column or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"{step_type} requires {parameter_name}")
    if value not in frame.columns:
        raise HTTPException(status_code=400, detail=f"{step_type} could not find column: {value}")
    return value


def _write_output_series(
    frame: pd.DataFrame,
    source_column: str,
    series: pd.Series,
    output_mode: dict[str, Any],
    selected_column_count: int,
    default_suffix: str,
) -> pd.DataFrame:
    mode = output_mode.get("mode") or "append_new_columns"
    if mode == "output_column_map":
        output_column_map = output_mode.get("output_column_map") or {}
        if not isinstance(output_column_map, dict):
            raise HTTPException(status_code=400, detail="output_column_map must be an object")
        mapped_column = output_column_map.get("value")
        if not mapped_column:
            raise HTTPException(status_code=400, detail="output_column_map for scalar feature steps requires a 'value' key")
        frame[str(mapped_column)] = series
        return frame
    if mode == "replace_existing":
        frame[source_column] = series
        return frame
    if mode == "append_new_columns":
        output_column = output_mode.get("output_column")
        suffix = output_mode.get("suffix") or default_suffix
        if selected_column_count > 1 and output_column:
            raise HTTPException(status_code=400, detail="output_column can only be used with a single feature field")
        frame[output_column or f"{source_column}{suffix}"] = series
        return frame
    raise HTTPException(status_code=400, detail=f"Unsupported feature output mode: {mode}")


def _write_combined_output_series(
    frame: pd.DataFrame,
    source_columns: list[str],
    series: pd.Series,
    output_mode: dict[str, Any],
    default_suffix: str,
) -> pd.DataFrame:
    mode = output_mode.get("mode") or "append_new_columns"
    if mode == "replace_existing":
        raise HTTPException(status_code=400, detail="replace_existing is not supported for multi-column feature steps")
    if mode == "output_column_map":
        output_column_map = output_mode.get("output_column_map") or {}
        if not isinstance(output_column_map, dict):
            raise HTTPException(status_code=400, detail="output_column_map must be an object")
        mapped_column = output_column_map.get("value")
        if not mapped_column:
            raise HTTPException(status_code=400, detail="output_column_map for multi-column feature steps requires a 'value' key")
        frame[str(mapped_column)] = series
        return frame
    if mode == "append_new_columns":
        output_column = output_mode.get("output_column")
        suffix = output_mode.get("suffix") or default_suffix
        frame[output_column or _default_combined_output_column(source_columns, suffix)] = series
        return frame
    raise HTTPException(status_code=400, detail=f"Unsupported feature output mode: {mode}")


def _write_multi_output_features(
    frame: pd.DataFrame,
    source_column: str,
    feature_map: dict[str, pd.Series],
    output_mode: dict[str, Any],
    selected_column_count: int,
) -> pd.DataFrame:
    mode = output_mode.get("mode") or "append_new_columns"
    if mode == "replace_existing":
        raise HTTPException(status_code=400, detail="replace_existing is not supported for multi-output feature steps")

    output_column_map = output_mode.get("output_column_map") or {}
    if output_column_map and not isinstance(output_column_map, dict):
        raise HTTPException(status_code=400, detail="output_column_map must be an object")
    if selected_column_count > 1 and output_column_map:
        raise HTTPException(status_code=400, detail="output_column_map can only be used when a feature step targets a single field")

    for feature_key, series in feature_map.items():
        mapped_column = output_column_map.get(feature_key) if isinstance(output_column_map, dict) else None
        target_column = str(mapped_column).strip() if mapped_column else f"{source_column}_{feature_key}"
        frame[target_column] = series
    return frame


def _default_combined_output_column(source_columns: list[str], suffix: str) -> str:
    if len(source_columns) <= 3:
        return f"{'_'.join(source_columns)}{suffix}"
    preview_columns = "_".join(source_columns[:3])
    return f"{preview_columns}_{len(source_columns) - 3}more{suffix}"


def _scale_numeric_series(series: pd.Series, method: str) -> pd.Series:
    if method == "minmax":
        min_value = series.min(skipna=True)
        max_value = series.max(skipna=True)
        if pd.isna(min_value) or pd.isna(max_value) or max_value == min_value:
            return pd.Series([0 if pd.notna(value) else None for value in series], index=series.index, dtype="Float64")
        return ((series - min_value) / (max_value - min_value)).astype("Float64")

    mean_value = series.mean(skipna=True)
    std_value = series.std(skipna=True)
    if pd.isna(mean_value) or pd.isna(std_value) or std_value == 0:
        return pd.Series([0 if pd.notna(value) else None for value in series], index=series.index, dtype="Float64")
    return ((series - mean_value) / std_value).astype("Float64")


def _append_ip_features(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    parsed = frame[column].map(_parse_ip)
    frame[f"{column}_is_private"] = parsed.map(lambda item: 1 if item and item.is_private else 0).astype("Int64")
    frame[f"{column}_is_loopback"] = parsed.map(lambda item: 1 if item and item.is_loopback else 0).astype("Int64")
    frame[f"{column}_ip_version"] = parsed.map(lambda item: item.version if item else None).astype("Int64")
    return frame


def _append_port_features(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    numeric = pd.to_numeric(frame[column], errors="coerce")
    frame[f"{column}_is_well_known"] = numeric.map(lambda value: 1 if pd.notna(value) and value < 1024 else 0).astype("Int64")
    frame[f"{column}_is_registered"] = numeric.map(lambda value: 1 if pd.notna(value) and 1024 <= value < 49152 else 0).astype("Int64")
    frame[f"{column}_is_dynamic"] = numeric.map(lambda value: 1 if pd.notna(value) and value >= 49152 else 0).astype("Int64")
    return frame


def _append_path_features(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    normalized = frame[column].astype("string").fillna("")
    parsed_paths = normalized.map(_normalize_path_string)
    frame[f"{column}_depth"] = parsed_paths.map(lambda value: len([part for part in value.split("/") if part])).astype("Int64")
    frame[f"{column}_extension"] = parsed_paths.map(_extract_path_extension)
    frame[f"{column}_has_query"] = normalized.map(lambda value: 1 if "?" in str(value) else 0).astype("Int64")
    return frame


def _parse_ip(value: Any) -> ipaddress._BaseAddress | None:
    try:
        return ipaddress.ip_address(str(value))
    except ValueError:
        return None


def _normalize_path_string(value: str) -> str:
    if value.startswith("http://") or value.startswith("https://"):
        return urlparse(value).path or "/"
    return value.split("?", 1)[0]


def _extract_path_extension(path: str) -> str | None:
    stripped = path.rsplit("/", 1)[-1]
    if "." not in stripped:
        return None
    extension = stripped.rsplit(".", 1)[-1].lower()
    return extension or None


def _status_category(value: Any) -> str | None:
    if pd.isna(value):
        return None
    try:
        integer_value = int(value)
    except (TypeError, ValueError):
        return None
    return f"{integer_value // 100}xx"


def _build_boolean_flag(series: pd.Series, operator: str, value: Any) -> pd.Series:
    if operator == "contains":
        return series.astype("string").str.contains(str(value), case=False, na=False)
    if operator == "eq":
        return series.astype("string") == str(value)
    if operator == "ne":
        return series.astype("string") != str(value)
    if operator == "gt":
        return pd.to_numeric(series, errors="coerce") > pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if operator == "lt":
        return pd.to_numeric(series, errors="coerce") < pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if operator == "is_null":
        return series.isna()
    if operator == "not_null":
        return series.notna()
    raise HTTPException(status_code=400, detail=f"Unsupported feature boolean operator: {operator}")


def _normalize_column_name(column: str) -> str:
    return "".join(character for character in str(column).strip().lower() if character.isalnum())


def _stringify_feature_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _byte_length(value: Any) -> int:
    text = "" if pd.isna(value) else str(value)
    return len(text.encode("utf-8"))


def _token_count(value: Any) -> int:
    text = "" if pd.isna(value) else str(value)
    return len(TOKEN_PATTERN.findall(text))


def _shannon_entropy(value: Any) -> float:
    text = "" if pd.isna(value) else str(value)
    if not text:
        return 0.0
    counts = Counter(text)
    length = len(text)
    return float(-sum((count / length) * math.log2(count / length) for count in counts.values()))


def _char_composition_features(series: pd.Series) -> dict[str, pd.Series]:
    return {
        "digit_ratio": series.map(lambda value: _character_ratio(value, str.isdigit)).astype("Float64"),
        "alpha_ratio": series.map(lambda value: _character_ratio(value, str.isalpha)).astype("Float64"),
        "upper_ratio": series.map(lambda value: _character_ratio(value, str.isupper)).astype("Float64"),
        "whitespace_ratio": series.map(lambda value: _character_ratio(value, str.isspace)).astype("Float64"),
        "special_ratio": series.map(_special_character_ratio).astype("Float64"),
    }


def _unique_char_ratio_features(series: pd.Series) -> dict[str, pd.Series]:
    return {
        "unique_char_count": series.map(lambda value: len(set("" if pd.isna(value) else str(value)))).astype("Int64"),
        "unique_char_ratio": series.map(_unique_char_ratio).astype("Float64"),
    }


def _character_ratio(value: Any, predicate) -> float:
    text = "" if pd.isna(value) else str(value)
    if not text:
        return 0.0
    return sum(1 for character in text if predicate(character)) / len(text)


def _special_character_ratio(value: Any) -> float:
    text = "" if pd.isna(value) else str(value)
    if not text:
        return 0.0
    special_count = sum(1 for character in text if not character.isalnum() and not character.isspace())
    return special_count / len(text)


def _unique_char_ratio(value: Any) -> float:
    text = "" if pd.isna(value) else str(value)
    if not text:
        return 0.0
    return len(set(text)) / len(text)


def _normalize_regex_patterns(params: dict[str, Any]) -> list[tuple[str, re.Pattern[str]]]:
    raw_patterns = params.get("patterns")
    if raw_patterns:
        entries = raw_patterns
    else:
        pattern = params.get("pattern")
        entries = [pattern] if pattern else []

    compiled_patterns: list[tuple[str, re.Pattern[str]]] = []
    for index, item in enumerate(entries):
        if not item:
            continue
        if isinstance(item, dict):
            name = str(item.get("name") or f"regex_{index + 1}")
            pattern_value = str(item.get("pattern") or "")
        else:
            name = f"regex_{index + 1}"
            pattern_value = str(item)
        try:
            compiled_patterns.append((name, re.compile(pattern_value, re.IGNORECASE)))
        except re.error as exc:
            raise HTTPException(status_code=400, detail=f"Invalid regex pattern {pattern_value!r}: {exc}") from exc

    if not compiled_patterns:
        raise HTTPException(status_code=400, detail="regex_match_count requires at least one regex pattern")
    return compiled_patterns


def _regex_match_count(value: Any, pattern: re.Pattern[str]) -> int:
    text = "" if pd.isna(value) else str(value)
    return len(pattern.findall(text))


def _normalize_pattern_names(params: dict[str, Any]) -> list[str]:
    patterns = params.get("patterns") or list(PATTERN_LIBRARY.keys())
    normalized = [str(pattern).strip() for pattern in patterns if str(pattern).strip()]
    invalid = [pattern for pattern in normalized if pattern not in PATTERN_LIBRARY]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unsupported pattern flags: {', '.join(invalid)}")
    return normalized


def _pattern_flag_features(series: pd.Series, pattern_names: list[str]) -> dict[str, pd.Series]:
    feature_map: dict[str, pd.Series] = {}
    for pattern_name in pattern_names:
        pattern = PATTERN_LIBRARY[pattern_name]
        feature_map[f"has_{pattern_name}"] = series.map(lambda value, compiled=pattern: _regex_match_count(value, compiled) > 0).astype("Int64")
    return feature_map
