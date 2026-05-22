from typing import Any

import pandas as pd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from sqlalchemy.orm import Session

from app.models.feature_pipeline import FeaturePipeline
from app.models.preprocess_pipeline import PreprocessPipeline
from app.services.dataset_import import get_dataset

BUSINESS_CONTEXT_HINTS: tuple[str, ...] = (
    "eventtime",
    "timestamp",
    "time",
    "sourceip",
    "srcip",
    "destip",
    "dstip",
    "host",
    "hostname",
    "sourcehost",
    "desthost",
    "path",
    "url",
    "uri",
    "method",
    "protocol",
    "status",
    "statuscode",
    "session",
    "request",
    "trace",
    "user",
    "userid",
    "account",
    "device",
    "process",
    "mailfrom",
    "rcptto",
    "helo",
    "domain",
    "rawmessage",
    "message",
    "port",
)

STEP_TASK_CATEGORY_MAP: dict[str, str] = {
    "text_length": "text_complexity",
    "byte_length": "text_complexity",
    "token_count": "text_complexity",
    "shannon_entropy": "text_complexity",
    "char_composition": "text_complexity",
    "unique_char_ratio": "text_complexity",
    "regex_match_count": "text_complexity",
    "pattern_flags": "text_complexity",
    "keyword_count": "text_complexity",
    "frequency_encode": "high_cardinality",
    "category_encode": "high_cardinality",
    "group_frequency": "behavior_tracking",
    "group_unique_count": "behavior_tracking",
    "group_duration": "behavior_tracking",
    "group_event_order": "behavior_tracking",
    "time_since_previous_event": "behavior_tracking",
    "time_until_next_event": "behavior_tracking",
    "group_value_change_flag": "behavior_tracking",
    "time_window_count": "time_behavior",
    "window_unique_count": "time_behavior",
    "window_target_unique_count": "behavior_tracking",
    "window_status_change_count": "behavior_tracking",
    "window_spike_flag": "time_behavior",
    "derive_time_parts": "time_behavior",
    "numeric_bucket": "numeric_statistics",
    "numeric_scale": "numeric_statistics",
    "ratio_feature": "numeric_statistics",
    "difference_feature": "numeric_statistics",
    "concat_fields": "high_cardinality",
    "equality_flag": "behavior_tracking",
    "ip_features": "behavior_tracking",
    "port_features": "behavior_tracking",
    "path_features": "text_complexity",
    "status_category": "numeric_statistics",
    "value_map": "numeric_statistics",
    "boolean_flag": "text_complexity",
}


def derive_feature_training_metadata(
    frame: pd.DataFrame,
    input_columns: list[str],
    *,
    feature_lineage: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[str], list[str], list[str], dict[str, dict[str, Any]]]:
    training_candidate_columns: list[str] = []
    business_context_columns: list[str] = []
    analysis_retained_columns: list[str] = []
    resolved_feature_lineage = dict(feature_lineage or {})

    for column in frame.columns:
        if column not in input_columns and _is_training_candidate_series(frame[column]):
            training_candidate_columns.append(column)
            continue
        if _is_business_context_column(column, frame[column], input_columns):
            business_context_columns.append(column)
            continue
        analysis_retained_columns.append(column)

    for column, entry in resolved_feature_lineage.items():
        if column not in frame.columns:
            continue
        entry["used_for_training"] = column in training_candidate_columns

    return (
        training_candidate_columns,
        business_context_columns,
        analysis_retained_columns,
        resolved_feature_lineage,
    )


def resolve_feature_pipeline_metadata(
    pipeline: FeaturePipeline,
    frame: pd.DataFrame,
    *,
    input_columns: list[str],
) -> tuple[list[str], list[str], list[str], dict[str, dict[str, Any]]]:
    stored_training = [column for column in (pipeline.training_candidate_columns or []) if column in frame.columns]
    stored_business_context = [column for column in (pipeline.business_context_columns or []) if column in frame.columns]
    stored_analysis = [column for column in (pipeline.analysis_retained_columns or []) if column in frame.columns]
    stored_lineage = {
        column: value
        for column, value in (pipeline.feature_lineage or {}).items()
        if column in frame.columns and isinstance(value, dict)
    }

    if stored_training or stored_business_context or stored_analysis:
        accounted = set(stored_training) | set(stored_business_context) | set(stored_analysis)
        remaining = [column for column in frame.columns if column not in accounted]
        inferred_training, inferred_business_context, inferred_analysis, _ = derive_feature_training_metadata(
            frame,
            input_columns,
            feature_lineage=stored_lineage,
        )
        return (
            stored_training or inferred_training,
            stored_business_context or inferred_business_context,
            stored_analysis + remaining if stored_analysis else inferred_analysis + [column for column in remaining if column not in inferred_analysis],
            stored_lineage,
        )

    return derive_feature_training_metadata(frame, input_columns)


def hydrate_feature_pipeline_metadata(db: Session, pipeline: FeaturePipeline) -> FeaturePipeline:
    input_columns = resolve_feature_pipeline_input_columns(db, pipeline)
    (
        training_candidate_columns,
        business_context_columns,
        analysis_retained_columns,
        feature_lineage,
    ) = resolve_feature_pipeline_schema_metadata(pipeline, input_columns)
    pipeline.training_candidate_columns = training_candidate_columns
    pipeline.business_context_columns = business_context_columns
    pipeline.analysis_retained_columns = analysis_retained_columns
    pipeline.feature_lineage = feature_lineage
    return pipeline


def resolve_feature_pipeline_schema_metadata(
    pipeline: FeaturePipeline,
    input_columns: list[str],
) -> tuple[list[str], list[str], list[str], dict[str, dict[str, Any]]]:
    stored_training = list(pipeline.training_candidate_columns or [])
    stored_business_context = list(pipeline.business_context_columns or [])
    stored_analysis = list(pipeline.analysis_retained_columns or [])
    stored_lineage = dict(pipeline.feature_lineage or {})
    output_columns = [field.get("name") for field in pipeline.output_schema if field.get("name")]

    if stored_training or stored_business_context or stored_analysis:
        accounted = set(stored_training) | set(stored_business_context) | set(stored_analysis)
        remaining = [column for column in output_columns if column not in accounted]
        inferred_training, inferred_business_context, inferred_analysis, _ = _infer_feature_training_metadata_from_schema(
            pipeline.output_schema,
            input_columns,
        )
        return (
            stored_training or inferred_training,
            stored_business_context or inferred_business_context,
            stored_analysis + remaining if stored_analysis else inferred_analysis + [column for column in remaining if column not in inferred_analysis],
            stored_lineage,
        )

    return _infer_feature_training_metadata_from_schema(pipeline.output_schema, input_columns)


def resolve_feature_pipeline_input_columns(db: Session, pipeline: FeaturePipeline) -> list[str]:
    if pipeline.preprocess_pipeline_id:
        preprocess_pipeline = db.get(PreprocessPipeline, pipeline.preprocess_pipeline_id)
        if preprocess_pipeline and preprocess_pipeline.output_schema:
            return [field.get("name") for field in preprocess_pipeline.output_schema if field.get("name")]

    dataset = get_dataset(db, pipeline.dataset_version_id)
    return [field.get("name") for field in dataset.schema_snapshot if field.get("name")]


def build_feature_lineage_entry(column: str, step: dict[str, Any]) -> dict[str, Any]:
    step_type = str(step.get("step_type") or step.get("type") or "")
    task_category = _infer_step_task_category(step_type)
    recipe_id = _infer_recipe_id(step_type, step)
    source_columns = _extract_step_source_columns(step)
    return {
        "source_columns": source_columns,
        "step_type": step_type,
        "task_category": task_category,
        "recipe_id": recipe_id,
        "description": _describe_feature_lineage(step_type, source_columns),
        "business_meaning": _describe_feature_business_meaning(column, step_type, source_columns, task_category),
        "used_for_training": False,
    }


def _infer_feature_training_metadata_from_schema(
    output_schema: list[dict[str, Any]],
    input_columns: list[str],
) -> tuple[list[str], list[str], list[str], dict[str, dict[str, Any]]]:
    training_candidate_columns: list[str] = []
    business_context_columns: list[str] = []
    analysis_retained_columns: list[str] = []
    for field in output_schema:
        column = field.get("name")
        if not column:
            continue
        dtype_name = str(field.get("dtype") or "")
        if column not in input_columns and _is_training_candidate_dtype(dtype_name):
            training_candidate_columns.append(column)
        elif _is_business_context_schema_field(column, dtype_name, input_columns):
            business_context_columns.append(column)
        else:
            analysis_retained_columns.append(column)
    return training_candidate_columns, business_context_columns, analysis_retained_columns, {}


def _is_training_candidate_series(series: pd.Series) -> bool:
    return is_numeric_dtype(series) or is_bool_dtype(series)


def _is_training_candidate_dtype(dtype_name: str) -> bool:
    normalized = dtype_name.strip().lower()
    return normalized.startswith(("int", "uint", "float", "bool")) or normalized in {"int64", "float64", "boolean"}


def _is_business_context_column(column: str, series: pd.Series, input_columns: list[str]) -> bool:
    normalized = _normalize_column_name(column)
    if normalized in {"sampleindex", "sourcerowid", "recordid"}:
        return True
    if column not in input_columns:
        return False
    if _looks_like_business_context(normalized):
        return True
    return is_datetime64_any_dtype(series) or is_string_dtype(series)


def _is_business_context_schema_field(column: str, dtype_name: str, input_columns: list[str]) -> bool:
    normalized = _normalize_column_name(column)
    if normalized in {"sampleindex", "sourcerowid", "recordid"}:
        return True
    if column not in input_columns:
        return False
    if _looks_like_business_context(normalized):
        return True
    normalized_dtype = dtype_name.strip().lower()
    return normalized_dtype.startswith("datetime") or normalized_dtype.startswith("timestamp") or "string" in normalized_dtype or normalized_dtype == "object"


def _looks_like_business_context(normalized_name: str) -> bool:
    return any(hint in normalized_name for hint in BUSINESS_CONTEXT_HINTS)


def _infer_step_task_category(step_type: str) -> str:
    return STEP_TASK_CATEGORY_MAP.get(step_type, "feature_engineering")


def _infer_recipe_id(step_type: str, step: dict[str, Any]) -> str:
    params = step.get("params") or {}
    if step_type in {"group_frequency", "group_unique_count", "group_duration", "group_event_order", "time_since_previous_event", "time_until_next_event", "group_value_change_flag"}:
        return "behavior_tracking_base"
    if step_type in {"time_window_count", "window_unique_count", "window_target_unique_count", "window_status_change_count", "window_spike_flag"}:
        return "behavior_tracking_window"
    if step_type in {"text_length", "byte_length", "token_count", "shannon_entropy", "char_composition", "unique_char_ratio", "regex_match_count", "pattern_flags", "keyword_count"}:
        return "text_complexity_core"
    if step_type in {"frequency_encode", "category_encode", "concat_fields"}:
        return "high_cardinality_frequency"
    if step_type in {"derive_time_parts"}:
        return "time_behavior_core"
    if step_type in {"numeric_bucket", "numeric_scale", "ratio_feature", "difference_feature", "status_category", "value_map"}:
        return "numeric_statistics_core"
    if step_type == "boolean_flag":
        operator = str(params.get("operator") or "flag")
        return f"boolean_flag:{operator}"
    return step_type or "feature_recipe"


def _extract_step_source_columns(step: dict[str, Any]) -> list[str]:
    selector = step.get("input_selector") or {}
    columns = selector.get("columns")
    if isinstance(columns, list):
        return [str(column) for column in columns if str(column).strip()]
    return []


def _describe_feature_lineage(step_type: str, source_columns: list[str]) -> str:
    if source_columns:
        return f"{step_type} 基于 {', '.join(source_columns)} 生成"
    return f"{step_type} 生成的派生特征"


def _describe_feature_business_meaning(
    column: str,
    step_type: str,
    source_columns: list[str],
    task_category: str,
) -> str:
    source_text = "、".join(source_columns) if source_columns else "相关字段"
    if step_type == "shannon_entropy":
        return f"{source_text} 的字符级香农熵，用来衡量内容复杂度。"
    if step_type == "text_length":
        return f"{source_text} 的文本长度，用来表示内容规模。"
    if step_type == "byte_length":
        return f"{source_text} 的字节长度，用来表示原始负载大小。"
    if step_type == "token_count":
        return f"{source_text} 的 token 数量，用来衡量消息拆分后的复杂度。"
    if step_type == "char_composition":
        return f"{source_text} 的字符组成比例，用来识别异常编码、混淆或机器生成痕迹。"
    if step_type == "keyword_count":
        return f"{source_text} 中关键字命中次数，用来捕捉异常消息模式。"
    if step_type in {"group_frequency", "time_window_count", "window_unique_count", "window_target_unique_count", "window_status_change_count", "window_spike_flag"}:
        return f"{source_text} 的行为统计特征，用来衡量时间窗内的活跃度、分散度或突增。"
    if step_type in {"group_duration", "group_event_order", "time_since_previous_event", "time_until_next_event", "group_value_change_flag"}:
        return f"{source_text} 的顺序/持续时长特征，用来描述行为链路中的位置和变化。"
    if step_type in {"numeric_bucket", "numeric_scale", "ratio_feature", "difference_feature"}:
        return f"{source_text} 的数值统计衍生特征，用来增强训练对异常波动的识别。"
    if step_type in {"frequency_encode", "category_encode"}:
        return f"{source_text} 的类别统计特征，用来替代直接训练原始高基数字段。"
    if task_category == "behavior_tracking":
        return f"{source_text} 的行为追踪特征，用来描述主体或流程在时间上的行为模式。"
    if task_category == "text_complexity":
        return f"{source_text} 的文本复杂度特征，用来把原始文本转成更适合训练的数值信号。"
    return f"{column} 由 {source_text} 派生，用来提升异常检测对业务行为的刻画能力。"


def _normalize_column_name(column: str) -> str:
    return "".join(character for character in str(column).strip().lower() if character.isalnum())
