from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from app.models.dataset_version import DatasetVersion
from app.services.dataset_import import get_dataset, load_parquet_frame
from app.services.field_mapping import get_or_create_field_mapping

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "timestamp": ("time", "timestamp", "datetime", "date", "ts", "event_time", "eventtime"),
    "source_ip": ("src_ip", "source", "source_ip", "client_ip", "remote_addr", "ip", "srcip", "sourceip"),
    "destination_ip": ("dst_ip", "dest_ip", "destination_ip", "server_ip", "dstip", "destip", "destinationip"),
    "url_path": ("path", "uri", "url", "request_uri", "endpoint", "requesturi"),
    "status_code": ("status", "status_code", "code", "http_status", "statuscode", "httpstatus"),
    "user": ("user", "username", "account", "uid", "user_id", "userid"),
    "host": ("host", "hostname", "domain", "server"),
    "process": ("process", "process_name", "proc", "exe", "image", "processname"),
    "label": ("label", "target", "is_anomaly", "anomaly", "y", "isanomaly"),
}

SUBJECT_FIELDS = ("source_ip", "destination_ip", "user", "host", "process")
BEHAVIOR_FIELDS = ("url_path", "status_code")
BEHAVIOR_NAME_HINTS = (
    "path",
    "uri",
    "url",
    "method",
    "status",
    "event",
    "action",
    "bytes",
    "duration",
    "latency",
    "process",
    "protocol",
)
TEMPLATE_BY_PROFILE = {
    "nginx_access": "nginx_access",
    "program_runtime": "program_runtime",
    "nta_flow": "nta_flow",
}


def analyze_dataset_readiness(
    db: Session,
    dataset_id: int,
    goal: str | None = None,
    log_type: str | None = None,
) -> dict[str, Any]:
    dataset = get_dataset(db, dataset_id)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    detected_fields = _detect_fields(dataset, mapping)
    sample_frame = _load_readiness_sample(dataset)
    score, warnings = _score_readiness(dataset, detected_fields, sample_frame, log_type)
    recommended_templates = _recommend_templates(dataset, detected_fields, log_type)
    recommended_goals = _recommend_goals(detected_fields, goal)
    recommended_mode = _recommend_mode(score, detected_fields, goal)
    missing_fields = _missing_fields_for_goal(recommended_goals, detected_fields)

    return {
        "score": score,
        "level": _readiness_level(score),
        "row_count": dataset.row_count,
        "column_count": len(dataset.schema_snapshot or []),
        "time_range": _infer_time_range(sample_frame, detected_fields.get("timestamp", [])),
        "detected_fields": detected_fields,
        "missing_fields": missing_fields,
        "recommended_goals": recommended_goals,
        "recommended_mode": recommended_mode,
        "recommended_templates": recommended_templates,
        "warnings": warnings + _build_goal_warnings(recommended_goals, missing_fields),
        "next_steps": _build_next_steps(score, detected_fields, recommended_templates, missing_fields),
    }


def _detect_fields(dataset: DatasetVersion, mapping: dict[str, str | None]) -> dict[str, list[str]]:
    columns = [str(field.get("name")) for field in dataset.schema_snapshot if field.get("name")]
    result = {field: [] for field in FIELD_ALIASES}

    for semantic_name, mapped_column in mapping.items():
        if mapped_column and mapped_column in columns:
            target_key = _semantic_to_readiness_key(semantic_name)
            if target_key and mapped_column not in result[target_key]:
                result[target_key].append(mapped_column)

    detected = dataset.detected_fields or {}
    for column in detected.get("timestamp_candidates", []) or []:
        _add_field(result, "timestamp", column, columns)
    for column in detected.get("label_candidates", []) or []:
        _add_field(result, "label", column, columns)

    for column in columns:
        normalized = _normalize_name(column)
        for field_name, aliases in FIELD_ALIASES.items():
            if _matches_alias(normalized, aliases):
                _add_field(result, field_name, column, columns)

    return result


def _score_readiness(
    dataset: DatasetVersion,
    detected_fields: dict[str, list[str]],
    sample_frame: pd.DataFrame | None,
    log_type: str | None,
) -> tuple[int, list[str]]:
    score = 20 if dataset.schema_snapshot and dataset.row_count > 0 else 0
    warnings: list[str] = []

    if detected_fields["timestamp"]:
        score += 15
    else:
        warnings.append("未发现明确时间字段，时间窗行为和趋势分析会受限。")

    if any(detected_fields[field] for field in SUBJECT_FIELDS):
        score += 20
    else:
        warnings.append("未发现 IP、用户、主机或进程等主体字段，行为归因能力较弱。")

    if _has_behavior_fields(detected_fields, dataset):
        score += 20
    else:
        warnings.append("未发现 path、status、event、bytes、duration 等行为字段，异常解释会比较有限。")

    if dataset.row_count >= 500:
        score += 10
    elif dataset.row_count >= 100:
        score += 5
    else:
        warnings.append("数据量偏少，无监督异常发现结果可能不稳定。")

    missing_risk = _sample_missing_risk(sample_frame)
    if missing_risk == "low":
        score += 10
    elif missing_risk == "medium":
        score += 5
        warnings.append("部分字段缺失率偏高，建议先做字段清洗或补空值。")
    elif missing_risk == "high":
        warnings.append("样本中存在大面积缺失字段，建议先检查解析模板或清洗规则。")

    if detected_fields["label"]:
        warnings.append("发现标签字段，可以考虑有标签监督训练。")

    if _recommend_templates(dataset, detected_fields, log_type):
        score += 5

    return min(score, 100), warnings


def _load_readiness_sample(dataset: DatasetVersion) -> pd.DataFrame | None:
    try:
        return load_parquet_frame(dataset.parquet_path, limit=500)
    except Exception:
        return None


def _infer_time_range(sample_frame: pd.DataFrame | None, timestamp_columns: list[str]) -> dict[str, str | None]:
    if sample_frame is None:
        return {"start": None, "end": None}

    for column in timestamp_columns:
        if column not in sample_frame.columns:
            continue
        parsed = pd.to_datetime(sample_frame[column], errors="coerce")
        non_null = parsed.dropna()
        if non_null.empty:
            continue
        return {
            "start": non_null.min().isoformat(),
            "end": non_null.max().isoformat(),
        }
    return {"start": None, "end": None}


def _recommend_templates(dataset: DatasetVersion, detected_fields: dict[str, list[str]], log_type: str | None) -> list[str]:
    explicit_template = TEMPLATE_BY_PROFILE.get(log_type or "") or TEMPLATE_BY_PROFILE.get(dataset.parser_profile)
    if explicit_template:
        return [explicit_template]
    if detected_fields["url_path"] and detected_fields["status_code"]:
        return ["nginx_access"]
    if detected_fields["process"] or _has_raw_message(dataset):
        return ["program_runtime"]
    if detected_fields["source_ip"] and detected_fields["destination_ip"]:
        return ["nta_flow"]
    return []


def _recommend_goals(detected_fields: dict[str, list[str]], goal: str | None) -> list[str]:
    if goal:
        return [goal]
    if detected_fields["url_path"] and detected_fields["status_code"]:
        return ["Web 访问异常"]
    if detected_fields["user"]:
        return ["登录异常"]
    if detected_fields["source_ip"] and detected_fields["destination_ip"]:
        return ["网络流量异常"]
    if detected_fields["process"] or detected_fields["host"]:
        return ["程序运行异常"]
    return ["自定义日志异常"]


def _recommend_mode(score: int, detected_fields: dict[str, list[str]], goal: str | None) -> str:
    if goal == "只做数据清洗与字段探测":
        return "只做数据清洗与字段探测"
    if detected_fields["label"]:
        return "有标签监督训练"
    if score >= 60:
        return "快速无监督异常发现"
    return "只做数据清洗与字段探测"


def _missing_fields_for_goal(goals: list[str], detected_fields: dict[str, list[str]]) -> list[str]:
    required_by_goal = {
        "Web 访问异常": ["timestamp", "source_ip", "url_path", "status_code"],
        "登录异常": ["timestamp", "user", "source_ip"],
        "网络流量异常": ["timestamp", "source_ip", "destination_ip"],
        "程序运行异常": ["timestamp", "host", "process"],
        "API 滥用": ["timestamp", "source_ip", "url_path", "user"],
    }
    required = required_by_goal.get(goals[0] if goals else "", ["timestamp"])
    return [field for field in required if not detected_fields.get(field)]


def _build_goal_warnings(goals: list[str], missing_fields: list[str]) -> list[str]:
    if not missing_fields:
        return []
    goal = goals[0] if goals else "当前目标"
    return [f"{goal} 缺少关键字段：{', '.join(missing_fields)}。"]


def _build_next_steps(
    score: int,
    detected_fields: dict[str, list[str]],
    recommended_templates: list[str],
    missing_fields: list[str],
) -> list[str]:
    steps: list[str] = []
    if missing_fields:
        steps.append("先确认字段映射，补齐缺失的关键字段。")
    if recommended_templates:
        steps.append(f"建议使用 {recommended_templates[0]} 模板生成异常分析特征。")
    if score >= 80:
        steps.append("数据已适合直接进入快速无监督异常发现。")
    elif score >= 60:
        steps.append("可以先运行快速分析，再根据结果补充字段清洗。")
    else:
        steps.append("建议先做导入清洗、字段映射和数据整理，再进入建模。")
    if detected_fields["label"]:
        steps.append("如果标签可信，可以切换到有标签监督训练。")
    return steps


def _readiness_level(score: int) -> str:
    if score >= 80:
        return "excellent"
    if score >= 60:
        return "good"
    if score >= 40:
        return "limited"
    return "poor"


def _sample_missing_risk(sample_frame: pd.DataFrame | None) -> str:
    if sample_frame is None or sample_frame.empty:
        return "medium"
    missing_ratio = sample_frame.isna().mean()
    high_missing_columns = int((missing_ratio >= 0.5).sum())
    if high_missing_columns == 0:
        return "low"
    if high_missing_columns <= max(1, len(sample_frame.columns) // 4):
        return "medium"
    return "high"


def _has_behavior_fields(detected_fields: dict[str, list[str]], dataset: DatasetVersion) -> bool:
    if any(detected_fields[field] for field in BEHAVIOR_FIELDS):
        return True
    for field in dataset.schema_snapshot or []:
        normalized = _normalize_name(str(field.get("name") or ""))
        if any(hint in normalized for hint in BEHAVIOR_NAME_HINTS):
            return True
    return False


def _has_raw_message(dataset: DatasetVersion) -> bool:
    return any("message" in _normalize_name(str(field.get("name") or "")) for field in dataset.schema_snapshot or [])


def _semantic_to_readiness_key(semantic_name: str) -> str | None:
    mapping = {
        "event_time": "timestamp",
        "source_ip": "source_ip",
        "dest_ip": "destination_ip",
        "status_code": "status_code",
        "label": "label",
        "raw_message": "process",
    }
    return mapping.get(semantic_name)


def _add_field(result: dict[str, list[str]], field_name: str, column: Any, available_columns: list[str]) -> None:
    column_name = str(column)
    if column_name in available_columns and column_name not in result[field_name]:
        result[field_name].append(column_name)


def _matches_alias(normalized: str, aliases: tuple[str, ...]) -> bool:
    normalized_aliases = {_normalize_name(alias) for alias in aliases}
    return normalized in normalized_aliases or any(alias and alias in normalized for alias in normalized_aliases)


def _normalize_name(value: str) -> str:
    return "".join(character for character in value.lower().strip() if character.isalnum())
