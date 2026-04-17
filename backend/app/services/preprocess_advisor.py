from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.job import Job
from app.models.preprocess_advisor_run import PreprocessAdvisorRun
from app.models.project import Project
from app.schemas.preprocess import PreprocessTrainingAdvisorRequest
from app.schemas.training import TrainingRequest
from app.services.dataset_import import get_dataset, load_parquet_frame
from app.services.field_mapping import get_or_create_field_mapping
from app.services.preprocess import _apply_steps, _normalize_step, apply_field_mapping
from app.services.training import _select_training_feature_columns

EXCLUDE_MISSING_RATIO = 0.98
HIGH_MISSING_RATIO = 0.30
NUMERIC_CAST_RATIO = 0.90
DATETIME_CAST_RATIO = 0.90
DEFAULT_QUICK_SAMPLE_LIMIT = 1500

REASON_TEXTS = {
    "target_column": "这是当前训练的目标列，不应直接作为输入特征。",
    "label_like_column": "字段名称或语义像标签列，容易造成标签泄漏。",
    "duplicates_target_column": "该字段与目标列内容重复，会直接泄漏答案。",
    "identifier_column": "字段更像唯一标识符，通常不利于模型泛化。",
    "raw_text_column": "字段是原始大文本，适合后续做特征工程，不建议直接进训练。",
    "high_cardinality": "字段唯一值过多，直接编码会带来高维和过拟合风险。",
    "empty_column": "字段没有有效值，无法提供训练信号。",
    "constant_column": "字段几乎不变化，对训练帮助很小。",
    "high_missing": "字段缺失率较高，建议先补全或谨慎使用。",
    "cast_numeric": "字段内容看起来是数值字符串，建议先转成数值。",
    "cast_datetime": "字段内容看起来是时间字符串，建议先转成时间类型。",
    "keep": "字段当前可以直接作为训练候选输入。",
}
ISSUE_GROUP_DEFINITIONS = {
    "direct_trainable": {
        "title": "可直接进入训练",
        "description": "这些字段当前已经比较适合作为训练候选输入，可以先保留。",
        "recommended_action": "keep",
        "handoff_target": None,
    },
    "needs_cleaning": {
        "title": "建议先清洗",
        "description": "这些字段在进入训练前还需要做补空值、转类型等基础整理。",
        "recommended_action": "clean_first",
        "handoff_target": None,
    },
    "route_to_features": {
        "title": "建议改走特征工程",
        "description": "这些字段更适合先生成统计、复杂度或行为追踪特征，而不是直接训练原值。",
        "recommended_action": "move_to_feature_engineering",
        "handoff_target": "feature",
    },
    "remove_from_output": {
        "title": "建议移除或从训练中排除",
        "description": "这些字段容易造成泄漏、没有有效信号，或会明显干扰训练，建议不要继续沿主链路使用。",
        "recommended_action": "exclude_or_drop",
        "handoff_target": None,
    },
}
FLOW_TRACKING_HINTS = ("request", "trace", "session", "span", "transaction", "correlation", "flow")
ENTITY_TRACKING_HINTS = ("userid", "user", "device", "host", "account", "sourceip", "srcip", "clientip")
TRACKING_TARGET_HINTS = (
    "path",
    "uri",
    "url",
    "dest",
    "dst",
    "process",
    "status",
    "result",
    "host",
    "domain",
    "method",
    "protocol",
)


def analyze_preprocess_training_advisor(
    db: Session,
    payload: PreprocessTrainingAdvisorRequest,
    *,
    analysis_mode: str = "quick",
) -> dict[str, Any]:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    dataset = get_dataset(db, payload.dataset_version_id)
    if dataset.project_id != payload.project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")

    frame = _load_preprocess_advisor_frame(db, dataset.id, payload.steps)
    sampled = _sample_frame(frame, payload.sample_limit)
    target_column = payload.target_column or dataset.label_column
    selection = _build_training_selection(sampled, payload, target_column, dataset.label_column)
    field_advice = _build_field_advice(sampled, target_column, selection["exclusion_reasons"])
    recommended_steps = _build_recommended_steps(sampled, field_advice)

    direct_trainable_fields = sum(1 for advice in field_advice if advice["recommended_action"] == "keep")
    high_risk_fields = sum(
        1
        for advice in field_advice
        if advice["recommended_action"] in {"drop_from_training", "exclude_column", "move_to_feature_engineering"}
    )
    pending_fields = len(field_advice) - direct_trainable_fields - high_risk_fields

    return {
        "summary": {
            "direct_trainable_fields": direct_trainable_fields,
            "high_risk_fields": high_risk_fields,
            "pending_fields": pending_fields,
            "total_fields": int(len(sampled.columns)),
            "target_column": target_column,
            "suggested_training_columns": selection["used_feature_columns"],
            "excluded_training_columns": selection["excluded_feature_columns"],
            "analysis_basis": "当前步骤链采样分析" if analysis_mode == "sampled_trainability" else "当前步骤链快速规则分析",
        },
        "field_advice": field_advice,
        "issue_groups": _build_issue_groups(field_advice),
        "recommended_steps": recommended_steps,
        "analysis_mode": analysis_mode,
        "sample_size": int(len(sampled.index)),
        "generated_at": datetime.now(UTC),
    }


def create_preprocess_training_advisor_job(
    db: Session,
    payload: PreprocessTrainingAdvisorRequest,
) -> tuple[Job, PreprocessAdvisorRun]:
    project = db.get(Project, payload.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    dataset = get_dataset(db, payload.dataset_version_id)
    if dataset.project_id != payload.project_id:
        raise HTTPException(status_code=400, detail="Dataset does not belong to the selected project")

    job = Job(
        name=f"{dataset.version_name}-training-advisor",
        job_type="preprocess_training_advisor",
        status="queued",
        progress=0,
        message="Waiting to start sampled trainability analysis",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    advisor_run = PreprocessAdvisorRun(
        project_id=payload.project_id,
        dataset_version_id=payload.dataset_version_id,
        job_id=job.id,
        status="queued",
        analysis_mode="sampled_trainability",
        request_payload=payload.model_dump(),
        result_json={},
        sample_size=0,
    )
    db.add(advisor_run)
    db.commit()
    db.refresh(advisor_run)
    return job, advisor_run


def run_preprocess_training_advisor_job(job_id: int, advisor_run_id: int) -> None:
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        advisor_run = db.get(PreprocessAdvisorRun, advisor_run_id)
        if not job or not advisor_run:
            return

        try:
            _set_job_state(db, job, status="running", progress=10, message="Preparing sampled trainability analysis")
            advisor_run.status = "running"
            db.add(advisor_run)
            db.commit()

            payload = PreprocessTrainingAdvisorRequest(**advisor_run.request_payload)
            result = analyze_preprocess_training_advisor(db, payload, analysis_mode="sampled_trainability")
            result_json = jsonable_encoder(result)

            advisor_run.status = "completed"
            advisor_run.result_json = result_json
            advisor_run.sample_size = int(result.get("sample_size", 0))
            db.add(advisor_run)
            db.commit()

            _set_job_state(db, job, status="completed", progress=100, message="Sampled trainability analysis completed")
        except HTTPException as exc:
            db.rollback()
            advisor_run = db.get(PreprocessAdvisorRun, advisor_run_id)
            advisor_run.status = "failed"
            advisor_run.result_json = {}
            db.add(advisor_run)
            db.commit()
            _set_job_state(db, job, status="failed", progress=100, message=str(exc.detail))
        except Exception as exc:
            db.rollback()
            advisor_run = db.get(PreprocessAdvisorRun, advisor_run_id)
            advisor_run.status = "failed"
            advisor_run.result_json = {}
            db.add(advisor_run)
            db.commit()
            _set_job_state(db, job, status="failed", progress=100, message=f"Trainability analysis failed: {exc}")


def get_preprocess_training_advisor_run(db: Session, advisor_run_id: int) -> PreprocessAdvisorRun:
    advisor_run = db.get(PreprocessAdvisorRun, advisor_run_id)
    if not advisor_run:
        raise HTTPException(status_code=404, detail="Preprocess advisor run not found")
    return advisor_run


def serialize_preprocess_training_advisor_run(advisor_run: PreprocessAdvisorRun) -> dict[str, Any]:
    return {
        "id": advisor_run.id,
        "project_id": advisor_run.project_id,
        "dataset_version_id": advisor_run.dataset_version_id,
        "job_id": advisor_run.job_id,
        "status": advisor_run.status,
        "analysis_mode": advisor_run.analysis_mode,
        "sample_size": advisor_run.sample_size,
        "result": advisor_run.result_json or None,
        "created_at": advisor_run.created_at,
        "updated_at": advisor_run.updated_at,
    }


def _load_preprocess_advisor_frame(
    db: Session,
    dataset_id: int,
    steps: list[Any],
) -> pd.DataFrame:
    dataset = get_dataset(db, dataset_id)
    frame = load_parquet_frame(dataset.parquet_path)
    mapping = get_or_create_field_mapping(db, dataset.id).mappings
    frame = apply_field_mapping(frame, mapping)
    normalized_steps = [_normalize_step(step.model_dump() if hasattr(step, "model_dump") else step) for step in steps]
    return _apply_steps(frame, normalized_steps)


def _sample_frame(frame: pd.DataFrame, sample_limit: int) -> pd.DataFrame:
    resolved_limit = max(int(sample_limit or DEFAULT_QUICK_SAMPLE_LIMIT), 100)
    if len(frame.index) <= resolved_limit:
        return frame.copy()
    return frame.sample(n=resolved_limit, random_state=42).reset_index(drop=True)


def _build_training_selection(
    frame: pd.DataFrame,
    payload: PreprocessTrainingAdvisorRequest,
    target_column: str | None,
    dataset_label_column: str | None,
) -> dict[str, Any]:
    training_payload = TrainingRequest(
        project_id=payload.project_id,
        dataset_version_id=payload.dataset_version_id,
        name="preprocess-training-advisor",
        mode="supervised" if target_column else "unsupervised",
        algorithm="random_forest" if target_column else "isolation_forest",
        target_column=target_column,
        feature_columns=[],
        training_params={},
    )
    return _select_training_feature_columns(
        frame,
        training_payload,
        dataset_label_column=dataset_label_column,
        target_column=target_column,
        dataset_schema_columns=list(frame.columns),
        preprocess_pipeline=None,
        feature_pipeline=None,
    )


def _build_field_advice(
    frame: pd.DataFrame,
    target_column: str | None,
    exclusion_reasons: dict[str, str],
) -> list[dict[str, Any]]:
    advice_list: list[dict[str, Any]] = []

    for column in frame.columns:
        series = frame[column]
        null_ratio = float(series.isna().mean()) if len(series.index) else 0.0
        exclusion_reason = exclusion_reasons.get(column)

        status = "recommended_keep"
        reason_code = "keep"
        recommended_action = "keep"
        confidence = "high"

        if exclusion_reason in {"target_column", "label_like_column", "duplicates_target_column"}:
            status = "suspected_label_leak"
            reason_code = exclusion_reason
            recommended_action = "drop_from_training"
        elif exclusion_reason == "identifier_column":
            status = "suspected_id"
            reason_code = exclusion_reason
            recommended_action = "move_to_feature_engineering" if _infer_tracking_type(column) else "exclude_column"
        elif exclusion_reason == "raw_text_column":
            status = "raw_text"
            reason_code = exclusion_reason
            recommended_action = "move_to_feature_engineering"
        elif exclusion_reason and exclusion_reason.startswith("high_cardinality"):
            status = "high_cardinality_risk"
            reason_code = "high_cardinality"
            recommended_action = "move_to_feature_engineering"
        elif exclusion_reason in {"empty_column", "constant_column"}:
            status = "suggest_delete"
            reason_code = exclusion_reason
            recommended_action = "exclude_column"
        elif null_ratio >= EXCLUDE_MISSING_RATIO:
            status = "suggest_delete"
            reason_code = "empty_column"
            recommended_action = "exclude_column"
        else:
            cast_reason = _infer_cast_reason(column, series, target_column)
            if cast_reason == "cast_numeric":
                status = "suggest_convert"
                reason_code = cast_reason
                recommended_action = "cast_numeric"
                confidence = "medium"
            elif cast_reason == "cast_datetime":
                status = "suggest_convert"
                reason_code = cast_reason
                recommended_action = "cast_datetime"
                confidence = "medium"
            elif null_ratio >= HIGH_MISSING_RATIO:
                status = "high_missing"
                reason_code = "high_missing"
                recommended_action = "fill_null"
                confidence = "medium"

        advice_list.append(
            {
                "field": column,
                "status": status,
                "reason_code": reason_code,
                "reason_text": _build_reason_text(reason_code, column),
                "recommended_action": recommended_action,
                "confidence": confidence,
                "feature_handoff": _build_feature_handoff(frame, column, reason_code, recommended_action),
            }
        )

    return advice_list


def _infer_cast_reason(column: str, series: pd.Series, target_column: str | None) -> str | None:
    if column == target_column or pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
        return None

    non_null = series.dropna()
    if non_null.empty:
        return None

    stringified = non_null.astype("string")
    numeric_ratio = pd.to_numeric(stringified, errors="coerce").notna().mean()
    if numeric_ratio >= NUMERIC_CAST_RATIO:
        return "cast_numeric"

    datetime_ratio = pd.to_datetime(stringified, errors="coerce").notna().mean()
    if datetime_ratio >= DATETIME_CAST_RATIO:
        return "cast_datetime"

    return None


def _build_reason_text(reason_code: str, column: str) -> str:
    if reason_code == "high_missing":
        return f"{column} 缺失较多，建议在训练前先补全。"
    if reason_code in {"identifier_column", "high_cardinality"} and _infer_tracking_type(column):
        return f"{column} 更适合与时间字段一起生成行为追踪特征，而不是直接作为训练输入。"
    return REASON_TEXTS.get(reason_code, f"{column} 需要进一步确认是否适合作为训练字段。")


def _build_feature_handoff(
    frame: pd.DataFrame,
    column: str,
    reason_code: str,
    recommended_action: str,
) -> dict[str, Any] | None:
    if recommended_action != "move_to_feature_engineering":
        return None

    tracking_type = _infer_tracking_type(column)
    time_columns = _find_time_columns(frame, exclude_column=column)

    if tracking_type and time_columns:
        target_columns = _find_tracking_target_columns(frame, exclude_columns={column, *time_columns[:1]})
        return {
            "issue_type": "behavior_tracking",
            "task_category": "behavior_tracking",
            "tracking_type": tracking_type,
            "recommended_group_key": column,
            "recommended_time_columns": time_columns,
            "recommended_target_columns": target_columns,
            "recipe_ids": (
                ["behavior_tracking_base", "behavior_tracking_sequence"]
                if tracking_type == "flow"
                else ["behavior_tracking_base", "behavior_tracking_window"]
            ),
            "reason_code": reason_code,
        }

    if reason_code == "raw_text_column":
        return {
            "issue_type": "raw_text_column",
            "task_category": "text_complexity",
            "tracking_type": "",
            "recommended_group_key": column,
            "recommended_time_columns": [],
            "recommended_target_columns": [],
            "recipe_ids": ["text_complexity_core"],
            "reason_code": reason_code,
        }

    if reason_code == "high_cardinality":
        return {
            "issue_type": "high_cardinality",
            "task_category": "high_cardinality",
            "tracking_type": "",
            "recommended_group_key": column,
            "recommended_time_columns": time_columns,
            "recommended_target_columns": _find_tracking_target_columns(frame, exclude_columns={column}),
            "recipe_ids": ["high_cardinality_frequency", "high_cardinality_window"],
            "reason_code": reason_code,
        }

    return None


def _infer_tracking_type(column: str) -> str | None:
    normalized = "".join(character for character in str(column).strip().lower() if character.isalnum())
    if any(hint in normalized for hint in FLOW_TRACKING_HINTS):
        return "flow"
    if any(hint in normalized for hint in ENTITY_TRACKING_HINTS):
        return "entity"
    return None


def _find_time_columns(frame: pd.DataFrame, *, exclude_column: str) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for column in frame.columns:
        if column == exclude_column:
            continue
        normalized = "".join(character for character in str(column).strip().lower() if character.isalnum())
        score = 0
        if any(token in normalized for token in ("time", "timestamp", "date", "datetime")):
            score += 3
        series = frame[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            score += 5
        else:
            non_null = series.dropna()
            if not non_null.empty:
                parsed_ratio = pd.to_datetime(non_null.astype("string"), errors="coerce").notna().mean()
                if parsed_ratio >= DATETIME_CAST_RATIO:
                    score += 2
        if score > 0:
            candidates.append((score, column))
    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [column for _, column in candidates[:3]]


def _find_tracking_target_columns(frame: pd.DataFrame, *, exclude_columns: set[str]) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for column in frame.columns:
        if column in exclude_columns:
            continue
        normalized = "".join(character for character in str(column).strip().lower() if character.isalnum())
        score = 0
        if any(hint in normalized for hint in TRACKING_TARGET_HINTS):
            score += 3
        series = frame[column]
        if pd.api.types.is_numeric_dtype(series):
            score += 1
        if not pd.api.types.is_numeric_dtype(series):
            cardinality = int(series.dropna().astype("string").nunique(dropna=True)) if not series.dropna().empty else 0
            if 1 < cardinality <= 200:
                score += 1
        if score > 0:
            candidates.append((score, column))
    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [column for _, column in candidates[:5]]


def _build_recommended_steps(frame: pd.DataFrame, field_advice: list[dict[str, Any]]) -> list[dict[str, Any]]:
    advice_by_action: dict[str, list[str]] = {}
    for advice in field_advice:
        advice_by_action.setdefault(advice["recommended_action"], []).append(advice["field"])

    recommendations: list[dict[str, Any]] = []

    numeric_fill_columns = [
        column
        for column in advice_by_action.get("fill_null", [])
        if column in frame.columns and pd.api.types.is_numeric_dtype(frame[column])
    ]
    text_fill_columns = [
        column
        for column in advice_by_action.get("fill_null", [])
        if column in frame.columns and not pd.api.types.is_numeric_dtype(frame[column])
    ]
    cast_numeric_columns = advice_by_action.get("cast_numeric", [])
    cast_datetime_columns = advice_by_action.get("cast_datetime", [])
    exclude_columns = advice_by_action.get("exclude_column", [])

    if numeric_fill_columns:
        recommendations.append(
            _build_recommended_step(
                recommendation_id="fill-null-numeric",
                title="为高缺失数值字段补 0",
                description="这些数值字段缺失较多，先补 0 可以减少训练时的空值干扰。",
                step={
                    "step_id": "advisor_fill_null_numeric",
                    "step_type": "fill_null",
                    "enabled": True,
                    "input_selector": {"mode": "explicit", "columns": numeric_fill_columns},
                    "params": {"value": "0"},
                    "output_mode": {"mode": "inplace"},
                },
            )
        )

    if text_fill_columns:
        recommendations.append(
            _build_recommended_step(
                recommendation_id="fill-null-text",
                title="为高缺失文本字段补 missing",
                description="这些文本字段缺失较多，先补一个统一占位值更方便后续特征处理。",
                step={
                    "step_id": "advisor_fill_null_text",
                    "step_type": "fill_null",
                    "enabled": True,
                    "input_selector": {"mode": "explicit", "columns": text_fill_columns},
                    "params": {"value": "missing"},
                    "output_mode": {"mode": "inplace"},
                },
            )
        )

    if cast_numeric_columns:
        recommendations.append(
            _build_recommended_step(
                recommendation_id="cast-numeric",
                title="将数值字符串转成浮点数",
                description="这些字段看起来是数值字符串，先转成数值更适合模型直接使用。",
                step={
                    "step_id": "advisor_cast_numeric",
                    "step_type": "cast_type",
                    "enabled": True,
                    "input_selector": {"mode": "explicit", "columns": cast_numeric_columns},
                    "params": {"target_type": "float"},
                    "output_mode": {"mode": "inplace"},
                },
            )
        )

    if cast_datetime_columns:
        recommendations.append(
            _build_recommended_step(
                recommendation_id="cast-datetime",
                title="将时间字符串转成时间类型",
                description="这些字段更像时间字段，建议先标准化成 datetime。",
                step={
                    "step_id": "advisor_cast_datetime",
                    "step_type": "cast_type",
                    "enabled": True,
                    "input_selector": {"mode": "explicit", "columns": cast_datetime_columns},
                    "params": {"target_type": "datetime"},
                    "output_mode": {"mode": "inplace"},
                },
            )
        )

    if exclude_columns:
        keep_columns = [column for column in frame.columns if column not in set(exclude_columns)]
        recommendations.append(
            _build_recommended_step(
                recommendation_id="exclude-columns",
                title="排除常量、空列和明显 ID 字段",
                description="这些字段大概率不会提供稳定训练信号，建议从预处理输出里排除。",
                step={
                    "step_id": "advisor_select_columns",
                    "step_type": "select_columns",
                    "enabled": True,
                    "input_selector": {"mode": "explicit", "columns": keep_columns},
                    "params": {},
                    "output_mode": {"mode": "inplace"},
                },
            )
        )

    return recommendations


def _build_issue_groups(field_advice: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped_fields = {
        "direct_trainable": [],
        "needs_cleaning": [],
        "route_to_features": [],
        "remove_from_output": [],
    }

    for advice in field_advice:
        action = advice["recommended_action"]
        if action == "keep":
            grouped_fields["direct_trainable"].append(advice["field"])
        elif action in {"fill_null", "cast_numeric", "cast_datetime"}:
            grouped_fields["needs_cleaning"].append(advice["field"])
        elif action == "move_to_feature_engineering":
            grouped_fields["route_to_features"].append(advice["field"])
        elif action in {"exclude_column", "drop_from_training"}:
            grouped_fields["remove_from_output"].append(advice["field"])

    issue_groups: list[dict[str, Any]] = []
    for issue_type, fields in grouped_fields.items():
        if not fields:
            continue
        definition = ISSUE_GROUP_DEFINITIONS[issue_type]
        issue_groups.append(
            {
                "issue_type": issue_type,
                "title": definition["title"],
                "description": definition["description"],
                "fields": fields,
                "recommended_action": definition["recommended_action"],
                "handoff_target": definition["handoff_target"],
            }
        )
    return issue_groups


def _build_recommended_step(
    *,
    recommendation_id: str,
    title: str,
    description: str,
    step: dict[str, Any],
) -> dict[str, Any]:
    return {
        "recommendation_id": recommendation_id,
        "title": title,
        "description": description,
        "step": step,
    }


def _set_job_state(db: Session, job: Job, *, status: str, progress: int, message: str) -> None:
    job.status = status
    job.progress = progress
    job.message = message
    db.add(job)
    db.commit()
    db.refresh(job)
