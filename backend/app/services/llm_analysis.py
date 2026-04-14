from __future__ import annotations

from datetime import UTC, datetime
import os
from typing import Any
import re

import httpx
import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.llm_provider_config import LlmProviderConfig
from app.models.model_version import ModelVersion
from app.models.project import Project
from app.schemas.analysis import LlmProviderConfigUpdate
from app.services.dataset_import import json_safe_records, load_parquet_frame
from app.services.training import get_model_version


THINK_TAG_PATTERN = re.compile(r"<think>(.*?)</think>", re.IGNORECASE | re.DOTALL)
VOLATILE_API_KEYS: dict[tuple[int, str], str] = {}


def get_or_create_llm_config(db: Session, project_id: int) -> LlmProviderConfig:
    project = db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    config = db.query(LlmProviderConfig).filter(LlmProviderConfig.project_id == project_id).first()
    if config:
        return config

    settings = get_settings()
    config = LlmProviderConfig(
        project_id=project_id,
        provider="ollama",
        enabled=True,
        base_url=settings.ollama_base_url,
        model_name="",
        api_key=None,
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return config


def update_llm_config(db: Session, project_id: int, payload: LlmProviderConfigUpdate) -> LlmProviderConfig:
    config = get_or_create_llm_config(db, project_id)
    provider = payload.provider.strip().lower()
    if provider not in {"ollama", "openai_compatible", "minimax"}:
        raise HTTPException(status_code=400, detail="provider must be ollama, minimax, or openai_compatible")

    previous_provider = config.provider
    config.provider = provider
    config.enabled = payload.enabled
    config.base_url = _normalize_base_url(provider, payload.base_url.strip())
    config.model_name = payload.model_name.strip()

    if payload.clear_api_key:
        VOLATILE_API_KEYS.pop((project_id, previous_provider), None)
        VOLATILE_API_KEYS.pop((project_id, provider), None)
        config.api_key = None
    elif payload.api_key is not None and payload.api_key.strip():
        api_key = payload.api_key.strip()
        if previous_provider != provider:
            VOLATILE_API_KEYS.pop((project_id, previous_provider), None)
        VOLATILE_API_KEYS[(project_id, provider)] = api_key
        config.api_key = _mask_api_key(api_key)
    elif previous_provider != provider:
        VOLATILE_API_KEYS.pop((project_id, previous_provider), None)
        config.api_key = None

    db.add(config)
    db.commit()
    db.refresh(config)
    return config


def serialize_llm_config(config: LlmProviderConfig) -> dict[str, Any]:
    resolved_api_key = _resolve_api_key(config.project_id, config.provider)
    return {
        "id": config.id,
        "project_id": config.project_id,
        "provider": config.provider,
        "enabled": bool(config.enabled),
        "base_url": config.base_url,
        "model_name": config.model_name,
        "has_api_key": bool(resolved_api_key),
        "api_key_hint": config.api_key,
        "created_at": config.created_at,
        "updated_at": config.updated_at,
    }


async def test_llm_config_payload(payload: LlmProviderConfigUpdate) -> dict[str, Any]:
    provider = payload.provider.strip().lower()
    if provider not in {"ollama", "openai_compatible", "minimax"}:
        raise HTTPException(status_code=400, detail="provider must be ollama, minimax, or openai_compatible")

    base_url = _normalize_base_url(provider, payload.base_url.strip())
    model_name = payload.model_name.strip()
    api_key = payload.api_key.strip() if payload.api_key else None

    if not base_url:
        raise HTTPException(status_code=400, detail="Please configure the model base URL first")
    if not model_name:
        raise HTTPException(status_code=400, detail="Please configure the model name first")
    if provider == "minimax" and not api_key:
        raise HTTPException(status_code=400, detail="MiniMax requires an API Key")

    await _test_provider_connection(provider, base_url, model_name, api_key)
    return {
        "provider": provider,
        "model_name": model_name,
        "base_url": base_url,
        "success": True,
        "detail": "Connection test succeeded",
    }


async def test_llm_config_for_project(
    db: Session,
    project_id: int,
    payload: LlmProviderConfigUpdate,
) -> dict[str, Any]:
    existing = get_or_create_llm_config(db, project_id)
    resolved_api_key = payload.api_key if payload.api_key not in (None, "") else _resolve_api_key(project_id, payload.provider)
    merged_payload = LlmProviderConfigUpdate(
        provider=payload.provider,
        enabled=payload.enabled,
        base_url=payload.base_url or existing.base_url,
        model_name=payload.model_name or existing.model_name,
        api_key=resolved_api_key,
        clear_api_key=False,
    )
    return await test_llm_config_payload(merged_payload)


async def explain_model_anomalies(db: Session, model_id: int, top_k: int) -> dict[str, Any]:
    model_version = get_model_version(db, model_id)
    config = get_or_create_llm_config(db, model_version.project_id)
    resolved_api_key = _resolve_api_key(model_version.project_id, config.provider)

    if not config.enabled:
        raise HTTPException(status_code=400, detail="LLM analysis is disabled for this project")
    if not config.base_url.strip():
        raise HTTPException(status_code=400, detail="Please configure the model base URL first")
    if not config.model_name.strip():
        raise HTTPException(status_code=400, detail="Please configure the model name first")

    source_frame = _load_top_rows_for_explanation(model_version, top_k)
    if source_frame.empty:
        raise HTTPException(status_code=400, detail="No prediction rows are available for explanation")

    prompt = _build_explanation_prompt(model_version, source_frame)
    explanation = await _call_provider(config, prompt, resolved_api_key)

    return {
        "model_id": model_version.id,
        "provider": config.provider,
        "model_name": config.model_name,
        "analyzed_rows": int(len(source_frame)),
        "explanation": explanation["final_content"].strip(),
        "final_content": explanation["final_content"].strip(),
        "reasoning_content": explanation["reasoning_content"].strip() if explanation["reasoning_content"] else None,
        "source_columns": list(source_frame.columns),
        "source_rows": json_safe_records(source_frame),
        "generated_at": datetime.now(UTC),
    }


def _load_top_rows_for_explanation(model_version: ModelVersion, top_k: int) -> pd.DataFrame:
    if not model_version.prediction_path:
        raise HTTPException(status_code=400, detail="Model version has no prediction output")

    frame = load_parquet_frame(model_version.prediction_path).copy()
    if frame.empty:
        return frame

    if "anomaly_score" in frame.columns:
        if "predicted_label" in frame.columns:
            anomaly_frame = frame.loc[frame["predicted_label"].astype(str) == "anomaly"].copy()
            if not anomaly_frame.empty:
                frame = anomaly_frame
        frame = frame.sort_values("anomaly_score", ascending=False)
    elif "prediction_proba" in frame.columns:
        frame = frame.sort_values("prediction_proba", ascending=False)

    return frame.head(top_k).reset_index(drop=True)


def _build_explanation_prompt(model_version: ModelVersion, rows: pd.DataFrame) -> str:
    metrics = model_version.metrics or {}
    report_json = model_version.report_json or {}
    rows_payload = rows.to_dict(orient="records")

    return (
        "你是一名网络安全日志分析专家。请基于下面的模型检测结果，对异常情况做面向安全分析师的解读。\n\n"
        "输出要求：\n"
        "1. 先给出总体判断，说明这批异常更像什么类型的问题。\n"
        "2. 总结最可疑的字段和模式，点出高风险特征。\n"
        "3. 说明这些异常为什么可能被模型判为异常。\n"
        "4. 给出 3 到 5 条人工排查建议。\n"
        "5. 如果证据不足，要明确说明不确定性。\n"
        "6. 用中文输出，结构清晰，避免空话。\n\n"
        f"模型模式: {model_version.mode}\n"
        f"算法: {model_version.algorithm}\n"
        f"指标摘要: {metrics}\n"
        f"附加报告: {report_json}\n"
        f"特征列: {model_version.feature_columns}\n"
        f"Top 异常样本: {rows_payload}\n"
    )


async def _call_provider(config: LlmProviderConfig, prompt: str, api_key: str | None) -> dict[str, str | None]:
    provider = config.provider.lower()
    timeout = httpx.Timeout(45.0, connect=10.0)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if provider == "ollama":
                response = await client.post(
                    f"{config.base_url.rstrip('/')}/api/chat",
                    json={
                        "model": config.model_name,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "options": {"temperature": 0.2},
                    },
                )
                if response.status_code >= 400:
                    raise HTTPException(status_code=502, detail=_provider_error("Ollama", response))
                payload = _response_json_or_error("Ollama", response)
                content = _extract_provider_content(provider, payload)
                if not content["final_content"]:
                    raise HTTPException(status_code=502, detail="Ollama returned an empty analysis result")
                return content

            if provider in {"openai_compatible", "minimax"}:
                if provider == "minimax" and not api_key:
                    raise HTTPException(status_code=400, detail="MiniMax requires an API Key")
                headers = {"Content-Type": "application/json"}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                response = await client.post(
                    f"{config.base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json={
                        "model": config.model_name,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.2,
                        **({"reasoning_split": True} if provider == "minimax" else {}),
                    },
                )
                if response.status_code >= 400:
                    provider_name = "MiniMax" if provider == "minimax" else "OpenAI-compatible"
                    raise HTTPException(status_code=502, detail=_provider_error(provider_name, response))
                payload = _response_json_or_error("MiniMax" if provider == "minimax" else "OpenAI-compatible", response)
                content = _extract_provider_content(provider, payload)
                if not content["final_content"]:
                    raise HTTPException(status_code=502, detail="Online model returned an empty analysis result")
                return content
    except HTTPException:
        raise
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail=f"{_provider_display_name(provider)} request timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{_provider_display_name(provider)} request failed: {exc}",
        ) from exc

    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


async def _test_provider_connection(provider: str, base_url: str, model_name: str, api_key: str | None) -> None:
    timeout = httpx.Timeout(25.0, connect=10.0)
    prompt = "Reply with exactly OK."

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if provider == "ollama":
                response = await client.post(
                    f"{base_url.rstrip('/')}/api/chat",
                    json={
                        "model": model_name,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "options": {"temperature": 0.1},
                    },
                )
                if response.status_code >= 400:
                    raise HTTPException(status_code=502, detail=_provider_error("Ollama", response))
                _response_json_or_error("Ollama", response)
                return

            if provider in {"openai_compatible", "minimax"}:
                headers = {"Content-Type": "application/json"}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                response = await client.post(
                    f"{base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json={
                        "model": model_name,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.1,
                        "max_tokens": 8,
                    },
                )
                if response.status_code >= 400:
                    provider_name = "MiniMax" if provider == "minimax" else "OpenAI-compatible"
                    raise HTTPException(status_code=502, detail=_provider_error(provider_name, response))
                _response_json_or_error("MiniMax" if provider == "minimax" else "OpenAI-compatible", response)
                return
    except HTTPException:
        raise
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail=f"{_provider_display_name(provider)} request timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{_provider_display_name(provider)} request failed: {exc}",
        ) from exc


def _provider_error(provider_name: str, response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = response.text
    return f"{provider_name} request failed with status {response.status_code}: {payload}"


def _response_json_or_error(provider_name: str, response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{provider_name} returned a non-JSON response: {response.text[:300]}",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail=f"{provider_name} returned an unexpected response shape")
    return payload


def _provider_display_name(provider: str) -> str:
    if provider == "ollama":
        return "Ollama"
    if provider == "minimax":
        return "MiniMax"
    if provider == "openai_compatible":
        return "OpenAI-compatible"
    return provider


def _mask_api_key(api_key: str | None) -> str | None:
    if not api_key:
        return None
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}...{api_key[-4:]}"


def _provider_env_var_names(project_id: int, provider: str) -> list[str]:
    normalized_provider = provider.strip().upper().replace("-", "_")
    names = [f"XTRAIN_PROJECT_{project_id}_{normalized_provider}_API_KEY"]
    if provider == "minimax":
        names.extend(["XTRAIN_MINIMAX_API_KEY", "MINIMAX_API_KEY"])
    elif provider == "openai_compatible":
        names.extend(["XTRAIN_OPENAI_COMPATIBLE_API_KEY", "OPENAI_API_KEY"])
    return names


def _resolve_api_key(project_id: int, provider: str) -> str | None:
    for env_name in _provider_env_var_names(project_id, provider):
        value = os.getenv(env_name)
        if value and value.strip():
            return value.strip()
    volatile = VOLATILE_API_KEYS.get((project_id, provider))
    if volatile and volatile.strip():
        return volatile.strip()
    return None


def _normalize_base_url(provider: str, base_url: str) -> str:
    if base_url:
        return base_url
    if provider == "minimax":
        return "https://api.minimaxi.com/v1"
    return base_url


def _extract_provider_content(provider: str, payload: dict[str, Any]) -> dict[str, str | None]:
    if provider == "ollama":
        message = payload.get("message") or {}
        content = _to_text(message.get("content"))
        reasoning = _extract_reasoning_from_message(message) or _extract_reasoning_from_content(content)
        final_content = _strip_think_tags(content)
        return {
            "final_content": final_content.strip(),
            "reasoning_content": reasoning.strip() if reasoning else None,
        }

    choices = payload.get("choices") or []
    message = ((choices[0] or {}).get("message") or {}) if choices else {}
    content = _to_text(message.get("content"))
    reasoning = _extract_reasoning_from_message(message) or _extract_reasoning_from_content(content)
    final_content = _strip_think_tags(content)
    return {
        "final_content": final_content.strip(),
        "reasoning_content": reasoning.strip() if reasoning else None,
    }


def _extract_reasoning_from_message(message: dict[str, Any]) -> str | None:
    direct_fields = [
        message.get("thinking"),
        message.get("reasoning"),
        message.get("reasoning_content"),
    ]
    for value in direct_fields:
        text = _to_text(value)
        if text.strip():
            return text

    reasoning_details = message.get("reasoning_details")
    if isinstance(reasoning_details, list):
        chunks: list[str] = []
        for item in reasoning_details:
            text = _to_text(item)
            if text.strip():
                chunks.append(text.strip())
            elif isinstance(item, dict):
                for key in ("text", "content", "reasoning"):
                    value = _to_text(item.get(key))
                    if value.strip():
                        chunks.append(value.strip())
                        break
        if chunks:
            return "\n\n".join(chunks)
    return None


def _extract_reasoning_from_content(content: str) -> str | None:
    matches = THINK_TAG_PATTERN.findall(content or "")
    if not matches:
        return None
    reasoning = "\n\n".join(match.strip() for match in matches if match.strip())
    return reasoning or None


def _strip_think_tags(content: str) -> str:
    return THINK_TAG_PATTERN.sub("", content or "").strip()


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                for key in ("text", "content", "reasoning"):
                    text = item.get(key)
                    if isinstance(text, str) and text:
                        parts.append(text)
                        break
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("text", "content", "reasoning"):
            text = value.get(key)
            if isinstance(text, str) and text:
                return text
    return str(value)
