from pathlib import Path

import httpx
from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.health import HealthComponent, HealthRead


router = APIRouter()


@router.get("/system/health", response_model=HealthRead)
async def system_health() -> HealthRead:
    settings = get_settings()
    sqlite_path = settings.sqlite_path_resolved
    storage_path = settings.storage_root_path
    ollama_url = settings.ollama_base_url.rstrip("/")

    ollama_component = HealthComponent(status="down", detail="Ollama service is not reachable")
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{ollama_url}/api/tags")
        if response.is_success:
            ollama_component = HealthComponent(status="up", detail="Ollama service is reachable")
    except httpx.HTTPError:
        pass

    return HealthRead(
        api=HealthComponent(status="up", detail="FastAPI service is running"),
        sqlite=HealthComponent(
            status="up" if sqlite_path.exists() else "pending",
            detail=f"SQLite path: {sqlite_path}",
        ),
        storage=HealthComponent(
            status="up" if storage_path.exists() else "pending",
            detail=f"Storage root: {storage_path}",
        ),
        ollama=ollama_component,
    )
