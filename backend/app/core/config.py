from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="xtrain", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    api_prefix: str = Field(default="/api/v1", alias="API_PREFIX")
    sqlite_path: str = Field(default="storage/xtrain.db", alias="SQLITE_PATH")
    storage_root: str = Field(default="storage", alias="STORAGE_ROOT")
    ollama_base_url: str = Field(default="http://127.0.0.1:11434", alias="OLLAMA_BASE_URL")
    max_concurrent_jobs: int = Field(default=2, alias="MAX_CONCURRENT_JOBS")

    model_config = SettingsConfigDict(
        env_file=("backend/.env", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    @property
    def sqlite_path_resolved(self) -> Path:
        path = Path(self.sqlite_path)
        if not path.is_absolute():
            path = self.project_root / path
        return path.resolve()

    @property
    def storage_root_path(self) -> Path:
        path = Path(self.storage_root)
        if not path.is_absolute():
            path = self.project_root / path
        return path.resolve()

    def resolve_storage_path(self, value: str | Path) -> Path:
        path = Path(value)
        if not path.is_absolute():
            path = self.project_root / path
        return path.resolve()

    @property
    def sqlite_url(self) -> str:
        return f"sqlite:///{self.sqlite_path_resolved.as_posix()}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
