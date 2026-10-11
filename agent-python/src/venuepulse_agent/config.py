"""Typed configuration loaded from VP_* environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VP_", env_file=".env", extra="ignore")

    log_level: Literal["debug", "info", "warning", "error"] = "info"
    log_json: bool = True

    # Required once the chat loop lands (M2); optional so the skeleton boots.
    anthropic_api_key: SecretStr | None = None
    agent_model: str = "claude-sonnet-5-5"
    agent_max_iterations: int = Field(default=8, ge=1, le=32)
    agent_timeout_s: float = Field(default=60, gt=0)

    gateway_internal_url: str = "http://localhost:8080/internal/v1"
    gateway_service_token: SecretStr | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
