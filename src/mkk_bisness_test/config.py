from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    rabbitmq_url: str
    api_key: SecretStr = Field(min_length=16, max_length=128)
    webhook_timeout: float = Field(default=5, gt=0, le=30)
    webhook_allowed_hosts: list[str] = Field(default_factory=list)
    retry_base_seconds: float = Field(default=2, gt=0, le=300)
    outbox_poll_seconds: float = Field(default=0.5, gt=0, le=30)


@lru_cache
def get_settings() -> Settings:
    return Settings()
