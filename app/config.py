"""Application configuration module."""

from typing import ClassVar, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Runtime Flask settings loaded from environment variables."""

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    flask_env: Literal["development", "production"] = "production"
    debug: bool = False


class KafkaSettings(BaseSettings):
    """Runtime Kafka producer settings loaded from environment variables."""

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )

    bootstrap_servers: str = Field(default="", min_length=1)
    topic: str = Field(default="", min_length=1)
    acks: str = "all"
    enable_idempotence: bool = True
    linger_ms: int = 5
    batch_size: int = 65536


app_settings: AppSettings = AppSettings()
kafka_settings: KafkaSettings = KafkaSettings()
