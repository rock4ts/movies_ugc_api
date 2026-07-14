"""Application configuration module."""

from functools import cached_property
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
    anonymous_topic: str = Field(default="", min_length=1)
    acks: str = "all"
    enable_idempotence: bool = True
    linger_ms: int = 5
    batch_size: int = 65536


class JWTSettings(BaseSettings):
    """Runtime JWT verification settings."""

    algorithm: str = "RS256"
    public_key_path: str = "certs/jwt-public.pem"

    @cached_property
    def public_key(self) -> bytes:
        with open(self.public_key_path, "rb") as key_file:
            return key_file.read()


app_settings: AppSettings = AppSettings()
kafka_settings: KafkaSettings = KafkaSettings()
jwt_settings: JWTSettings = JWTSettings()
