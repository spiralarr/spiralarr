"""Configuration settings using Pydantic BaseSettings."""

from functools import lru_cache
import os
from pathlib import Path
import secrets
from typing import ClassVar, Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Define the base directory for Spiralarr configuration and data
SPIRALARR_HOME = os.path.expanduser("~/.spiralarr")


class CoreSettings(BaseSettings):
    """Core application settings."""

    spiralarr_home: str = SPIRALARR_HOME
    dags_folder: str = os.path.join(SPIRALARR_HOME, "dags")
    logs_folder: str = os.path.join(SPIRALARR_HOME, "logs")
    # A secret key is crucial for security features like signing sessions.
    # Auto-generated if not provided for security.
    secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    # Default to a simple SQLite database within the SPIRALARR_HOME.
    connection_string: str = f"sqlite:///{os.path.join(SPIRALARR_HOME, 'spiralarr.db')}"
    echo_sql: bool = False


class SchedulerSettings(BaseSettings):
    """Scheduler-specific settings."""

    dag_discovery_interval: int = 300  # In seconds
    max_threads: int = 4
    heartbeat_sec: int = 5
    zombie_task_threshold_sec: int = 300


class ApiSettings(BaseSettings):
    """API server settings."""

    host: str = "127.0.0.1"
    port: int = 8080


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    # Component-level logging (for scheduler, executor, etc.)
    component_log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"] = "WARN"
    # Task-level logging (for individual task runs)
    task_log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"] = "INFO"
    # Airflow-compatible log file naming template
    log_file_template: str = (
        "dag_id={{ dag_id }}/run_id={{ run_id }}/task_id={{ task_id }}/"
        "{% if map_index >= 0 %}map_index={{ map_index }}/{% endif %}"
        "attempt={{ try_number }}.log"
    )


def find_toml_config() -> Optional[str]:
    """Find spiralarr.toml config file in search order."""
    search_paths = [
        Path.cwd() / "spiralarr.toml",  # Current working directory
        Path(SPIRALARR_HOME) / "spiralarr.toml",  # ~/.spiralarr/spiralarr.toml
        Path.home() / "spiralarr.toml",  # ~/spiralarr.toml
    ]

    for path in search_paths:
        if path.exists():
            return str(path)
    return None


class AppSettings(BaseSettings):
    """Main application settings container."""

    # Prefix for environment variables, e.g., SPIRALARR_CORE_DAGS_FOLDER
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="SPIRALARR_",
        env_nested_delimiter="__",
        toml_file=find_toml_config(),
        validate_default=True,
    )

    core: CoreSettings = CoreSettings()
    database: DatabaseSettings = DatabaseSettings()
    scheduler: SchedulerSettings = SchedulerSettings()
    api: ApiSettings = ApiSettings()
    logging: LoggingSettings = LoggingSettings()


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Get singleton settings instance with lazy initialization."""
    settings = AppSettings()
    initialize_spiralarr_home(settings)
    return settings


def initialize_spiralarr_home(settings: AppSettings):
    """Create necessary directories for Spiralarr."""
    os.makedirs(settings.core.dags_folder, exist_ok=True)
    os.makedirs(settings.core.logs_folder, exist_ok=True)


# Singleton instance to be used across the application
settings = get_settings()
