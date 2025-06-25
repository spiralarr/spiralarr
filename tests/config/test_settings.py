"""Tests for the configuration system."""

import os
from unittest.mock import patch

from spiralarr.config.settings import AppSettings, get_settings


class TestConfigurationSystem:
    """Test the Pydantic-based configuration system."""

    def test_default_configuration(self):
        """Test that default configuration values are loaded correctly."""
        settings = AppSettings()

        # Test core settings
        assert settings.core.spiralarr_home == os.path.expanduser("~/.spiralarr")
        assert settings.core.dags_folder == os.path.join(
            os.path.expanduser("~/.spiralarr"), "dags"
        )
        assert settings.core.logs_folder == os.path.join(
            os.path.expanduser("~/.spiralarr"), "logs"
        )
        assert len(settings.core.secret_key) > 0  # Auto-generated

        # Test database settings
        assert "sqlite:///" in settings.database.connection_string
        assert settings.database.echo_sql is False

        # Test scheduler settings
        assert settings.scheduler.dag_discovery_interval == 300
        assert settings.scheduler.max_threads == 4
        assert settings.scheduler.heartbeat_sec == 5
        assert settings.scheduler.zombie_task_threshold_sec == 300

        # Test API settings
        assert settings.api.host == "127.0.0.1"
        assert settings.api.port == 8080

        # Test logging settings
        assert settings.logging.component_log_level == "WARN"
        assert settings.logging.task_log_level == "INFO"
        assert "dag_id=" in settings.logging.log_file_template

    def test_environment_variable_override(self):
        """Test that environment variables override default values."""
        env_vars = {
            "SPIRALARR_API__HOST": "0.0.0.0",
            "SPIRALARR_API__PORT": "9090",
            "SPIRALARR_SCHEDULER__MAX_THREADS": "8",
            "SPIRALARR_LOGGING__COMPONENT_LOG_LEVEL": "DEBUG",
            "SPIRALARR_DATABASE__ECHO_SQL": "true",
        }

        with patch.dict(os.environ, env_vars, clear=False):
            settings = AppSettings()

            assert settings.api.host == "0.0.0.0"
            assert settings.api.port == 9090
            assert settings.scheduler.max_threads == 8
            assert settings.logging.component_log_level == "DEBUG"
            assert settings.database.echo_sql is True

    def test_get_settings_singleton(self):
        """Test that get_settings returns a singleton instance."""
        # Clear any existing cache
        get_settings.cache_clear()

        settings1 = get_settings()
        settings2 = get_settings()

        # Should be the same instance
        assert settings1 is settings2

    def test_nested_settings_structure(self):
        """Test that nested settings structure works correctly."""
        settings = AppSettings()

        # Test that we can access nested attributes
        assert hasattr(settings, "core")
        assert hasattr(settings, "database")
        assert hasattr(settings, "scheduler")
        assert hasattr(settings, "api")
        assert hasattr(settings, "logging")

        # Test that nested objects have expected attributes
        assert hasattr(settings.core, "spiralarr_home")
        assert hasattr(settings.database, "connection_string")
        assert hasattr(settings.scheduler, "max_threads")
        assert hasattr(settings.api, "host")
        assert hasattr(settings.logging, "component_log_level")
