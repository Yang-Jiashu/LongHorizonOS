"""Public tests for the config_loader task.

These tests are visible to the runtime and the LLM.
They define the minimum acceptable behavior.
"""

import json
import tempfile

import pytest


def test_load_valid_json_config():
    """Config loader should load a valid JSON config file."""
    from sample_app.config_loader import ConfigLoader

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"app_name": "TestApp", "port": 3000}, f)
        f.flush()
        loader = ConfigLoader(f.name)
        config = loader.load()
        assert config["app_name"] == "TestApp"
        assert config["port"] == 3000


def test_missing_file_raises_clear_error():
    """Config loader should raise a clear error for missing files."""
    from sample_app.config_loader import ConfigFileNotFoundError, ConfigLoader

    loader = ConfigLoader("/nonexistent/path/config.json")
    with pytest.raises(ConfigFileNotFoundError, match="not found"):
        loader.load()


def test_invalid_json_raises_clear_error():
    """Config loader should raise a clear error for invalid JSON."""
    from sample_app.config_loader import ConfigLoader, ConfigParseError

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("{invalid json content")
        f.flush()
        loader = ConfigLoader(f.name)
        with pytest.raises(ConfigParseError, match="invalid"):
            loader.load()


def test_app_uses_config_loader():
    """The app module should use the new config_loader instead of inline loading."""
    from sample_app.app import load_settings

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"app_name": "NewApp", "database_url": "postgres://localhost"}, f)
        f.flush()
        settings = load_settings(f.name)
        assert settings["app_name"] == "NewApp"
        assert settings["database_url"] == "postgres://localhost"
