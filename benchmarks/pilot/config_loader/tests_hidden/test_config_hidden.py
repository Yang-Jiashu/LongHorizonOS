"""Hidden tests for the config_loader task.

These tests are NOT visible to the runtime or the LLM.
They are used by the external grader only.
"""

import json
import tempfile
from pathlib import Path

import pytest


def test_empty_file_raises_error():
    """An empty config file should raise a parse error."""
    from sample_app.config_loader import ConfigLoader, ConfigParseError

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("")
        f.flush()
        loader = ConfigLoader(f.name)
        with pytest.raises(ConfigParseError):
            loader.load()


def test_empty_json_object():
    """An empty JSON object should load successfully."""
    from sample_app.config_loader import ConfigLoader

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("{}")
        f.flush()
        loader = ConfigLoader(f.name)
        config = loader.load()
        assert config == {}


def test_nested_config():
    """Nested config should be loaded correctly."""
    from sample_app.config_loader import ConfigLoader

    data = {"database": {"host": "localhost", "port": 5432}, "features": ["auth", "logging"]}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        f.flush()
        loader = ConfigLoader(f.name)
        config = loader.load()
        assert config["database"]["host"] == "localhost"
        assert "auth" in config["features"]


def test_config_loader_has_get_method():
    """ConfigLoader should have a get() method with default."""
    from sample_app.config_loader import ConfigLoader

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"app_name": "Test"}, f)
        f.flush()
        loader = ConfigLoader(f.name)
        loader.load()
        assert loader.get("app_name") == "Test"
        assert loader.get("nonexistent", "default") == "default"


def test_existing_tests_still_pass():
    """All existing tests (test_app.py) should still pass after migration."""
    from sample_app.app import get_app_name, get_database_url, get_port, load_settings

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"app_name": "MyApp", "database_url": "sqlite:///test.db", "port": 9999}, f)
        f.flush()
        settings = load_settings(f.name)
        assert get_database_url(settings) == "sqlite:///test.db"
        assert get_app_name(settings) == "MyApp"
        assert get_port(settings) == 9999


def test_readme_updated():
    """README should mention the config_loader module."""
    readme = Path(__file__).parent.parent / "initial_repo" / "README.md"
    # This test checks the workspace copy, not the original.
    # The grader will check the workspace README.
    # If the README exists in the workspace, check it.
    if readme.exists():
        content = readme.read_text(encoding="utf-8")
        assert "config" in content.lower()
