from unittest.mock import MagicMock

import pytest


@pytest.fixture
def write_log(tmp_path):
    """Write bytes to a fresh log file and return its Path."""

    def make(data):
        path = tmp_path / "q.log"
        path.write_bytes(data)
        return path

    return make


@pytest.fixture
def mock_command(monkeypatch):
    def _mock(module_name: str, function_name: str, override=None):
        if override:
            monkeypatch.setattr(f"{module_name}.{function_name}", override)
            return override
        mock = MagicMock(name=f"{function_name}_mock")
        monkeypatch.setattr(f"{module_name}.{function_name}", mock)
        return mock

    return _mock
