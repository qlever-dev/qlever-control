from unittest.mock import MagicMock

import pytest


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
