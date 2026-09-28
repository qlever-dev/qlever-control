import os
from unittest.mock import MagicMock

import pytest


# A signal to a process id or group of 1 or less goes to the caller's own
# group (0), to every process of the user (-1, which is also what `killpg(1)`
# means), or to a group given as a negative number. No test may do that: on
# 2026-09-13, a `killpg` with the `pid` of a mocked `Popen` (a `MagicMock`,
# which evaluates to 1) killed every process of the user on two machines,
# including the production servers. Such a call now fails the test instead.
@pytest.fixture(autouse=True)
def no_broadcast_signals(monkeypatch):
    def guarded(name, original):
        def send(target, sig, *rest):
            if not isinstance(target, int) or target <= 1:
                raise AssertionError(
                    f"os.{name}({target!r}, {sig!r}) would signal processes "
                    "that are not children of the test, refused"
                )
            return original(target, sig, *rest)

        return send

    monkeypatch.setattr(os, "kill", guarded("kill", os.kill))
    monkeypatch.setattr(os, "killpg", guarded("killpg", os.killpg))


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
