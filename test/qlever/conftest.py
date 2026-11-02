import pytest


@pytest.fixture
def write_log(tmp_path):
    """Write bytes to a fresh log file and return its Path."""

    def make(data):
        path = tmp_path / "q.log"
        path.write_bytes(data)
        return path

    return make
