from types import SimpleNamespace
from unittest.mock import MagicMock

from qeval.base_commands.status import BaseStatusCommand


class FakeStatusCommand(BaseStatusCommand):
    def default_regex(self):
        return "^fake-server"


def fake_process(pid, cmdline):
    """A process as `psutil.process_iter` returns it."""
    process = MagicMock()
    process.as_dict.return_value = {
        "pid": pid,
        "username": "user",
        "create_time": 0,
        "memory_info": SimpleNamespace(rss=0),
        "cmdline": cmdline,
    }
    return process


def test_base_status_shows_only_engine_processes(
    parse_args, monkeypatch, caplog
):
    processes = [
        fake_process(4711, ["fake-server", "-i", "olympics"]),
        fake_process(4712, ["other-program"]),
    ]
    monkeypatch.setattr(
        "qlever.commands.status.psutil.process_iter", lambda: processes
    )
    command = FakeStatusCommand()
    args = parse_args(command, [])

    assert command.execute(args)

    assert "fake-server -i olympics" in caplog.text
    assert "other-program" not in caplog.text
