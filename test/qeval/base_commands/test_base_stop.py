import pytest

from qeval.base_commands.status import BaseStatusCommand
from qeval.base_commands.stop import BaseStopCommand


class FakeStatusCommand(BaseStatusCommand):
    def default_regex(self):
        # Differs from `status_regex` of the stop command, so that a test can
        # tell which one the fallback used.
        return "^fake-"


class FakeStopCommand(BaseStopCommand):
    def default_regex(self):
        return "^fake-server .* %%NAME%%"

    def status_command(self):
        return FakeStatusCommand()

    def status_regex(self):
        return "^fake-server"


# The stop functions are replaced in every test, so that no test can stop a
# real systemd unit, container, or process. By default, there is no container
# and one matching process, which is stopped.
@pytest.fixture(autouse=True)
def stop_systemd_unit(mock_command):
    mock_command("qlever.commands.stop", "systemd_unit_is_active")
    return mock_command("qlever.commands.stop", "stop_systemd_unit")


@pytest.fixture(autouse=True)
def stop_container(mock_command):
    mock = mock_command("qlever.commands.stop", "stop_container")
    mock.return_value = False
    return mock


@pytest.fixture(autouse=True)
def stop_process_with_regex(mock_command):
    mock = mock_command("qlever.commands.stop", "stop_process_with_regex")
    mock.return_value = [True]
    return mock


def test_base_stop_puts_dataset_name_into_regex(
    parse_args, stop_process_with_regex
):
    command = FakeStopCommand()
    args = parse_args(command, ["--name", "olympics"])

    command.execute(args)

    stop_process_with_regex.assert_called_once_with("^fake-server .* olympics")


def test_base_stop_never_touches_systemd(parse_args, stop_systemd_unit):
    command = FakeStopCommand()
    args = parse_args(command, ["--name", "olympics"])

    command.execute(args)

    stop_systemd_unit.assert_not_called()


def test_base_stop_shows_servers_when_nothing_matches(
    parse_args, stop_process_with_regex, monkeypatch, capsys
):
    stop_process_with_regex.return_value = []
    monkeypatch.setattr(
        "qlever.commands.status.psutil.process_iter", lambda: []
    )
    command = FakeStopCommand()
    args = parse_args(command, ["--name", "olympics"])

    assert command.execute(args)

    assert args.cmdline_regex == "^fake-server"
    assert "No processes found" in capsys.readouterr().out
