from configparser import RawConfigParser

from qeval.base_commands.setup_config import BaseSetupConfigCommand


class FakeSetupConfigCommand(BaseSetupConfigCommand):
    def image(self):
        return "docker.io/fake/engine:1.0"

    def engine_settings(self, args):
        return {
            "server": {"PORT": "8888", "TIMEOUT": "60s"},
            # The olympics Qleverfile has no such section.
            "extra": {"OPTION": "value"},
        }


def read_qleverfile(path):
    """The Qleverfile at `path`, read without resolving `${...}`."""
    qleverfile = RawConfigParser()
    qleverfile.read(path)
    return qleverfile


def test_base_setup_config_writes_filtered_qleverfile(
    parse_args, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    command = FakeSetupConfigCommand()
    args = parse_args(command, ["olympics"])

    assert command.execute(args)

    qleverfile = read_qleverfile(tmp_path / "Qleverfile")
    assert qleverfile.sections() == [
        "data",
        "index",
        "server",
        "runtime",
        "ui",
    ]
    assert not qleverfile.has_option("server", "ACCESS_TOKEN")
    assert qleverfile.get("data", "NAME") == "olympics"
    assert qleverfile.get("runtime", "IMAGE") == "docker.io/fake/engine:1.0"
    assert qleverfile.get("server", "PORT") == "8888"
    assert qleverfile.get("server", "TIMEOUT") == "60s"


def test_base_setup_config_port_option_beats_engine_setting(
    parse_args, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    command = FakeSetupConfigCommand()
    args = parse_args(command, ["olympics", "--port", "9999"])

    assert command.execute(args)

    qleverfile = read_qleverfile(tmp_path / "Qleverfile")
    assert qleverfile.get("server", "PORT") == "9999"


def test_base_setup_config_keeps_existing_qleverfile(
    parse_args, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "Qleverfile").write_text("[data]\nNAME = mine\n")
    command = FakeSetupConfigCommand()
    args = parse_args(command, ["olympics"])

    assert not command.execute(args)

    assert (tmp_path / "Qleverfile").read_text() == "[data]\nNAME = mine\n"
