from qlever import load_commands
from qlever.command import QleverCommand


def test_load_commands():
    """
    Load all commands of the `qlever` package and check that they are sorted
    by name, that the module names are mapped to command names as expected,
    and that each command is an instance of `QleverCommand`.
    """
    commands = load_commands("qlever")
    assert list(commands) == sorted(commands)
    assert "index" in commands
    assert "index-stats" in commands
    assert all(isinstance(c, QleverCommand) for c in commands.values())
