import argparse

import pytest

from qeval.base_commands.index_stats import BaseIndexStatsCommand
from qlever.config import add_subparser_for_command
from qlever.qleverfile import Qleverfile


class FakeIndexStatsCommand(BaseIndexStatsCommand):
    def index_size_patterns(self, args):
        return [f"{args.name}.fake-index.*"]

    def parse_index_durations(self, log_text):
        phases = (line.rsplit(maxsplit=1) for line in log_text.splitlines())
        return {heading: float(seconds) for heading, seconds in phases}


@pytest.fixture
def fake_index_stats():
    """
    The `index-stats` command of a fake engine. Its index log has one line
    per phase, like `TOTAL time 90`, and its index files are the files
    `<name>.fake-index.*`.
    """
    return FakeIndexStatsCommand()


@pytest.fixture
def parse_args():
    """Parse a command line with the real parser of a command."""

    def parse(command, argv):
        parser = argparse.ArgumentParser()
        parser.set_defaults(
            script_name="qeval",
            engine_short_name="fake",
            engine_display_name="Fake",
            endpoint_path="/sparql",
            main_command_name="qeval fake",
        )
        subparsers = parser.add_subparsers()
        add_subparser_for_command(
            subparsers,
            "cmd",
            command,
            Qleverfile.all_arguments("qeval fake"),
        )
        return parser.parse_args(["cmd", *argv])

    return parse
