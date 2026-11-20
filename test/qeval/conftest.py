import argparse

import pytest

from qeval.engines import ENGINES, add_engine_qleverfile_args
from qlever.config import add_subparser_for_command
from qlever.qleverfile import Qleverfile


@pytest.fixture
def parse_args():
    """
    Parse a command line with the real parser of a command. Without an
    `engine`, the command belongs to a fake engine that has only the shared
    Qleverfile arguments.
    """

    def parse(command, argv, engine=None):
        if engine is None:
            engine_short_name = "fake"
            engine_display_name = "Fake"
            endpoint_path = "/sparql"
        else:
            engine_short_name = engine
            engine_display_name = ENGINES[engine].display_name
            endpoint_path = ENGINES[engine].endpoint_path
        main_command_name = f"qeval {engine_short_name}"
        all_args = Qleverfile.all_arguments(main_command_name)
        if engine is not None:
            add_engine_qleverfile_args(engine, all_args)

        parser = argparse.ArgumentParser()
        parser.set_defaults(
            script_name="qeval",
            engine_short_name=engine_short_name,
            engine_display_name=engine_display_name,
            endpoint_path=endpoint_path,
            main_command_name=main_command_name,
        )
        subparsers = parser.add_subparsers()
        add_subparser_for_command(subparsers, "cmd", command, all_args)
        return parser.parse_args(["cmd", *argv])

    return parse
