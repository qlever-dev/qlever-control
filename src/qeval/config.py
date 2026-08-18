from __future__ import annotations

import argparse
import os
import sys
from importlib.metadata import version

import argcomplete
from termcolor import colored

from qeval.engines import ENGINES, add_engine_qleverfile_args
from qlever import load_commands
from qlever.config import (
    add_qleverfile_option,
    add_subparser_for_command,
    post_parse_warnings,
    resolve_qleverfile,
    warn_if_not_registered_for_argcomplete,
)
from qlever.qleverfile import Qleverfile

SCRIPT_NAME = "qeval"


def parse_command_line() -> argparse.Namespace:
    """
    Build the parser, with defaults from the Qleverfile, and parse the command
    line with it.

    Unlike for `qlever`, the engine is a positional argument, so it has to be
    peeked at before the real parser can be built. Its commands are then the
    only ones loaded.

    IMPORTANT: This runs on every execution of the `qeval` script, in
    particular each time the user triggers autocompletion. Keep everything
    before the call to `argcomplete.autocomplete(...)` cheap.
    """
    # Determine whether we are in autocomplete mode or not.
    autocomplete_mode = "COMP_LINE" in os.environ

    warn_if_not_registered_for_argcomplete(script_name=SCRIPT_NAME)

    # Peek at the engine and the `--qleverfile` option with a throwaway
    # parser. In autocomplete mode the words have to come from `COMP_LINE`,
    # because the completion hook is invoked without any arguments.
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument("engine", type=str, nargs="?")
    add_qleverfile_option(temp_parser)
    temp_parser.add_argument("command", type=str, nargs="?")
    if autocomplete_mode:
        comp_line = os.environ["COMP_LINE"]
        comp_point = int(os.environ.get("COMP_POINT", len(comp_line)))
        # Drop the first word (the name of the script).
        comp_words = comp_line[:comp_point].split()[1:]
        parsed_args, _ = temp_parser.parse_known_args(comp_words)
    else:
        parsed_args, _ = temp_parser.parse_known_args()
    # No engine, as for `qeval <TAB>`, is normal here and not an error.
    parsed_engine = parsed_args.engine
    qleverfile_path_name = parsed_args.qleverfile

    supported_engines = ", ".join(ENGINES)
    tool_description = (
        f"{SCRIPT_NAME} sets up, indexes, queries, and benchmarks "
        "graph databases in a uniform way. "
        f"Supported engines: {supported_engines}"
    )
    parser = argparse.ArgumentParser(
        description=colored(tool_description, attrs=["bold"])
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('qlever')}",
    )

    # Only the top-level parser gets `--qleverfile`. If the engine subparsers
    # had it too, argparse would apply their default last and silently
    # overwrite a value given before the engine.
    add_qleverfile_option(parser)

    # One subparser per engine, so that engine names complete and a wrong one
    # is reported properly. Only the selected engine gets commands, so that a
    # single invocation never imports another engine's code.
    engine_subparsers = parser.add_subparsers(dest="engine", required=True)
    qleverfile_config, qleverfile_exists = None, False
    for engine, engine_info in ENGINES.items():
        main_command_name = f"{SCRIPT_NAME} {engine}"
        engine_parser = engine_subparsers.add_parser(
            engine,
            help=engine_info.display_name,
            description=colored(
                "Set up, index, query, and benchmark "
                f"{engine_info.display_name}",
                attrs=["bold"],
            ),
        )
        engine_parser.set_defaults(
            script_name=SCRIPT_NAME,
            engine_short_name=engine,
            engine_display_name=engine_info.display_name,
            main_command_name=main_command_name,
        )
        subparsers = engine_parser.add_subparsers(
            dest="command", required=True
        )
        if engine == parsed_engine:
            # Container names are keyed on the engine, so the Qleverfile can
            # only be read once we know which engine this is.
            qleverfile_config, qleverfile_exists = resolve_qleverfile(
                qleverfile_path_name, parsed_engine, autocomplete_mode
            )
            all_args = Qleverfile.all_arguments(main_command_name)
            add_engine_qleverfile_args(engine, all_args)
            for command_name, command_object in load_commands(
                engine_info.package
            ).items():
                add_subparser_for_command(
                    subparsers,
                    command_name,
                    command_object,
                    all_args,
                    qleverfile_config,
                )

    # Enable autocompletion for the commands and their options.
    #
    # NOTE: All code executed before this line should be relatively cheap
    # because it is executed whenever the user triggers the autocompletion.
    argcomplete.autocomplete(parser, always_complete_options="long")

    # If called without arguments, show the help message.
    if len(sys.argv) == 1:
        parser.print_help()
        exit(0)

    # Parse the command line arguments.
    args = parser.parse_args()

    post_parse_warnings(args, qleverfile_exists)
    return args
