from __future__ import annotations

import argparse
import os
import shlex
import sys
import traceback
from configparser import ConfigParser
from importlib.metadata import version
from pathlib import Path

import argcomplete
from termcolor import colored

from qlever import load_commands
from qlever.command import QleverCommand
from qlever.log import log, log_levels
from qlever.qleverfile import Qleverfile


# Simple exception class for configuration errors (the class need not do
# anything, we just want a distinct exception type).
class ConfigException(Exception):
    def __init__(self, message):
        stack = traceback.extract_stack()[-2]  # Caller's frame.
        self.filename = stack.filename
        self.lineno = stack.lineno
        full_message = f"{message} [in {self.filename}:{self.lineno}]"
        super().__init__(full_message)


# Default name of the Qleverfile.
QLEVERFILE_DEFAULT_NAME = "Qleverfile"


def add_qleverfile_option(parser: argparse.ArgumentParser) -> None:
    """
    Add the `--qleverfile` option to the given parser.
    """
    parser.add_argument(
        "--qleverfile", "-q", type=str, default=QLEVERFILE_DEFAULT_NAME
    )


def resolve_qleverfile(
    path_name: str, engine_short_name: str, autocomplete_mode: bool
) -> tuple[ConfigParser | None, bool]:
    """
    Read the Qleverfile for the given engine, if there is one. Returns the parsed
    config (`None` when there is nothing to read) and whether the file
    exists, which the caller needs for its "no Qleverfile" warning.

    In autocompletion mode nothing is parsed, because it is expensive and not
    needed to complete anything.
    """
    qleverfile_path = Path(path_name)
    qleverfile_exists = qleverfile_path.is_file()

    # A missing Qleverfile is only an error if the user named it explicitly.
    if not qleverfile_exists and path_name != QLEVERFILE_DEFAULT_NAME:
        raise ConfigException(
            f"Qleverfile with non-default name "
            f"`{path_name}` specified, "
            f"but it does not exist"
        )

    # TODO: What if `command.should_have_qleverfile()` is `False`, should
    # we then parse the Qleverfile or not.
    if not qleverfile_exists or autocomplete_mode:
        return None, qleverfile_exists

    try:
        return Qleverfile.read(
            qleverfile_path, engine_short_name
        ), qleverfile_exists
    except Exception as e:
        log.info("")
        log.error(f"Error parsing Qleverfile `{qleverfile_path}`: {e}")
        log.info("")
        exit(1)


def add_subparser_for_command(
    subparsers,
    command_name: str,
    command_object: QleverCommand,
    all_qleverfile_args: dict,
    qleverfile_config: ConfigParser | None = None,
) -> None:
    """
    Add subparser for the given command. Take the arguments from
    `command_object.relevant_qleverfile_arguments()` and report an error if
    one of them is not contained in `all_qleverfile_args`. Overwrite the
    default values with the values from `qleverfile_config` if specified.
    """

    arg_names = command_object.relevant_qleverfile_arguments()

    # Helper function that shows a detailed error messahe when an argument
    # from `relevant_qleverfile_arguments` is not contained in
    # `all_qleverfile_args`.
    def argument_error(prefix):
        log.info("")
        log.error(
            f"{prefix} in `Qleverfile.all_arguments()` for command "
            f"`{command_name}`"
        )
        log.info("")
        log.info(
            f"Value of `relevant_qleverfile_arguments` for "
            f"command `{command_name}`:"
        )
        log.info("")
        log.info(f"{arg_names}")
        log.info("")
        exit(1)

    # Add the subparser.
    description = command_object.description()
    subparser = subparsers.add_parser(
        command_name, description=description, help=description
    )
    subparser.set_defaults(command_object=command_object)

    # Add the arguments relevant for the command.
    for section in arg_names:
        if section not in all_qleverfile_args:
            argument_error(f"Section `{section}` not found")
        for arg_name in arg_names[section]:
            if arg_name not in all_qleverfile_args[section]:
                argument_error(
                    f"Argument `{arg_name}` of section `{section}` not found"
                )
            args, kwargs = all_qleverfile_args[section][arg_name]
            kwargs_copy = kwargs.copy()
            action_type = kwargs_copy.get("action", "store")
            if action_type == "store" and "metavar" not in kwargs_copy:
                metavar = arg_name.upper()
                kwargs_copy["metavar"] = (
                    f"(in Qleverfile: [{section}] {metavar})"
                )
            # If `qleverfile_config` is given, add info about default
            # values to the help string.
            if qleverfile_config is not None:
                default_value = kwargs.get("default", None)
                qleverfile_value = qleverfile_config.get(
                    section, arg_name, fallback=None
                )
                if qleverfile_value is not None:
                    qleverfile_default = qleverfile_value
                    if "nargs" in kwargs_copy:
                        qleverfile_default = shlex.split(qleverfile_value)
                    kwargs_copy["default"] = qleverfile_default
                    kwargs_copy["required"] = False
                    escaped_value = qleverfile_value.replace("%", "%%")
                    kwargs_copy["help"] += (
                        f" [default, from Qleverfile: {escaped_value}]"
                    )
                else:
                    escaped_default = str(default_value).replace("%", "%%")
                    kwargs_copy["help"] += f" [default: {escaped_default}]"
            subparser.add_argument(*args, **kwargs_copy)

    # Additional arguments that are shared by all commands.
    command_object.additional_arguments(subparser)
    subparser.add_argument(
        "--show",
        action="store_true",
        default=False,
        help="Only show what would be executed, but don't execute it",
    )
    subparser.add_argument(
        "--log-level",
        choices=log_levels.keys(),
        default="INFO",
        help="Set the log level",
    )


def post_parse_warnings(
    args: argparse.Namespace, qleverfile_exists: bool
) -> None:
    """
    Warn about problems that are only visible once the arguments are parsed.
    """
    # If the command says that we should have a Qleverfile, but we don't,
    # issue a warning.
    if args.command_object.should_have_qleverfile():
        if not qleverfile_exists:
            log.warning(
                f"Invoking command `{args.command}` without a "
                "Qleverfile. You have to specify all required "
                "arguments on the command line. This is possible, "
                "but not recommended."
            )

    # Warn if the old binary names are still being used.
    if "IndexBuilderMain" in getattr(args, "index_binary", ""):
        log.warning(
            "The index binary has been renamed from "
            "`IndexBuilderMain` to `qlever-index`. Please update "
            "your Qleverfile or other configuration."
        )
    if "ServerMain" in getattr(args, "server_binary", ""):
        log.warning(
            "The server binary has been renamed from "
            "`ServerMain` to `qlever-server`. Please update "
            "your Qleverfile or other configuration."
        )


def warn_if_not_registered_for_argcomplete(script_name: str) -> None:
    """
    Remind the user to register the given console script for autocompletion.
    The script name is what `register-python-argcomplete` takes, and it also
    prefixes the two environment variables checked here.
    """
    prefix = script_name.upper()
    argcomplete_check_off = os.environ.get(f"{prefix}_ARGCOMPLETE_CHECK_OFF")
    argcomplete_enabled = os.environ.get(f"{prefix}_ARGCOMPLETE_ENABLED")
    if not argcomplete_enabled and not argcomplete_check_off:
        log.info("")
        log.warning(
            "To enable autocompletion, run the following command, "
            "and consider adding it to your `.bashrc` or `.zshrc`:\n\n"
            f'eval "$(register-python-argcomplete {script_name})" '
            f"&& export {prefix}_ARGCOMPLETE_ENABLED=1"
        )
        log.info("")


def parse_command_line() -> argparse.Namespace:
    """
    Build the parser, with defaults from the Qleverfile, and parse the command
    line with it.

    IMPORTANT: This runs on every execution of the `qlever` script, in
    particular each time the user triggers autocompletion. Keep everything
    before the call to `argcomplete.autocomplete(...)` cheap; in particular,
    do not parse the Qleverfile before that point, it is not needed for
    autocompletion.
    """
    # Determine whether we are in autocomplete mode or not.
    autocomplete_mode = "COMP_LINE" in os.environ

    warn_if_not_registered_for_argcomplete(script_name="qlever")

    # Create a temporary parser only to parse the `--qleverfile` option, in
    # case it is given, and to determine whether a command was given that
    # requires a Qleverfile. This is because in the actual parser below we
    # want the values from the Qleverfile to be shown in the help strings,
    # but only if this is actually necessary.
    qleverfile_parser = argparse.ArgumentParser(add_help=False)
    add_qleverfile_option(qleverfile_parser)
    qleverfile_parser.add_argument("command", type=str, nargs="?")
    qleverfile_args, _ = qleverfile_parser.parse_known_args()
    qleverfile_path_name = qleverfile_args.qleverfile

    qleverfile_config, qleverfile_exists = resolve_qleverfile(
        path_name=qleverfile_path_name,
        engine_short_name="qlever",
        autocomplete_mode=autocomplete_mode,
    )

    # Now the regular parser with commands and a subparser for each
    # command. We have a dedicated class for each command. These classes
    # are defined in the modules in `qlever/commands`, from where
    # `load_commands` creates one object of each.
    parser = argparse.ArgumentParser(
        description=colored(
            "This is the qlever command line tool, "
            "it's all you need to work with QLever",
            attrs=["bold"],
        )
    )

    # The short name of the engine (used for the container names), the name
    # of the engine as shown to the user, and the name of the main command
    # (what the user types before a command). These are provided to all
    # commands via `args`.
    parser.set_defaults(
        engine_short_name="qlever",
        engine_display_name="QLever",
        main_command_name="qlever",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('qlever')}",
    )
    add_qleverfile_option(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)
    all_args = Qleverfile.all_arguments(main_command_name="qlever")
    commands = load_commands(package="qlever")
    for command_name, command_object in commands.items():
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
