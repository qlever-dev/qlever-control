from __future__ import annotations

import argparse
import os
import traceback
from importlib.metadata import version
from pathlib import Path

import argcomplete
from termcolor import colored

from qlever import command_objects, engine_name, script_name
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


class QleverConfig:
    """
    Class that manages all config parameters, and overwrites them with the
    settings from a Qleverfile.

    IMPORTANT: An instance of this class is created for each execution of
    the `qlever` script, in particular, each time the user triggers
    autocompletion. Therefore, pay attention that no unnecessary work is done
    before the call to `argcomplete.autocomplete(...)`. In particular, avoid
    parsing the Qleverfile before that point, it's not needed for
    autocompletion.
    """

    def add_subparser_for_command(self, subparsers, command_name,
                                  command_object, all_qleverfile_args,
                                  qleverfile_config=None):
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
            log.error(f"{prefix} in `Qleverfile.all_arguments()` for command "
                      f"`{command_name}`")
            log.info("")
            log.info(f"Value of `relevant_qleverfile_arguments` for "
                     f"command `{command_name}`:")
            log.info("")
            log.info(f"{arg_names}")
            log.info("")
            exit(1)

        # Add the subparser.
        description = command_object.description()
        subparser = subparsers.add_parser(command_name,
                                          description=description,
                                          help=description)
        
        # qleverfile_group = subparser.add_argument_group(
        #     "Qleverfile options",
        #     "These options can also be specified in the Qleverfile under "
        #     "the relevant [section]"
        # )

        # cli_only_group = subparser.add_argument_group(
        #     "Command line only options",
        #     "These options cannot be set in the Qleverfile"
        # )

        # Add the arguments relevant for the command.
        for section in arg_names:
            if section not in all_qleverfile_args:
                argument_error(f"Section `{section}` not found")
            for arg_name in arg_names[section]:
                if arg_name not in all_qleverfile_args[section]:
                    argument_error(f"Argument `{arg_name}` of section "
                                   f"`{section}` not found")
                args, kwargs = all_qleverfile_args[section][arg_name]
                kwargs_copy = kwargs.copy()
                # kwargs_copy["help"] = f"[{section}] {kwargs_copy["help"]}"
                action_type = kwargs_copy.get("action", "store")
                if action_type == "store" and "metavar" not in kwargs_copy:
                    metavar = arg_name.upper()
                    kwargs_copy["metavar"] = f"(In Qleverfile: [{section}] {metavar})"
                # If `qleverfile_config` is given, add info about default
                # values to the help string.
                if qleverfile_config is not None:
                    default_value = kwargs.get("default", None)
                    qleverfile_value = qleverfile_config.get(
                            section, arg_name, fallback=None)
                    if qleverfile_value is not None:
                        kwargs_copy["default"] = qleverfile_value
                        kwargs_copy["required"] = False
                        kwargs_copy["help"] += (f" [default, from Qleverfile:"
                                                f" {qleverfile_value}]")
                    else:
                        kwargs_copy["help"] += f" [default: {default_value}]"
                subparser.add_argument(*args, **kwargs_copy)

        # Additional arguments that are shared by all commands.
        command_object.additional_arguments(subparser)
        subparser.add_argument("--show", action="store_true",
                               default=False,
                               help="Only show what would be executed"
                                    ", but don't execute it")
        subparser.add_argument("--log-level",
                               choices=log_levels.keys(),
                               default="INFO",
                               help="Set the log level")

    def parse_args(self):
        # Determine whether we are in autocomplete mode or not.
        autocomplete_mode = "COMP_LINE" in os.environ

        # Check if the user has registered this script for argcomplete.
        argcomplete_check_off = os.environ.get("QLEVER_ARGCOMPLETE_CHECK_OFF")
        argcomplete_enabled = os.environ.get("QLEVER_ARGCOMPLETE_ENABLED")
        if not argcomplete_enabled and not argcomplete_check_off:
            log.info("")
            log.warn(f"To enable autocompletion, run the following command, "
                     f"and consider adding it to your `.bashrc` or `.zshrc`:"
                     f"\n\n"
                     f"eval \"$(register-python-argcomplete {script_name})\""
                     f" && export QLEVER_ARGCOMPLETE_ENABLED=1")
            log.info("")

        # Create a temporary parser only to parse the `--qleverfile` option, in
        # case it is given, and to determine whether a command was given that
        # requires a Qleverfile. This is because in the actual parser below we
        # want the values from the Qleverfile to be shown in the help strings,
        # but only if this is actually necessary.
        def add_qleverfile_option(parser):
            parser.add_argument("--qleverfile", "-q", type=str,
                                default="Qleverfile")
        qleverfile_parser = argparse.ArgumentParser(add_help=False)
        add_qleverfile_option(qleverfile_parser)
        qleverfile_parser.add_argument("command", type=str, nargs="?")
        qleverfile_args, _ = qleverfile_parser.parse_known_args()
        qleverfile_path_name = qleverfile_args.qleverfile
        # command = qleverfile_args.command
        # should_have_qleverfile = command in command_objects \
        #     and command_objects[command].should_have_qleverfile()

        # Check if the Qleverfile exists and if we are using the default name.
        # We need this again further down in the code, so remember it.
        qleverfile_path = Path(qleverfile_path_name)
        qleverfile_exists = qleverfile_path.is_file()
        qleverfile_is_default = qleverfile_path_name \
            == qleverfile_parser.get_default("qleverfile")
        # If a Qleverfile with a non-default name was specified, but it does
        # not exist, that's an error.
        if not qleverfile_exists and not qleverfile_is_default:
            raise ConfigException(f"Qleverfile with non-default name "
                                  f"`{qleverfile_path_name}` specified, "
                                  f"but it does not exist")
        # If it exists and we are not in the autocompletion mode, parse it.
        #
        # IMPORTANT: No need to parse the Qleverfile in autocompletion mode and
        # it would be unnecessarily expensive to do so.
        #
        # TODO: What if `command.should_have_qleverfile()` is `False`, should
        # we then parse the Qleverfile or not.
        if qleverfile_exists and not autocomplete_mode:
            try:
                qleverfile_config = Qleverfile.read(qleverfile_path)
            except Exception as e:
                log.info("")
                log.error(f"Error parsing Qleverfile `{qleverfile_path}`"
                          f": {e}")
                log.info("")
                exit(1)
        else:
            qleverfile_config = None

        # Now the regular parser with commands and a subparser for each
        # command. We have a dedicated class for each command. These classes
        # are defined in the modules in `qlever/commands`. In `__init__.py`
        # an object of each class is created and stored in `command_objects`.
        parser = argparse.ArgumentParser(
            description=colored(
                f"This is the {script_name} command line tool, "
                f"it's all you need to work with {engine_name}",
                attrs=["bold"],
            )
        )
        if script_name == "qlever":
            parser.add_argument(
                "--version",
                action="version",
                version=f"%(prog)s {version('qlever')}",
            )
        add_qleverfile_option(parser)
        subparsers = parser.add_subparsers(dest='command')
        subparsers.required = True
        all_args = Qleverfile.all_arguments()
        for command_name, command_object in command_objects.items():
            self.add_subparser_for_command(
                    subparsers, command_name, command_object,
                    all_args, qleverfile_config)

        # Enable autocompletion for the commands and their options.
        #
        # NOTE: All code executed before this line should be relatively cheap
        # because it is executed whenever the user triggers the autocompletion.
        argcomplete.autocomplete(parser, always_complete_options="long")

        # If called without arguments, show the help message.
        if len(os.sys.argv) == 1:
            parser.print_help()
            exit(0)

        # Parse the command line arguments.
        args = parser.parse_args()

        # If the command says that we should have a Qleverfile, but we don't,
        # issue a warning.
        if command_objects[args.command].should_have_qleverfile():
            if not qleverfile_exists:
                log.warning(f"Invoking command `{args.command}` without a "
                            "Qleverfile. You have to specify all required "
                            "arguments on the command line. This is possible, "
                            "but not recommended.")

        return args
