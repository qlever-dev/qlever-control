from __future__ import annotations

import argparse
import re
import traceback
from abc import ABC, abstractmethod

from termcolor import colored

from qlever.log import log


class QleverCommand(ABC):
    """
    Abstract base class for all the commands in `qlever/commands`.
    """

    @abstractmethod
    def __init__(self):
        """
        Initialize the command.

        IMPORTANT: This should be very LIGHTWEIGHT (typically: a few
        assignments, if any) because we create one object per command and
        initialize each of them.
        """
        pass

    @abstractmethod
    def description(self) -> str:
        """
        A concise description of the command, which will be shown when the user
        types `qlever --help` or `qlever <command> --help`.
        """
        pass

    @abstractmethod
    def should_have_qleverfile(self) -> bool:
        """
        Return `True` if the command should have a Qleverfile, `False`
        otherwise. If a command should have a Qleverfile, but none is
        specified, the command can still be executed if all the required
        arguments are specified on the command line, but there will be warning.
        """
        pass

    @abstractmethod
    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        """
        Retun the arguments relevant for this command. This must be a subset of
        the names of `all_arguments` defined in `Qleverfile`. Only these
        arguments can then be used in the `execute` method.
        """
        pass

    @abstractmethod
    def additional_arguments(self, subparser):
        """
        Add additional command-specific arguments (which are not in
        `Qleverfile.all_arguments` and cannot be specified in the Qleverfile)
        to the given `subparser`. If there are no additional arguments, just
        implement as `pass`.
        """
        pass

    @abstractmethod
    def execute(self, args) -> bool:
        """
        Execute the command with the given `args`. Return `True` if the command
        executed normally. Return `False` if it did not execute normally, but
        the problem could be identified and handled. In all other cases, raise
        a `CommandException`.
        """
        pass

    @staticmethod
    def show(command_description: str, only_show: bool = False):
        """
        Helper function that shows the command line or description of an
        action, together with an explanation.
        """

        log.info(colored(command_description, "blue"))
        log.info("")
        if only_show:
            log.info(
                'You passed the argument "--show", therefore the command '
                'is only shown, but not executed (omit the "--show" to '
                "execute it)"
            )


def execute_command(args: argparse.Namespace) -> None:
    """
    Run the command selected by `args` and handle its failure modes. Shared by
    the `qlever` and `qeval` entry points so that both behave identically.
    """
    command_object = args.command_object
    try:
        log.info("")
        log.info(colored(f"Command: {args.command}", attrs=["bold"]))
        log.info("")
        command_successful = command_object.execute(args)
        log.info("")
        if not command_successful:
            exit(1)
    except KeyboardInterrupt:
        log.warning("\rCtrl-C pressed, exiting ...")
        log.info("")
        exit(1)
    except Exception as e:
        # Check if it's a certain kind of `AttributeError` and give a hint in
        # that case.
        log.debug(
            "Command failed with exception, full traceback: "
            f"{traceback.format_exc()}"
        )
        # Path of this engine's command modules (`qlever/commands`, later
        # `qeval/oxigraph/commands`), so the hint below only fires for
        # tracebacks coming from them.
        package = type(command_object).__module__.split(".commands.")[0]
        commands_path = package.replace(".", "/") + "/commands"

        match_error = re.search(r"object has no attribute '(.+)'", str(e))
        match_trace = re.search(
            rf"({commands_path}/.+\.py)\", line (\d+)",
            traceback.format_exc(),
        )
        if isinstance(e, AttributeError) and match_error and match_trace:
            attribute = match_error.group(1)
            trace_command = match_trace.group(1)
            trace_line = match_trace.group(2)
            log.error(f"{e} in `{trace_command}` at line {trace_line}")
            log.info("")
            log.info(
                f"Likely cause: you used `args.{attribute}`, but it was "
                f"neither defined in `relevant_qleverfile_arguments` "
                f"nor in `additional_arguments`"
            )
            log.info("")
            log.info(
                f"If you did not implement `{trace_command}` yourself, "
                f"please report this issue"
            )
            log.info("")
        else:
            log.error(f"An unexpected error occurred: {e}")
            log.info("")
            log.info(traceback.format_exc())
        exit(1)
