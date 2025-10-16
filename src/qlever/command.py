from __future__ import annotations

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
    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        """
        Retun the arguments relevant for this command. This must be a subset of
        the names of `all_arguments` defined in `QleverConfig`. Only these
        arguments can then be used in the `execute` method.
        """
        pass

    @abstractmethod
    def additional_arguments(self, subparser):
        """
        Add additional command-specific arguments (which are not in
        `QleverConfig.all_arguments` and cannot be specified in the Qleverfile)
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
