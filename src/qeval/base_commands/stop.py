from __future__ import annotations

from abc import abstractmethod

from qeval.base_commands.status import BaseStatusCommand
from qlever.command import QleverCommand
from qlever.commands.stop import stop_container
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import stop_process_with_regex


class BaseStopCommand(QleverCommand):
    """
    Stop the server of an engine for a given dataset: natively by killing
    the processes that match a regex, in a container by stopping and
    removing the server container.
    """

    def __init__(self):
        pass

    @abstractmethod
    def default_regex(self) -> str:
        """
        Regex for the command line of the engine's server, where `%%NAME%%`
        stands for the dataset name.
        """

    @abstractmethod
    def status_command(self) -> BaseStatusCommand:
        """The engine's `status` command, shown when no process matches."""

    def description(self) -> str:
        return "Stop the server of this graph database for a given dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "runtime": ["system", "server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--cmdline-regex",
            default=self.default_regex(),
            help="Stop the processes whose command line matches this regex",
        )

    def execute(self, args) -> bool:
        # Only match the server of this dataset.
        cmdline_regex = args.cmdline_regex.replace("%%NAME%%", args.name)
        is_containerized = args.system in Containerize.supported_systems()
        description = (
            f"Checking for container with name {args.server_container}"
            if is_containerized
            else f'Checking for processes matching "{cmdline_regex}"'
        )
        self.show(description, only_show=args.show)
        if args.show:
            return True

        if is_containerized:
            return stop_container(args.server_container)

        stop_process_results = stop_process_with_regex(cmdline_regex)
        if stop_process_results is None:
            return False
        if len(stop_process_results) > 0:
            return all(stop_process_results)

        # Nothing matched, so show what is running instead.
        log.error("No matching process found")
        status_command = self.status_command()
        args.cmdline_regex = status_command.default_regex()
        log.info("")
        status_command.execute(args)
        return True
