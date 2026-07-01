from __future__ import annotations

from qlever.command import QleverCommand
from qlever.commands import stop as qlever_stop
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import stop_process_with_regex
from qoxigraph.commands.status import StatusCommand


class StopCommand(QleverCommand):
    """
    Stop the Oxigraph server for a given dataset. For native execution,
    finds and kills processes matching the dataset-name regex. For
    containers, stops and removes the server container.
    """

    # Override this with StatusCommand from child class for execute
    # method to work as intended
    STATUS_COMMAND = StatusCommand()
    # %%NAME%% is replaced at runtime with the dataset name from the Qleverfile
    DEFAULT_REGEX = "oxigraph\\s+serve.*%%NAME%%_index"

    def __init__(self):
        pass

    def description(self) -> str:
        return "Stop Oxigraph server for a given dataset"

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
            default=self.DEFAULT_REGEX,
            help="Show only processes where the command "
            "line matches this regex",
        )

    def execute(self, args) -> bool:
        # Substitute the dataset name into the regex template so we only
        # match the server running for this dataset.
        cmdline_regex = args.cmdline_regex
        if "%%NAME%%" in args.cmdline_regex and hasattr(args, "name"):
            cmdline_regex = args.cmdline_regex.replace(
                "%%NAME%%", str(args.name)
            )
        description = (
            f"Checking for container with name {args.server_container}"
            if args.system in Containerize.supported_systems()
            else f'Checking for processes matching "{cmdline_regex}"'
        )

        self.show(description, only_show=args.show)
        if args.show:
            return True

        if args.system not in Containerize.supported_systems():
            stop_process_results = stop_process_with_regex(cmdline_regex)
            if stop_process_results is None:
                return False
            if len(stop_process_results) > 0:
                return all(stop_process_results)

            # If no matching process found, show a message and the output of the
            # status command.
            log.error("No matching process found")
            args.cmdline_regex = self.STATUS_COMMAND.DEFAULT_REGEX
            log.info("")
            StatusCommand().execute(args)
            return True

        # First check if container is running and if yes, stop and remove it
        return qlever_stop.stop_container(args.server_container)
