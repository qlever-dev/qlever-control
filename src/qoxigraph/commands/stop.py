from __future__ import annotations

from qlever.command import QleverCommand
from qlever.commands import stop as qlever_stop
from qlever.log import log
from qlever.util import stop_process_with_regex
from qoxigraph.commands.status import StatusCommand


class StopCommand(QleverCommand):
    # Override this with StatusCommand from child class for execute
    # method to work as intended
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = "oxigraph\\s+serve.*:%%PORT%%"

    def __init__(self):
        pass

    def description(self) -> str:
        return "Stop Oxigraph server for a given dataset or port"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": ["port"],
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
        cmdline_regex = args.cmdline_regex
        if "%%PORT%%" in args.cmdline_regex and hasattr(args, "port"):
            cmdline_regex = args.cmdline_regex.replace(
                "%%PORT%%", str(args.port)
            )
        description = (
            f'Checking for processes matching "{cmdline_regex}"'
            if args.system == "native"
            else f"Checking for container with name {args.server_container}"
        )

        self.show(description, only_show=args.show)
        if args.show:
            return True

        if args.system == "native":
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
