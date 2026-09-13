from __future__ import annotations

from qlever.command import QleverCommand
from qlever.commands.status import StatusCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    stop_process_with_regex,
    stop_systemd_unit,
    systemd_unit_is_active,
    systemd_unit_name,
)


def stop_container(server_container: str) -> bool:
    """
    Try to stop and remove container. return True iff it was stopped
    successfully. Gives log info accordingly.
    """
    for container_system in Containerize.supported_systems():
        if Containerize.stop_and_remove_container(
            container_system, server_container
        ):
            log.info(
                f"{container_system.capitalize()} container with "
                f'name "{server_container}" stopped and removed'
            )
            return True
    return False


class StopCommand(QleverCommand):
    """
    Class for executing the `stop` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Stop QLever server for a given dataset or port"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["port"],
            "runtime": ["server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--cmdline-regex",
            default="qlever-server.* -i [^ ]*%%NAME%%",
            help="Show only processes where the command "
            "line matches this regex",
        )
        subparser.add_argument(
            "--no-containers",
            action="store_true",
            default=False,
            help="Do not look for containers, only for native processes",
        )

    def execute(self, args) -> bool:
        # Show action description.
        cmdline_regex = args.cmdline_regex.replace("%%NAME%%", args.name)
        description = f'Checking for processes matching "{cmdline_regex}"'
        if not args.no_containers:
            description += (
                f" and for Docker container with name "
                f'"{args.server_container}"'
            )
        self.show(description, only_show=args.show)
        if args.show:
            return True

        # A server that runs as a systemd user service (see `start`) has to
        # be stopped via its unit (otherwise it would just be restarted). A
        # unit that is not active any more (for example, one that has hit its
        # start limit) is only cleaned up, and the search for the server
        # continues below.
        unit = systemd_unit_name(args.name)
        unit_was_active = systemd_unit_is_active(unit)
        if stop_systemd_unit(unit) and unit_was_active:
            log.info(f'Systemd unit "{unit}" stopped')
            return True

        # First check if there is container running and if yes, stop and remove
        # it (unless the user has specified `--no-containers`).
        if not args.no_containers:
            if stop_container(args.server_container):
                return True

        # Check if there is a process running on the server port using psutil.
        # NOTE: On MacOS, some of the proc's returned by psutil.process_iter()
        # no longer exist when we try to access them, so we just skip them.
        stop_process_results = stop_process_with_regex(cmdline_regex)
        if stop_process_results is None:
            return False
        if len(stop_process_results) > 0:
            return all(stop_process_results)

        # If no matching process found, show a message and the output of the
        # status command.
        message = (
            "No matching process found"
            if args.no_containers
            else "No matching process or container found"
        )
        log.error(message)
        args.cmdline_regex = "^qlever-server.* -i [^ ]*"
        log.info("")
        StatusCommand().execute(args)
        return True
