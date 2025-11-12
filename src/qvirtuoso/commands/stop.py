from __future__ import annotations

from pathlib import Path

import psutil

from qlever.command import QleverCommand
from qlever.commands.stop import stop_container
from qlever.log import log
from qlever.util import run_command


class StopCommand(QleverCommand):
    def __init__(self):
        pass

    def description(self) -> str:
        return "Stop Virtuoso server for a given dataset or port"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "runtime": ["system", "server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        if not hasattr(args, "suppress_output"):
            args.suppress_output = False
        pid = None
        if args.system == "native": 
            if not Path("virtuoso.lck").exists():
                log.error("No virtuoso.lck file found with a process id to kill")
                return False
            try:
                pid = int(
                    run_command(
                        "cat virtuoso.lck | sed 's/VIRT_PID=//'",
                        return_output=True,
                    )
                )
            except Exception as e:
                log.error(
                    f"Couldn't get a Process ID from virtuoso.lck file: {e}"
                )
                return False
            description = f'Checking for process matching pid = {pid}'
        else:
            description = (
                f"Checking for container with name {args.server_container}"
            )

        if not args.suppress_output:
            self.show(description, only_show=args.show)
        if args.show:
            return True

        if args.system == "native":
            proc = psutil.Process(pid)
            try:
                proc.kill()
                if not args.suppress_output:
                    log.info(f"Killed process {pid}")
                return True
            except Exception as e:
                log.error(f"Could not kill process with PID "
                        f"{pid} ({e}) ... try to kill it "
                        f"manually")
                log.info("")
                return False

        # First check if container is running and if yes, stop and remove it
        return stop_container(args.server_container)
