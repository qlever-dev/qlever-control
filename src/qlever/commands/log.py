from __future__ import annotations

import subprocess

from qlever.command import QleverCommand
from qlever.log import log


class LogCommand(QleverCommand):
    """
    Class for executing the `log` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return ("Show the last lines of the server log file and follow it")

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"data": ["name"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument("--tail-num-lines", type=int, default=20,
                               help="Show this many of the last lines of the "
                                    "log file")
        subparser.add_argument("--from-beginning", action="store_true",
                               default=False,
                               help="Show all lines of the log file")
        subparser.add_argument("--no-follow", action="store_true",
                               default=False,
                               help="Don't follow the log file")

    def execute(self, args) -> bool:
        # Construct the command and show it.
        log_cmd = "tail"
        if args.from_beginning:
            log_cmd += " -n +1"
        else:
            log_cmd += f" -n {args.tail_num_lines}"
        if not args.no_follow:
            log_cmd += " -f"
        log_file = f"{args.name}.server-log.txt"
        log_cmd += f" {log_file}"
        self.show(log_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the command.
        log.info(f"Follow log file {log_file}, press Ctrl-C to stop"
                 f" following (will not stop the server)")
        log.info("")
        try:
            subprocess.run(log_cmd, shell=True)
            return True
        except Exception as e:
            log.error(e)
            return False





