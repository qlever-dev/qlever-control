from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.commands.start import StartCommand
from qlever.commands.stop import StopCommand
from qlever.log import log
from qlever.util import (
    run_command,
)


class RebuildIndexCommand(QleverCommand):
    """
    Class for executing the `rebuild-index` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Rebuild the index from the current data (including updates)"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port", "access_token"],
            "runtime": ["server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--index-dir",
            type=str,
            required=True,
            help="Directory for the new index (required, no default)",
        )
        subparser.add_argument(
            "--index-name",
            type=str,
            help="Base name of the new index (default: use the same as the "
            "current index)",
        )
        subparser.add_argument(
            "--restart-when-finished",
            action="store_true",
            default=False,
            help="When the rebuild is finished, stop the server with the old "
            "index and start it again with the new index",
        )

    def execute(self, args) -> bool:
        # Default values for arguments.
        if args.index_name is None:
            args.index_name = args.name
        if args.index_dir.endswith("/"):
            args.index_dir = args.index_dir[:-1]

        # Check that the index directory either does not exist or is empty.
        index_path = Path(args.index_dir)
        if index_path.exists() and any(index_path.iterdir()):
            log.error(
                f"The specified index directory '{args.index_dir}' already "
                "exists and is not empty; please specify an empty or "
                "non-existing directory"
            )
            return False

        # Split `index_dir` into path and dir name. For example, if `index_dir`
        # is `path/to/index`, then the path is `path/to` and the dir name
        # is `index`.
        index_dir_path = str(Path(args.index_dir).parent)
        index_dir_name = str(Path(args.index_dir).name)

        # Command for rebuilding the index.
        rebuild_index_cmd = (
            f"mkdir -p {index_dir_name} && "
            f"curl -s {args.host_name}:{args.port} "
            f"-d cmd=rebuild-index "
            f"-d index-name={index_dir_name}/{args.index_name} "
            f"-d access-token={args.access_token}"
        )
        move_index_cmd = (
            f"mv {index_dir_name} {index_dir_path}"
        )
        restart_server_cmd = (
            f"cp -a Qleverfile {args.index_dir} && "
            f"cd {args.index_dir} && "
            f"qlever start --kill-existing-with-same-port"
        )

        # Show the command lines.
        self.show(rebuild_index_cmd, args.show)
        if index_dir_path != ".":
            self.show(move_index_cmd, args.show)
        if args.restart_when_finished:
            self.show(restart_server_cmd, args.show)
        if args.show:
            return True

        # Show the server log while rebuilding the index.
        #
        # NOTE: This will only work satisfactorily when no other quieres are
        # being processed at the same time. It would be better if QLever
        # logged the rebuild-index output to a separate log file.
        tail_cmd = f"exec tail -n 0 -f {args.name}.server-log.txt"
        tail_proc = subprocess.Popen(tail_cmd, shell=True)

        # Run the command (and time it).
        time_start = time.monotonic()
        try:
            run_command(rebuild_index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Rebuilding the index failed: {e}")
            return False
        time_end = time.monotonic()
        duration_seconds = round(time_end - time_start)
        log.info("")
        log.info("")
        log.info(
            f"Rebuilt index in {duration_seconds:,} seconds; new "
            f"files are in {args.index_dir}/{args.index_name}"
        )

        # Stop showing the server log.
        tail_proc.terminate()

        # Move the new index to the specified directory, if needed.
        if index_dir_path != ".":
            try:
                run_command(move_index_cmd)
            except Exception as e:
                log.error(f"Moving the new index failed: {e}")
                return False


        # Restart the server with the new index, if requested.
        if args.restart_when_finished:
            try:
                run_command(restart_server_cmd)
            except Exception as e:
                log.error(f"Restarting the server failed: {e}")
                return False

        return True
