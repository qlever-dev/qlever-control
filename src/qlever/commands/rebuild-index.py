from __future__ import annotations

import subprocess
import time
from pathlib import Path

from termcolor import colored

from qlever.command import QleverCommand
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

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port", "access_token"],
            "runtime": ["server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--index-dir",
            type=str,
            help="Directory for the new index (default: subdirectory "
            "`rebuild.YYYY-MM-DDTHH:MM` of the current directory)",
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
        if args.index_dir is None:
            timestamp = time.strftime("%Y-%m-%dT%H:%M", time.localtime())
            args.index_dir = f"rebuild.{timestamp}"
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
        #
        # NOTE: We keep this separate because we can always create a
        # subdirectory in the current directory (even when running in a
        # container), but not necessarily a directory at an arbitrary path. If
        # a path outside the current directory is desired, we move the index
        # there after it has been built.
        index_dir_path = str(Path(args.index_dir).parent)
        index_dir_name = str(Path(args.index_dir).name)
        log_file_name = f"{args.index_name}.rebuild-index-log.txt"

        # Command for rebuilding the index.
        mkdir_cmd = (
            f"mkdir -p {index_dir_name} && "
            f"> {index_dir_name}/{log_file_name} && "
            f"cp -a Qleverfile {index_dir_name}"
        )
        rebuild_index_cmd = (
            f"curl -s {args.host_name}:{args.port} "
            f"-d cmd=rebuild-index "
            f"-d index-name={index_dir_name}/{args.index_name} "
            f"-d access-token={args.access_token}"
        )
        move_index_cmd = f"mv {index_dir_name} {index_dir_path}"
        restart_server_cmd = (
            f"cd {args.index_dir} && "
            f"qlever start --kill-existing-with-same-port"
        )

        # Show the command lines.
        cmds_to_show = [mkdir_cmd, rebuild_index_cmd]
        if index_dir_path != ".":
            cmds_to_show.append(move_index_cmd)
        if args.restart_when_finished:
            cmds_to_show.append(restart_server_cmd)
        self.show("\n".join(cmds_to_show), only_show=args.show)
        if args.show:
            return True

        # Create the index directory and the log file.
        try:
            run_command(mkdir_cmd)
        except Exception as e:
            log.error(f"Creating the index directory failed: {e}")
            return False

        # Show the server log while rebuilding the index.
        #
        # NOTE: This will only work satisfactorily when no other queries are
        # being processed at the same time. It would be better if QLever
        # logged the rebuild-index output to a separate log file.
        tail_cmd = f"exec tail -n 0 -f {index_dir_name}/{log_file_name}"
        tail_proc = subprocess.Popen(tail_cmd, shell=True)

        # Run the index rebuild command (and time it).
        try:
            time_start = time.monotonic()
            try:
                run_command(rebuild_index_cmd, show_output=False)
            except Exception as e:
                log.error(f"Rebuilding the index failed: {e}")
                return False
            time_end = time.monotonic()
            duration_seconds = round(time_end - time_start)
            log.info("")
            rebuild_done_msg = f"Rebuilt index in {duration_seconds:,} seconds"
            if index_dir_path == ".":
                rebuild_done_msg += (
                    f", in the new directory '{args.index_dir}'"
                )
            log.info(rebuild_done_msg)
        finally:
            tail_proc.terminate()
            tail_proc.wait()

        # Move the new index to the specified directory, if needed.
        if index_dir_path != ".":
            try:
                log.info(f"Moving the new index to {args.index_dir}")
                run_command(move_index_cmd)
            except Exception as e:
                log.error(f"Moving the new index failed: {e}")
                return False

        # Restart the server with the new index, if requested.
        if args.restart_when_finished:
            try:
                log.info("Restarting the server with the new index ...")
                log.info("")
                log.info(colored("Command: start", attrs=["bold"]))
                log.info("")
                run_command(restart_server_cmd, show_output=True)
            except Exception as e:
                log.error(f"Restarting the server failed: {e}")
                return False

        return True
