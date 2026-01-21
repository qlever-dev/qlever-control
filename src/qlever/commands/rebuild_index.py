from __future__ import annotations

import shlex
import subprocess
import time
from pathlib import Path

from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import (
    get_existing_index_files,
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
            "--new-index-dir",
            type=str,
            help="Target directory for the new index (default: not set, "
            "move the old index instead; see `--old-index-dir`)",
        )
        subparser.add_argument(
            "--old-index-dir",
            type=str,
            help="Directory where to move the current index once the rebuild "
            "is finished (default: subdirectory `previous.YYYY-MM-DDTHH:MM`, "
            "where the timestamp is the time of the earliest index file)",
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
        # Either `--new-index-dir` or `--old-index-dir`.
        if args.new_index_dir is not None and args.old_index_dir is not None:
            log.error(
                "Please specify either --new-index-dir (the target directory "
                "for the new index) or --old-index-dir (the directory where "
                "to move the current index), but not both"
            )
            return False

        # Get the list of all files from the current index and get the date of
        # the earliest one (in UTC). Add the `Qleverfile` as well.
        old_index_files = get_existing_index_files(
            args.name, add_non_essential=True
        )
        old_index_date = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(min(Path(f).stat().st_mtime for f in old_index_files)),
        )
        new_index_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        old_index_files.append("Qleverfile")

        # Default values for arguments.
        #
        # NOTE 1: When `--old-index-dir` is specified but not `--new-index-dir`,
        # we nevertheless first build the new index in a temporary directory,
        # and only when that is successful do we move the current index to the
        # directory specified by `--old-index-dir` and move the new index to
        # the current index directory. That way, if the rebuild fails, we still
        # have the current index in its original location.
        #
        # NOTE 2: As a consequence of this logic, `args.new_index_dir` is
        # always defined after this block, even when it was not specified on
        # the command line.
        if args.index_name is None:
            args.index_name = args.name
        if args.new_index_dir is None:
            args.new_index_dir = f"rebuild.{new_index_date}.tmp"
            if args.old_index_dir is None:
                args.old_index_dir = f"previous.{old_index_date}"
        if args.new_index_dir.endswith("/"):
            args.new_index_dir = args.new_index_dir[:-1]

        # Check that the new index directory either does not exist or is empty.
        # Same for the old index directory, if specified.
        new_index_path = Path(args.new_index_dir)
        if new_index_path.exists() and any(new_index_path.iterdir()):
            log.error(
                f"The target directory '{args.new_index_dir}' for the new "
                "index already exists and is not empty; please specify an "
                "empty or non-existing directory"
            )
            return False
        if args.old_index_dir is not None:
            old_index_path = Path(args.old_index_dir)
            if old_index_path.exists() and any(old_index_path.iterdir()):
                log.error(
                    f"The target directory '{args.old_index_dir}' for the "
                    "old index already exists and is not empty; please "
                    "specify an empty or non-existing directory"
                )
                return False

        # Split `new_index_dir` into path and dir name. For example, if
        # `new_index_dir` is `path/to/index`, then the path is `path/to` and
        # the dir name is `index`.
        #
        # NOTE: We keep this separate because we can always create a
        # subdirectory in the current directory (even when running in a
        # container), but not necessarily a directory at an arbitrary path. If
        # a path outside the current directory is desired, we move the index
        # there after it has been built.
        new_index_dir_path = str(Path(args.new_index_dir).parent)
        new_index_dir_name = str(Path(args.new_index_dir).name)
        log_file_name = f"{args.index_name}.rebuild-index-log.txt"

        # Note which indexes we have to move when done.
        move_new_index_when_done = new_index_dir_path != "."
        move_old_index_when_done = args.old_index_dir is not None

        # Command for rebuilding the index.
        mkdir_cmd = (
            f"mkdir -p {new_index_dir_name} && "
            f"cp -a Qleverfile {new_index_dir_name}"
        )
        rebuild_index_cmd = (
            f"curl -s {args.host_name}:{args.port} "
            f"-d cmd=rebuild-index "
            f"-d index-name={new_index_dir_name}/{args.index_name} "
            f"-d access-token={args.access_token}"
        )
        move_new_index_cmd = f"mv {new_index_dir_name} {new_index_dir_path}"
        move_old_index_cmd = (
            f"mkdir -p {shlex.quote(args.old_index_dir)} && "
            f"mv {' '.join(shlex.quote(f) for f in old_index_files)} "
            f"{shlex.quote(args.old_index_dir)} && "
            f"mv {shlex.quote(new_index_dir_name)}/* . && "
            f"rmdir {shlex.quote(new_index_dir_name)}"
        )
        restart_server_cmd = "qlever start --kill-existing-with-same-port"
        if not move_old_index_when_done:
            restart_server_cmd = (
                f"cd {args.new_index_dir} && ${restart_server_cmd}"
            )

        # Show the command lines.
        cmds_to_show = [mkdir_cmd, rebuild_index_cmd]
        if move_old_index_when_done:
            cmds_to_show.append(move_old_index_cmd)
        if move_new_index_when_done:
            cmds_to_show.append(move_new_index_cmd)
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
        tail_cmd = (
            f"touch -c {new_index_dir_name}/{log_file_name} && "
            f"exec tail -n 0 -f {new_index_dir_name}/{log_file_name}"
        )
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
            if new_index_dir_path == ".":
                rebuild_done_msg += (
                    f", in the new directory '{args.new_index_dir}'"
                )
            log.info(rebuild_done_msg)
        finally:
            tail_proc.terminate()
            tail_proc.wait()

        # Move the old index to the specified directory, if needed.
        if move_old_index_when_done:
            try:
                log.info(f"Moving the old index to {args.old_index_dir}")
                run_command(move_old_index_cmd)
            except Exception as e:
                log.error(f"Moving the old index failed: {e}")
                return False

        # Move the new index to the specified directory, if needed.
        if move_new_index_when_done:
            try:
                log.info(f"Moving the new index to {args.new_index_dir}")
                run_command(move_new_index_cmd)
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
