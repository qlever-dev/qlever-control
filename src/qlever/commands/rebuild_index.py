from __future__ import annotations

import copy
import os
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

from termcolor import colored

from qlever.command import QleverCommand
from qlever.commands.start import StartCommand
from qlever.commands.stop import StopCommand
from qlever.log import log
from qlever.util import (
    get_existing_index_files,
    run_command,
)


def validate_index(args, index_dir: str) -> bool:
    """
    Validate a newly built index by starting a temporary server on a
    different port, sending a simple query, and checking that it succeeds.
    Returns True if the index is usable, False otherwise.
    """
    # Find a free port and set the container name accordingly.
    s = socket.socket()
    s.bind(("", 0))
    validation_port = s.getsockname()[1]
    s.close()
    validation_container = f"qlever-server.validate.{validation_port}"

    # Create args for the validation server: minimal resources, different
    # port and container name, working directory is the new index dir.
    validation_args = copy.copy(args)
    validation_args.port = validation_port
    validation_args.server_container = validation_container
    validation_args.memory_for_queries = "100M"
    validation_args.cache_max_size = "10M"
    validation_args.cache_max_size_single_entry = "10M"
    validation_args.timeout = "1s"
    validation_args.warmup_cmd = ""
    validation_args.show = False
    # Additional arguments expected by StartCommand and StopCommand
    # (normally set by argparse, but we call execute() directly).
    validation_args.kill_existing_with_same_port = False
    validation_args.no_warmup = True
    validation_args.run_in_foreground = False
    validation_args.runtime_parameters = []
    validation_args.cmdline_regex = "qlever-server.* -i [^ ]*%%NAME%%"
    validation_args.no_containers = False
    validation_args.restart_policy = "no"

    log.info(
        f'Validating new index in "{index_dir}" (port {validation_port}) ...'
    )

    # Start the validation server from the new index directory.
    original_dir = Path.cwd()
    try:
        os.chdir(index_dir)
        if not StartCommand().execute(validation_args):
            log.error("Validation failed: could not start server")
            return False
    except Exception as e:
        log.error(f"Validation failed: {e}")
        return False
    finally:
        os.chdir(original_dir)

    # Send a simple query to check the index works.
    try:
        endpoint = f"{args.host_name}:{validation_port}"
        query_cmd = (
            f"curl -s {endpoint}"
            f' --data-urlencode "query=SELECT * WHERE'
            f' {{ ?s ?p ?o }} LIMIT 1"'
            f' -o /dev/null -w "%{{http_code}}"'
        )
        result = run_command(query_cmd, return_output=True)
        query_ok = result.strip() == "200"
    except Exception:
        query_ok = False

    # Stop the validation server.
    try:
        os.chdir(index_dir)
        StopCommand().execute(validation_args)
    except Exception:
        pass
    finally:
        os.chdir(original_dir)

    if query_ok:
        log.info("Validation successful: new index is usable")
    else:
        log.error("Validation failed: server started but query failed")

    # Remove the log files written by the validation server inside
    # `index_dir`. Otherwise the subsequent `mv {index_dir}/* .` would
    # clobber the live `{name}.metrics-log.jsonl` and `{name}.server-log.txt`
    # of the running server in the parent directory.
    for suffix in (".metrics-log.jsonl", ".server-log.txt"):
        (Path(index_dir) / f"{args.name}{suffix}").unlink(missing_ok=True)

    return query_ok


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
            "data": ["name", "description", "text_description"],
            "server": [
                "server_binary",
                "host_name",
                "port",
                "access_token",
                "memory_for_queries",
                "cache_max_size",
                "cache_max_size_single_entry",
                "cache_max_num_entries",
                "num_threads",
                "timeout",
                "persist_updates",
                "only_pso_and_pos_permutations",
                "use_patterns",
                "use_text_index",
                "warmup_cmd",
            ],
            "runtime": [
                "system",
                "image",
                "server_container",
                "restart_policy",
            ],
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
            "--new-index-dir-basename",
            type=str,
            default="rebuild.",
            help="Basename prefix for the new index directory when "
            "`--new-index-dir` is not specified (default: `rebuild.`)",
        )
        subparser.add_argument(
            "--old-index-dir-basename",
            type=str,
            default="previous.",
            help="Basename prefix for the old index directory when "
            "`--old-index-dir` is not specified (default: `previous.`)",
        )
        subparser.add_argument(
            "--keep-previous-index-dirs",
            choices=[
                "all",
                "none",
                "original-only",
                "most-recent-only",
                "original-and-most-recent",
            ],
            default="original-and-most-recent",
            help="Which previous index directories to keep: "
            "all (keep all), "
            "none (delete all), "
            "original-only (keep only the very first), "
            "most-recent-only (keep only the most recently created), "
            "original-and-most-recent (keep both) "
            "(default: original-and-most-recent)",
        )
        subparser.add_argument(
            "--index-name",
            type=str,
            help="Base name of the files of the new index (default: use "
            "the same basename as for the current index)",
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
            args.new_index_dir = (
                f"{args.new_index_dir_basename}{new_index_date}.tmp"
            )
            if args.old_index_dir is None:
                # Check if this is the first rebuild (no previous.* directories exist)
                existing_previous_dirs = list(
                    Path(".").glob(f"{args.old_index_dir_basename}*")
                )
                is_first_rebuild = len(existing_previous_dirs) == 0

                args.old_index_dir = (
                    f"{args.old_index_dir_basename}{old_index_date}"
                    + (".ORIGINAL" if is_first_rebuild else "")
                )
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
            f"curl -s -w '\\n%{{http_code}}' {args.host_name}:{args.port} "
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
        restart_server_cmd = "qlever stop && qlever start"
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
            f"while [ ! -f {new_index_dir_name}/{log_file_name} ]; "
            f"do sleep 0.1; done && "
            f"exec tail -f {new_index_dir_name}/{log_file_name}"
        )
        tail_proc = subprocess.Popen(tail_cmd, shell=True)

        # Run the index rebuild command (and time it).
        try:
            time_start = time.monotonic()
            try:
                result = run_command(rebuild_index_cmd, return_output=True)
                lines = result.rstrip("\n").rsplit("\n", 1)
                http_code = lines[-1].strip() if len(lines) >= 2 else ""
                response_body = lines[0] if len(lines) >= 2 else result
                if http_code != "200":
                    log.error(f"Rebuilding the index failed: {response_body}")
                    return False
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

        # Validate the new index before moving anything.
        if not validate_index(args, new_index_dir_name):
            log.error(
                "The new index is dysfunctional, aborting. "
                f'Files are in "{new_index_dir_name}"'
            )
            return False

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

        # Clean up previous index directories according to
        # `--keep-previous-index-dirs`. Find all subdirectories starting
        # with `old_index_dir_basename`, ordered from oldest to newest
        # (by creation time), and keep or delete them according to the
        # specified policy.
        if move_old_index_when_done:
            old_index_dirs = sorted(
                [
                    dir
                    for dir in Path(".").iterdir()
                    if dir.is_dir()
                    and dir.name.startswith(args.old_index_dir_basename)
                ],
                key=lambda dir: dir.stat().st_ctime,
            )
            if old_index_dirs:
                policy = args.keep_previous_index_dirs
                log.info("")
                log.info(
                    colored(
                        f"Iterate over previous index directories (oldest"
                        f" to newest), and check which ones to keep or"
                        f" delete (keep_previous_index_dirs = {policy}):",
                        color="blue",
                    )
                )
                for i, dir in enumerate(old_index_dirs):
                    is_original = i == 0
                    is_most_recent = i == len(old_index_dirs) - 1
                    if policy == "all":
                        action = "KEEP"
                    elif policy == "none":
                        action = "DELETE"
                    elif policy == "original-only":
                        action = "KEEP" if is_original else "DELETE"
                    elif policy == "most-recent-only":
                        action = "KEEP" if is_most_recent else "DELETE"
                    elif policy == "original-and-most-recent":
                        action = (
                            "KEEP"
                            if is_original or is_most_recent
                            else "DELETE"
                        )

                    log.info(f"  {dir.name:<50} {action}")

                    # Actually perform the deletion
                    if action == "DELETE":
                        try:
                            shutil.rmtree(dir)
                            log.info(f"    → Deleted {dir.name}")
                        except Exception as e:
                            log.error(
                                f"    → Failed to delete {dir.name}: {e}"
                            )

                log.info("")

        return True
