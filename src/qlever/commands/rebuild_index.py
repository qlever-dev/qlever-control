from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path

from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


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
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--rebuild-tmp-dir",
            type=str,
            help="Directory in which the server builds the new index "
            "(default: chosen by the server, `rebuild.<current datetime>.tmp`)",
        )
        subparser.add_argument(
            "--rebuild-previous-index-dir",
            type=str,
            help="Directory to which the server moves the current index when "
            "the new index is swapped in (default: chosen by the server, "
            "`previous.<datetime of the build of the current index>`)",
        )
        subparser.add_argument(
            "--test-query",
            action="store_true",
            default=False,
            help="After the rebuild, send a simple test query to the server "
            "and report whether it succeeds",
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
            default="all",
            help="Which `previous.*` index directories to keep after a "
            "successful rebuild: "
            "all (keep all), "
            "none (delete all), "
            "original-only (keep only the very first), "
            "most-recent-only (keep only the most recently created), "
            "original-and-most-recent (keep both) "
            "(default: all)",
        )

    def execute(self, args) -> bool:
        # The server does all the work: it builds the new index in a temporary
        # directory (from the current data, including updates), then moves the
        # current index to the previous-index directory, moves the new index
        # into its place, and swaps it in without a restart. The two
        # directories can optionally be chosen via the command parameters.
        rebuild_index_cmd = (
            f"curl -s -w '\\n%{{http_code}}' {args.host_name}:{args.port} "
            f"-d cmd=rebuild-index"
        )
        if args.rebuild_tmp_dir is not None:
            rebuild_index_cmd += f" -d rebuild-tmp-dir={args.rebuild_tmp_dir}"
        if args.rebuild_previous_index_dir is not None:
            rebuild_index_cmd += (
                f" -d rebuild-previous-index-dir="
                f"{args.rebuild_previous_index_dir}"
            )
        rebuild_index_cmd += f" -d access-token={args.access_token}"

        # Show the command line.
        self.show(rebuild_index_cmd, only_show=args.show)
        if args.show:
            return True

        # Show the rebuild log while the rebuild is running. The log is
        # written to `<rebuild-tmp-dir>/<name>.rebuild-index-log.txt`; when
        # the server chooses the temporary directory (the default), its name
        # contains the start time of the rebuild, so wait for the most
        # recently modified match.
        log_file_glob = (
            args.rebuild_tmp_dir
            if args.rebuild_tmp_dir is not None
            else "rebuild.*.tmp"
        ) + f"/{args.name}.rebuild-index-log.txt"
        tail_cmd = (
            f"while true; do LOG_FILE=$(ls -t {log_file_glob} 2> /dev/null "
            f'| head -1); [ -n "$LOG_FILE" ] && break; sleep 0.1; done && '
            f'exec tail -f "$LOG_FILE"'
        )
        tail_proc = subprocess.Popen(tail_cmd, shell=True)

        # Trigger the rebuild and wait for it to finish (the server keeps the
        # HTTP request open until the new index has been swapped in, which
        # can take from minutes to hours, depending on the size of the index).
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
            duration_seconds = round(time.monotonic() - time_start)
        finally:
            tail_proc.terminate()
            tail_proc.wait()

        # Report the result, in particular, where the server moved the
        # previous index (its directory name is derived from the build date of
        # the previous index, so it is not known in advance).
        log.info("")
        log.info(
            f"Rebuilt the index and swapped it in "
            f"({duration_seconds:,} seconds)"
        )
        try:
            previous_index_dir = json.loads(response_body)[
                "previous-index-dir"
            ]
            log.info(f'The previous index is now in "{previous_index_dir}"')
        except (json.JSONDecodeError, KeyError):
            log.warning(
                f"Could not parse the server response: {response_body}"
            )

        # If requested, send a simple test query to the server (which now
        # serves the new index).
        if args.test_query:
            test_query_cmd = (
                f"curl -s {args.host_name}:{args.port}"
                f' --data-urlencode "query=SELECT * WHERE'
                f' {{ ?s ?p ?o }} LIMIT 1"'
                f' -o /dev/null -w "%{{http_code}}"'
            )
            try:
                query_ok = (
                    run_command(test_query_cmd, return_output=True).strip()
                    == "200"
                )
            except Exception:
                query_ok = False
            if query_ok:
                log.info("Test query succeeded")
            else:
                log.error("Test query failed")
                return False

        # Clean up previous index directories according to
        # `--keep-previous-index-dirs`. Find all subdirectories starting with
        # `previous.`, ordered from oldest to newest (by creation time), and
        # keep or delete them according to the specified policy.
        if args.keep_previous_index_dirs != "all":
            previous_index_dirs = sorted(
                [
                    dir
                    for dir in Path(".").iterdir()
                    if dir.is_dir() and dir.name.startswith("previous.")
                ],
                key=lambda dir: dir.stat().st_ctime,
            )
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
            for i, dir in enumerate(previous_index_dirs):
                is_original = i == 0
                is_most_recent = i == len(previous_index_dirs) - 1
                if policy == "none":
                    action = "DELETE"
                elif policy == "original-only":
                    action = "KEEP" if is_original else "DELETE"
                elif policy == "most-recent-only":
                    action = "KEEP" if is_most_recent else "DELETE"
                elif policy == "original-and-most-recent":
                    action = (
                        "KEEP" if is_original or is_most_recent else "DELETE"
                    )
                log.info(f"  {dir.name:<50} {action}")
                if action == "DELETE":
                    try:
                        shutil.rmtree(dir)
                        log.info(f"    → Deleted {dir.name}")
                    except Exception as e:
                        log.error(f"    → Failed to delete {dir.name}: {e}")
            log.info("")

        return True
