from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import is_server_alive, run_command
from qvirtuoso.commands.index import (
    log_virtuoso_ini_changes,
    update_virtuoso_ini,
    virtuoso_ini_help_msg,
)


class StartCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qvirtuoso"

    def description(self) -> str:
        return (
            "Start the server for Virtuoso (must be done before building an "
            "index for Virtuoso)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": [
                "host_name",
                "port",
                "server_binary",
                "timeout",
                "max_query_memory",
                "extra_args",
            ],
            "runtime": ["system", "image", "server_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--run-in-foreground",
            action="store_true",
            default=False,
            help=(
                "Run the start command in the foreground "
                "(default: run in the background)"
            ),
        )

    def config_dict_for_update_ini(self, args) -> dict[str, dict[str, str]]:
        """
        Construct the parameter dictionary for all the necessary sections and
        options of virtuoso.ini that need updating for the start process
        """
        config_dict = {
            "Parameters": {},
            "HTTPServer": {},
            "Database": {},
            "SPARQL": {},
        }
        http_port = (
            f"{args.host_name}:{args.port}"
            if args.system == "native"
            else str(args.port)
        )

        try:
            timeout_s = int(args.timeout[:-1])
        except ValueError as e:
            log.warning(f"Invalid timeout value {args.timeout}. Error: {e}")
            log.info("Setting timeout to 30s!")
            timeout_s = 30

        # config_dict["Parameters"]["ServerPort"] = str(args.isql_port)
        config_dict["Parameters"]["MaxQueryMem"] = str(args.max_query_memory)
        config_dict["HTTPServer"]["ServerPort"] = http_port
        config_dict["Database"]["ErrorLogFile"] = f"{args.name}.server-log.txt"
        config_dict["SPARQL"]["MaxQueryCostEstimationTime"] = "-1"
        config_dict["SPARQL"]["MaxConstructTriples"] = "0"
        config_dict["SPARQL"]["MaxQueryExecutionTime"] = str(timeout_s)
        return config_dict

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        run_subcommand = "run --restart=unless-stopped"
        if not args.run_in_foreground:
            run_subcommand += " -d"
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand=run_subcommand,
            image_name=args.image,
            container_name=args.server_container,
            volumes=[("$(pwd)", "/database")],
            ports=[(args.port, args.port)],
        )

    def execute(self, args) -> bool:
        start_cmd = (
            f"{args.server_binary} -c {args.name}.virtuoso.ini {args.extra_args} "
        )
        if args.system == "native":
            if args.run_in_foreground:
                start_cmd += " -f"
        else:
            start_cmd = self.wrap_cmd_in_container(args, f"{start_cmd} -f")

        ini_files = [str(ini) for ini in Path(".").glob("*.ini")]
        if not Path(f"{args.name}.virtuoso.ini").exists():
            self.show(
                f"{args.name}.virtuoso.ini configfile "
                "not found in the current directory! "
                f"{virtuoso_ini_help_msg(self.script_name, args, ini_files)}"
            )

        virtuoso_ini_config_dict = self.config_dict_for_update_ini(args)
        log_virtuoso_ini_changes(args.name, virtuoso_ini_config_dict)
        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        endpoint_url = f"http://{args.host_name}:{args.port}/sparql"

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not shutil.which(args.server_binary):
                log.error(
                    f'Running "{args.server_binary}" failed, '
                    "set `--server-binary` to a different binary or "
                    "set `--system to a container system`"
                )
                return False

        # Check if index db virtuoso.db present in cwd
        if not Path("virtuoso.db").exists():
            log.error(f"No Virtuoso index db for {args.name} found!\n")
            log.info(
                f"Did you call `{self.script_name} index`? If you did, check "
                "if virtuoso.db is present in current working directory."
            )
            return False

        if is_server_alive(url=endpoint_url):
            log.error(f"Virtuoso server already running on {endpoint_url}\n")
            log.info(
                f"To kill the existing server, use `{self.script_name} stop`"
            )
            return False

        # Rename the virtuoso.ini file to {args.name}.virtuoso.ini if needed
        if not Path(f"{args.name}.virtuoso.ini").exists():
            if len(ini_files) == 1:
                Path(ini_files[0]).rename(f"{args.name}.virtuoso.ini")
                log.info(
                    f"{ini_files[0]} renamed to {args.name}.virtuoso.ini!"
                )
            else:
                log.error(
                    f"{args.name}.virtuoso.ini configfile "
                    "not found in the current directory! "
                    f"{virtuoso_ini_help_msg(self.script_name, args, ini_files)}"
                )
                return False

        if not update_virtuoso_ini(args.name, virtuoso_ini_config_dict):
            return False

        try:
            process = run_command(
                start_cmd,
                use_popen=args.run_in_foreground,
            )
        except Exception as e:
            log.error(f"Starting the Virtuoso server failed ({e})")
            return False

        # Tail the server log until the server is ready (note that the `exec`
        # is important to make sure that the tail process is killed and not
        # just the bash process).
        if args.run_in_foreground:
            log.info(
                "Follow the server logs as long as the server is"
                " running (Ctrl-C stops the server)"
            )
        else:
            log.info(
                "Follow the server logs until the server is ready"
                " (Ctrl-C stops following the log, but NOT the server)"
            )
        log.info("")
        log_cmd = f"exec tail -f {args.name}.server-log.txt"
        log_proc = subprocess.Popen(log_cmd, shell=True)
        while not is_server_alive(endpoint_url):
            time.sleep(1)

        log.info(
            f"Virtuoso server webapp for {args.name} will be available "
            f"at http://{args.host_name}:{args.port} and the sparql "
            f"endpoint for queries is http://{args.host_name}:{args.port}/sparql"
        )

        # Kill the log process
        if not args.run_in_foreground:
            log_proc.terminate()

        # With `--run-in-foreground`, wait until the server is stopped.
        if args.run_in_foreground:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
            log_proc.terminate()

        return True
