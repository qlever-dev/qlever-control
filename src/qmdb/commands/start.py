from __future__ import annotations

import subprocess
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import binary_exists, is_server_alive, run_command

MDB_SPECIFIC_SERVER_ARGS = [
    "strings_dynamic",
    "strings_static",
    "tensors_dynamic",
    "tensors_static",
    "private_buffer",
    "versioned_buffer",
    "unversioned_buffer",
]


class StartCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qmdb"

    def description(self) -> str:
        return (
            "Start the server for MillenniumDB (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": [
                "host_name",
                "server_binary",
                "timeout",
                "port",
                "threads",
                "extra_args",
                *MDB_SPECIFIC_SERVER_ARGS,
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

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        run_subcommand = "run --restart=unless-stopped"
        if not args.run_in_foreground:
            run_subcommand += " -d"
        if not args.run_in_foreground:
            cmd = f"{cmd} > {args.name}.server-log.txt 2>&1"
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand=run_subcommand,
            image_name=args.image,
            container_name=args.server_container,
            volumes=[("$(pwd)", "/data")],
            working_directory="/data",
            ports=[(args.port, args.port)],
        )

    def execute(self, args) -> bool:
        try:
            timeout = int(args.timeout[:-1])
        except ValueError as e:
            log.error(f"Invalid timeout value {args.timeout}. Error: {e}")
            return False

        start_cmd = (
            f"{args.server_binary} server {args.name}_index "
            f"--port {args.port} --threads {args.threads} --timeout {timeout} "
        )
        for arg in MDB_SPECIFIC_SERVER_ARGS:
            if (arg_value := getattr(args, arg)) is not None and arg_value:
                start_cmd += f"--{arg.replace('_', '-')} {arg_value}B "

        if args.extra_args:
            start_cmd += args.extra_args

        if args.system == "native":
            if not args.run_in_foreground:
                start_cmd = (
                    f"nohup {start_cmd} > {args.name}.server-log.txt 2>&1 &"
                )
        else:
            start_cmd = self.wrap_cmd_in_container(args, start_cmd)

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not binary_exists(args.server_binary, "server-binary"):
                return False

        index_dir = Path(f"{args.name}_index")
        if not index_dir.exists() or not any(index_dir.iterdir()):
            log.info(f"No MillenniumDB index files for {args.name} found! ")
            log.info(
                f"Did you call `{self.script_name} index`? If you did, check "
                "if index files are present in the index directory"
            )
            return False

        endpoint_url = f"http://{args.host_name}:{args.port}"
        if is_server_alive(url=endpoint_url):
            log.error(
                f"MillenniumDB server already running on {endpoint_url}/sparql\n"
            )
            log.info(
                f"To kill the existing server, use `{self.script_name} stop`"
            )
            return False

        try:
            process = run_command(
                start_cmd,
                use_popen=args.run_in_foreground,
            )
        except Exception as e:
            log.error(f"Starting the MillenniumDB server failed ({e})")
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
            "MillenniumDB server sparql endpoint for queries is "
            f"{endpoint_url}/sparql"
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
