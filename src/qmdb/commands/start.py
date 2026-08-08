from __future__ import annotations

import time
from pathlib import Path

import qlever.util as util
from qlever import script_name
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qmdb.commands.stop import StopCommand

MDB_SPECIFIC_SERVER_ARGS = [
    "strings_dynamic",
    "strings_static",
    "tensors_dynamic",
    "tensors_static",
    "private_buffer",
    "versioned_buffer",
    "unversioned_buffer",
]


def wrap_cmd_in_container(args, cmd: str) -> str:
    """Wrap the server start command in a container with restart policy."""
    run_subcommand = "run --restart=unless-stopped"
    if not args.run_in_foreground:
        run_subcommand += " -d"
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


class StartCommand(QleverCommand):
    """
    Start the MillenniumDB SPARQL server for an already-indexed dataset.
    Supports both native and containerized execution, with an option
    to run in the foreground. Appends optional memory buffer arguments
    (strings, tensors, versioned/unversioned) when configured.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Start the server for MillenniumDB (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
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

    def execute(self, args) -> bool:
        try:
            timeout = int(args.timeout[:-1])
        except ValueError as e:
            log.error(f"Invalid timeout value {args.timeout}. Error: {e}")
            return False

        start_cmd = (
            f"{args.server_binary} server {args.name}_index "
            f"--port {args.port} --timeout {timeout} "
        )
        if args.threads is not None:
            start_cmd += f"--threads {args.threads} "
        # Append optional MillenniumDB-specific buffer arguments when set.
        for arg in MDB_SPECIFIC_SERVER_ARGS:
            if (arg_value := getattr(args, arg)) is not None and arg_value:
                start_cmd += f"--{arg.replace('_', '-')} {arg_value}B "

        if args.extra_args:
            start_cmd += args.extra_args

        start_cmd = f"{start_cmd} > {args.name}.server-log.txt 2>&1"
        if args.system in Containerize.supported_systems():
            start_cmd = wrap_cmd_in_container(args, start_cmd)
        else:
            if not args.run_in_foreground:
                start_cmd = f"nohup {start_cmd} &"

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        # When running natively, check if the binary exists and works.
        if args.system not in Containerize.supported_systems():
            if not util.binary_exists(args.server_binary, "server-binary", args):
                return False

        # Check if index files are present in the index directory.
        index_dir = Path(f"{args.name}_index")
        if not index_dir.exists() or not any(index_dir.iterdir()):
            log.error(f"No MillenniumDB index files for {args.name} found!\n")
            log.info(
                f"Did you call `{script_name} index`? If you did, check "
                "if index files are present in the index directory."
            )
            return False

        # Check if server already alive at endpoint url from a previous run.
        endpoint_url = f"http://{args.host_name}:{args.port}"
        if util.is_server_alive(url=endpoint_url):
            log.error(
                f"MillenniumDB server already running on {endpoint_url}/sparql\n"
            )
            log.info(
                f"To kill the existing server, use `{script_name} stop`"
            )
            return False

        # Remove old log file so that tail starts clean.
        log_file = Path(f"{args.name}.server-log.txt")
        log_file.unlink(missing_ok=True)

        try:
            process = util.run_command(
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
        log_proc = util.tail_log_file(log_file)
        if log_proc is None:
            return False
        while not util.is_server_alive(endpoint_url):
            time.sleep(1)

        log.info(
            "MillenniumDB server sparql endpoint for queries is "
            f"{endpoint_url}/sparql"
        )

        # Kill the log process
        if not args.run_in_foreground:
            log_proc.terminate()

        # With `--run-in-foreground`, wait until the server is stopped.
        # On Ctrl-C, terminate the process and clean up the container.
        if args.run_in_foreground:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
                # Remove the container if the user stops the server process
                if args.system in Containerize.supported_systems():
                    args.cmdline_regex = StopCommand.DEFAULT_REGEX
                    StopCommand().execute(args)
            log_proc.terminate()

        return True
