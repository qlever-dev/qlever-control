from __future__ import annotations

import subprocess
import time
from pathlib import Path

from qlever import script_name
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    binary_exists,
    is_server_alive,
    run_command,
    tail_log_file,
)
from qoxigraph.commands.stop import StopCommand


def timeout_supported(args, serve_ps: str) -> bool:
    """Check whether the oxigraph server binary supports query timeouts."""
    help_cmd = f"{serve_ps} --help"
    if args.system in Containerize.supported_systems():
        help_cmd = f"{args.system} run --rm {args.image} {help_cmd}"
    else:
        help_cmd = f"{args.server_binary} {help_cmd}"
    try:
        help_output = run_command(help_cmd, return_output=True)
        return "timeout-s" in help_output
    except Exception as e:
        log.warning(
            "Could not determine if query timeouts are supported by this version "
            f"of Oxigraph! Falling back to no timeouts. Error: {e}",
        )
        return False


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
        volumes=[("$(pwd)", "/opt")],
        ports=[(args.port, args.port)],
        working_directory="/opt",
        use_bash=False,
    )


class StartCommand(QleverCommand):
    """
    Start the Oxigraph SPARQL server for an already-indexed dataset.
    Supports both native and containerized execution, with an option
    to run in the foreground. Uses `serve-read-only` or `serve`
    depending on the read_only setting.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Start the server for Oxigraph (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": [
                "host_name",
                "port",
                "read_only",
                "server_binary",
                "timeout",
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

    def execute(self, args) -> bool:
        # Inside a container, bind to 0.0.0.0 so the port mapping is
        # reachable from the host; natively, bind to the configured host.
        bind = (
            f"0.0.0.0:{args.port}"
            if args.system in Containerize.supported_systems() 
            else f"{args.host_name}:{args.port}"
        )
        process = "serve-read-only" if args.read_only == "yes" else "serve"
        timeout_str = ""
        if timeout_supported(args, process):
            try:
                timeout_s = int(args.timeout[:-1])
            except ValueError as e:
                log.warning(
                    f"Invalid timeout value {args.timeout}. Error: {e}"
                )
                log.info("Setting timeout to 60s!")
                timeout_s = 60
            timeout_str = f"--timeout-s {timeout_s}"
        else:
            log.info(
                f"Ignoring the set timeout value of {args.timeout} as your "
                "version of Oxigraph doesn't currently support query timeouts!"
            )

        start_cmd = (
            f"{process} --location {args.name}_index/ {args.extra_args} "
            f"{timeout_str} --bind={bind}"
        )

        if args.system in Containerize.supported_systems():
            start_cmd = wrap_cmd_in_container(args, start_cmd)
        else:
            start_cmd = f"{args.server_binary} {start_cmd} > {args.name}.server-log.txt 2>&1"
            if not args.run_in_foreground:
                start_cmd = f"nohup {start_cmd} &"

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        endpoint_url = f"http://{args.host_name}:{args.port}/query"

        # When running natively, check if the binary exists and works.
        if args.system not in Containerize.supported_systems():
            if not binary_exists(args.server_binary, "server-binary", args):
                return False

        # Check if index files (*.sst) present in index directory
        if (
            len([p.name for p in Path(f"{args.name}_index/").glob("*.sst")])
            == 0
        ):
            log.error(f"No Oxigraph index files for {args.name} found!\n")
            log.info(
                f"Did you call `{script_name} index`? If you did, check "
                "if .sst index files are present in index directory."
            )
            return False

        # Check if server already alive at endpoint url from a previous run
        if is_server_alive(url=endpoint_url):
            log.error(f"Oxigraph server already running on {endpoint_url}\n")
            log.info(
                f"To kill the existing server, use `{script_name} stop`"
            )
            return False

        # Remove old log file so that tail starts clean.
        log_file = Path(f"{args.name}.server-log.txt")
        log_file.unlink(missing_ok=True)

        try:
            process = run_command(
                start_cmd,
                use_popen=args.run_in_foreground,
            )
        except Exception as e:
            log.error(f"Starting the Oxigraph server failed ({e})")
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
        # For containers, use `docker/podman logs -f` as Oxigraph doesn't
        # support redirecting logs to a log file. A short delay ensures
        # the container is up before attaching.
        if args.system in Containerize.supported_systems():
            time.sleep(2)
            log_cmd = f"exec {args.system} logs -f {args.server_container}"
            log_proc = subprocess.Popen(log_cmd, shell=True)
        else:
            log_proc = tail_log_file(log_file)
            if log_proc is None:
                return False
        while not is_server_alive(endpoint_url):
            time.sleep(1)

        log.info(
            f"Oxigraph server webapp for {args.name} will be available at "
            f"http://{args.host_name}:{args.port} and the sparql endpoint for "
            f"queries is {endpoint_url} when the server is ready"
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
                if args.system in Containerize.supported_systems():
                    args.cmdline_regex = StopCommand.DEFAULT_REGEX
                    StopCommand().execute(args)
            log_proc.terminate()

        return True
