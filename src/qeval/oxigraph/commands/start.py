from __future__ import annotations

import contextlib
import subprocess
import time
from pathlib import Path

from qeval.oxigraph.commands.stop import StopCommand
from qlever.command import QleverCommand
from qlever.commands.start import (
    make_server_liveness_check,
    show_log_follow_info,
    wait_for_foreground_server,
    wait_until_server_ready,
)
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    binary_exists,
    is_server_alive,
    run_command,
    stop_tailing,
    tail_log_file,
    timeout_seconds,
)


def timeout_supported(args, serve_subcommand: str) -> bool:
    """Check whether the oxigraph server binary supports query timeouts."""
    help_cmd = f"{serve_subcommand} --help"
    if args.system in Containerize.supported_systems():
        help_cmd = f"{args.system} run --rm {args.image} {help_cmd}"
    else:
        help_cmd = f"{args.server_binary} {help_cmd}"
    try:
        help_output = run_command(help_cmd, return_output=True)
        return "timeout-s" in help_output
    except Exception as e:
        log.warning(
            "Could not determine if query timeouts are supported by this "
            f"version of Oxigraph! Falling back to no timeouts. Error: {e}",
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
        is_containerized = args.system in Containerize.supported_systems()
        # Inside a container, bind to 0.0.0.0 so the port mapping is
        # reachable from the host; natively, bind to the configured host.
        bind = (
            f"0.0.0.0:{args.port}"
            if is_containerized
            else f"{args.host_name}:{args.port}"
        )
        serve_subcommand = (
            "serve-read-only" if args.read_only == "yes" else "serve"
        )
        timeout_str = ""
        if timeout_supported(args, serve_subcommand):
            timeout_str = f"--timeout-s {timeout_seconds(args.timeout)}"
        else:
            log.info(
                f"Ignoring the set timeout value of {args.timeout} as your "
                "version of Oxigraph doesn't currently support query timeouts!"
            )

        start_cmd = (
            f"{serve_subcommand} --location {args.name}_index/ "
            f"{args.extra_args} {timeout_str} --bind={bind}"
        )

        if is_containerized:
            start_cmd = wrap_cmd_in_container(args, start_cmd)
        else:
            start_cmd = (
                f"{args.server_binary} {start_cmd} "
                f"> {args.name}.server-log.txt 2>&1"
            )
            if not args.run_in_foreground:
                # The `echo $!` reports the PID of the server process, which
                # the liveness check uses to detect a server that exits
                # before it becomes ready.
                start_cmd = f"nohup {start_cmd} & echo $!"

        # Show the command line.
        self.show(start_cmd, only_show=args.show)
        if args.show:
            return True

        endpoint_url = (
            f"http://{args.host_name}:{args.port}{args.endpoint_path}"
        )

        # When running natively, check if the binary exists and works.
        if not is_containerized:
            if not binary_exists(args.server_binary, "server-binary", args):
                return False

        # Check if index files (*.sst) present in index directory
        if not any(Path(f"{args.name}_index").glob("*.sst")):
            log.error(f"No Oxigraph index files for {args.name} found!\n")
            log.info(
                f"Did you call `{args.main_command_name} index`? If you did, "
                "check if .sst index files are present in index directory."
            )
            return False

        # Check if server already alive at endpoint url from a previous run
        if is_server_alive(url=endpoint_url):
            log.error(f"Oxigraph server already running on {endpoint_url}\n")
            log.info(
                "To kill the existing server, use "
                f"`{args.main_command_name} stop`"
            )
            return False

        # Remove old log file so that tail starts clean.
        log_file = Path(f"{args.name}.server-log.txt")
        log_file.unlink(missing_ok=True)

        # Keep what the liveness check needs: the process handle or the PID.
        process = None
        pid = None
        try:
            if args.run_in_foreground:
                process = run_command(start_cmd, use_popen=True)
            elif is_containerized:
                run_command(start_cmd)
            else:
                output = run_command(start_cmd, return_output=True)
                with contextlib.suppress(ValueError):
                    pid = int(output.strip())
        except Exception as error:
            log.error(f"Starting the Oxigraph server failed ({error})")
            return False

        # Follow the server log until the server is ready. For containers,
        # use `docker/podman logs -f` as Oxigraph doesn't support
        # redirecting its log to a file.
        if is_containerized:
            log_name = f"the logs of container {args.server_container}"
        else:
            log_name = str(log_file)
        show_log_follow_info(log_name, args.run_in_foreground)
        if is_containerized:
            # A short delay ensures the container is up before attaching.
            # In a session of its own, like `tail_log_file`, so that
            # `stop_tailing` can stop it.
            time.sleep(2)
            log_cmd = f"{args.system} logs -f {args.server_container}"
            log_proc = subprocess.Popen(
                log_cmd, shell=True, start_new_session=True
            )
        else:
            log_proc = tail_log_file(log_file)
        if log_proc is None:
            return False
        try:
            server_ready = wait_until_server_ready(
                lambda: is_server_alive(endpoint_url),
                make_server_liveness_check(args, process, pid),
            )
        except KeyboardInterrupt:
            # The log follower runs in a session of its own, so the Ctrl-C
            # does not reach it.
            stop_tailing(log_proc)
            raise
        if not server_ready:
            stop_tailing(log_proc)
            return False

        log.info(
            f"Oxigraph server webapp for {args.name} will be available at "
            f"http://{args.host_name}:{args.port} and the sparql endpoint for "
            f"queries is {endpoint_url} when the server is ready"
        )

        # Stop following the log.
        if not args.run_in_foreground:
            stop_tailing(log_proc)

        # With `--run-in-foreground`, wait until the server is stopped.
        # On Ctrl-C, terminate the process and clean up the container.
        if args.run_in_foreground:

            def stop_container() -> None:
                if is_containerized:
                    stop_command = StopCommand()
                    args.cmdline_regex = stop_command.default_regex()
                    stop_command.execute(args)

            wait_for_foreground_server(process, log_proc, stop_container)

        return True
