from __future__ import annotations

import subprocess
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import binary_exists, is_server_alive, run_command


class StartCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qoxigraph"

    def description(self) -> str:
        return (
            "Start the server for Oxigraph (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port"],
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
        subparser.add_argument(
            "--server-binary",
            type=str,
            default="oxigraph",
            help=(
                "The binary for starting the server (default: oxigraph) "
                "(this requires that you have oxigraph-cli installed "
                "on your machine)"
            ),
        )

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
            volumes=[("$(pwd)", "/index")],
            ports=[(args.port, args.port)],
            working_directory="/index",
            use_bash=False,
        )

    def execute(self, args) -> bool:
        bind = (
            f"{args.host_name}:{args.port}"
            if args.system == "native"
            else f"0.0.0.0:{args.port}"
        )
        start_cmd = f"serve-read-only --location . --bind={bind}"

        if args.system == "native":
            start_cmd = f"{args.server_binary} {start_cmd}"
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

        endpoint_url = f"http://{args.host_name}:{args.port}/query"

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not binary_exists(args.server_binary, "server-binary"):
                return False
        else:
            if Containerize().is_running(args.system, args.server_container):
                log.error(
                    f"Server container {args.server_container} already exists!\n"
                )
                log.info(
                    f"To kill the existing server, use `{self.script_name} stop`"
                )
                return False

        # Check if index files (*.sst) present in cwd
        if len([p.name for p in Path.cwd().glob("*.sst")]) == 0:
            log.error(f"No Oxigraph index files for {args.name} found!\n")
            log.info(
                f"Did you call `{self.script_name} index`? If you did, check "
                "if .sst index files are present in current working directory."
            )
            return False

        if is_server_alive(url=endpoint_url):
            log.error(f"Oxigraph server already running on {endpoint_url}\n")
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
        if args.system == "native":
            log_cmd = f"exec tail -f {args.name}.server-log.txt"
        else:
            time.sleep(2)
            log_cmd = f"exec {args.system} logs -f {args.server_container}"
        log_proc = subprocess.Popen(log_cmd, shell=True)
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
        if args.run_in_foreground:
            try:
                process.wait()
            except KeyboardInterrupt:
                process.terminate()
            log_proc.terminate()

        return True
