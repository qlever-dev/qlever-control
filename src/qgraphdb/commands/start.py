from __future__ import annotations

import subprocess
import time

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class StartCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qgraphdb"

    def description(self) -> str:
        return (
            "Start the server for GraphDB (requires that you have built an "
            "index before)"
        )

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": [
                "host_name",
                "heap_size_gb",
                "server_binary",
                "port",
                "extra_env_args",
                "extra_args",
            ],
            "runtime": [
                "system",
                "image",
                "server_container",
                "license_file_path",
            ],
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
            volumes=[
                ("$(pwd)", "/opt/graphdb/home"),
                (
                    str(args.license_file_path.resolve()),
                    "/opt/graphdb/graphdb.license",
                ),
            ],
            working_directory="/opt/graphdb/home",
            ports=[(args.port, args.port)],
        )

    def execute(self, args) -> bool:
        license_file_path = (
            str(args.license_file_path.resolve())
            if args.system == "native"
            else "/opt/graphdb/graphdb.license"
        )
        start_cmd = (
            f'env GDB_HEAP_SIZE="{args.heap_size_gb}" {args.extra_env_args} '
            f"{args.server_binary} -s -Dgraphdb.home={args.name}_index "
            f"-Dgraphdb.connector.port={args.port} {args.extra_args} "
            f"-Dgraphdb.license.file={license_file_path}"
        )

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
            if not util.binary_exists(args.server_binary, "server-binary"):
                return False

        endpoint_url = f"http://{args.host_name}:{args.port}/repositories"
        if util.is_server_alive(url=endpoint_url):
            log.error(f"GraphDB server already running on {endpoint_url}\n")
            log.info(
                f"To kill the existing server, use `{self.script_name} stop`"
            )
            return False

        try:
            process = util.run_command(
                start_cmd,
                use_popen=args.run_in_foreground,
            )
        except Exception as e:
            log.error(f"Starting the GraphDB server failed ({e})")
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
        time.sleep(2)
        while not util.is_server_alive(endpoint_url):
            time.sleep(1)

        log.info(f"GraphDB sparql endpoint for queries is {endpoint_url}")

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
