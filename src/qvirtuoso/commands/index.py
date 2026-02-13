from __future__ import annotations

import glob
import shlex
import shutil
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import is_port_used, is_server_alive, run_command
from qvirtuoso.commands.stop import StopCommand


class IndexCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qvirtuoso"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name", "format"],
            "index": ["input_files"],
            "server": ["host_name", "port", "isql_port"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--index-binary",
            type=str,
            default="isql",
            help=(
                "The isql binary for building the index (default: isql) "
                "(this requires that you have virtuoso binaries installed "
                "on your machine)"
            ),
        )
        subparser.add_argument(
            "--server-binary",
            type=str,
            default="virtuoso-t",
            help=(
                "The binary for starting the server (default: virtuoso-t) "
                "(this requires that you have virtuoso binaries installed "
                "on your machine)"
            ),
        )

    def virtuoso_ini_help_msg(self, args, ini_files: list[str]) -> str:
        ini_msg = (
            "No .ini configfile present. Did you call "
            f"`{self.script_name} setup-config`?"
        )
        if len(ini_files) == 1:
            ini_msg = (
                f"{str(ini_files[0])} would be renamed to "
                f"{args.name}.virtuoso.ini and used as the configfile"
            )
        elif len(ini_files) > 1:
            ini_msg = (
                "More than 1 .ini files found in the current "
                f"directory: {ini_files}\n"
                f"Make sure to only have a unique {args.name}.virtuoso.ini!"
            )
        return ini_msg

    @staticmethod
    def replace_ini_sections(
        name: str, isql_port: int, http_port: str
    ) -> bool:
        """
        Replace the ServerPort in [Parameters] and [HTTPServer] sections
        of {name}.virtuoso.ini with isql_port and http_port respectively
        and ErrorLogFile in [Database] section with {name}.index-log.txt
        """
        try:
            for section, option, new_value in [
                ("Parameters", "ServerPort", isql_port),
                ("HTTPServer", "ServerPort", http_port),
                ("Database", "ErrorLogFile", f"{name}.index-log.txt"),
            ]:
                sed_cmd = (
                    rf"sed -i '/^\[{section}\]/,/^\[/{{s/^\({option}"
                    rf"[[:space:]]*=[[:space:]]*\)[a-zA-Z0-9:.-]*/\1{new_value}/}}'"
                )
                run_command(f"{sed_cmd} {name}.virtuoso.ini")
            return True
        except Exception as e:
            log.error(
                f"Couldn't replace the necessary sections in virtuoso.ini: {e}"
            )
            return False

    @staticmethod
    def wrap_cmd_in_container(
        args, cmds: tuple[str, str, str]
    ) -> tuple[str, str, str]:
        """
        Given a tuple (start_cmd, ld_dir_cmd, run_cmd), wrap the 3 commands
        in a containerized command
        """
        start_cmd, ld_dir_cmd, run_cmd = cmds

        start_cmd = Containerize().containerize_command(
            cmd=f"{start_cmd} -f",
            container_system=args.system,
            run_subcommand="run -d -e DBA_PASSWORD=dba",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/database")],
            ports=[(args.port, args.port)],
            use_bash=False,
        )
        exec_cmd = f"{args.system} exec {args.index_container}"

        ld_dir_cmd = f"{exec_cmd} {ld_dir_cmd}"
        run_cmd = f'{exec_cmd} bash -c "{run_cmd}"'

        return start_cmd, ld_dir_cmd, run_cmd

    def execute(self, args) -> bool:
        start_cmd = f"{args.server_binary} -c {args.name}.virtuoso.ini"

        isql_cmd = f"{args.index_binary} {args.isql_port} dba dba "
        ld_dir_cmd = (
            isql_cmd + f"exec=\"ld_dir('.', '{args.input_files}', '');\""
        )
        run_cmd = (
            isql_cmd
            + "exec='rdf_loader_run();' && "
            + isql_cmd
            + "exec='checkpoint;'"
        )

        if args.system != "native":
            start_cmd, ld_dir_cmd, run_cmd = self.wrap_cmd_in_container(
                args, (start_cmd, ld_dir_cmd, run_cmd)
            )

        ini_files = [str(ini) for ini in Path(".").glob("*.ini")]
        if not Path(f"{args.name}.virtuoso.ini").exists():
            self.show(
                f"{args.name}.virtuoso.ini configfile not found in the current "
                f"directory! {self.virtuoso_ini_help_msg(args, ini_files)}"
            )

        cmd_to_show = f"{start_cmd}\n\n{ld_dir_cmd}\n{run_cmd}"

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        for pattern in shlex.split(args.input_files):
            if len(glob.glob(pattern)) == 0:
                log.error(f'No file matching "{pattern}" found')
                log.info("")
                log.info(
                    f"Did you call `{self.script_name} get-data`? If you did, "
                    "check GET_DATA_CMD and INPUT_FILES in the Qleverfile"
                )
                return False

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            for binary, ps in [
                (args.index_binary, "index"),
                (args.server_binary, "server"),
            ]:
                if not shutil.which(binary):
                    log.error(
                        f'Running "{binary}" failed, '
                        f"set `--{ps}-binary` to a different binary or "
                        "set `--system to a container system`"
                    )
                    return False
        else:
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False

        if Path("virtuoso.db").exists():
            log.error(
                "virtuoso.db found in current directory "
                "which shows presence of a previous index"
            )
            log.info("")
            log.info("Aborting the index operation...")
            return False

        if args.system == "native":
            if is_port_used(args.isql_port):
                log.error(
                    f"The isql port {args.isql_port} is already used! "
                    "Please specify a different isql_port either as --isql-port "
                    "or in the Qleverfile"
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
                    f"{args.name}.virtuoso.ini configfile not found in the current "
                    f"directory! {self.virtuoso_ini_help_msg(args, ini_files)}"
                )
                return False

        http_port = (
            f"{args.host_name}:{args.port}"
            if args.system == "native"
            else str(args.port)
        )
        if not self.replace_ini_sections(
            name=args.name, isql_port=args.isql_port, http_port=http_port
        ):
            return False

        # Run the index command.
        try:
            # Run the index container in detached mode
            run_command(start_cmd)
            log.info("Waiting for Virtuoso server to be online...")
            start_time = time.time()
            timeout = 60
            # Wait until the Virtuoso server is online
            while not is_server_alive(
                f"http://{args.host_name}:{args.port}/sparql"
            ):
                if time.time() - start_time > timeout:
                    log.error("Timed out waiting for Virtuoso to be online.")
                    return False
                time.sleep(1)
            # Execute the ld_dir and rdf_loader_run commands
            log.info("Virtuoso server online! Loading data into Virtuoso...\n")
            run_command(ld_dir_cmd, show_output=True)
            run_command(run_cmd, show_output=True)
            # Stop the index container as we have the indexed files in cwd
            log.info("")
            log.info("Data loading has finished!")
            args.server_container = args.index_container
            args.suppress_output = True
            return StopCommand().execute(args)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False
