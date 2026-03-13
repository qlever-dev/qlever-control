from __future__ import annotations

import shutil
import time
from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qvirtuoso.commands.stop import StopCommand


def get_update_ini_cmd(
    arg_name: str, config_params: dict[str, dict[str, str]]
) -> str:
    """
    Build a cross-platform sed pipeline that updates key=value pairs in
    virtuoso.ini. Writes to a temporary file first and then replaces the
    original to avoid truncation from reading and writing the same file
    in a single pipeline.
    """
    ini_file = f"{arg_name}.virtuoso.ini"
    sed_pipeline = f"cat {ini_file}"
    for section, option_dict in config_params.items():
        for option, new_value in option_dict.items():
            sed_pipeline += (
                f" | {util.get_ini_sed_cmd(section, option, new_value)}"
            )
    sed_pipeline += f" > {ini_file}.tmp && mv {ini_file}.tmp {ini_file}"
    return sed_pipeline


def update_virtuoso_ini(arg_name: str, update_ini_cmd: str) -> bool:
    """
    Execute the sed pipeline command to update virtuoso.ini options.
    """
    try:
        log.debug(update_ini_cmd)
        util.run_command(update_ini_cmd)
        return True
    except Exception as e:
        log.error(
            f"Couldn't replace the necessary sections in "
            f"{arg_name}.virtuoso.ini: {e}"
        )
        return False


def log_virtuoso_ini_changes(
    arg_name: str,
    virtuoso_ini_config_dict: dict[str, dict[str, str]],
    update_ini_cmd: str,
):
    """
    Log the sed command and the section/option values that will be written
    to virtuoso.ini. Called before execution so the user can review what
    will change (also visible with --show).
    """
    log.info(
        f"Following options of {arg_name}.virtuoso.ini will be updated "
        "with the values from Qleverfile using the following sed command:"
    )
    IndexCommand.show(f"\n{update_ini_cmd}")
    for section, option_dict in virtuoso_ini_config_dict.items():
        log_values = [f"[{section}]"]
        for option, new_value in option_dict.items():
            log_values.append(f"{option}  =  {new_value}")
        log.info("\n".join(log_values))
        log.info("")


def virtuoso_ini_help_msg(script_name: str, args, ini_files: list[str]) -> str:
    """
    Return a help message depending on how many .ini files are present in the
    current directory: none (suggest setup-config), exactly one (will be
    renamed), or multiple (ambiguous, user must resolve).
    """
    ini_msg = (
        "No .ini configfile present. Did you call "
        f"`{script_name} setup-config`?"
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


class IndexCommand(QleverCommand):
    """
    Build a Virtuoso index for an RDF dataset. The indexing workflow is:
    1. Update virtuoso.ini with Qleverfile settings (ports, memory buffers)
    2. Start the Virtuoso server (virtuoso-t)
    3. Register input files via isql ld_dir()
    4. Load data via rdf_loader_run() (optionally with parallel loaders)
    5. Checkpoint and stop the server

    When using a container system, the Docker image is built from the local
    Dockerfile if not already present (or if --rebuild-image is passed).
    """

    # Virtuoso buffer tuning constants (per GB of free memory)
    NUM_BUFFERS_PER_GB = 85_000
    MAX_DIRTY_BUFFERS_PER_GB = 65_000

    def __init__(self):
        self.script_name = "qvirtuoso"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name", "format"],
            "index": [
                "input_files",
                "index_binary",
                "isql_port",
                "num_parallel_loaders",
                "free_memory_gb",
            ],
            "server": ["host_name", "port", "server_binary"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--extend-existing-index",
            action="store_true",
            default=False,
            help=(
                "Continue loading into the existing virtuoso.db "
                "with new input files. This option can be used to "
                "incrementally load data (with checkpoints) for very "
                "large datasets to prevent total data loss in case of failure."
            ),
        )
        subparser.add_argument(
            "--rebuild-image",
            action="store_true",
            default=False,
            help="Rebuild the Docker image to get the latest updates",
        )

    def config_dict_for_update_ini(self, args) -> dict[str, dict[str, str]]:
        """
        Construct the parameter dictionary for all the necessary sections and
        options of virtuoso.ini that need updating for the index process
        """
        config_dict = {
            "Parameters": {},
            "HTTPServer": {},
            "Database": {},
        }
        http_port = (
            f"{args.host_name}:{args.port}"
            if args.system == "native"
            else str(args.port)
        )
        try:
            free_memory_gb = int(args.free_memory_gb[:-1])
        except ValueError as e:
            log.warning(
                f"Invalid --free-memory-gb value {args.free_memory_gb}. Error: {e}"
            )
            log.info("Setting free system memory to 4GB")
            free_memory_gb = 4

        config_dict["Parameters"]["ServerPort"] = str(args.isql_port)
        config_dict["HTTPServer"]["ServerPort"] = http_port
        config_dict["Database"]["ErrorLogFile"] = f"{args.name}.index-log.txt"
        config_dict["Parameters"]["NumberOfBuffers"] = str(
            self.NUM_BUFFERS_PER_GB * free_memory_gb
        )
        config_dict["Parameters"]["MaxDirtyBuffers"] = str(
            self.MAX_DIRTY_BUFFERS_PER_GB * free_memory_gb
        )
        return config_dict

    @staticmethod
    def wrap_cmd_in_container(
        args, start_cmd: str, ld_dir_cmd: str, run_cmds: list[str]
    ) -> tuple[str, str, str]:
        """
        Wrap the three indexing phases (start server, register files, load
        data) into container commands. The server runs detached, while
        ld_dir and rdf_loader_run are executed via `docker exec`.
        """
        start_cmd = Containerize().containerize_command(
            cmd=f"{start_cmd} -f",
            container_system=args.system,
            run_subcommand="run -d -e DBA_PASSWORD=dba",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/database")],
            ports=[(args.port, args.port)],
            use_bash=True,
        )
        exec_cmd = f"{args.system} exec {args.index_container}"

        ld_dir_cmd = f"{exec_cmd} {ld_dir_cmd}"
        separator = " " if len(run_cmds) > 2 else "; "
        run_cmd = f'{exec_cmd} bash -c "{separator.join(run_cmds)}"'

        return start_cmd, ld_dir_cmd, run_cmd

    def execute(self, args) -> bool:
        num_parallel_loaders = args.num_parallel_loaders
        start_cmd = f"{args.server_binary} -c {args.name}.virtuoso.ini"

        isql_cmd = f"{args.index_binary} {args.isql_port} dba dba"
        ld_dir_cmd = (
            isql_cmd + f" exec=\"ld_dir('.', '{args.input_files}', '');\""
        )
        if num_parallel_loaders > 1:
            run_cmds = [
                f"{isql_cmd} exec='rdf_loader_run();' &"
            ] * num_parallel_loaders
            run_cmds.append("wait;")
        else:
            run_cmds = [f"{isql_cmd} exec='rdf_loader_run();'"]
        run_cmds.append(f"{isql_cmd} exec='checkpoint;'")
        separator = " " if num_parallel_loaders > 1 else "; "
        run_cmd = separator.join(run_cmds)

        run_cmd_to_show = "\n".join(run_cmds)
        image_id = build_cmd = cmd_to_show = ""
        if args.system != "native":
            start_cmd, ld_dir_cmd, run_cmd = self.wrap_cmd_in_container(
                args, start_cmd, ld_dir_cmd, run_cmds
            )
            run_cmd_to_show = run_cmd
            dockerfile_dir = Path(__file__).parent.parent
            dockerfile_path = dockerfile_dir / "Dockerfile"
            build_cmd = (
                f"{args.system} build -f {dockerfile_path} -t {args.image} --build-arg "
                f"UID=$(id -u) --build-arg GID=$(id -g) {dockerfile_dir}"
            )
            image_id = util.get_container_image_id(args.system, args.image)
            if not image_id or args.rebuild_image:
                cmd_to_show = f"{build_cmd}\n\n"

        ini_files = [str(ini) for ini in Path(".").glob("*.ini")]
        if not Path(f"{args.name}.virtuoso.ini").exists():
            self.show(
                f"{args.name}.virtuoso.ini configfile not found in the current "
                f"directory! {virtuoso_ini_help_msg(self.script_name, args, ini_files)}"
            )

        virtuoso_ini_config_dict = self.config_dict_for_update_ini(args)
        update_ini_cmd = get_update_ini_cmd(args.name, virtuoso_ini_config_dict)
        log_virtuoso_ini_changes(args.name, virtuoso_ini_config_dict, update_ini_cmd)

        cmd_to_show += f"{start_cmd}\n\n{ld_dir_cmd}\n{run_cmd_to_show}"

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(args.input_files, self.script_name):
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

            if not image_id or args.rebuild_image:
                build_successful = util.build_image(
                    build_cmd, args.system, args.image
                )
                if not build_successful:
                    return False
            else:
                log.info(f"{args.image} image present on the system\n")

        if Path("virtuoso.db").exists() and not args.extend_existing_index:
            log.error(
                "virtuoso.db found in current directory "
                "which shows presence of a previous index"
            )
            log.info("")
            log.info(
                "Aborting the index operation as --extend-existing-index "
                "option not passed!"
            )
            return False

        if args.system == "native":
            if util.is_port_used(args.isql_port):
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
                    f"directory! {virtuoso_ini_help_msg(self.script_name, args, ini_files)}"
                )
                return False

        if not update_virtuoso_ini(args.name, update_ini_cmd):
            return False

        # Run the index command.
        try:
            # Delete any existing old log files for a fresh index so that the
            # index time computation is not affected
            if not args.extend_existing_index:
                Path(f"{args.name}.index-log.txt").unlink(missing_ok=True)
            # Run the index container in detached mode
            util.run_command(start_cmd)
            log.info("Waiting for Virtuoso server to be online...")
            start_time = time.time()
            timeout = 60
            # Wait until the Virtuoso server is online
            while not util.is_server_alive(
                f"http://{args.host_name}:{args.port}/sparql"
            ):
                if time.time() - start_time > timeout:
                    log.error("Timed out waiting for Virtuoso to be online.")
                    return False
                time.sleep(1)
            # Execute the ld_dir and rdf_loader_run commands
            log.info("Virtuoso server online! Loading data into Virtuoso...\n")
            util.run_command(ld_dir_cmd, show_output=True)
            util.run_command(run_cmd, show_output=True)
            log.info("")
            log.info("Data loading has finished!")

            # Construct args for Stop Command to stop running virtuoso-t process
            args.server_container = args.index_container
            args.cmdline_regex = StopCommand.DEFAULT_REGEX
            return StopCommand().execute(args)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False
