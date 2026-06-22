from __future__ import annotations

import re
import shutil
import time
from contextlib import nullcontext
from pathlib import Path

import qlever.util as util
from qlever import script_name
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.memory_monitor import MemoryMonitor
from qvirtuoso.commands.stop import StopCommand

# Virtuoso buffer tuning constants (per GB of free memory)
NUM_BUFFERS_PER_GB = 85_000
MAX_DIRTY_BUFFERS_PER_GB = 65_000


def find_virtuoso_pid(lck_path: Path) -> int | None:
    """
    Read virtuoso.lck and return the PID of the running Virtuoso
    server. The file is written by virtuoso-t on startup and contains
    a ``VIRT_PID=<pid>`` entry.
    """
    try:
        content = lck_path.read_text()
    except OSError:
        return None
    match = re.search(r"VIRT_PID\s*=\s*(\d+)", content)
    return int(match.group(1)) if match else None


def update_virtuoso_ini(
    arg_name: str,
    config_dict: dict[str, dict[str, tuple[str, bool]]],
) -> bool:
    """
    Read the virtuoso.ini file, apply the updates from config_dict,
    and write it back.
    """
    ini_path = Path(f"{arg_name}.virtuoso.ini")
    try:
        lines = ini_path.read_text().splitlines()
        result = util.update_ini_values(lines, config_dict)
        ini_path.write_text("\n".join(result) + "\n")
        return True
    except Exception as e:
        log.error(
            f"Couldn't update {arg_name}.virtuoso.ini: {e}"
        )
        return False


def log_virtuoso_ini_changes(
    arg_name: str,
    config_dict: dict[str, dict[str, tuple[str, bool]]],
):
    """
    Log the section/option values that will be written to virtuoso.ini.
    Called before execution so the user can review what will change.
    """
    log.info(
        f"Following options of {arg_name}.virtuoso.ini will be updated "
        "with the values from Qleverfile:\n"
    )
    for section, option_dict in config_dict.items():
        log_values = [f"[{section}]"]
        for option, (new_value, _) in option_dict.items():
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


def config_dict_for_update_ini(
    args,
) -> dict[str, dict[str, tuple[str, bool]]]:
    """
    Construct the parameter dictionary for all the necessary sections and
    options of virtuoso.ini that need updating for the index process.
    Each value is a (new_value, is_suffix) tuple.
    """
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

    return {
        "Parameters": {
            "ServerPort": (str(args.isql_port), False),
            "NumberOfBuffers": (str(NUM_BUFFERS_PER_GB * free_memory_gb), False),
            "MaxDirtyBuffers": (str(MAX_DIRTY_BUFFERS_PER_GB * free_memory_gb), False),
        },
        "HTTPServer": {
            "ServerPort": (http_port, False),
        },
        "Database": {
            "ErrorLogFile": (f"{args.name}.index-log.txt", False),
        },
    }


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
        run_subcommand="run -d -e DBA_PASSWORD=dba --group-add virtuoso",
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


class IndexCommand(QleverCommand):
    """
    Build a Virtuoso index for an RDF dataset. The indexing workflow is:
    1. Update virtuoso.ini with Qleverfile settings (ports, memory buffers)
    2. Start the Virtuoso server (virtuoso-t)
    3. Register input files via isql ld_dir()
    4. Load data via rdf_loader_run() (optionally with parallel loaders)
    5. Checkpoint and stop the server
    """

    def __init__(self):
        pass

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

    def execute(self, args) -> bool:
        num_parallel_loaders = args.num_parallel_loaders
        start_cmd = f"{args.server_binary} -c {args.name}.virtuoso.ini"

        isql_cmd = f"{args.index_binary} {args.isql_port} dba dba"
        ld_dir_stmts = " ".join(
            f"ld_dir('.', '{f}', '');"
            for f in args.input_files.split()
        )
        ld_dir_cmd = isql_cmd + f' exec="{ld_dir_stmts}"'

        # Multiple parallel loaders i.e. rdf_loader_run()
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
        if args.system in Containerize.supported_systems():
            start_cmd, ld_dir_cmd, run_cmd = wrap_cmd_in_container(
                args, start_cmd, ld_dir_cmd, run_cmds
            )
            run_cmd_to_show = run_cmd

        ini_files = [str(ini) for ini in Path(".").glob("*.ini")]
        if not Path(f"{args.name}.virtuoso.ini").exists():
            self.show(
                f"{args.name}.virtuoso.ini configfile not found in the current "
                f"directory! {virtuoso_ini_help_msg(script_name, args, ini_files)}"
            )

        virtuoso_ini_config_dict = config_dict_for_update_ini(args)
        log_virtuoso_ini_changes(args.name, virtuoso_ini_config_dict)

        cmd_to_show = f"{start_cmd}\n\n{ld_dir_cmd}\n{run_cmd_to_show}"

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(args.input_files):
            return False

        if args.system in Containerize.supported_systems():
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False
        else:
            # When running natively, check if the binary exists and works.
            # We use shutil.which instead of util.binary_exists because
            # isql --help writes to stderr instead of stdout
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

        # Check if previous index exists and user is not trying to extend it
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

        if args.system not in Containerize.supported_systems():
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
                    f"directory! {virtuoso_ini_help_msg(script_name, args, ini_files)}"
                )
                return False

        if not update_virtuoso_ini(args.name, virtuoso_ini_config_dict):
            return False

        # Helper to stop the server/container after a failure so it does
        # not block the next indexing attempt.
        def stop_server():
            try:
                args.server_container = args.index_container
                args.cmdline_regex = StopCommand.DEFAULT_REGEX
                StopCommand().execute(args)
            except Exception as stop_err:
                log.warning(f"Failed to stop Virtuoso server: {stop_err}")

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
            log_file = Path(f"{args.name}.index-log.txt")
            log_proc = None
            # Wait until the Virtuoso server is online, and start tailing
            # the index log file as soon as it exists (note that the `exec`
            # is important to make sure that the tail process is killed and
            # not just the bash process).
            while not util.is_server_alive(
                f"http://{args.host_name}:{args.port}/sparql"
            ):
                if time.time() - start_time > timeout:
                    log.error("Timed out waiting for Virtuoso to be online.")
                    stop_server()
                    return False
                if log_proc is None and log_file.exists():
                    log_proc = util.run_command(
                        f"exec tail -n +1 -f {log_file}",
                        use_popen=True,
                        show_output=True,
                    )
                time.sleep(1)
            # Execute the ld_dir and rdf_loader_run commands
            log.info("Virtuoso server online! Loading data into Virtuoso...\n")

            # Resolve virtuoso-t's PID so the memory monitor can follow
            # the detached server process in native mode.
            virtuoso_pid = None
            if args.system not in Containerize.supported_systems():
                virtuoso_pid = find_virtuoso_pid(Path("virtuoso.lck"))
                if virtuoso_pid is None:
                    log.warning(
                        "Could not resolve virtuoso-t PID from "
                        "virtuoso.lck; memory monitoring will be skipped"
                    )

            monitor_ctx = (
                MemoryMonitor(
                    dataset=args.name,
                    cmdline_regex=args.server_binary,
                    container=args.index_container,
                    system=args.system,
                    parent_pid=virtuoso_pid,
                )
                if args.system in Containerize.supported_systems()
                or virtuoso_pid is not None
                else nullcontext()
            )

            with monitor_ctx:
                util.run_command(ld_dir_cmd)
                util.run_command(run_cmd)
            if log_proc is not None:
                log_proc.terminate()
            log.info("")
            log.info("Data loading has finished!")

            stop_server()
            return True
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            stop_server()
            return False
