from __future__ import annotations

import shlex
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.commands.index import render_usage_plot
from qlever.containerize import Containerize
from qlever.log import log
from qlever.resource_usage.resource_monitor import ResourceMonitor
from qlever.util import (
    binary_exists,
    get_total_file_size,
    input_files_exist,
    run_command,
)

USAGE_PLOT_MODULE = "qeval.oxigraph.resource_usage.usage_plot"


def wrap_cmd_in_container(args, cmd: str, ulimit: int | None = None) -> str:
    """
    Wrap an indexing command in a container that is automatically removed
    after the process exits (`--rm`) Use `use_bash=False` as Oxigraph image
    doesn't support bash entrypoint.
    """
    run_subcommand = "run --rm"
    if ulimit:
        run_subcommand += f" --ulimit nofile={ulimit}:{ulimit}"
    return Containerize().containerize_command(
        cmd=cmd,
        container_system=args.system,
        run_subcommand=run_subcommand,
        image_name=args.image,
        container_name=args.index_container,
        volumes=[("$(pwd)", "/opt")],
        working_directory="/opt",
        use_bash=False,
    )


class IndexCommand(QleverCommand):
    """
    Build an Oxigraph index for an RDF dataset. The indexing workflow is:
    1. Run `oxigraph load` to import input files into a RocksDB store.
    2. Optionally run `oxigraph optimize` to compact storage for read-only use.

    For large datasets (>5 GB), the file descriptor ulimit is raised
    automatically because RocksDB opens many .sst files concurrently.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "index": [
                "input_files",
                "ulimit",
                "index_binary",
                "lenient",
                "extra_args",
                "resource_usage_interval",
                "resource_usage_plot_max_points",
            ],
            "server": ["read_only"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--resource-usage-plot-only",
            action="store_true",
            default=False,
            help="Only render the resource-usage plot from the existing "
            "`<name>.oxigraph.index.resource-usage-log.tsv`; do not build "
            "the index. Use to re-render with a different "
            "`--resource-usage-plot-max-points`",
        )

    def execute(self, args) -> bool:
        # Render the resource-usage plot from the existing log without
        # rebuilding the index.
        if args.resource_usage_plot_only:
            plot_path = render_usage_plot(
                args, plot_only=True, engine_module=USAGE_PLOT_MODULE
            )
            if plot_path is None:
                return False
            log.info(f"Resource-usage plot saved to `{plot_path.name}`")
            return True

        cmds_to_execute = []
        index_cmd = (
            f"load {'--lenient ' if args.lenient == 'yes' else ''}"
            f"--location {args.name}_index/ --file {args.input_files} "
            f"{args.extra_args} |& tee {args.name}.index-log.txt"
        )

        ulimit = args.ulimit
        # RocksDB opens many .sst files concurrently. For datasets larger
        # than 5 GB, raise the file descriptor limit so the process does
        # not hit the default OS soft limit.
        total_file_size = get_total_file_size(shlex.split(args.input_files))
        if not ulimit and total_file_size > 5e9:
            ulimit = 500_000
        if args.system in Containerize.supported_systems():
            index_cmd = wrap_cmd_in_container(args, index_cmd, ulimit)
        else:
            index_cmd = f"{args.index_binary} {index_cmd}"
            if ulimit:
                index_cmd = f"ulimit -Sn {ulimit} && {index_cmd}"

        cmds_to_execute.append(index_cmd)

        # Compact the RocksDB storage for read-only serving. This reduces
        # disk usage and speeds up queries but makes the index immutable.
        optimize_cmd = None
        if args.read_only == "yes":
            optimize_cmd = f"optimize -l {args.name}_index/"
            if args.system in Containerize.supported_systems():
                optimize_cmd = wrap_cmd_in_container(args, optimize_cmd)
            else:
                optimize_cmd = f"{args.index_binary} {optimize_cmd}"
            cmds_to_execute.append(optimize_cmd)

        # Show the command line.
        self.show("\n".join(cmds_to_execute), only_show=args.show)
        if args.show:
            return True

        if not input_files_exist(args.input_files, args.main_command_name):
            return False

        # When running natively, check if the binary exists and works.
        if args.system in Containerize.supported_systems():
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False
        else:
            if not binary_exists(args.index_binary, "index-binary", args):
                return False

        # Abort if a previous index already exists. RocksDB .sst files in
        # the index directory indicate an existing store.
        if (
            len([p.name for p in Path(f"{args.name}_index").glob("*.sst")])
            != 0
        ):
            log.error(
                f"Index files (*.sst) found in {args.name}_index directory "
                "which shows presence of a previous index"
            )
            log.info("")
            log.info("Aborting the index operation...")
            return False

        # Run the index command and record the elapsed time in the log
        # file. Oxigraph's progress output is unreliable (may not print a
        # final summary line when loading multiple files), so we measure
        # the time externally.
        #
        log_file_name = f"{args.name}.index-log.txt"
        with ResourceMonitor.from_args(args):
            try:
                load_start = time.time()
                run_command(index_cmd, show_output=True, show_stderr=True)
                load_s = time.time() - load_start
            except Exception as e:
                log.error(f"Building the index failed: {e}")
                return False

            optimize_s = 0.0
            if optimize_cmd:
                try:
                    log.info("")
                    log.info("Optimizing read-only database storage:")
                    self.show(optimize_cmd)
                    optimize_start = time.time()
                    run_command(
                        optimize_cmd, show_output=True, show_stderr=True
                    )
                    optimize_s = time.time() - optimize_start
                except Exception as e:
                    log.error(f"Optimizing the database storage failed: {e}")
                    log.info(
                        f"Please run manually: "
                        f"{args.index_binary} optimize -l {args.name}_index/"
                    )

        with open(log_file_name, "a") as f:
            f.write(f"Load time: {load_s:.0f}s\n")
            if optimize_cmd:
                f.write(f"Optimize time: {optimize_s:.0f}s\n")
            f.write(f"TOTAL time: {load_s + optimize_s:.0f}s\n")

        plot_path = render_usage_plot(
            args, plot_only=False, engine_module=USAGE_PLOT_MODULE
        )
        if plot_path is not None:
            log.info(f"Resource-usage plot saved to `{plot_path.name}`")

        return True
