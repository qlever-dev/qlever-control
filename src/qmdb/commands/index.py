from __future__ import annotations

from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.memory_monitor import MemoryMonitor


def wrap_cmd_in_container(args, cmd: str) -> str:
    """
    Wrap an indexing command in a container that is automatically
    removed after the process exits (--rm).
    """
    return Containerize().containerize_command(
        cmd=cmd,
        container_system=args.system,
        run_subcommand="run --rm",
        image_name=args.image,
        container_name=args.index_container,
        volumes=[("$(pwd)", "/data")],
        working_directory="/data",
    )


class IndexCommand(QleverCommand):
    """
    Build a MillenniumDB index for an RDF dataset. The indexing workflow is:
    1. Run `mdb import` to import input files into the index directory.
    2. For compressed data, pipe input through stdin with --format.

    Supports native and containerized execution. When using containers,
    the Docker image is built from the MillenniumDB GitHub repository
    if not already present.
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
                "cat_input_files",
                "buffer_strings",
                "buffer_tensors",
                "btree_permutations",
                "prefixes",
                "extra_args",
            ],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "--index-binary",
            type=str,
            default="mdb",
            help=(
                "The binary for building the index (default: mdb) "
                "(this requires that you have Millennium DB built from source "
                "on your machine)"
            ),
        )
        subparser.add_argument(
            "--rebuild-image",
            action="store_true",
            default=False,
            help="Rebuild the Docker image to get the latest updates",
        )

    def execute(self, args) -> bool:
        system = args.system
        input_files = args.input_files

        # For compressed data, pipe the data from stdin with mandatory
        # --format so MillenniumDB knows the RDF serialization.
        if args.cat_input_files:
            index_cmd = (
                f"{args.cat_input_files} | {args.index_binary} import "
                f"{args.name}_index --format {args.format}"
            )
        else:
            index_cmd = (
                f"{args.index_binary} import {input_files} {args.name}_index"
            )

        # Append MillenniumDB-specific index options (btree permutations,
        # buffer sizes, prefix compression).
        index_cmd += f" --btree-permutations {args.btree_permutations}"

        if args.buffer_strings:
            index_cmd += f" --buffer-strings {args.buffer_strings}B"
        if args.buffer_tensors:
            index_cmd += f" --buffer-tensors {args.buffer_tensors}B"

        if args.prefixes:
            index_cmd += f" --prefixes {args.prefixes}"
        if args.extra_args:
            index_cmd += f" {args.extra_args}"
        index_cmd += f" | tee {args.name}.index-log.txt"

        # For container execution, build the Docker image from the
        # MillenniumDB repository if it is not already present.
        image_id = build_cmd = ""
        if args.system in Containerize.supported_systems():
            index_cmd = wrap_cmd_in_container(args, index_cmd)
            dockerfile_url = "https://github.com/MillenniumDB/MillenniumDB.git"
            build_cmd = f"{system} build {dockerfile_url} -t {args.image}"

            image_id = util.get_container_image_id(system, args.image)

            cmd_to_show = (
                f"{build_cmd}\n\n{index_cmd}"
                if not image_id or args.rebuild_image
                else index_cmd
            )
        else:
            cmd_to_show = index_cmd

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(input_files):
            return False

        # Abort if a previous index already exists. Any files in the
        # index directory indicate an existing store.
        index_dir = Path(f"{args.name}_index")
        if index_dir.exists() and any(index_dir.iterdir()):
            log.error(
                f"Index files found in {args.name}_index directory "
                "which shows presence of a previous index\n"
            )
            log.info("Aborting the index operation...")
            return False

        if args.system in Containerize.supported_systems():
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False

            # Build the docker image if not found on the system
            if not image_id or args.rebuild_image:
                build_successful = util.build_image(
                    build_cmd, system, args.image
                )
                if not build_successful:
                    return False
            else:
                log.info(f"{args.image} image present on the system\n")
        else:
            # When running natively, check if the binary exists and works.
            if not util.binary_exists(args.index_binary, "index-binary", args):
                return False

        # Run the index command.
        try:
            with MemoryMonitor(
                engine="millenniumdb",
                dataset=args.name,
                cmdline_regex=args.index_binary,
                container=args.index_container,
                system=args.system,
            ):
                util.run_command(index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
