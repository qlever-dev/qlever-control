from __future__ import annotations

from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class IndexCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qmdb"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
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

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand="run --rm",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/data")],
            working_directory="/data",
        )

    def execute(self, args) -> bool:
        system = args.system
        input_files = args.input_files

        # For compressed data, pipe the data from stdin with mandatory --format
        if args.cat_input_files:
            index_cmd = (
                f"{args.cat_input_files} | {args.index_binary} import {args.name}_index "
                f"--format {args.format}"
            )
        else:
            index_cmd = (
                f"{args.index_binary} import {input_files} {args.name}_index"
            )

        # Additional mdb index args
        index_cmd += f" --btree-permutations {args.btree_permutations}"

        if args.buffer_strings:
            index_cmd += (
                f" --buffer-strings {args.buffer_strings}B"
                f" --buffer-tensors {args.buffer_tensors}B"
            )

        if args.prefixes:
            index_cmd += f" --prefixes {args.prefixes}"
        if args.extra_args:
            index_cmd += f" {args.extra_args}"
        index_cmd += f" | tee {args.name}.index-log.txt"

        if args.system == "native":
            cmd_to_show = index_cmd
        else:
            index_cmd = self.wrap_cmd_in_container(args, index_cmd)
            dockerfile_url = "https://github.com/MillenniumDB/MillenniumDB.git"
            build_cmd = f"{system} build {dockerfile_url} -t {args.image}"

            image_id = util.get_container_image_id(system, args.image)

            cmd_to_show = (
                f"{build_cmd}\n\n{index_cmd}" if not image_id else index_cmd
            )

        # Show the command line.
        self.show(cmd_to_show, only_show=args.show)
        if args.show:
            return True

        # Check if all of the input files exist.
        if not util.input_files_exist(input_files, self.script_name):
            return False

        # Check if index files from a previous index exist in CWD
        index_dir = Path(f"{args.name}_index")
        if index_dir.exists() and any(index_dir.iterdir()):
            log.error(
                f"Index files found in {args.name}_index directory "
                "which shows presence of a previous index\n"
            )
            log.info("Aborting the index operation...")
            return False

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            if not util.binary_exists(args.index_binary, "index-binary"):
                return False
        else:
            if Containerize().is_running(args.system, args.index_container):
                log.info(
                    f"{args.system} container {args.index_container} is still up, "
                    "which means that data loading is in progress. Please wait..."
                )
                return False

            # Build the docker image if not found on the system
            if not image_id:
                build_successful = util.build_image(
                    build_cmd, system, args.image
                )
                if not build_successful:
                    return False
            else:
                log.info(f"{args.image} image present on the system\n")

        # Run the index command.
        try:
            util.run_command(index_cmd, show_output=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
