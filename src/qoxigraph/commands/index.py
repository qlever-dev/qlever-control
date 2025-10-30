from __future__ import annotations

import shlex
from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class IndexCommand(QleverCommand):
    def __init__(self):
        self.script_name = "qoxigraph"

    def description(self) -> str:
        return "Build the index for a given RDF dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name", "format"],
            "index": [
                "input_files",
                "ulimit",
                "index_binary",
                "lenient",
                "extra_args",
            ],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        pass

    @staticmethod
    def wrap_cmd_in_container(args, cmd: str) -> str:
        return Containerize().containerize_command(
            cmd=cmd,
            container_system=args.system,
            run_subcommand="run --rm",
            image_name=args.image,
            container_name=args.index_container,
            volumes=[("$(pwd)", "/opt")],
            working_directory="/opt",
            use_bash=False,
        )

    def execute(self, args) -> bool:
        index_cmd = (
            f"load {'--lenient ' if args.lenient else ''}"
            f"--location {args.name}_index/ --file {args.input_files} "
            f"{args.extra_args} |& tee {args.name}.index-log.txt"
        )

        if args.system == "native":
            index_cmd = f"{args.index_binary} {index_cmd}"
            # If the total file size is larger than 10 GB, set ulimit (such that a
            # large number of open files is allowed).
            total_file_size = util.get_total_file_size(
                shlex.split(args.input_files)
            )
            if args.ulimit is not None:
                index_cmd = f"ulimit -Sn {args.ulimit} && {index_cmd}"
            elif total_file_size > 1e10:
                index_cmd = f"ulimit -Sn 500000 && {index_cmd}"
        else:
            index_cmd = self.wrap_cmd_in_container(args, index_cmd)

        # Show the command line.
        self.show(index_cmd, only_show=args.show)
        if args.show:
            return True

        if not util.input_files_exist(args.input_files, self.script_name):
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

        # Run the index command.
        try:
            util.run_command(index_cmd, show_output=True, show_stderr=True)
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        return True
