from __future__ import annotations

import shlex
import time
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

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name", "format"],
            "index": [
                "input_files",
                "ulimit",
                "index_binary",
                "lenient",
                "extra_args",
            ],
            "server": ["read_only"],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser):
        pass

    @staticmethod
    def wrap_cmd_in_container(
        args, cmd: str, ulimit: int | None = None
    ) -> str:
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

    def execute(self, args) -> bool:
        cmds_to_execute = []
        index_cmd = (
            f"load {'--lenient ' if args.lenient else ''}"
            f"--location {args.name}_index/ --file {args.input_files} "
            f"{args.extra_args} |& tee {args.name}.index-log.txt"
        )

        ulimit = args.ulimit
        # If the total file size is larger than 5 GB, set ulimit (such that a
        # large number of open files is allowed).
        total_file_size = util.get_total_file_size(
            shlex.split(args.input_files)
        )
        if not ulimit and total_file_size > 5e9:
            ulimit = 500_000
        if args.system == "native":
            index_cmd = f"{args.index_binary} {index_cmd}"
            if ulimit:
                index_cmd = f"ulimit -Sn {ulimit} && {index_cmd}"
        else:
            index_cmd = self.wrap_cmd_in_container(args, index_cmd, ulimit)

        cmds_to_execute.append(index_cmd)

        # Optimize database storage for read-only index
        optimize_cmd = None
        if args.read_only == "yes":
            optimize_cmd = f"optimize -l {args.name}_index/"
            if args.system == "native":
                optimize_cmd = f"{args.index_binary} {optimize_cmd}"
            else:
                optimize_cmd = self.wrap_cmd_in_container(args, optimize_cmd)
            cmds_to_execute.append(optimize_cmd)

        # Show the command line.
        self.show("\n".join(cmds_to_execute), only_show=args.show)
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

        # Run the index command and record the elapsed time in the log
        # file. Oxigraph's progress output is unreliable (may not print a 
        # final summary line when loading multiple files), so we measure 
        # the time externally.
        log_file_name = f"{args.name}.index-log.txt"
        try:
            start_time = time.time()
            util.run_command(index_cmd, show_output=True, show_stderr=True)
            elapsed_s = time.time() - start_time
            with open(log_file_name, "a") as f:
                f.write(f"Total elapsed time: {elapsed_s:.0f}s\n")
        except Exception as e:
            log.error(f"Building the index failed: {e}")
            return False

        if optimize_cmd:
            try:
                log.info("")
                log.info(
                    "Optimizing read-only database storage:"
                )
                self.show(optimize_cmd)
                util.run_command(
                    optimize_cmd, show_output=True, show_stderr=True
                )
            except Exception as e:
                log.error(f"Optimizing the database storage failed: {e}")
                log.info(
                    f"Please run manually: "
                    f"{args.index_binary} optimize -l {args.name}_index/"
                )

        return True
