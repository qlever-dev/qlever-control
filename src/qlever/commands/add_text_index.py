from __future__ import annotations

import subprocess

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import get_existing_index_files, run_command


class AddTextIndexCommand(QleverCommand):
    """
    Class for executing the `index` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Add text index to an index built with `qlever index`"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "index": [
                "index_binary",
                "text_index",
                "text_words_file",
                "text_docs_file",
            ],
            "runtime": ["system", "image", "index_container"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--overwrite-existing",
            action="store_true",
            help="Overwrite existing text index files",
        )

    def execute(self, args) -> bool:
        # Check that there is actually something to add.
        if args.text_index == "none":
            log.error("You specified `--text_index none`, nothing to add")
            return False

        # Construct the command line.
        add_text_index_cmd = f"{args.index_binary} -A -i {args.name}"
        if args.text_index in [
            "from_text_records",
            "from_text_records_and_literals",
        ]:
            add_text_index_cmd += (
                f" -w {args.text_words_file}" f" -d {args.text_docs_file}"
            )
        if args.text_index in [
            "from_literals",
            "from_text_records_and_literals",
        ]:
            add_text_index_cmd += " --text-words-from-literals"
        add_text_index_cmd += f" | tee {args.name}.text-index-log.txt"

        # Run the command in a container (if so desired).
        if args.system in Containerize.supported_systems():
            add_text_index_cmd = Containerize().containerize_command(
                add_text_index_cmd,
                args.system,
                "run --rm",
                args.image,
                args.index_container,
                volumes=[("$(pwd)", "/index")],
                working_directory="/index",
            )

        # Show the command line.
        self.show(add_text_index_cmd, only_show=args.show)
        if args.show:
            return True

        # When running natively, check if the binary exists and works.
        if args.system == "native":
            try:
                run_command(f"{args.index_binary} --help")
            except Exception as e:
                log.error(
                    f'Running "{args.index_binary}" failed ({e}), '
                    f"set `--index-binary` to a different binary or "
                    f"use `--container_system`"
                )
                return False

        # Check if text index files already exist.
        existing_text_index_files = get_existing_index_files(
            f"{args.name}.text.*"
        )
        if len(existing_text_index_files) > 0 and not args.overwrite_existing:
            log.error(
                "Text index files found, if you want to overwrite them, "
                "use --overwrite-existing"
            )
            log.info("")
            log.info(f"Index files found: {existing_text_index_files}")
            return False

        # Run the index command.
        try:
            subprocess.run(add_text_index_cmd, shell=True, check=True)
        except Exception as e:
            log.error(f'Running "{add_text_index_cmd}" failed ({e})')
            return False

        return True
