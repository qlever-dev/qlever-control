from __future__ import annotations

from configparser import RawConfigParser
from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.qleverfile import Qleverfile


class SetupConfigCommand(QleverCommand):
    IMAGE = "ghcr.io/oxigraph/oxigraph"

    FILTER_CRITERIA = {
        "data": [],
        "index": ["INPUT_FILES"],
        "server": ["PORT"],
        "runtime": ["SYSTEM", "IMAGE"],
        "ui": ["UI_CONFIG"],
    }

    def __init__(self):
        self.qleverfiles_path = (
            Path(__file__).parent.parent.parent / "qlever" / "Qleverfiles"
        )
        self.qleverfile_names = [
            p.name.split(".")[1]
            for p in self.qleverfiles_path.glob("Qleverfile.*")
        ]

    def description(self) -> str:
        return "Get a pre-configured Qleverfile"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "config_name",
            type=str,
            choices=self.qleverfile_names,
            help="The name of the pre-configured Qleverfile to create",
        )

    def validate_qleverfile_setup(
        self, args, qleverfile_path: Path
    ) -> bool | None:
        # Construct the command line and show it.
        setup_config_show = (
            f"Creating Qleverfile for {args.config_name} using "
            f"Qleverfile.{args.config_name} file in {self.qleverfiles_path}"
        )
        self.show(setup_config_show, only_show=args.show)
        if args.show:
            return True

        # If there is already a Qleverfile in the current directory, exit.
        if qleverfile_path.exists():
            log.error("`Qleverfile` already exists in current directory")
            log.info("")
            log.info(
                "If you want to create a new Qleverfile using "
                "`qlever setup-config`, delete the existing Qleverfile "
                "first"
            )
            return False
        return None

    def get_filtered_qleverfile_parser(
        self, config_name: str
    ) -> RawConfigParser:
        qleverfile_config_path = (
            self.qleverfiles_path / f"Qleverfile.{config_name}"
        )
        qleverfile_parser = Qleverfile.filter(
            qleverfile_config_path, self.FILTER_CRITERIA
        )
        if qleverfile_parser.has_section("runtime"):
            qleverfile_parser.set("runtime", "IMAGE", self.IMAGE)
        return qleverfile_parser

    def execute(self, args) -> bool:
        qleverfile_path = Path("Qleverfile")
        exit_status = self.validate_qleverfile_setup(args, qleverfile_path)
        if exit_status is not None:
            return exit_status

        qleverfile_parser = self.get_filtered_qleverfile_parser(
            args.config_name
        )
        # Copy the Qleverfile to the current directory.
        try:
            with qleverfile_path.open("w") as f:
                qleverfile_parser.write(f)
        except Exception as e:
            log.error(
                f'Could not copy "{qleverfile_path}" to current directory: {e}'
            )
            return False

        # If we get here, everything went well.
        log.info(
            f'Created Qleverfile for config "{args.config_name}"'
            f" in current directory"
        )
        return True
