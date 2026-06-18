from __future__ import annotations

from configparser import RawConfigParser
from pathlib import Path

from qlever.commands.setup_config import (
    SetupConfigCommand as QleverSetupConfigCommand,
)
from qlever.log import log
from qlever.qleverfile import Qleverfile


class SetupConfigCommand(QleverSetupConfigCommand):
    """
    Create a Qleverfile for Oxigraph from a dataset template from `src/qlever/Qleverfiles`.
    Filters the template to keep only the relevant sections and adds Oxigraph-specific
    defaults (read-only mode, query timeout).
    This class is used as the base SetupConfigCommand by all the other new engines.
    """

    IMAGE = "ghcr.io/oxigraph/oxigraph"

    # Sections and keys to retain when filtering a Qleverfile template.
    FILTER_CRITERIA = {
        "data": [],
        "index": ["INPUT_FILES"],
        "server": ["PORT"],
        "runtime": ["SYSTEM", "IMAGE"],
        "ui": ["UI_CONFIG"],
    }

    @staticmethod
    def construct_engine_specific_params(args) -> dict[str, dict[str, str]]:
        """Return Oxigraph-specific defaults to inject into the Qleverfile."""
        return {"server": {"READ_ONLY": "yes", "TIMEOUT": "60s"}}

    @staticmethod
    def add_engine_specific_option_values(
        qleverfile_parser: RawConfigParser,
        engine_specific_params: dict[str, dict[str, str]],
    ) -> None:
        """Merge engine-specific parameters into the Qleverfile parser."""
        for section, option_dict in engine_specific_params.items():
            if qleverfile_parser.has_section(section):
                for option, value in option_dict.items():
                    qleverfile_parser.set(section, option, value)

    def execute(self, args) -> bool:
        # Construct the command line and show it.
        template_path = (
            self.qleverfiles_path / f"Qleverfile.{args.config_name}"
        )
        setup_config_show = (
            f"Qleverfile for {args.config_name} will be created using "
            f"Qleverfile.{args.config_name} file in {template_path}"
        )
        self.show(setup_config_show, only_show=args.show)
        if args.show:
            return True

        # If there is already a Qleverfile in the current directory, exit.
        if self.check_qleverfile_exists():
            return False

        qleverfile_path = Path("Qleverfile")

        try:
            qleverfile_parser = Qleverfile.filter(
                template_path, self.FILTER_CRITERIA
            )
            qleverfile_parser.set("runtime", "IMAGE", self.IMAGE)
            params = self.construct_engine_specific_params(args)
            self.add_engine_specific_option_values(qleverfile_parser, params)
            for section, arg_name in self.override_args:
                if arg_value := getattr(args, arg_name, None):
                    qleverfile_parser.set(
                        section, arg_name.upper(), str(arg_value)
                    )
            with qleverfile_path.open("w") as f:
                qleverfile_parser.write(f)

            log.info(
                f'Created Qleverfile for config "{args.config_name}"'
                f" in current directory"
            )
            return True
        except Exception as e:
            log.error(
                f'Could not copy "{qleverfile_path}" to current directory: {e}'
            )
            return False
