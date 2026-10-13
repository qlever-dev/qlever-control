from __future__ import annotations

from abc import abstractmethod
from pathlib import Path

from qlever.commands.setup_config import (
    SetupConfigCommand as QleverSetupConfigCommand,
)
from qlever.commands.setup_config import check_qleverfile_exists
from qlever.log import log
from qlever.qleverfile import Qleverfile


class BaseSetupConfigCommand(QleverSetupConfigCommand):
    """
    Create a Qleverfile for an engine from one of QLever's pre-configured
    Qleverfiles: keep only the parts that every engine needs, then add the
    engine's image and its own settings.
    """

    def filter_criteria(self) -> dict[str, list[str] | None]:
        """
        The sections and options of a pre-configured Qleverfile to keep, by
        default those that every engine needs. `None` keeps every option of
        the section. An engine that needs more can add to what this returns.
        """
        return {
            "data": None,
            "index": ["INPUT_FILES"],
            "server": ["PORT"],
            "runtime": ["SYSTEM", "IMAGE"],
            "ui": ["UI_CONFIG"],
        }

    @abstractmethod
    def image(self) -> str:
        """The container image of the engine."""

    @abstractmethod
    def engine_settings(self, args) -> dict[str, dict[str, str]]:
        """The engine's own Qleverfile values, as {section: {option: value}}."""

    def execute(self, args) -> bool:
        template_path = (
            self.qleverfiles_path / f"Qleverfile.{args.config_name}"
        )
        self.show(
            f"Create a Qleverfile from {template_path}, keeping only the "
            "parts that apply to this graph database",
            only_show=args.show,
        )
        if args.show:
            return True

        if check_qleverfile_exists(args.main_command_name):
            return False

        # Each step can override the values of the steps before it.
        try:
            qleverfile = Qleverfile.filter(
                template_path, self.filter_criteria()
            )
            qleverfile.set("runtime", "IMAGE", self.image())
            for section, options in self.engine_settings(args).items():
                # Skip sections that the pre-configured Qleverfile lacks.
                if qleverfile.has_section(section):
                    for option, value in options.items():
                        qleverfile.set(section, option, value)
            for section, arg_name in self.override_args:
                if arg_value := getattr(args, arg_name):
                    qleverfile.set(section, arg_name.upper(), str(arg_value))
            with Path("Qleverfile").open("w") as qleverfile_file:
                qleverfile.write(qleverfile_file)
        except Exception as e:
            log.error(f"Could not create the Qleverfile: {e}")
            return False

        log.info(
            f'Created Qleverfile for config "{args.config_name}" in the '
            "current directory"
        )
        return True
