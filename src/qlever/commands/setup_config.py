from __future__ import annotations

from os import environ
from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.log import log


class SetupConfigCommand(QleverCommand):
    """
    Class for executing the `setup-config` command.
    """

    def __init__(self):
        self.qleverfiles_path = (
            Path(__file__).parent.parent.parent / "qlever/Qleverfiles"
        )
        self.qleverfile_names = [
            p.name.split(".")[1]
            for p in self.qleverfiles_path.glob("Qleverfile.*")
        ]
        # Arguments that can be overridden when generating a Qleverfile,
        # as (section, arg_name) pairs.
        self.override_args = [
            ("server", "port"),
            ("server", "timeout"),
            ("runtime", "system"),
        ]

    def description(self) -> str:
        return "Get a pre-configured Qleverfile"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        result = {}
        for section, arg_name in self.override_args:
            result.setdefault(section, []).append(arg_name)
        return result

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "config_name",
            type=str,
            choices=self.qleverfile_names,
            help="The name of the pre-configured Qleverfile to create",
        )
        # Override defaults to None so we can detect which args the user
        # explicitly passed vs. which are just defaults.
        for _, arg_name in self.override_args:
            subparser.set_defaults(**{arg_name: None})

    def check_qleverfile_exists(self) -> bool:
        """Return True if a Qleverfile already exists (and log an error)."""
        if Path("Qleverfile").exists():
            log.error("`Qleverfile` already exists in current directory")
            log.info("")
            log.info(
                "If you want to create a new Qleverfile using "
                "`qlever setup-config`, delete the existing Qleverfile "
                "first"
            )
            return True
        return False

    def execute(self, args) -> bool:
        # Show a warning if `QLEVER_OVERRIDE_SYSTEM_NATIVE` is set.
        qlever_is_running_in_container = environ.get(
            "QLEVER_IS_RUNNING_IN_CONTAINER"
        )
        if qlever_is_running_in_container:
            log.warning(
                "The environment variable `QLEVER_IS_RUNNING_IN_CONTAINER` is set, "
                "therefore the Qleverfile is modified to use `SYSTEM = native` "
                "(since inside the container, QLever should run natively)"
            )
            log.info("")
        # Build the updates dict for the Qleverfile.
        qleverfile_path = (
            self.qleverfiles_path / f"Qleverfile.{args.config_name}"
        )
        random_token = util.get_random_string(12)
        updates = {
            "server": {"ACCESS_TOKEN": (random_token, True)},
        }
        if qlever_is_running_in_container:
            updates.setdefault("runtime", {})["SYSTEM"] = ("native", False)
        else:
            for section, arg_name in self.override_args:
                if arg_value := getattr(args, arg_name, None):
                    updates.setdefault(section, {})[arg_name.upper()] = (
                        arg_value,
                        False,
                    )

        # Show the updates that will be applied.
        show_lines = [f"Copy {qleverfile_path} to Qleverfile with following changes:"]
        for section, option_dict in updates.items():
            show_lines.append(f"\n[{section}]")
            for option, (value, is_suffix) in option_dict.items():
                show_lines.append(f"  {option} = {value}")
        self.show("\n".join(show_lines), only_show=args.show)
        if args.show:
            return True

        if self.check_qleverfile_exists():
            return False

        # Read the template, apply updates, and write the Qleverfile.
        try:
            lines = qleverfile_path.read_text().splitlines()
            result = util.update_ini_values(lines, updates)
            Path("Qleverfile").write_text("\n".join(result) + "\n")
        except Exception as e:
            log.error(
                f'Could not copy "{qleverfile_path}" to current directory: {e}'
            )
            return False

        log.info(
            f'Created Qleverfile for config "{args.config_name}"'
            f" in current directory"
        )
        return True
