from __future__ import annotations

import subprocess
from os import environ
from pathlib import Path

import qlever.util as util
from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log


class SetupConfigCommand(QleverCommand):
    """
    Class for executing the `setup-config` command.
    """

    def __init__(self):
        self.qleverfiles_path = Path(__file__).parent.parent / "Qleverfiles"
        self.qleverfile_names = [
            p.name.split(".")[1]
            for p in self.qleverfiles_path.glob("Qleverfile.*")
        ]

    def description(self) -> str:
        return "Get a pre-configured Qleverfile"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "config_name",
            type=str,
            choices=self.qleverfile_names,
            help="The name of the pre-configured Qleverfile to create",
        )
        subparser.add_argument(
            "--port",
            type=int,
            default=None,
            help=(
                "Override the default PORT value in the [server] section of "
                "the generated Qleverfile"
            ),
        )
        subparser.add_argument(
            "--timeout",
            type=str,
            default=None,
            help=(
                "Override the default TIMEOUT value in the [server] section of "
                "the generated Qleverfile"
            ),
        )
        subparser.add_argument(
            "--system",
            type=str,
            choices=Containerize.supported_systems() + ["native"],
            default=None,
            help=(
                "Override the default SYSTEM value in the [runtime] section of "
                "the generated Qleverfile"
            ),
        )

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
        # Construct the command line and show it.
        qleverfile_path = (
            self.qleverfiles_path / f"Qleverfile.{args.config_name}"
        )
        setup_config_cmd = (
            f"cat {qleverfile_path}"
            f" | {
                util.get_ini_sed_cmd(
                    'server', 'ACCESS_TOKEN', util.get_random_string(12), True
                )
            }"
        )
        if qlever_is_running_in_container:
            setup_config_cmd += (
                f" | {util.get_ini_sed_cmd('runtime', 'SYSTEM', 'native')}"
            )
        else:
            for section, override_arg in [
                ("server", "port"),
                ("server", "timeout"),
                ("runtime", "system"),
            ]:
                if arg_value := getattr(args, override_arg):
                    setup_config_cmd += (
                        f" | {util.get_ini_sed_cmd(section, override_arg.upper(), arg_value)}"
                    )

        setup_config_cmd += "> Qleverfile"
        self.show(setup_config_cmd, only_show=args.show)
        if args.show:
            return True

        # If there is already a Qleverfile in the current directory, exit.
        qleverfile_path = Path("Qleverfile")
        if qleverfile_path.exists():
            log.error("`Qleverfile` already exists in current directory")
            log.info("")
            log.info(
                "If you want to create a new Qleverfile using "
                "`qlever setup-config`, delete the existing Qleverfile "
                "first"
            )
            return False

        # Copy the Qleverfile to the current directory.
        try:
            subprocess.run(
                setup_config_cmd,
                shell=True,
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
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
