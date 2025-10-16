from __future__ import annotations

import subprocess
from os import environ

from qlever.command import QleverCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import is_port_used


class UiCommand(QleverCommand):
    """
    Class for launching the QLever UI web application.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Launch the QLever UI web application"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port"],
            "ui": ["ui_port", "ui_config", "ui_system", "ui_image", "ui_container"],
        }

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        # If QLEVER_OVERRIDE_DISABLE_UI is set, this command is disabled.
        qlever_is_running_in_container = environ.get("QLEVER_IS_RUNNING_IN_CONTAINER")
        if qlever_is_running_in_container:
            log.error(
                "The environment variable `QLEVER_OVERRIDE_DISABLE_UI` is set, "
                "therefore `qlever ui` is not available (it should not be called "
                "from inside a container)"
            )
            log.info("")
            if not args.show:
                log.info(
                    "For your information, showing the commands that are "
                    "executed when `qlever ui` is available:"
                )
                log.info("")

        # Construct commands and show them.
        server_url = f"http://{args.host_name}:{args.port}"
        ui_url = f"http://{args.host_name}:{args.ui_port}"
        pull_cmd = f"{args.ui_system} pull -q {args.ui_image}"
        cmd = Containerize().containerize_command(
            cmd="python manage.py configure",
            ports=args.ui_portm
        )
        run_cmd = (
            f"{args.ui_system} run -d "
            f"--publish {args.ui_port}:7000 "
            f"--name {args.ui_container} "
            f"{args.ui_image}"
        )
        exec_cmd = (
            f"{args.ui_system} exec " # -it
            f"{args.ui_container} "
            f'bash -c "python manage.py configure '
            f'{args.ui_config} {server_url}"'
        )
        self.show(
            "\n".join(["Stop running containers", pull_cmd, run_cmd, exec_cmd]),
            only_show=args.show,
        )
        if qlever_is_running_in_container:
            return False
        if args.show:
            return True

        # Stop running containers.
        for container_system in Containerize.supported_systems():
            Containerize.stop_and_remove_container(container_system, args.ui_container)

        # Check if the UI port is already being used.
        if is_port_used(args.ui_port):
            log.warning(
                f"It looks like the specified port for the UI ({args.ui_port}) is already in use. You can set another port in the Qleverfile in the [ui] section with the UI_PORT variable."
            )

        # Try to start the QLever UI.
        try:
            subprocess.run(pull_cmd, shell=True, stdout=subprocess.DEVNULL)
            subprocess.run(run_cmd, shell=True, stdout=subprocess.DEVNULL)
            subprocess.run(exec_cmd, shell=True, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            log.error(f"Failed to start the QLever UI ({e})")
            return False

        # Success.
        log.info(
            f"The QLever UI should now be up at {ui_url} ..."
            f"You can log in as QLever UI admin with username and "
            f'password "demo"'
        )
        return True
