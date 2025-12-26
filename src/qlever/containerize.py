# Copyright 2024, University of Freiburg,
# Chair of Algorithms and Data Structures
# Author: Hannah Bast <bast@cs.uni-freiburg.de>

from __future__ import annotations

import shlex
import subprocess
from typing import Optional

from qlever.log import log
from qlever.util import get_random_string, run_command


class ContainerizeException(Exception):
    pass


class Containerize:
    """
    This class contains functions specific for running commands with various
    container engines.
    """

    @staticmethod
    def supported_systems() -> list[str]:
        """
        Return a list of the supported container systems. Make sure that they
        are all indeed supported by `containerize_command` below.
        """
        return ["docker", "podman"]

    @staticmethod
    def containerize_command(
        cmd: str,
        container_system: str,
        run_subcommand: str,
        image_name: str,
        container_name: str,
        volumes: list[tuple[str, str]] = [],
        ports: list[tuple[int, int]] = [],
        working_directory: Optional[str] = None,
        use_bash: bool = True,
    ) -> str:
        """
        Get the command to run `cmd` with the given `container_system` and the
        given options.
        """

        # Check that `container_system` is supported.
        if container_system not in Containerize.supported_systems():
            return ContainerizeException(
                f'Invalid container system "{container_system}"'
                f" (must be one of {Containerize.supported_systems()})"
            )

        # Set user and group ids. This is important so that the files created
        # by the containerized command are owned by the user running the
        # command.
        if container_system == "docker":
            user_option = " -u $(id -u):$(id -g)"
        elif container_system == "podman":
            user_option = " -u root"
        else:
            user_option = ""

        # Options for mounting volumes, setting ports, and setting the working
        # dir.
        volume_options = "".join(
            [
                f' --mount type=bind,src="{v1}",target={v2}'
                for v1, v2 in volumes
            ]
        )
        port_options = "".join([f" -p {p1}:{p2}" for p1, p2 in ports])
        working_directory_option = (
            f" -w {working_directory}" if working_directory is not None else ""
        )

        # Construct the command that runs `cmd` with the given container
        # system.
        containerized_cmd = (
            f"{container_system} {run_subcommand}"
            f"{user_option}"
            f" -v /etc/localtime:/etc/localtime:ro"
            f"{volume_options}"
            f"{port_options}"
            f"{working_directory_option}"
            f" --name {container_name}"
            f" --init"
        )
        if use_bash:
            containerized_cmd += (
                f" --entrypoint bash {image_name} -c {shlex.quote(cmd)}"
            )
        else:
            containerized_cmd += f" {image_name} {cmd}"
        return containerized_cmd

    @staticmethod
    def is_running(container_system: str, container_name: str) -> bool:
        # Note: the `{{{{` and `}}}}` result in `{{` and `}}`, respectively.
        containers = (
            run_command(
                f'{container_system} ps --format="{{{{.Names}}}}"',
                return_output=True,
            )
            .strip()
            .splitlines()
        )
        return container_name in containers

    @staticmethod
    def stop_and_remove_container(
        container_system: str, container_name: str
    ) -> bool:
        """
        Stop the container with the given name using the given system. Return
        `True` if a container with that name was found and stopped, `False`
        otherwise.
        """

        # Check that `container_system` is supported.
        if container_system not in Containerize.supported_systems():
            return ContainerizeException(
                f'Invalid container system "{container_system}"'
                f" (must be one of {Containerize.supported_systems()})"
            )

        # Construct the command that stops the container.
        stop_cmd = (
            f"{container_system} stop {container_name} && "
            f"{container_system} rm {container_name}"
        )

        # Run the command.
        try:
            subprocess.run(
                stop_cmd,
                shell=True,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception as e:
            log.debug(f'Error running "{stop_cmd}": {e}')
            return False

    @staticmethod
    def run_in_container(cmd: str, args) -> Optional[str]:
        """
        Run an arbitrary command in the qlever container and return its output.
        """
        if args.system in Containerize.supported_systems():
            if not args.server_container:
                args.server_container = get_random_string(20)
            run_cmd = Containerize().containerize_command(
                cmd,
                args.system,
                'run --rm -it --entrypoint "" ',
                args.image,
                args.server_container,
                volumes=[("$(pwd)", "/index")],
                working_directory="/index",
            )
            return run_command(run_cmd, return_output=True)
