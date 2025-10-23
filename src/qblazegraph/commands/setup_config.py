from __future__ import annotations

import shutil
from pathlib import Path

from qlever.log import log
from qoxigraph.commands.setup_config import (
    SetupConfigCommand as QoxigraphSetupConfigCommand,
)


class SetupConfigCommand(QoxigraphSetupConfigCommand):
    """
    Should behave exactly the same as setup-config command in qoxigraph,
    just with a different Docker image name
    """

    IMAGE = "adfreiburg/qblazegraph"

    def execute(self, args) -> bool:
        qleverfile_path = Path("Qleverfile")
        exit_status = self.validate_qleverfile_setup(args, qleverfile_path)
        if exit_status is not None:
            return exit_status

        qleverfile_parser = self.get_filtered_qleverfile_parser(
            args.config_name
        )
        # Add the java_heap_gb to index and server sections
        qleverfile_parser.set("index", "JAVA_HEAP_GB", 6)
        qleverfile_parser.set("server", "JAVA_HEAP_GB", 6)

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
        log.info("")

        log.info("Fetching blazegraph.properties file...")
        properties_file_path = (
            Path(__file__).parent.parent / "blazegraph.properties"
        )
        destination = Path("blazegraph.properties")
        try:
            shutil.copy(properties_file_path, destination)
            log.info("Copied blazegraph.properties to current directory!")
            return True
        except Exception as e:
            file_url = (
                "https://github.com/ad-freiburg/qlever-control/tree/main/src/"
                "qblazegraph/blazegraph.properties"
            )
            log.error(
                "Couldn't copy blazegraph.properties file to current working "
                f"directory! Error: {e}\n"
            )
            log.info(f"Download it manually from {file_url}")
            return False
