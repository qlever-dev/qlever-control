import subprocess
from pathlib import Path
from os import environ

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


class SetupCommand(QleverCommand):
    """
    Class for executing the `setup` command.
    """

    def __init__(self):
        self.qleverfiles_path = Path(__file__).parent.parent / "Qleverfiles"
        self.testsuite_command = f"""
git clone --sparse --filter=blob:none --depth 1 https://github.com/w3c/rdf-tests ./testsuite-files && \
git -C ./testsuite-files sparse-checkout set sparql/sparql11
"""

    def description(self) -> str:
        return "Setup a pre-configured Qleverfile and download test suite for the SPARQL conformance tests"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {}

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "engine_name",
            type=str,
            choices=["qlever", "qlever-native"],
            help="The engine name for the pre-configured Qleverfile to create",
        )

    def execute(self, args) -> bool:
        # Show a warning if `QLEVER_OVERRIDE_SYSTEM_NATIVE` is set.
        qlever_is_running_in_container = environ.get("QLEVER_IS_RUNNING_IN_CONTAINER")
        if qlever_is_running_in_container:
            log.warning(
                "The environment variable `QLEVER_IS_RUNNING_IN_CONTAINER` is set, "
                "therefore the Qleverfile is modified to use `SYSTEM = native` "
                "(since inside the container, QLever should run natively)"
            )
            log.info("")
        # Construct the command line and show it.
        qleverfile_path = self.qleverfiles_path / f"Qleverfile.{args.engine_name} "
        setup_config_cmd = f"cat {qleverfile_path}"
        if qlever_is_running_in_container:
            setup_config_cmd += (
                " | sed -E 's/(^SYSTEM[[:space:]]*=[[:space:]]*).*/\\1native/'"
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
                "`sparql_conformance setup`, delete the existing Qleverfile "
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
                f'Could not copy "{qleverfile_path}"' f" to current directory: {e}"
            )
            return False

        # If we get here, everything went well.
        log.info(
            f'Created Qleverfile for engine "{args.engine_name}"'
            f" in current directory"
        )

        # If there is already a test suite in the current directory, exit.
        testsuite_path = Path("./testsuite-files/sparql/sparql11")
        if testsuite_path.exists():
            log.error("`Test suite` already exists in current directory")
            log.info("")
            log.info(
                "If you want to download the test suite using "
                "`sparql_conformance setup`, delete the existing test suite "
                "first"
            )
            return False
        testsuite_command = (
            "git clone --sparse --filter=blob:none --depth 1 https://github.com/w3c/rdf-tests ./testsuite-files && \ "
            "git -C ./testsuite-files sparse-checkout set sparql/sparql11"
        )
        try:
            run_command(self.testsuite_command)
        except Exception as e:
            log.error(
                f'Could not download test suite from https://github.com/w3c/rdf-tests' f" to current directory: {e}"
            )
            return False
        return True

