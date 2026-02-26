from __future__ import annotations

import qlever.util as util
from qlever.log import log
from qlever.util import run_command
from qoxigraph.commands.setup_config import (
    SetupConfigCommand as QoxigraphSetupConfigCommand,
)


class SetupConfigCommand(QoxigraphSetupConfigCommand):
    """
    Generate a Qleverfile and download the default virtuoso.ini configuration
    file. Extends the base setup-config with Virtuoso-specific memory budget
    options (--total-index-memory, --total-server-memory) that are used to
    auto-generate sensible Qleverfile defaults.
    """

    IMAGE = "adfreiburg/virtuoso-opensource-7"
    VIRTUOSO_INI_URL = (
        "https://raw.githubusercontent.com/openlink/virtuoso-opensource/"
        "23abbfbe9eb78d47dd70d8aca08cef0e81202bfb/binsrc/virtuoso/virtuoso.ini"
    )

    def additional_arguments(self, subparser) -> None:
        super().additional_arguments(subparser)
        util.add_memory_options(subparser)

    @staticmethod
    def construct_engine_specific_params(args) -> dict[str, dict[str, str]]:
        """
        Derive Virtuoso-specific Qleverfile parameters from the memory budget.
        Allocates 1/5 of server memory (min 2G) to the query processor.
        """
        index_params = {
            "ISQL_PORT": 1111,
            "FREE_MEMORY_GB": args.total_index_memory,
            "NUM_PARALLEL_LOADERS": 1,
        }
        total_server_memory = int(args.total_server_memory[:-1])
        max_query_memory = max(2, total_server_memory // 5)
        server_params = {
            "MAX_QUERY_MEMORY": f"{max_query_memory}G",
            "TIMEOUT": "30s",
        }
        return {"index": index_params, "server": server_params}

    def execute(self, args) -> bool:
        """
        Create the Qleverfile via the parent class, then download the default
        virtuoso.ini into the current working directory.
        """
        qleverfile_successfully_created = super().execute(args)
        if not qleverfile_successfully_created:
            return False

        log.info("Fetching virtuoso.ini configuration file...")
        try:
            curl_cmd = f"curl -o virtuoso.ini {self.VIRTUOSO_INI_URL}"
            run_command(cmd=curl_cmd, show_output=True)
            log.info(
                "Successfully downloaded virtuoso.ini to the current working "
                "directory!"
            )
        except Exception as e:
            url = (
                "https://github.com/openlink/virtuoso-opensource/blob/"
                "23abbfbe9eb78d47dd70d8aca08cef0e81202bfb/binsrc/virtuoso/"
                "virtuoso.ini"
            )
            log.error(
                "Couldn't download the virtuoso.ini configuration file."
                f"If possible, please download it manually from {url} "
                f"and place it in the current directory. Error -> {e}"
            )
        return True
