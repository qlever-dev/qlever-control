from __future__ import annotations

from qlever.log import log
from qlever.util import add_memory_options, run_curl_command
from qoxigraph.commands.setup_config import (
    SetupConfigCommand as QoxigraphSetupConfigCommand,
)


class SetupConfigCommand(QoxigraphSetupConfigCommand):
    """
    Should behave exactly the same as setup-config command in qoxigraph,
    just with a different Docker image name
    """

    IMAGE = "docker.io/ontotext/graphdb:11.0.1"

    def additional_arguments(self, subparser) -> None:
        super().additional_arguments(subparser)
        add_memory_options(subparser)

    @staticmethod
    def construct_engine_specific_params(args) -> dict[str, dict[str, str]]:
        index_memory = int(args.total_index_memory[:-1])
        server_memory = int(args.total_server_memory[:-1])
        entity_index_size = min(2_140_000_000, (10_000_000 * index_memory * 3))
        return {
            "index": {
                "ENTITY_INDEX_SIZE": entity_index_size,
                "RULESET": "empty",
                "JVM_ARGS": f"-Xms{index_memory}G -Xmx{index_memory}G",
            },
            "server": {"HEAP_SIZE_GB": f"{server_memory}G", "TIMEOUT": "60s"},
        }

    def execute(self, args) -> bool:
        qleverfile_successfully_created = super().execute(args)
        if not qleverfile_successfully_created:
            return False

        repo_config_ttl = (
            "https://graphdb.ontotext.com/documentation/11.0/_downloads/"
            "565be93599bf4c3324147fb94b562595/repo-config.ttl"
        )
        try:
            run_curl_command(url=repo_config_ttl, result_file="config.ttl")
            log.info(
                "Successfully downloaded config.ttl for specifying config "
                "parameters. There is no need to change this file as all the "
                "relevant entries will be overwritten by entries from the Qleverfile."
            )
            return True
        except Exception as e:
            log.error(f"Failed to download config.ttl file: {e}")
            log.info(f"Download it manually from {repo_config_ttl}")
            log.info(
                "There is no need to change this file after downloading it as "
                "all the relevant entries will be overwritten by entries from "
                "the Qleverfile."
            )
            return False
