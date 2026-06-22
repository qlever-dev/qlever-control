from __future__ import annotations

from qlever.log import log
from qlever.util import add_memory_options, run_command
from qoxigraph.commands.setup_config import (
    SetupConfigCommand as QoxigraphSetupConfigCommand,
)


class SetupConfigCommand(QoxigraphSetupConfigCommand):
    """
    Should behave exactly the same as setup-config command in qoxigraph,
    just with a different Docker image name
    """

    IMAGE = "adfreiburg/qjena"

    @staticmethod
    def construct_engine_specific_params(args) -> dict[str, dict[str, str]]:
        index_memory = args.total_index_memory
        server_memory = args.total_server_memory
        threads = 2
        try:
            threads_cmd = (
                "echo $(( $(lscpu | grep \"Core(s) per socket:\" | "
                "awk '{print $NF}') * $(lscpu | grep \"Socket(s):\" | "
                "awk '{print $NF}') - 1 ))"
            )
            threads = run_command(threads_cmd, return_output=True)
        except Exception as e:
            log.warning(
                "Threads count could not be automatically read from the system. "
                f"Setting it to default = 2. The error: {e}"
            )
        return {
            "index": {
                "JVM_ARGS": f"-Xms{index_memory} -Xmx{index_memory}",
                "THREADS": threads,
            },
            "server": {
                "JVM_ARGS": f"-Xms{server_memory} -Xmx{server_memory}",
                "TIMEOUT": "60s",
            },
        }

    def additional_arguments(self, subparser) -> None:
        super().additional_arguments(subparser)
        add_memory_options(subparser)
