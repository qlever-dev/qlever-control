from __future__ import annotations

import math

from qlever.util import add_memory_options
from qoxigraph.commands.setup_config import (
    SetupConfigCommand as QoxigraphSetupConfigCommand,
)


class SetupConfigCommand(QoxigraphSetupConfigCommand):
    """
    Create a Qleverfile for MillenniumDB from a dataset template. Extends
    the base setup-config with MillenniumDB-specific memory budget options
    (--total-index-memory, --total-server-memory) that auto-generate
    sensible buffer size defaults for indexing and serving.
    """

    IMAGE = "adfreiburg/millenniumdb"

    FILTER_CRITERIA = QoxigraphSetupConfigCommand.FILTER_CRITERIA
    FILTER_CRITERIA["index"].append("CAT_INPUT_FILES")

    @staticmethod
    def construct_engine_specific_params(args) -> dict[str, dict[str, str]]:
        """
        Derive MillenniumDB-specific Qleverfile parameters from the memory
        budget. Splits index memory evenly between tensor and string buffers.
        For server memory >4G, allocates versioned, unversioned, and string
        buffers using a log-based heuristic.
        """
        index_memory = int(args.total_index_memory[:-1])
        server_memory = int(args.total_server_memory[:-1])

        mdb_index_config = {}
        if index_memory >= 2:
            buffer_tensors = index_memory // 2
            mdb_index_config["BUFFER_TENSORS"] = f"{buffer_tensors}G"
            mdb_index_config["BUFFER_STRINGS"] = (
                f"{index_memory - buffer_tensors}G"
            )

        mdb_server_config = {
            "TIMEOUT": "60s",
            "THREADS": "2",
        }

        if server_memory > 4:
            unversioned_buffer = 1 if server_memory < 32 else 2
            strings_buffer = max(1, int(math.log2(server_memory)) - 1)
            versioned_buffer = (
                server_memory - unversioned_buffer - (2 * strings_buffer)
            )

            mdb_server_config["VERSIONED_BUFFER"] = f"{versioned_buffer}G"
            mdb_server_config["UNVERSIONED_BUFFER"] = f"{unversioned_buffer}G"
            mdb_server_config["STRINGS_STATIC"] = f"{strings_buffer}G"
            mdb_server_config["STRINGS_DYNAMIC"] = f"{strings_buffer}G"

        final_config = {}
        if mdb_index_config:
            final_config["index"] = mdb_index_config
        final_config["server"] = mdb_server_config

        return final_config

    def additional_arguments(self, subparser) -> None:
        super().additional_arguments(subparser)
        add_memory_options(subparser)
