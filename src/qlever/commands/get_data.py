from __future__ import annotations

import shlex

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import get_total_file_size, run_command


class GetDataCommand(QleverCommand):
    """
    Class for executing the `get-data` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Get data using the GET_DATA_CMD in the Qleverfile"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"data": ["name", "get_data_cmd"], "index": ["input_files"]}

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        # Construct the command line and show it.
        self.show(args.get_data_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the command line.
        try:
            run_command(args.get_data_cmd, show_output=True)
        except Exception as e:
            log.error(f"Problem executing \"{args.get_data_cmd}\": {e}")
            return False

        # Show the total file size in GB and return.
        patterns = shlex.split(args.input_files)
        total_file_size = get_total_file_size(patterns)
        print(f"Download successful, total file size: "
              f"{total_file_size:,} bytes")
        return True
