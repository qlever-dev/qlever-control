from __future__ import annotations

import shlex
import time
import traceback

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


class UpdateCommand(QleverCommand):
    """
    Class for executing a SPARQL UPDATE against a SPARQL endpoint.

    The command accepts the update either directly on the command line or
    via a file path provided with --update-file.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Send an update to a SPARQL endpoint"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str,list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "update",
            type=str,
            nargs="?",
            default=None,
            help="SPARQL UPDATE to send (use --update-file to send from a file)",
        )
        subparser.add_argument(
            "--update-file",
            type=str,
            help="Path to a file containing the SPARQL UPDATE to send",
        )
        subparser.add_argument(
            "--sparql-endpoint", type=str, help="URL of the SPARQL endpoint"
        )

    def execute(self, args) -> bool:
        sparql_endpoint = (
            args.sparql_endpoint if args.sparql_endpoint else f"{args.host_name}:{args.port}"
        )

        curl_cmd = (
            f"curl -s {sparql_endpoint} -X POST "
            f"-H 'Authorization: Bearer {args.access_token}' "
            f"-H 'Content-Type: application/sparql-update' "
        )

        if args.update:
            curl_cmd += f"--data-binary {shlex.quote(args.update)}"
        elif args.update_file:
            curl_cmd += f"--data-binary @{shlex.quote(args.update_file)}"
        else:
            log.error("No SPARQL UPDATE provided. Pass it as an argument or via --update-file.")
            return False

        # Show and exit if requested
        self.show(curl_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute update
        try:
            start_time = time.time()
            run_command(curl_cmd)
            time_msecs = round(1000 * (time.time() - start_time))
            if args.log_level != "NO_LOG":
                log.info("")
                log.info(
                    f"Update processing time (end-to-end): {time_msecs:,d} ms"
                )
        except Exception as e:
            if args.log_level == "DEBUG":
                traceback.print_exc()
            log.error(e)
            return False

        return True

