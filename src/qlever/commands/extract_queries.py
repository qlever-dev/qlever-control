from __future__ import annotations

import re

from qlever.command import QleverCommand
from qlever.log import log


class ExtractQueriesCommand(QleverCommand):
    """
    Class for executing the `extract-queries` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Extract all SPARQL queries from the server log"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"data": ["name"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--description-base",
            type=str,
            default="Log extract",
            help="Base name for the query descriptions"
            " (default: `Log extract`)",
        )
        subparser.add_argument(
            "--log-file",
            type=str,
            help="Name of the log file to extract queries from"
            " (default: `<name>.server-log.txt`)",
        )
        subparser.add_argument(
            "--output-file",
            type=str,
            default="log-queries.txt",
            help="Output file for the extracted queries (default: `log-queries.txt`)",
        )

    def execute(self, args) -> bool:
        # Show what the command does.
        if args.log_file is not None:
            log_file_name = args.log_file
        else:
            log_file_name = f"{args.name}.server-log.txt"
        self.show(
            f"Extract SPARQL queries from `{log_file_name}`"
            f" and write them to `{args.output_file}`",
            only_show=args.show,
        )
        if args.show:
            return True

        # Regex for log entries of the form
        # 2025-01-14 04:47:44.950 - INFO
        log_line_regex = (
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) - [A-Z]+:"
        )

        # Read the log file line by line.
        log_file = open(log_file_name, "r")
        queries_file = open(args.output_file, "w")
        query = None
        description_base = args.description_base
        description_base_count = {}
        tsv_line_short_width = 150
        for line in log_file:
            # An "Alive check" message contains a tag, which we use as the base
            # name of the query description.
            alive_check_regex = r"Alive check with message \"(.*)\""
            match = re.search(alive_check_regex, line)
            if match:
                description_base = match.group(1)
                continue

            # A new query in the log.
            if "Processing the following SPARQL query" in line:
                query = []
                query_index = (
                    description_base_count.get(description_base, 0) + 1
                )
                description_base_count[description_base] = query_index
                continue
            # If we have started a query: extend until we meet the next log
            # line, then push the query. Remove comments.
            if query is not None:
                if not re.match(log_line_regex, line):
                    if not re.match(r"^\s*#", line):
                        line = re.sub(r" #.*", "", line)
                        query.append(line)
                else:
                    query = re.sub(r"\s+", " ", "\n".join(query)).strip()
                    description = f"{description_base}, Query #{query_index}"
                    tsv_line = f"{description}\t{query}"
                    tsv_line_short = (
                        tsv_line
                        if len(tsv_line) < tsv_line_short_width
                        else tsv_line[:tsv_line_short_width] + "..."
                    )
                    log.info(tsv_line_short)
                    print(tsv_line, file=queries_file)
                    query = None

        log_file.close()
        queries_file.close()
        return True
