from __future__ import annotations

import csv
import json
import re
import shlex
import subprocess
import time
import traceback
from io import StringIO
from pathlib import Path
from typing import Any

import rdflib
import yaml
from termcolor import colored

from qlever import script_name
from qlever.command import QleverCommand
from qlever.commands.clear_cache import ClearCacheCommand
from qlever.commands.ui import dict_to_yaml
from qlever.log import log, mute_log
from qlever.util import run_command, run_curl_command


class BenchmarkQueriesCommand(QleverCommand):
    """
    Class for running a given sequence of benchmark or example queries and
    showing their processing times and result sizes.
    """

    def __init__(self):
        self.benchmark_title = None
        self.benchmark_description = None

    def description(self) -> str:
        return (
            "Run the given benchmark or example queries and show their "
            "processing times and result sizes. Optionally, store the "
            "benchmark results in a YML file."
        )

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "server": ["host_name", "port", "timeout"],
            "ui": ["ui_config"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint", type=str, help="URL of the SPARQL endpoint"
        )
        subparser.add_argument(
            "--sparql-endpoint-preset",
            choices=[
                "https://qlever.dev/api/wikidata",
                "https://qlever.dev/api/uniprot",
                "https://qlever.dev/api/pubchem",
                "https://qlever.dev/api/osm-planet",
                "https://wikidata.demo.openlinksw.com/sparql",
                "https://sparql.uniprot.org/sparql",
            ],
            help="SPARQL endpoint from fixed list (to save typing)",
        )
        subparser.add_argument(
            "--queries-tsv",
            type=str,
            default=None,
            help=(
                "Path to a TSV file containing the benchmark queries "
                "(short_query_name, full_sparql_query)"
            ),
        )
        subparser.add_argument(
            "--queries-yml",
            type=str,
            default=None,
            help=(
                "Path to a YML file containing the benchmark queries. "
                "The YAML must follow this structure ->"
                "title: <benchmark title (str)>, "
                "description: <benchmark description (str)>, "
                "queries: <list[query]> where each query contains: "
                "name: <short query name (mandatory)>, "
                "description <query description (optional)>, "
                "query: <full sparql query (mandatory)>"
            ),
        )
        subparser.add_argument(
            "--query-ids",
            type=str,
            default="1-$",
            help="Query IDs as comma-separated list of "
            "ranges (e.g., 1-5,7,12-$)",
        )
        subparser.add_argument(
            "--query-regex",
            type=str,
            help="Only consider example queries matching "
            "this regex (using grep -Pi)",
        )
        subparser.add_argument(
            "--example-queries",
            action="store_true",
            default=False,
            help=(
                "Run the example queries for the given --ui-config "
                "instead of the benchmark queries from a TSV or YML file"
            ),
        )
        subparser.add_argument(
            "--download-or-count",
            choices=["download", "count"],
            default="download",
            help="Whether to download the full result "
            "or just compute the size of the result",
        )
        subparser.add_argument(
            "--limit", type=int, help="Limit on the number of results"
        )
        subparser.add_argument(
            "--remove-offset-and-limit",
            action="store_true",
            default=False,
            help="Remove OFFSET and LIMIT from the query",
        )
        subparser.add_argument(
            "--accept",
            type=str,
            choices=[
                "text/tab-separated-values",
                "text/csv",
                "application/sparql-results+json",
                "application/qlever-results+json",
                "application/octet-stream",
                "text/turtle",
                "AUTO",
            ],
            default="application/sparql-results+json",
            help="Accept header for the SPARQL query; AUTO means "
            "`text/turtle` for CONSTRUCT AND DESCRIBE queries, "
            "`application/sparql-results+json` for all others",
        )
        subparser.add_argument(
            "--clear-cache",
            choices=["yes", "no"],
            default="no",
            help="Clear the cache before each query (only works for QLever)",
        )
        subparser.add_argument(
            "--width-query-name",
            type=int,
            default=70,
            help="Width for printing the query name",
        )
        subparser.add_argument(
            "--width-error-message",
            type=int,
            default=50,
            help="Width for printing the error message (0 = no limit)",
        )
        subparser.add_argument(
            "--width-result-size",
            type=int,
            default=14,
            help="Width for printing the result size",
        )
        subparser.add_argument(
            "--add-query-type-to-description",
            action="store_true",
            default=False,
            help="Add the query type (SELECT, ASK, CONSTRUCT, DESCRIBE, "
            "UNKNOWN) to the query description",
        )
        subparser.add_argument(
            "--show-query",
            choices=["always", "never", "on-error"],
            default="never",
            help="Show the queries that will be executed (always, never, on error)",
        )
        subparser.add_argument(
            "--show-prefixes",
            action="store_true",
            default=False,
            help="When showing the query, also show the prefixes",
        )
        subparser.add_argument(
            "--results-dir",
            type=str,
            default=".",
            help=(
                "The directory where the YML result file would be saved "
                "for the evaluation web app (Default = current working directory)"
            ),
        )
        subparser.add_argument(
            "--result-file",
            type=str,
            default=None,
            help=(
                "Base name used for the result YML file, should be of the "
                "form `<dataset>.<engine>`, e.g., `wikidata.qlever`"
            ),
        )
        subparser.add_argument(
            "--max-results-output-file",
            type=int,
            default=5,
            help=(
                "Maximum number of results per query in the output result "
                "YML file (Default = 5)"
            ),
        )
        subparser.add_argument(
            "--title",
            type=str,
            default=None,
            help=(
                "Benchmark title to be saved in result YML file (This will "
                "override the 'title' field in --queries-yml file). This benchmark "
                "title would be displayed as header title when comparing engines "
                "on the evaluation web app for the given dataset. Only relevant "
                "when --result-file argument is passed."
            ),
        )
        subparser.add_argument(
            "--description",
            type=str,
            default=None,
            help=(
                "Benchmark description to be saved in result YML file. (This "
                "will override the 'description' field in --queries-yml file) "
                " This benchmark description would be displayed as additional "
                "help text on the evaluation web app for the given dataset. "
                "Only relevant when --result-file argument is passed."
            ),
        )

    def pretty_printed_query(self, query: str, show_prefixes: bool) -> str:
        remove_prefixes_cmd = (
            " | sed '/^PREFIX /Id'" if not show_prefixes else ""
        )
        pretty_print_query_cmd = (
            f"echo {shlex.quote(query)}"
            f" | docker run -i --rm sparqling/sparql-formatter"
            f"{remove_prefixes_cmd} | grep -v '^$'"
        )
        try:
            query_pretty_printed = run_command(
                pretty_print_query_cmd, return_output=True
            )
            return query_pretty_printed.rstrip()
        except Exception as e:
            log.error(
                f"Failed to pretty-print query, returning original query: {e}"
            )
            return query.rstrip()

    def sparql_query_type(self, query: str) -> str:
        match = re.search(
            r"(SELECT|ASK|CONSTRUCT|DESCRIBE)\s", query, re.IGNORECASE
        )
        if match:
            return match.group(1).upper()
        else:
            return "UNKNOWN"

    @staticmethod
    def filter_queries(
        queries: list[tuple[str, str, str]], query_ids: str, query_regex: str
    ) -> list[tuple[str, str, str]]:
        """
        Given a list of queries (tuple of query desc and full sparql query),
        filter them and keep the ones which are a part of query_ids
        or match with query_regex
        """
        # Get the list of query indices to keep
        total_queries = len(queries)
        query_indices = []
        for part in query_ids.split(","):
            if "-" in part:
                start, end = part.split("-")
                if end == "$":
                    end = total_queries
                query_indices.extend(range(int(start) - 1, int(end)))
            else:
                idx = int(part) if part != "$" else total_queries
                query_indices.append(idx - 1)

        try:
            filtered_queries = []
            pattern = (
                re.compile(query_regex, re.IGNORECASE) if query_regex else None
            )
            for query_idx in query_indices:
                if query_idx >= total_queries:
                    continue

                name, description, query = queries[query_idx]

                # Only include queries that match the query_regex if present
                if pattern and not (
                    pattern.search(name)
                    or pattern.search(description)
                    or pattern.search(query)
                ):
                    continue

                filtered_queries.append((name, description, query))
            return filtered_queries
        except Exception as exc:
            log.error(f"Error filtering queries: {exc}")
            return []

    @staticmethod
    def parse_queries_tsv(queries_cmd: str) -> list[tuple[str, str, str]]:
        """
        Execute the given bash command to fetch tsv queries and return a
        list of queries i.e. tuple(query_name, "", full_sparql_query)
        Note: query_description is returned as empty to match the return
        structure of parse_queries_yml and for backward compatibility
        """
        try:
            tsv_queries_str = run_command(queries_cmd, return_output=True)
            if len(tsv_queries_str) == 0:
                log.error("No queries found in the TSV queries file")
                return []
            return [
                (query_name, "", sparql_query)
                for line in tsv_queries_str.strip().splitlines()
                for query_name, sparql_query in [line.split("\t", 1)]
            ]
        except Exception as exc:
            log.error(f"Failed to read the TSV queries file: {exc}")
            return []

    def parse_queries_yml(
        self, queries_file: str
    ) -> list[tuple[str, str, str]]:
        """
        Parse a YML file, validate its structure and return a list of
        queries i.e. tuple(query_name, query_description, full_sparql_query)
        """
        with open(queries_file, "r", encoding="utf-8") as q_file:
            try:
                data = yaml.safe_load(q_file)  # Load YAML safely
            except yaml.YAMLError as exc:
                log.error(f"Error parsing {queries_file} file: {exc}")
                return []

        # Validate the structure
        if not isinstance(data, dict) or "queries" not in data:
            log.error(
                "Error: YAML file must contain a top-level 'queries' key"
            )
            return []

        if not isinstance(data["queries"], list):
            log.error("Error: 'queries' key in YML file must hold a list.")
            return []

        if title := data.get("title"):
            self.benchmark_title = title
        if description := data.get("description"):
            self.benchmark_description = description

        queries = []
        for query in data["queries"]:
            if (
                not isinstance(query, dict)
                or "query" not in query
                or "name" not in query
            ):
                log.error(
                    "Error: Each item in 'queries' must contain "
                    "'name' and 'query' keys."
                )
                return []
            queries.append(
                (query["name"], query.get("description") or "", query["query"])
            )
        return queries

    def get_result_size(
        self,
        count_only: bool,
        query_type: str,
        accept_header: str,
        result_file: str,
    ) -> tuple[int, dict[str, str] | None]:
        """
        Get the result size and error_msg dict (if query failed) for
        different accept headers
        """

        def get_json_error_msg(e: Exception) -> dict[str, str]:
            error_msg = {
                "short": "Malformed JSON",
                "long": "curl returned with code 200, "
                "but the JSON is malformed: " + re.sub(r"\s+", " ", str(e)),
            }
            return error_msg

        result_size = 0
        error_msg = None
        # CASE 0: The result is empty despite a 200 HTTP code (not a
        # problem for CONSTRUCT and DESCRIBE queries).
        if Path(result_file).stat().st_size == 0 and (
            not query_type == "CONSTRUCT" and not query_type == "DESCRIBE"
        ):
            result_size = 0
            error_msg = {
                "short": "Empty result",
                "long": "curl returned with code 200, but the result is empty",
            }

        # CASE 1: Just counting the size of the result (TSV or JSON).
        elif count_only:
            if accept_header in ("text/tab-separated-values", "text/csv"):
                result_size = run_command(
                    f"sed 1d {result_file}", return_output=True
                )
            elif accept_header == "application/qlever-results+json":
                try:
                    # sed cmd to get the number between 2nd and 3rd double_quotes
                    result_size = run_command(
                        f"jq '.res[0]' {result_file}"
                        " | sed 's/[^0-9]*\\([0-9]*\\).*/\\1/'",
                        return_output=True,
                    )
                except Exception as e:
                    error_msg = get_json_error_msg(e)
            else:
                try:
                    result_size = run_command(
                        f'jq -r ".results.bindings[0]'
                        f" | to_entries[0].value.value"
                        f' | tonumber" {result_file}',
                        return_output=True,
                    )
                except Exception as e:
                    error_msg = get_json_error_msg(e)

        # CASE 2: Downloading the full result (TSV, CSV, Turtle, JSON).
        else:
            if accept_header in ("text/tab-separated-values", "text/csv"):
                result_size = run_command(
                    f"sed 1d {result_file} | wc -l", return_output=True
                )
            elif accept_header == "text/turtle":
                result_size = run_command(
                    f"sed '1d;/^@prefix/d;/^\\s*$/d' {result_file} | wc -l",
                    return_output=True,
                )
            elif accept_header == "application/qlever-results+json":
                try:
                    result_size = run_command(
                        f'jq -r ".resultsize" {result_file}',
                        return_output=True,
                    )
                except Exception as e:
                    error_msg = get_json_error_msg(e)
            else:
                try:
                    result_size = int(
                        run_command(
                            f'jq -r ".results.bindings | length"'
                            f" {result_file}",
                            return_output=True,
                        ).rstrip()
                    )
                except Exception as e:
                    error_msg = get_json_error_msg(e)
        return int(result_size), error_msg

    @staticmethod
    def get_single_int_result(result_file: str) -> int | None:
        """
        When downloading the full result of a query with accept header as
        application/sparql-results+json and result_size == 1, get the single
        integer result value (if any).
        """
        single_int_result = None
        try:
            single_int_result = int(
                run_command(
                    f'jq -e -r ".results.bindings[0][] | .value"'
                    f" {result_file}",
                    return_output=True,
                ).rstrip()
            )
        except Exception:
            pass
        return single_int_result

    def execute(self, args) -> bool:
        # We can't have both `--remove-offset-and-limit` and `--limit`.
        if args.remove_offset_and_limit and args.limit:
            log.error("Cannot have both --remove-offset-and-limit and --limit")
            return False

        # Extract dataset and sparql_engine name from result file
        dataset = engine = None
        if args.result_file is not None:
            result_file_parts = args.result_file.split(".")
            if len(result_file_parts) != 2:
                log.error(
                    "The argument of --result-file should be of the form "
                    "`<dataset>.<engine>`, e.g., `wikidata.qlever`"
                )
                return False
            results_dir_path = Path(args.results_dir)
            if results_dir_path.exists():
                if not results_dir_path.is_dir():
                    log.error(
                        f"{results_dir_path} exists but is not a directory"
                    )
                    return False
            else:
                log.info(
                    f"Creating results directory: {results_dir_path.absolute()}"
                )
                results_dir_path.mkdir(parents=True, exist_ok=True)
            dataset, engine = result_file_parts
            self.benchmark_title = (
                f"Performance Evaluation for {dataset.capitalize()}"
            )
            self.benchmark_description = (
                f"{dataset.capitalize()} benchmark ran using {script_name} "
                "benchmark-queries"
            )

        # If `args.accept` is `application/sparql-results+json` or
        # `application/qlever-results+json` or `AUTO`, we need `jq`.
        if args.accept in (
            "application/sparql-results+json",
            "application/qlever-results+json",
            "AUTO",
        ):
            try:
                subprocess.run(
                    "jq --version",
                    shell=True,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except Exception as e:
                log.error(f"Please install `jq` for {args.accept} ({e})")
                return False

        if not any((args.queries_tsv, args.queries_yml, args.example_queries)):
            log.error(
                "No benchmark or example queries to read! Either pass benchmark "
                "queries using --queries-tsv or --queries-yml, or pass the "
                "argument --example-queries to run example queries for the "
                f"given ui_config {args.ui_config}"
            )
            return False

        if all((args.queries_tsv, args.queries_yml)):
            log.error("Cannot have both --queries-tsv and --queries-yml")
            return False

        if any((args.queries_tsv, args.queries_yml)) and args.example_queries:
            queries_file_arg = "tsv" if args.queries_tsv else "yml"
            log.error(
                f"Cannot have both --queries-{queries_file_arg} and "
                "--example-queries"
            )
            return False

        # Handle shortcuts for SPARQL endpoint.
        if args.sparql_endpoint_preset:
            args.sparql_endpoint = args.sparql_endpoint_preset

        # Limit only works with full result.
        if args.limit and args.download_or_count == "count":
            log.error("Limit only works with full result")
            return False

        # Clear cache only works for QLever.
        is_qlever = (
            not args.sparql_endpoint
            or args.sparql_endpoint.startswith("https://qlever")
        )
        if engine is not None:
            is_qlever = is_qlever or "qlever" in engine.lower()
        if args.clear_cache == "yes":
            if is_qlever:
                log.warning(
                    "Clearing the cache before each query"
                    " (only works for QLever)"
                )
            else:
                log.warning(
                    "Clearing the cache only works for QLever"
                    ", option `--clear-cache` is ignored"
                )
                args.clear_cache = "no"

        # Show what the command will do.
        example_queries_cmd = (
            "curl -sv https://qlever.cs.uni-freiburg.de/"
            f"api/examples/{args.ui_config}"
        )
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )

        self.show(
            f"Obtain queries via: {args.queries_yml or args.queries_tsv or example_queries_cmd}\n"
            f"SPARQL endpoint: {sparql_endpoint}\n"
            f"Accept header: {args.accept}\n"
            f"Download result for each query or just count:"
            f" {args.download_or_count.upper()}"
            + (f" with LIMIT {args.limit}" if args.limit else ""),
            only_show=args.show,
        )
        if args.show:
            return True

        if args.queries_yml:
            queries = self.parse_queries_yml(args.queries_yml)
        elif args.queries_tsv:
            queries = self.parse_queries_tsv(f"cat {args.queries_tsv}")
        else:
            queries = self.parse_queries_tsv(example_queries_cmd)

        filtered_queries = self.filter_queries(
            queries, args.query_ids, args.query_regex
        )

        if len(filtered_queries) == 0 or not filtered_queries[0]:
            log.error("No queries to process!")
            return False

        # We want the width of the query description to be an uneven number (in
        # case we have to truncated it, in which case we want to have a " ... "
        # in the middle).
        width_query_name_half = args.width_query_name // 2
        width_query_name = 2 * width_query_name_half + 1

        try:
            timeout = int(args.timeout[:-1])
        except ValueError:
            timeout = None

        # Launch the queries one after the other and for each print: the
        # description, the result size (number of rows), and the query
        # processing time (seconds).
        query_times = []
        result_sizes = []
        if args.result_file:
            result_yml_query_records = {
                "queries": [],
                "title": self.benchmark_title,
                "description": self.benchmark_description,
            }
            if timeout:
                result_yml_query_records["timeout"] = timeout
            # Override the title and description if provided as args
            if args.title:
                result_yml_query_records["title"] = args.title
            if args.description:
                result_yml_query_records["description"] = args.description

        num_failed = 0
        for name, description, query in filtered_queries:
            if len(query) == 0:
                log.error(
                    "Could not parse name, description and query, line is:"
                )
                log.info("")
                log.info(f"{name}\t{description}\t{query}")
                return False
            query_type = self.sparql_query_type(query)
            if args.add_query_type_to_description or args.accept == "AUTO":
                # If no query description, use name and append query type to it
                description = f"{description or name} [{query_type}]"

            # Clear the cache.
            if args.clear_cache == "yes":
                args.server_url = sparql_endpoint
                args.complete = False
                clear_cache_successful = False
                with mute_log():
                    clear_cache_successful = ClearCacheCommand().execute(args)
                if not clear_cache_successful:
                    log.warn("Failed to clear the cache")

            # Remove OFFSET and LIMIT (after the last closing bracket).
            if args.remove_offset_and_limit or args.limit:
                closing_bracket_idx = query.rfind("}")
                regexes = [
                    re.compile(r"OFFSET\s+\d+\s*", re.IGNORECASE),
                    re.compile(r"LIMIT\s+\d+\s*", re.IGNORECASE),
                ]
                for regex in regexes:
                    match = re.search(regex, query[closing_bracket_idx:])
                    if match:
                        query = (
                            query[: closing_bracket_idx + match.start()]
                            + query[closing_bracket_idx + match.end() :]
                        )

            # Limit query.
            if args.limit:
                query += f" LIMIT {args.limit}"

            # Count query.
            if args.download_or_count == "count":
                # First find out if there is a FROM clause.
                regex_from_clause = re.compile(
                    r"\s*FROM\s+<[^>]+>\s*", re.IGNORECASE
                )
                match_from_clause = re.search(regex_from_clause, query)
                from_clause = " "
                if match_from_clause:
                    from_clause = match_from_clause.group(0)
                    query = (
                        query[: match_from_clause.start()]
                        + " "
                        + query[match_from_clause.end() :]
                    )
                # Now we can add the outer SELECT COUNT(*).
                query = (
                    re.sub(
                        r"SELECT ",
                        "SELECT (COUNT(*) AS ?qlever_count_)"
                        + from_clause
                        + "WHERE { SELECT ",
                        query,
                        count=1,
                        flags=re.IGNORECASE,
                    )
                    + " }"
                )

            # A bit of pretty-printing.
            query = re.sub(r"\s+", " ", query)
            query = re.sub(r"\s*\.\s*\}", " }", query)
            if args.show_query == "always":
                log.info("")
                log.info(
                    colored(
                        self.pretty_printed_query(query, args.show_prefixes),
                        "cyan",
                    )
                )

            # Accept header. For "AUTO", use `text/turtle` for CONSTRUCT
            # queries and `application/sparql-results+json` for all others.
            accept_header = args.accept
            if accept_header == "AUTO":
                if query_type == "CONSTRUCT" or query_type == "DESCRIBE":
                    accept_header = "text/turtle"
                else:
                    accept_header = "application/sparql-results+json"

            # Launch query.
            curl_cmd = (
                f"curl -Ls {sparql_endpoint}"
                f' -w "HTTP code: %{{http_code}}\\n"'
                f' -H "Accept: {accept_header}"'
                f" --data-urlencode query={shlex.quote(query)}"
            )
            log.debug(curl_cmd)
            result_file = (
                f"qlever.example_queries.result.{abs(hash(curl_cmd))}.tmp"
            )
            start_time = time.time()
            try:
                http_code = run_curl_command(
                    sparql_endpoint,
                    headers={"Accept": accept_header},
                    params={"query": query},
                    result_file=result_file,
                ).strip()
                if http_code == "200":
                    time_seconds = time.time() - start_time
                    error_msg = None
                else:
                    time_seconds = time.time() - start_time
                    error_msg = {
                        "short": f"HTTP code: {http_code}",
                        "long": re.sub(
                            r"\s+", " ", Path(result_file).read_text()
                        ),
                    }
            except Exception as e:
                time_seconds = time.time() - start_time
                if args.log_level == "DEBUG":
                    traceback.print_exc()
                error_msg = {
                    "short": "Exception",
                    "long": re.sub(r"\s+", " ", str(e)),
                }

            # Get result size (via the command line, in order to avoid loading
            # a potentially large JSON file into Python, which is slow).
            if error_msg is None:
                result_size, error_msg = self.get_result_size(
                    args.download_or_count == "count",
                    query_type,
                    accept_header,
                    result_file,
                )
                single_int_result = None
                if (
                    result_size == 1
                    and accept_header == "application/sparql-results+json"
                    and args.download_or_count == "download"
                ):
                    single_int_result = self.get_single_int_result(result_file)

            # Get the result yaml record if output file needs to be generated
            if args.result_file is not None:
                result_length = None if error_msg is not None else 1
                result_length = (
                    result_size
                    if args.download_or_count == "download"
                    and result_length is not None
                    else result_length
                )
                query_results = (
                    error_msg if error_msg is not None else result_file
                )
                query_record = self.get_result_yml_query_record(
                    name=name,
                    description=description,
                    query=self.pretty_printed_query(query, args.show_prefixes),
                    client_time=time_seconds,
                    result=query_results,
                    result_size=result_length,
                    max_result_size=args.max_results_output_file,
                    accept_header=accept_header,
                )
                result_yml_query_records["queries"].append(query_record)

            # Print name, time, result in tabular form.
            if len(name) > width_query_name:
                name = (
                    name[: width_query_name_half - 2]
                    + " ... "
                    + name[-width_query_name_half + 2 :]
                )
            if error_msg is None:
                result_size = int(result_size)
                single_int_result = (
                    f"   [single int result: {single_int_result:,}]"
                    if single_int_result is not None
                    else ""
                )
                log.info(
                    f"{name:<{width_query_name}}  "
                    f"{time_seconds:6.2f} s  "
                    f"{result_size:>{args.width_result_size},}"
                    f"{single_int_result}"
                )
                query_times.append(time_seconds)
                result_sizes.append(result_size)
            else:
                num_failed += 1
                if (
                    args.width_error_message > 0
                    and len(error_msg["long"]) > args.width_error_message
                    and args.log_level != "DEBUG"
                    and args.show_query != "on-error"
                ):
                    error_msg["long"] = (
                        error_msg["long"][: args.width_error_message - 3]
                        + "..."
                    )
                seperator_short_long = (
                    "\n" if args.show_query == "on-error" else "  "
                )
                log.info(
                    f"{name:<{width_query_name}}    "
                    f"{colored('FAILED   ', 'red')}"
                    f"{colored(error_msg['short'], 'red'):>{args.width_result_size}}"
                    f"{seperator_short_long}"
                    f"{colored(error_msg['long'], 'red')}"
                )
                if args.show_query == "on-error":
                    log.info(
                        colored(
                            self.pretty_printed_query(
                                query, args.show_prefixes
                            ),
                            "cyan",
                        )
                    )
                    log.info("")

            # Remove the result file (unless in debug mode).
            if args.log_level != "DEBUG":
                Path(result_file).unlink(missing_ok=True)

        # Check that each query has a time and a result size, or it failed.
        assert len(result_sizes) == len(query_times)
        assert len(query_times) + num_failed == len(filtered_queries)

        if args.result_file:
            if len(result_yml_query_records["queries"]) != 0:
                outfile_name = f"{dataset}.{engine}.results.yaml"
                outfile = Path(args.results_dir) / outfile_name
                self.write_query_records_to_result_file(
                    query_data=result_yml_query_records,
                    out_file=outfile,
                )
            else:
                log.error(
                    f"Nothing to write to output result YML file: {args.result_file}"
                )

        # Show statistics.
        if len(query_times) > 0:
            n = len(query_times)
            total_query_time = sum(query_times)
            average_query_time = total_query_time / n
            median_query_time = sorted(query_times)[n // 2]
            total_result_size = sum(result_sizes)
            average_result_size = round(total_result_size / n)
            median_result_size = sorted(result_sizes)[n // 2]
            query_or_queries = "query" if n == 1 else "queries"
            description = f"TOTAL   for {n} {query_or_queries}"
            log.info("")
            log.info(
                f"{description:<{width_query_name}}  "
                f"{total_query_time:6.2f} s  "
                f"{total_result_size:>14,}"
            )
            description = f"AVERAGE for {n} {query_or_queries}"
            log.info(
                f"{description:<{width_query_name}}  "
                f"{average_query_time:6.2f} s  "
                f"{average_result_size:>14,}"
            )
            description = f"MEDIAN  for {n} {query_or_queries}"
            log.info(
                f"{description:<{width_query_name}}  "
                f"{median_query_time:6.2f} s  "
                f"{median_result_size:>14,}"
            )

        # Show number of failed queries.
        if num_failed > 0:
            log.info("")
            description = "Number of FAILED queries"
            num_failed_string = f"{num_failed:>6}"
            if num_failed == len(filtered_queries):
                num_failed_string += "  [all]"
            log.info(
                colored(
                    f"{description:<{width_query_name}}  {num_failed:>24}",
                    "red",
                )
            )

        # Return success (has nothing to do with how many queries failed).
        return True

    def get_result_yml_query_record(
        self,
        name: str,
        description: str,
        query: str,
        client_time: float,
        result: str | dict[str, str],
        result_size: int | None,
        max_result_size: int,
        accept_header: str,
    ) -> dict[str, Any]:
        """
        Construct a dictionary with query information for output result yaml file
        """
        record = {
            "name": name,
            "description": description,
            "query": query,
            "runtime_info": {},
        }
        if result_size is None:
            results = f"{result['short']}: {result['long']}"
            headers = []
        else:
            record["result_size"] = result_size
            result_size = (
                max_result_size
                if result_size > max_result_size
                else result_size
            )
            headers, results = self.get_query_results(
                result, result_size, accept_header
            )
            if accept_header == "application/qlever-results+json":
                runtime_info_cmd = (
                    f"jq 'if .runtimeInformation then"
                    f" .runtimeInformation else"
                    f' "null" end\' {result}'
                )
                runtime_info_str = run_command(
                    runtime_info_cmd, return_output=True
                )
                if runtime_info_str != "null":
                    record["runtime_info"] = json.loads(runtime_info_str)
        record["runtime_info"]["client_time"] = client_time
        record["headers"] = headers
        record["results"] = results
        return record

    def get_query_results(
        self, result_file: str, result_size: int, accept_header: str
    ) -> tuple[list[str], list[list[str]]]:
        """
        Return headers and query results as a tuple for various accept headers
        """
        if accept_header in ("text/tab-separated-values", "text/csv"):
            separator = "," if accept_header == "text/csv" else "\t"
            get_result_cmd = f"sed -n '1,{result_size + 1}p' {result_file}"
            results_str = run_command(get_result_cmd, return_output=True)
            results = results_str.splitlines()
            reader = csv.reader(StringIO(results_str), delimiter=separator)
            headers = next(reader)
            results = [row for row in reader]
            return headers, results

        elif accept_header == "application/qlever-results+json":
            get_result_cmd = (
                f"jq '{{headers: .selected, results: .res[0:{result_size}]}}' "
                f"{result_file}"
            )
            results_str = run_command(get_result_cmd, return_output=True)
            results_json = json.loads(results_str)
            return results_json["headers"], results_json["results"]

        elif accept_header == "application/sparql-results+json":
            get_result_cmd = (
                f"jq '{{headers: .head.vars, "
                f"bindings: .results.bindings[0:{result_size}]}}' "
                f"{result_file}"
            )
            results_str = run_command(get_result_cmd, return_output=True)
            results_json = json.loads(results_str)
            results = []
            bindings = results_json.get("bindings", [])
            for binding in bindings:
                result = []
                if not binding or not isinstance(binding, dict):
                    results.append([])
                    continue
                for obj in binding.values():
                    value = '"' + obj["value"] + '"'
                    if obj["type"] == "uri":
                        value = "<" + value.strip('"') + ">"
                    elif "datatype" in obj:
                        value += "^^<" + obj["datatype"] + ">"
                    elif "xml:lang" in obj:
                        value += "@" + obj["xml:lang"]
                    result.append(value)
                results.append(result)
            return results_json["headers"], results

        else:  # text/turtle
            graph = rdflib.Graph()
            graph.parse(result_file, format="turtle")
            headers = ["?subject", "?predicate", "?object"]
            results = []
            for i, (s, p, o) in enumerate(graph):
                if i >= result_size:
                    break
                results.append([str(s), str(p), str(o)])
            return headers, results

    @staticmethod
    def write_query_records_to_result_file(
        query_data: dict[str, list[dict[str, Any]]], out_file: Path
    ) -> None:
        """
        Write yaml record for all queries to output yaml file
        """
        config_yaml = dict_to_yaml(query_data)
        with open(out_file, "w") as eval_yaml_file:
            eval_yaml_file.write(config_yaml)
            log.info("")
            log.info(
                f"Generated result yaml file: {out_file.stem}{out_file.suffix} "
                f"in the directory {out_file.parent.resolve()}"
            )
