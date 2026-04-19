from __future__ import annotations

import json
import random
import re
import shlex
import statistics
import subprocess
import threading
import time

import yaml

from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log


def parse_duration(s: str) -> float:
    """
    Parse a duration string like "10s", "34min", "2h", "1.5h", "90" (seconds
    by default) and return the duration in seconds.
    """
    s = s.strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(s|sec|min|m|h|hr)?", s)
    if not match:
        raise ValueError(f"Cannot parse duration: \"{s}\" (examples: "
                         f"\"10s\", \"34min\", \"2h\", \"90\")")
    value = float(match.group(1))
    unit = match.group(2) or "s"
    if unit in ("s", "sec"):
        return value
    elif unit in ("min", "m"):
        return value * 60
    elif unit in ("h", "hr"):
        return value * 3600
    else:
        raise ValueError(f"Unknown duration unit: \"{unit}\"")


def read_queries_tsv(path: str) -> list[str]:
    """Read queries from a TSV file (name<TAB>query per line)."""
    queries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t", 1)
            queries.append(parts[1].strip() if len(parts) == 2
                           else parts[0].strip())
    return queries


def read_queries_yml(path: str) -> list[str]:
    """Read queries from a YAML file (benchmark-queries format)."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict) or "queries" not in data:
        raise ValueError("YAML file must contain a top-level 'queries' key")
    if not isinstance(data["queries"], list):
        raise ValueError("'queries' key must hold a list")
    queries = []
    for entry in data["queries"]:
        if not isinstance(entry, dict) or "query" not in entry:
            raise ValueError("Each query entry must contain a 'query' key")
        queries.append(entry["query"].strip())
    return queries


def read_queries_jsonl(path: str) -> list[str]:
    """Read queries from a JSONL file (one JSON object per line with a
    'sparql' field)."""
    queries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                query = data.get("sparql")
                if query:
                    queries.append(query.strip())
            except (json.JSONDecodeError, AttributeError):
                continue
    return queries


class LoadTestCommand(QleverCommand):
    """
    Class for executing the `load-test` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return ("Send many concurrent queries to a SPARQL endpoint")

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--queries-tsv",
            type=str,
            default=None,
            help="Path to a TSV file with queries"
                 " (short_query_name<TAB>sparql_query)",
        )
        subparser.add_argument(
            "--queries-yml",
            type=str,
            default=None,
            help="Path to a YAML file with queries"
                 " (same format as for benchmark-queries)",
        )
        subparser.add_argument(
            "--queries-jsonl",
            type=str,
            default=None,
            help="Path to a JSONL file with queries"
                 " (one JSON object per line with a 'sparql' field)",
        )
        subparser.add_argument(
            "--query-selection",
            type=str,
            choices=["random", "cycle"],
            default="random",
            help="How to pick the next query (default: random)",
        )
        subparser.add_argument(
            "--request-rate",
            type=float,
            default=10.0,
            help="Number of queries per second (default: 10)",
        )
        subparser.add_argument(
            "--num-queries",
            type=int,
            default=1000,
            help="Total number of queries to send (default: 1000)",
        )
        subparser.add_argument(
            "--log-frequency",
            type=str,
            default="5s",
            help="How often to print a status line (default: 5s)",
        )
        subparser.add_argument(
            "--sparql-endpoint",
            type=str,
            help="URL of the SPARQL endpoint",
        )
        subparser.add_argument(
            "--show-error-messages",
            action="store_true",
            default=False,
            help="Show query and error message for failed queries",
        )
        subparser.add_argument(
            "--width-error-message",
            type=int,
            default=150,
            help="Truncate error messages to this width (default: 150)",
        )
        subparser.add_argument(
            "--show-queries",
            type=int,
            nargs="?",
            const=10,
            default=None,
            metavar="N",
            help="Show the first N queries (default: 10) and exit"
                 " without running the load test",
        )

    def execute(self, args) -> bool:
        # Parse log frequency.
        try:
            log_frequency_secs = parse_duration(args.log_frequency)
        except ValueError as e:
            log.error(e)
            return False

        # Exactly one of the three query sources must be specified.
        sources = {
            "--queries-tsv": args.queries_tsv,
            "--queries-yml": args.queries_yml,
            "--queries-jsonl": args.queries_jsonl,
        }
        specified = {k: v for k, v in sources.items() if v is not None}
        if len(specified) != 1:
            log.error("Please specify exactly one of --queries-tsv,"
                      " --queries-yml, or --queries-jsonl")
            return False

        # Read all queries into memory.
        source_option, source_path = next(iter(specified.items()))
        try:
            if source_option == "--queries-tsv":
                queries = read_queries_tsv(source_path)
            elif source_option == "--queries-yml":
                queries = read_queries_yml(source_path)
            else:
                queries = read_queries_jsonl(source_path)
        except Exception as e:
            log.error(f"Could not read queries: {e}")
            return False
        if not queries:
            log.error(f"No queries found in \"{source_path}\"")
            return False

        # Query selection helper.
        cycle_index = 0

        def next_query() -> str:
            nonlocal cycle_index
            if args.query_selection == "random":
                return random.choice(queries)
            else:
                query = queries[cycle_index % len(queries)]
                cycle_index += 1
                return query

        # If --show-queries is given, show queries and exit.
        if args.show_queries is not None:
            n = min(args.show_queries, len(queries))
            log.info(f"Showing {n} of {len(queries)} queries"
                     f" from \"{source_path}\":")
            log.info("")
            for i in range(n):
                query = next_query()
                log.info(f"Query {i + 1}:")
                log.info(colored(query, "magenta"))
                log.info("")
            return True

        # Determine the SPARQL endpoint.
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )

        # Show what the command will do.
        self.show(
            f"Send {args.num_queries} queries from \"{source_path}\""
            f" ({len(queries)} queries, {args.query_selection} selection)"
            f" to {sparql_endpoint} at {args.request_rate} queries/s"
            f" (log every {log_frequency_secs:.0f}s)",
            only_show=args.show,
        )
        if args.show:
            return True

        # Shared state for the worker threads.
        lock = threading.Lock()
        num_launched = 0
        num_done = 0
        num_errors = 0
        completed_times: list[float] = []
        status_codes: dict[str, int] = {}

        show_errors = args.show_error_messages
        max_error_width = args.width_error_message

        def send_query(query: str):
            nonlocal num_done, num_errors
            result_file = f"/tmp/qlever.load-test.{threading.get_ident()}"
            curl_cmd = (
                f"curl -s {sparql_endpoint}"
                f" -H \"Accept: application/qlever-results+json\""
                f" --data-urlencode query={shlex.quote(query)}"
                f" -o {result_file} -w \"%{{http_code}}\""
            )
            start = time.time()
            try:
                result = subprocess.run(
                    curl_cmd, shell=True, capture_output=True,
                    text=True, timeout=300,
                )
                elapsed = time.time() - start
                http_code = result.stdout.strip()
                with lock:
                    num_done += 1
                    completed_times.append(elapsed)
                    status_codes[http_code] = \
                        status_codes.get(http_code, 0) + 1
                    if http_code != "200":
                        num_errors += 1
                        if show_errors:
                            try:
                                with open(result_file) as f:
                                    body = f.read().strip()
                                msg = re.sub(r"\s+", " ", body)
                                if len(msg) > max_error_width:
                                    msg = msg[:max_error_width] + "..."
                            except Exception:
                                msg = "(could not read error response)"
                            log.info("")
                            log.info(colored(re.sub(r"\s+", " ", query),
                                             "magenta"))
                            log.error(f"HTTP {http_code}: {msg}")
            except Exception:
                elapsed = time.time() - start
                with lock:
                    num_done += 1
                    num_errors += 1
                    completed_times.append(elapsed)
                    status_codes["timeout"] = \
                        status_codes.get("timeout", 0) + 1
                    if show_errors:
                        log.info("")
                        log.info(colored(re.sub(r"\s+", " ", query),
                                         "magenta"))
                        log.error("Request timed out or failed")

        # Print status header and status lines.
        header = (
            f"{'Elapsed':>8s}  {'Launched':>8s}  {'Done':>8s}"
            f"  {'Running':>8s}  {'Errors':>8s}"
            f"  {'Median':>8s}  {'Mean':>8s}"
            f"  {'95p':>8s}  {'Max':>8s}"
        )
        separator = "  ".join(["--------"] * 9)

        def print_table_header():
            log.info(header)
            log.info(separator)

        log.info("")
        print_table_header()

        def print_status(elapsed: float):
            with lock:
                n_launched = num_launched
                n_done = num_done
                n_errors = num_errors
                times = list(completed_times)
            n_running = n_launched - n_done
            if times:
                sorted_times = sorted(times)
                median = statistics.median(sorted_times)
                mean = statistics.mean(sorted_times)
                p95_idx = int(len(sorted_times) * 0.95)
                p95 = sorted_times[min(p95_idx, len(sorted_times) - 1)]
                max_t = sorted_times[-1]
                time_stats = (
                    f"  {median:>7.2f}s  {mean:>7.2f}s"
                    f"  {p95:>7.2f}s  {max_t:>7.2f}s"
                )
            else:
                time_stats = (
                    f"  {'':>8s}  {'':>8s}"
                    f"  {'':>8s}  {'':>8s}"
                )
            log.info(
                f"{elapsed:>7.0f}s"
                f"  {n_launched:>8d}  {n_done:>8d}"
                f"  {n_running:>8d}  {n_errors:>8d}"
                f"{time_stats}"
            )

        def print_final_status():
            print_table_header()
            print_status(time.time() - start_time)

        # Main loop: launch queries at the specified rate.
        try:
            start_time = time.time()
            next_log_time = start_time + log_frequency_secs
            interval = 1.0 / args.request_rate
            next_launch_time = start_time + interval

            while num_launched < args.num_queries:
                now = time.time()
                elapsed = now - start_time

                # Launch queries that are due.
                while next_launch_time <= now \
                        and num_launched < args.num_queries:
                    query = next_query()
                    thread = threading.Thread(
                        target=send_query, args=(query,), daemon=True,
                    )
                    thread.start()
                    with lock:
                        num_launched += 1
                    next_launch_time += interval

                # Print status line if due.
                if now >= next_log_time:
                    print_status(elapsed)
                    next_log_time += log_frequency_secs

                # Sleep briefly to avoid busy waiting.
                sleep_until = min(next_launch_time, next_log_time)
                sleep_time = sleep_until - time.time()
                if sleep_time > 0:
                    time.sleep(min(sleep_time, 0.1))

            interrupted = False
        except KeyboardInterrupt:
            interrupted = True
            log.warning("\rCtrl+C pressed, stopping load test")

        # Print final status line. After Ctrl+C, reprint the header
        # since the warning message breaks the table flow. After normal
        # completion, just print a separator + status line.
        if interrupted:
            print_final_status()
        else:
            log.info(separator)
            print_status(time.time() - start_time)

        # Show HTTP status code histogram.
        http_status_names = {
            "200": "OK", "400": "Bad Request",
            "429": "Too Many Requests",
            "500": "Internal Server Error", "502": "Bad Gateway",
        }
        with lock:
            codes = dict(status_codes)
        if codes:
            parts = []
            for code, count in sorted(codes.items()):
                name = http_status_names.get(code)
                label = f"{code} ({name})" if name else code
                parts.append(f"{count} x {label}")
            log.info("")
            log.info(f"HTTP status codes: {', '.join(parts)}")

        return True
