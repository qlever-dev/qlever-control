from __future__ import annotations

import json
import os
import random
import re
import shlex
import statistics
import subprocess
import threading
import time

import yaml

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


def format_duration(seconds: float) -> str:
    """
    Format a duration in seconds as a human-readable string like "1h 23min"
    or "45s" or "2min 30s".
    """
    if seconds >= 3600:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}min" if m > 0 else f"{h}h"
    elif seconds >= 60:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}min {s}s" if s > 0 else f"{m}min"
    else:
        return f"{seconds:.0f}s"


class QuerySource:
    """
    Unified query source that supports three formats: TSV, YAML, and a
    directory of JSON files. For TSV and YAML, all queries are read into
    memory upfront. For a directory, only the file names are read upfront
    and individual queries are read on demand.
    """

    def __init__(self, queries: list[str] | None = None,
                 query_dir: str | None = None,
                 query_files: list[str] | None = None):
        self._queries = queries
        self._query_dir = query_dir
        self._query_files = query_files
        self._cycle_index = 0

    @staticmethod
    def from_tsv(path: str) -> QuerySource:
        queries = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t", 1)
                queries.append(parts[1].strip() if len(parts) == 2
                               else parts[0].strip())
        return QuerySource(queries=queries)

    @staticmethod
    def from_yml(path: str) -> QuerySource:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict) or "queries" not in data:
            raise ValueError("YAML file must contain a top-level"
                             " 'queries' key")
        if not isinstance(data["queries"], list):
            raise ValueError("'queries' key must hold a list")
        queries = []
        for entry in data["queries"]:
            if not isinstance(entry, dict) or "query" not in entry:
                raise ValueError("Each query entry must contain"
                                 " a 'query' key")
            queries.append(entry["query"].strip())
        return QuerySource(queries=queries)

    @staticmethod
    def from_dir(path: str) -> QuerySource:
        query_files = sorted(f for f in os.listdir(path)
                             if f.endswith(".json"))
        return QuerySource(query_dir=path, query_files=query_files)

    def __len__(self):
        if self._queries is not None:
            return len(self._queries)
        return len(self._query_files)

    @property
    def description(self):
        if self._query_dir is not None:
            return "JSON files"
        return "queries"

    def get_query(self, selection: str) -> str:
        """
        Pick the next query according to the selection strategy ("random"
        or "cycle") and return the SPARQL query string.
        """
        n = len(self)
        if selection == "random":
            idx = random.randint(0, n - 1)
        else:
            idx = self._cycle_index % n
            self._cycle_index += 1

        if self._queries is not None:
            return self._queries[idx]

        # Directory mode: read the JSON file on demand. If the file does
        # not contain a valid query, try the next one.
        for attempt in range(min(10, n)):
            actual_idx = (idx + attempt) % n
            path = os.path.join(self._query_dir,
                                self._query_files[actual_idx])
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                output = data.get("output")
                if not output:
                    continue
                query = output.get("sparql_fixed") or output.get("sparql")
                if query:
                    return query
            except (KeyError, TypeError, json.JSONDecodeError, AttributeError):
                continue
        raise ValueError("Could not find a valid query in the directory")


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
            "--queries-dir",
            type=str,
            default=None,
            help="Path to a directory with one JSON file per query"
                 " (query in output.sparql)",
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
            "--duration",
            type=str,
            default="30s",
            help="How long to run (e.g., 10s, 5min, 2h; default: 30s)",
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
        # Parse duration and log frequency.
        try:
            duration_secs = parse_duration(args.duration)
            log_frequency_secs = parse_duration(args.log_frequency)
        except ValueError as e:
            log.error(e)
            return False

        # Exactly one of the three query sources must be specified.
        sources = [args.queries_tsv, args.queries_yml, args.queries_dir]
        if sum(s is not None for s in sources) != 1:
            log.error("Please specify exactly one of --queries-tsv,"
                      " --queries-yml, or --queries-dir")
            return False

        # Build the query source.
        try:
            if args.queries_tsv:
                source = QuerySource.from_tsv(args.queries_tsv)
                source_path = args.queries_tsv
            elif args.queries_yml:
                source = QuerySource.from_yml(args.queries_yml)
                source_path = args.queries_yml
            else:
                source = QuerySource.from_dir(args.queries_dir)
                source_path = args.queries_dir
        except Exception as e:
            log.error(f"Could not read queries: {e}")
            return False
        if len(source) == 0:
            log.error(f"No queries found in \"{source_path}\"")
            return False

        # If --show-queries is given, show queries and exit.
        if args.show_queries is not None:
            n = min(args.show_queries, len(source))
            log.info(f"Showing {n} of {len(source)}"
                     f" {source.description} from \"{source_path}\":")
            log.info("")
            for i in range(n):
                query = source.get_query(args.query_selection)
                log.info(f"Query {i + 1}: {query[:200]}"
                         f"{'...' if len(query) > 200 else ''}")
            return True

        # Determine the SPARQL endpoint.
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )

        # Show what the command will do.
        self.show(
            f"Send queries from \"{source_path}\""
            f" ({len(source)} {source.description},"
            f" {args.query_selection} selection)"
            f" to {sparql_endpoint} at {args.request_rate} queries/s"
            f" for {format_duration(duration_secs)}"
            f" (log every {format_duration(log_frequency_secs)})",
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

        def send_query(query: str):
            nonlocal num_done, num_errors
            curl_cmd = (
                f"curl -s {sparql_endpoint}"
                f" -H \"Accept: application/qlever-results+json\""
                f" --data-urlencode query={shlex.quote(query)}"
                f" -o /dev/null -w \"%{{http_code}}\""
            )
            start = time.time()
            try:
                result = subprocess.run(
                    curl_cmd, shell=True, capture_output=True,
                    text=True, timeout=max(duration_secs * 2, 300),
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
            except Exception:
                elapsed = time.time() - start
                with lock:
                    num_done += 1
                    num_errors += 1
                    completed_times.append(elapsed)
                    status_codes["timeout"] = \
                        status_codes.get("timeout", 0) + 1

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
                f"{format_duration(elapsed):>8s}"
                f"  {n_launched:>8d}  {n_done:>8d}"
                f"  {n_running:>8d}  {n_errors:>8d}"
                f"{time_stats}"
            )

        # Main loop: launch queries at the specified rate.
        try:
            start_time = time.time()
            next_log_time = start_time + log_frequency_secs
            interval = 1.0 / args.request_rate
            next_launch_time = start_time + interval

            while True:
                now = time.time()
                elapsed = now - start_time
                if elapsed >= duration_secs:
                    break

                # Launch queries that are due.
                while next_launch_time <= now:
                    query = source.get_query(args.query_selection)
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
                sleep_until = min(next_launch_time, next_log_time,
                                  start_time + duration_secs)
                sleep_time = sleep_until - time.time()
                if sleep_time > 0:
                    time.sleep(min(sleep_time, 0.1))

            interrupted = False
        except KeyboardInterrupt:
            interrupted = True
            log.warning("\rCtrl+C pressed, stopping load test")

        # Helper to print header + status (used after interruption
        # and after waiting for stragglers).
        def print_final_status():
            print_table_header()
            print_status(time.time() - start_time)

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
