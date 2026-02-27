from __future__ import annotations

import json
import statistics
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import yaml

from qlever.command import QleverCommand
from qlever.log import log

EVAL_DIR = Path(__file__).parent.parent / "evaluation"

# Default values for the performance statistics returned by the
# /yaml_data API endpoint and consumed by the evaluation web app.
PERFORMANCE_STATS_DICT = {
    "ameanTime": None,
    "gmeanTime2": None,
    "gmeanTime10": None,
    "medianTime": None,
    "under1s": 0.0,
    "between1to5s": 0.0,
    "over5s": 0.0,
    "failed": 0.0,
}


def get_performance_data(result_data: dict[str, Any]) -> dict[str, Any]:
    """
    Compute aggregate performance statistics from benchmark result data.
    Returns a dict with aggregate metrics, timeout and the individual query records.
    """
    queries = result_data.get("queries")
    timeout = result_data.get("timeout")
    performance_data = PERFORMANCE_STATS_DICT.copy()
    if not queries:
        return performance_data
    failed = under_1 = bw_1_to_5 = over_5 = 0
    # Two runtime lists with different penalties for failed queries:
    # runtimes_2x uses a 2x penalty, runtimes_10x uses a 10x penalty.
    runtimes_2x = []
    runtimes_10x = []

    for query in queries:
        # Have the old query and sparql keys to not break the web app
        query["serverRestarted"] = bool(query.get("server_restarted"))
        query["sparql"] = query.pop("query")
        query["query"] = query.pop("name")
        runtime = float(query["runtime_info"]["client_time"])
        # A query is considered failed if it has no headers and the
        # results field is an error string (not a list of rows).
        if len(query["headers"]) == 0 and isinstance(query["results"], str):
            failed += 1
            penalty_base = timeout if timeout is not None else runtime
            runtimes_2x.append(penalty_base * 2)
            runtimes_10x.append(penalty_base * 10)
        else:
            if runtime <= 1:
                under_1 += 1
            elif runtime > 5:
                over_5 += 1
            else:
                bw_1_to_5 += 1
            runtimes_2x.append(runtime)
            runtimes_10x.append(runtime)

    num_queries = len(queries)
    performance_data["timeout"] = timeout
    performance_data["indexTime"] = result_data.get("index_time")
    performance_data["indexSize"] = result_data.get("index_size")
    performance_data["ameanTime"] = statistics.mean(runtimes_2x)
    performance_data["gmeanTime2"] = statistics.geometric_mean(runtimes_2x)
    performance_data["gmeanTime10"] = statistics.geometric_mean(runtimes_10x)
    performance_data["medianTime"] = statistics.median(runtimes_2x)
    performance_data["failed"] = (failed / num_queries) * 100
    performance_data["under1s"] = (under_1 / num_queries) * 100
    performance_data["between1to5s"] = (bw_1_to_5 / num_queries) * 100
    performance_data["over5s"] = (over_5 / num_queries) * 100
    performance_data["queries"] = queries
    return performance_data


def create_json_data(yaml_dir: Path | None, title: str) -> dict | None:
    """
    Scan `yaml_dir` for `<dataset>.<engine>.results.yaml` files and
    aggregate them into a dictionary.
    Returns None if `yaml_dir` is invalid.

    The returned structure is:
    {
        "performance_data": { <dataset>: { <engine>: { ... } } },
        "additional_data":  { "title": ..., "kbs": { <dataset>: ... } }
    }
    """
    data = {
        "performance_data": None,
        "additional_data": {
            "title": title,
            "kbs": {},
        },
    }
    performance_data = {}
    if not yaml_dir or not yaml_dir.is_dir():
        return None
    for yaml_file in yaml_dir.glob("*.results.yaml"):
        file_name_split = yaml_file.stem.split(".")
        if len(file_name_split) != 3:
            continue
        dataset, engine, _ = file_name_split
        performance_data.setdefault(dataset, {})
        with yaml_file.open("r", encoding="utf-8") as result_file:
            result_data = yaml.safe_load(result_file)
            data["additional_data"]["kbs"][dataset] = {
                "name": result_data.get("name"),
                "description": result_data.get("description"),
                "scale": result_data.get("scale"),
            }
            performance_data[dataset][engine] = get_performance_data(
                result_data
            )
    data["performance_data"] = performance_data
    return data


class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
    """
    HTTP handler that serves the static evaluation web app and exposes
    a `/yaml_data` API endpoint returning aggregated benchmark results
    as JSON.
    """

    def __init__(
        self,
        *args,
        yaml_dir: Path | None = None,
        title: str = "RDF Graph Database Performance Evaluation",
        **kwargs,
    ) -> None:
        self.yaml_dir = yaml_dir
        self.title = title
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        """
        Handle GET requests. The `/yaml_data` path returns aggregated
        benchmark JSON.
        """
        path = unquote(self.path)

        if path == "/yaml_data":
            try:
                data = create_json_data(self.yaml_dir, self.title)
                json_data = json.dumps(data, indent=2).encode("utf-8")

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(json_data)))
                self.end_headers()
                self.wfile.write(json_data)

            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error loading YAMLs: {e}".encode("utf-8"))
        else:
            super().do_GET()


class ServeEvaluationAppCommand(QleverCommand):
    """
    Serve a local web app that visualises and compares RDF Graph Database
    benchmark results from YAML files produced by `benchmark-queries` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Serve the web app for the RDF Graph "
            "Database Performance Evaluation"
        )

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--port",
            type=int,
            default=8000,
            help=(
                "Port where the Performance comparison webapp will be "
                "served (Default = 8000)"
            ),
        )
        subparser.add_argument(
            "--host",
            type=str,
            default="localhost",
            help=(
                "Host where the Performance comparison webapp will be "
                "served (Default = localhost)"
            ),
        )
        subparser.add_argument(
            "--results-dir",
            type=str,
            default=".",
            help=(
                "Path to the directory where yaml result files from "
                "example-queries are saved (Default = current working dir)"
            ),
        )
        subparser.add_argument(
            "--title-overview-page",
            type=str,
            default="RDF Graph Database Performance Evaluation",
            help="Title text displayed in the navigation bar of the Overview page.",
        )

    def execute(self, args) -> bool:
        yaml_dir = Path(args.results_dir)
        handler = partial(
            CustomHTTPRequestHandler,
            directory=EVAL_DIR,
            yaml_dir=yaml_dir,
            title=args.title_overview_page,
        )
        httpd = HTTPServer((args.host, args.port), handler)
        log.info(
            f"Performance Comparison Web App is available at "
            f"http://{args.host}:{args.port}/www"
        )
        httpd.serve_forever()
        return True
