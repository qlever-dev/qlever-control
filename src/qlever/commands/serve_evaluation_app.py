from __future__ import annotations

import json
import statistics
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import unquote

import yaml

from qlever.command import QleverCommand
from qlever.log import log

EVAL_DIR = Path(__file__).parent.parent / "evaluation"


QUERY_STATS_DICT = {
    "ameanTime": None,
    "gmeanTime2": None,
    "gmeanTime10": None,
    "medianTime": None,
    "under1s": 0.0,
    "between1to5s": 0.0,
    "over5s": 0.0,
    "failed": 0.0,
}


def get_query_data(
    queries: list[dict], timeout: int | None
) -> dict[str, float | None]:
    query_data = {stat: val for stat, val in QUERY_STATS_DICT.items()}
    if not queries:
        return query_data
    failed = under_1 = bw_1_to_5 = over_5 = 0
    runtimes_gm2 = []
    runtimes_gm10 = []

    for query in queries:
        # Have the old query and sparql keys to not break the web app
        query["serverRestarted"] = bool(query.get("server_restarted"))
        query["sparql"] = query.pop("query")
        query["query"] = query.pop("name")
        runtime = float(query["runtime_info"]["client_time"])
        if len(query["headers"]) == 0 and isinstance(query["results"], str):
            failed += 1
            runtime_gm2 = (
                runtime * 2
                if timeout is None
                else timeout * 2
            )
            runtime_gm10 = (
                runtime * 10
                if timeout is None
                else timeout * 10
            )
            runtimes_gm2.append(runtime_gm2)
            runtimes_gm10.append(runtime_gm10)
        else:
            if runtime <= 1:
                under_1 += 1
            elif runtime > 5:
                over_5 += 1
            else:
                bw_1_to_5 += 1
            runtimes_gm2.append(runtime)
            runtimes_gm10.append(runtime)

    query_data["timeout"] = timeout
    query_data["ameanTime"] = statistics.mean(runtimes_gm2)
    query_data["gmeanTime2"] = statistics.geometric_mean(runtimes_gm2)
    query_data["gmeanTime10"] = statistics.geometric_mean(runtimes_gm10)
    query_data["medianTime"] = statistics.median(runtimes_gm2)
    query_data["failed"] = (failed / len(queries)) * 100
    query_data["under1s"] = (under_1 / len(queries)) * 100
    query_data["between1to5s"] = (bw_1_to_5 / len(queries)) * 100
    query_data["over5s"] = (over_5 / len(queries)) * 100
    query_data["queries"] = queries
    return query_data


def create_json_data(
    yaml_dir: Path, title: str
) -> dict | None:
    data = {
        "performance_data": None,
        "additional_data": {
            # "penalty": error_penalty,
            "title": title,
            "kbs": {},
        },
    }
    performance_data = {}
    if not yaml_dir.is_dir():
        return None
    for yaml_file in yaml_dir.glob("*.results.yaml"):
        file_name_split = yaml_file.stem.split(".")
        if len(file_name_split) != 3:
            continue
        dataset, engine, _ = file_name_split
        if performance_data.get(dataset) is None:
            performance_data[dataset] = {}
        if performance_data[dataset].get(engine) is None:
            performance_data[dataset][engine] = {}
        with yaml_file.open("r", encoding="utf-8") as queries_file:
            queries_data = yaml.safe_load(queries_file)
            data["additional_data"]["kbs"][dataset] = {
                "description": queries_data.get("description"),
                "title": queries_data.get("title"),
                "name": queries_data.get("name"),
                "scale": queries_data.get("scale"),
            }
            query_data = get_query_data(
                queries_data["queries"],
                queries_data.get("timeout"),
                # error_penalty,
            )
            performance_data[dataset][engine] = {**query_data}
    data["performance_data"] = performance_data
    return data


class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args,
        yaml_dir: Path | None = None,
        # error_penalty: int = 2,
        title: str = "SPARQL Engine Performance Evaluation",
        **kwargs,
    ) -> None:
        self.yaml_dir = yaml_dir
        # self.error_penalty = error_penalty
        self.title = title
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        path = unquote(self.path)

        if path == "/yaml_data":
            try:
                data = create_json_data(
                    self.yaml_dir, self.title
                )
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
    Class for executing the `serve-evaluation-app` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Serve the SPARQL Engine performance comparison webapp"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
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
            default="SPARQL Engine Performance Evaluation",
            help="Title text displayed in the navigation bar of the Overview page.",
        )
        # subparser.add_argument(
        #     "--error-penalty",
        #     type=int,
        #     default=2,
        #     help=(
        #         "The timeout (or failed runtime) will be multiplied with this "
        #         "error penalty factor when computing aggregate query metrics "
        #         "(Default = 2)"
        #     ),
        # )

    def execute(self, args) -> bool:
        yaml_dir = Path(args.results_dir)
        handler = partial(
            CustomHTTPRequestHandler,
            directory=EVAL_DIR,
            yaml_dir=yaml_dir,
            # error_penalty=args.error_penalty,
            title=args.title_overview_page,
        )
        httpd = HTTPServer(("", args.port), handler)
        log.info(
            f"Performance Comparison Web App is available at "
            f"http://{args.host}:{args.port}/www"
        )
        httpd.serve_forever()
        return True
