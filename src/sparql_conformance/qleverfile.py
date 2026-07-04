from __future__ import annotations

import json


def qleverfile_args(all_args: dict[str, dict[str, tuple]]) -> None:
    """Define all SPARQL-conformance-specific Qleverfile parameters"""

    # Helper that packages positional + keyword args exactly like
    # `argparse.add_argument`, matching the convention in
    # `Qleverfile.all_arguments()`.
    def arg(*args, **kwargs):
        return (args, kwargs)

    # ---------------------------------------------------------------- [conformance]
    conformance = all_args.setdefault("conformance", {})
    conformance["name"] = arg(
        "--name",
        type=str,
        required=True,
        help="Name of the result file of the conformance check.",
    )
    conformance["port"] = arg(
        "--port",
        type=str,
        required=True,
        help="Port which will be used for the SPARQL sever.",
    )
    conformance["graph_store"] = arg(
        "--graph-store",
        type=str,
        required=True,
        help="Name of the graph store endpoint used for graph store protocol tests.",
    )
    conformance["testsuite_dir"] = arg(
        "--testsuite-dir",
        type=str,
        default=None,
        help="Path to the test suite directory (used by the analyze command).",
    )
    conformance["sparql11_dir"] = arg(
        "--sparql11-dir",
        type=str,
        default=None,
        help="Path to the SPARQL 1.1 test suite directory.",
    )
    conformance["sparql10_dir"] = arg(
        "--sparql10-dir",
        type=str,
        default=None,
        help="Path to the SPARQL 1.0 test suite directory.",
    )
    conformance["custom"] = arg(
        "--custom",
        type=json.loads,
        default=None,
        help=(
            "JSON object mapping suite names to directories.\n"
            "Example: --custom '{\"my-suite\": \"/path/to/dir\"}'"
        ),
    )
    conformance["type_alias"] = arg(
        "--type-alias",
        type=json.loads,
        required=False,
        help=("Type mismatches that will be considered intended."
              "ex. \"[['http://www.w3.org/2001/XMLSchema#integer', "
              "'http://www.w3.org/2001/XMLSchema#int']..."
              "['http://www.w3.org/2001/XMLSchema#float',"
              "'http://www.w3.org/2001/XMLSchema#double']]\""
        ),
    )
    conformance["engine"] = arg(
        "--engine",
        type=str,
        choices=[
            "qlever",
            "qlever-binaries",
            "blazegraph",
            "graphdb",
            "jena",
            "mdb",
            "oxigraph",
            "virtuoso",
        ],
        default="docker",
        help="Which system to use to run the tests in"
    )
    conformance["exclude"] = arg(
        "--exclude",
        type=lambda s: s.split(","),
        default=[],
        help=("Tests (names) or test groups to exclude from the run."
              "ex. service,entailment,POST - existing graph"
        )
    )
    conformance["include"] = arg(
        "--include",
        type=lambda s: s.split(","),
        default=None,
        help=("Tests (names) or test groups to include in the run."
              "ex. service,entailment,POST - existing graph"
        )
    )
    conformance["binaries_directory"] = arg(
        "--binaries-directory",
        type=str,
        required=False,
        help="Path to the directory of the IndexBuilderMain and ServerMain binaries.",
        default=""
    )
    conformance["results_dir"] = arg(
        "--results-dir",
        type=str,
        default="./results",
        help="Directory for the output JSON file (default: ./results).",
    )
    conformance["report"] = arg(
        "--report",
        type=str,
        default="none",
        choices=["none", "summary", "line"],
        help=(
            "Console output verbosity (default: none, only the JSON is "
            "written): 'summary' prints end-of-run totals plus a list of "
            "failed tests; 'line' additionally prints a live PASS/FAIL "
            "line per test."
        ),
    )
    conformance["compare_to"] = arg(
        "--compare-to",
        type=str,
        default=None,
        help=(
            "Path to a previous <name>.json.bz2 run to compare against; "
            "prints regressions (newly failing) and fixes (newly passing) "
            "at the end."
        ),
    )

    # ------------------------------------------------------ per-engine image args
    from qvirtuoso.commands.setup_config import (
        SetupConfigCommand as VirtuosoSetupConfigCommand,
    )

    all_args.setdefault("qlever", {})["qlever_image"] = arg(
        "--qlever-image",
        type=str,
        default="docker.io/adfreiburg/qlever:commit-5c6a72a",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("oxigraph", {})["oxigraph_image"] = arg(
        "--oxigraph-image",
        type=str,
        default="ghcr.io/oxigraph/oxigraph",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("blazegraph", {})["blazegraph_image"] = arg(
        "--blazegraph-image",
        type=str,
        default="adfreiburg/qblazegraph",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("graphdb", {})["graphdb_image"] = arg(
        "--graphdb-image",
        type=str,
        default="docker.io/ontotext/graphdb:11.2.1",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("jena", {})["jena_image"] = arg(
        "--jena-image",
        type=str,
        default="adfreiburg/qjena",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("mdb", {})["mdb_image"] = arg(
        "--mdb-image",
        type=str,
        default="adfreiburg/millenniumdb",
        help="The name of the image when running in a container",
    )
    all_args.setdefault("virtuoso", {})["virtuoso_image"] = arg(
        "--virtuoso-image",
        type=str,
        default=VirtuosoSetupConfigCommand.IMAGE,
        help="The name of the image when running in a container",
    )

    # ------------------------------------------------------------- [conformance_ui]
    conformance_ui = all_args.setdefault("conformance_ui", {})
    conformance_ui["port"] = arg(
        '--port',
        required=False,
        help='Port of the webserver (default: 3000)',
        default='3000'
    )
    conformance_ui["result_directory"] = arg(
        '--result-directory',
        required=False,
        help='Directory containing the results of the SPARQL conformance tests (default: current directory)',
        default='$(pwd)'
    )
    conformance_ui["ui_branch"] = arg(
        '--ui-branch',
        required=False,
        help='Branch of sparql-conformance-ui to build the visualization from (default: main)',
        default='main'
    )
