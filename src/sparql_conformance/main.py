import argparse
import importlib.util
import json
import os
import sys

from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.extract_tests import extract_tests
from sparql_conformance.testsuite import TestSuite

try:
    from qlever.log import log
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    log = logging.getLogger(__name__)


def load_engine_from_file(path: str) -> EngineManager:
    """Dynamically load the first EngineManager subclass found in a Python file."""
    abs_path = os.path.abspath(path)
    spec = importlib.util.spec_from_file_location("engine_module", abs_path)
    if spec is None:
        raise ValueError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and issubclass(obj, EngineManager) and obj is not EngineManager:
            return obj()
    raise ValueError(f"No EngineManager subclass found in {path}")


def get_engine_manager_by_name(name: str) -> EngineManager:
    """Resolve a named engine type using the qlever-control factory (requires qlever-control)."""
    try:
        from sparql_conformance.commands.test import get_engine_manager
        return get_engine_manager(name)
    except ImportError:
        print(f"Named engine '{name}' requires qlever-control to be installed. "
              "Provide a file path to --engine instead.", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Run SPARQL conformance tests against a SPARQL engine.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--engine",
        required=True,
        metavar="FILE_OR_TYPE",
        help=(
            "Path to a Python file containing an EngineManager subclass, "
            "or a named engine type (requires qlever-control).\n"
            "Example: --engine ./qlever-binaries-manager.py"
        ),
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Name for this run; used as the output filename: <results-dir>/<name>.json.bz2",
    )
    parser.add_argument(
        "--results-dir",
        default="./results",
        dest="results_dir",
        help="Directory for the output JSON file (default: ./results).",
    )
    parser.add_argument(
        "--port",
        default="7001",
        help="Port for the SPARQL server (default: 7001)",
    )
    parser.add_argument(
        "--graph-store",
        default="sparql",
        dest="graph_store",
        help="Graph store endpoint name for graph store protocol tests (default: sparql)",
    )
    parser.add_argument(
        "--sparql11-dir",
        default=None,
        dest="sparql11_dir",
        help="Path to the SPARQL 1.1 test suite directory.",
    )
    parser.add_argument(
        "--sparql10-dir",
        default=None,
        dest="sparql10_dir",
        help="Path to the SPARQL 1.0 test suite directory.",
    )
    parser.add_argument(
        "--custom",
        default=None,
        dest="custom",
        type=json.loads,
        metavar="NAME_TO_DIR_JSON",
        help=(
            "JSON object mapping suite names to directories.\n"
            "Example: --custom '{\"my-suite\": \"/path/to/dir\", \"proto\": \"/path/to/proto\"}'"
        ),
    )
    parser.add_argument(
        "--binaries-directory",
        default="",
        dest="binaries_directory",
        help="Directory containing qlever binaries (used by file-based engine managers).",
    )
    parser.add_argument(
        "--exclude",
        default=[],
        type=lambda s: s.split(","),
        help="Comma-separated list of test names or groups to exclude.",
    )
    parser.add_argument(
        "--include",
        default=None,
        type=lambda s: s.split(","),
        help="Comma-separated list of test names or groups to include.",
    )
    parser.add_argument(
        "--type-alias",
        default=None,
        dest="type_alias",
        type=json.loads,
        help=(
            "JSON list of type pairs considered as intended deviations.\n"
            "Example: --type-alias \"[['xsd:integer','xsd:int']]\""
        ),
    )

    args = parser.parse_args()

    standard_suites = [
        ("sparql11", args.sparql11_dir),
        ("sparql10", args.sparql10_dir),
    ]
    active_suites = [(key, d) for key, d in standard_suites if d is not None]
    if args.custom:
        active_suites.extend(args.custom.items())

    if not active_suites:
        parser.error("Provide at least one of --sparql11-dir, --sparql10-dir, --custom.")

    for _, d in active_suites:
        if not os.path.isdir(d):
            parser.error(f"Test suite directory not found: {d}")

    if os.path.isfile(args.engine):
        engine_manager = load_engine_from_file(args.engine)
    else:
        engine_manager = get_engine_manager_by_name(args.engine)

    alias = [tuple(x) for x in args.type_alias] if args.type_alias else []

    suites_data = {}
    total_info = {"passed": 0, "tests": 0, "failed": 0, "passedFailed": 0, "notTested": 0}
    last_suite = None

    for suite_key, suite_dir in active_suites:
        print(f"Running suite '{suite_key}' from {suite_dir}...")
        config = Config(
            image=None,
            system="native",
            port=args.port,
            graph_store=args.graph_store,
            testsuite_dir=suite_dir,
            type_alias=alias,
            binaries_directory=args.binaries_directory,
            exclude=args.exclude,
            include=args.include,
        )
        tests, test_count = extract_tests(config)
        suite = TestSuite(
            name=args.name,
            tests=tests,
            test_count=test_count,
            config=config,
            engine_manager=engine_manager,
            results_dir=args.results_dir,
        )
        suite.run()
        tests_dict, info_dict = suite.build_results_dict()
        suites_data[suite_key] = {"tests": tests_dict, "info": info_dict}
        for key in total_info:
            total_info[key] += info_dict[key]
        last_suite = suite

    output = {
        "version": 2,
        "suites": suites_data,
        "info": {"name": "info", **total_info},
    }

    os.makedirs(args.results_dir, exist_ok=True)
    last_suite.compress_json_bz2(output, os.path.join(args.results_dir, f"{args.name}.json.bz2"))
    print("Finished!")


if __name__ == "__main__":
    main()
