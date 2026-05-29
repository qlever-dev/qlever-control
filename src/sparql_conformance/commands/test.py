import os
from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from sparql_conformance.config import Config
from sparql_conformance.engines.blazegraph_manager import BlazegraphManager
from sparql_conformance.engines.graphdb_manager import GraphdbManager
from sparql_conformance.engines.jena_manager import JenaManager
from sparql_conformance.engines.mdb_manager import MdbManager
from sparql_conformance.engines.oxigraph_manager import OxigraphManager
from sparql_conformance.engines.virtuoso_manager import VirtuosoManager
from sparql_conformance.extract_tests import extract_tests
from sparql_conformance.testsuite import TestSuite
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.engines.qlever import QLeverManager
from sparql_conformance.util import warn_if_missing_image


def get_engine_manager(engine_type: str) -> EngineManager:
    """Get the appropriate engine manager for the given engine type"""
    managers = {
        'qlever': QLeverManager,
        'qlever-binaries': QLeverManager,
        'blazegraph': BlazegraphManager,
        'graphdb': GraphdbManager,
        'jena': JenaManager,
        'mdb': MdbManager,
        'oxigraph': OxigraphManager,
        'virtuoso': VirtuosoManager
    }

    manager_class = managers.get(engine_type)
    if manager_class is None:
        raise ValueError(f"Unsupported engine type: {engine_type}")

    return manager_class()


class TestCommand(QleverCommand):
    """
    Class for executing the `test` command.
    """

    def __init__(self):
        self.options = [
            'qlever',
            'qlever-binaries',
            'blazegraph',
            'graphdb',
            'jena',
            'mdb',
            'oxigraph',
            'virtuoso'
        ]

    def description(self) -> str:
        return "Run SPARQL conformance tests against different engines"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "conformance": ["name", "port", "engine",
                            "graph_store", "sparql11_dir", "sparql10_dir", "custom",
                            "type_alias", "exclude", "include", "binaries_directory"],
            "runtime": ["system"],
            "qlever": ["qlever_image"],
            "oxigraph": ["oxigraph_image"],
            "blazegraph": ["blazegraph_image"],
            "virtuoso": ["virtuoso_image"],
            "graphdb": ["graphdb_image"],
            "jena": ["jena_image"],
            "mdb": ["mdb_image"],
        }

    def additional_arguments(self, subparser):
        pass

    def execute(self, args) -> bool:
        if args.engine not in self.options:
            log.error(f"Invalid engine type: {args.engine}")
            return False
        image = getattr(args, f"{args.engine}_image", None)
        if (args.system == "native" and args.binaries_directory == "" or
                args.system != "native" and image is None and args.engine != "blazegraph"):
            log.error(
                f"Selected system {args.system} not compatible with image: {image}"
                f" and binaries_directory: {args.binaries_directory}"
            )
            return False

        warn_if_missing_image(args.system, image, args.engine)

        standard_suites = [
            ("sparql11", args.sparql11_dir),
            ("sparql10", args.sparql10_dir),
        ]
        active_suites = [(key, d) for key, d in standard_suites if d is not None]
        if args.custom:
            active_suites.extend(args.custom.items())

        if not active_suites:
            log.error("Provide at least one of --sparql11-dir, --sparql10-dir, --custom.")
            return False

        for _, d in active_suites:
            if not Path(d).is_dir():
                log.error(f"Test suite directory not found: {d}. Use `sparql_conformance setup` to download it.")
                return False

        if args.engine == "blazegraph" and args.graph_store == "sparql":
            args.graph_store = "blazegraph/namespace/kb/sparql"
        if args.engine == "jena" and args.graph_store == "sparql":
            args.graph_store = "qlever-sparql-conformance/data"

        alias = [tuple(x) for x in args.type_alias] if args.type_alias else []

        suites_data = {}
        total_info = {"passed": 0, "tests": 0, "failed": 0, "passedFailed": 0, "notTested": 0}
        last_suite = None

        for suite_key, suite_dir in active_suites:
            print(f"Running suite '{suite_key}' from {suite_dir}...")
            config = Config(image, args.system, args.port, args.graph_store, suite_dir, alias,
                            args.binaries_directory, args.exclude, args.include,
                            run_id=args.name)
            tests, test_count = extract_tests(config)
            suite = TestSuite(name=args.name, tests=tests, test_count=test_count,
                              config=config, engine_manager=get_engine_manager(args.engine))
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

        os.makedirs("./results", exist_ok=True)
        last_suite.compress_json_bz2(output, f"./results/{args.name}.json.bz2")
        print("Finished!")
        return True
