from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command
from sparql_conformance.config import Config
from sparql_conformance.extract_tests import extract_tests
from sparql_conformance.testsuite import TestSuite
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.engines.qlever import QLeverManager



def get_engine_manager(engine_type: str) -> EngineManager:
    """Get the appropriate engine manager for the given engine type"""
    managers = {
        'qlever-binaries': QLeverBinaryManager,
        'qlever': QLeverManager,
        # 'mdb': MDBManager,
        # 'oxigraph': OxigraphManager
    }

    manager_class = managers.get(engine_type)
    if manager_class is None:
        raise ValueError(f"Unsupported engine type: {engine_type}")

    return manager_class()


class AnalyzeCommand(QleverCommand):
    """
    Class for executing the `test` command.
    """

    def __init__(self):
        self.options = [
            'qlever',
            #'mdb',
            #'oxigraph'
        ]

    def description(self) -> str:
        return "Run SPARQL conformance tests against different engines"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {
            "conformance": ["name", "port", "engine", "graph_store",
                            "testsuite_dir", "type_alias", "exclude"],
            "runtime": ["system"],
            "qlever": ["qlever_image"],
            "oxigraph": ["oxigraph_image"]
        }

    def additional_arguments(self, subparser):
        subparser.add_argument(
            "test_name",
            type=str,
            help="The name of the test to start the server for.",
        )

    def execute(self, args) -> bool:
        if args.engine not in self.options:
            log.error(f"Invalid engine type: {args.engine}")
            return False
        image = getattr(args, f"{args.engine}_image", None)
        if (args.system == "native" and args.binaries_directory == "" or
                args.system != "native" and image is None):
            log.error(
                f"Selected system {args.system} not compatible with image: {image}"
                f" and binaries_directory: {args.binaries_directory}"
            )
            return False

        if args.testsuite_dir is None or not Path(args.testsuite_dir).is_dir():
            log.error("Could not find testsuite directory. Use `sparql_conformance setup` to download it.")
            return False

        alias = [tuple(x) for x in args.type_alias] if args.type_alias else []
        config = Config(image, args.system, args.port, args.graph_store, args.testsuite_dir, alias,
                        args.binaries_directory, args.exclude, args.test_name)
        print("Preparing ...")
        if "qlever" in args.engine:
            print("access_token='abc'")
        tests, test_count = extract_tests(config)
        test_suite = TestSuite(name=args.name, tests=tests, test_count=test_count, config=config,
                               engine_manager=get_engine_manager(args.engine))
        test_suite.analyze()
        return True
