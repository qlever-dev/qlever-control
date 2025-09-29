from qlever.command import QleverCommand
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.engines.qlever_binary import QLeverBinaryManager
from sparql_conformance.engines.qlever import QLeverManager
from sparql_conformance.engines.mdb import MDBManager
from sparql_conformance.engines.oxigraph import OxigraphManager
from sparql_conformance.extract_tests import extract_tests
from sparql_conformance.testsuite import TestSuite

class TestCommand(QleverCommand):
    """
    Class for executing the `test` command.
    """

    def __init__(self):
        self.options = [
            'qlever',
            'qlever-binaries',
            #'mdb',
            #'oxigraph'
        ]
        self.options_with_binaries = [
            'qlever-binaries'
        ]
        self.options_with_images = [
            'qlever',
            'oxigraph'
        ]

    def get_engine_manager(self, engine_type: str) -> EngineManager:
        """Get the appropriate engine manager for the given engine type"""
        managers = {
            'qlever-binaries': QLeverBinaryManager,
            'qlever': QLeverManager,
            'mdb': MDBManager,
            'oxigraph': OxigraphManager
        }

        manager_class = managers.get(engine_type)
        if manager_class is None:
            raise ValueError(f"Unsupported engine type: {engine_type}")

        return manager_class()

    def description(self) -> str:
        return "Run SPARQL conformance tests against different engines"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {
            "conformance": ["name", "port", "engine", "graph_store",
                            "testsuite_dir", "type_alias", "exclude"],
            "runtime": ["system"],
            "qlever_binaries": ["binaries_directory"],
            "qlever": ["qlever_image"],
            "oxigraph": ["oxigraph_image"]
        }

    def additional_arguments(self, subparser):
        pass

    def execute(self, args) -> bool:
        if args.engine not in self.options:
            return False
        if args.engine in self.options_with_binaries and args.binaries_directory is "":
            return False
        image = getattr(args, f"{args.engine}_image", None)
        if args.engine in self.options_with_images and (image is None or args.system is "native"):
            return False
        alias = [tuple(x) for x in args.type_alias] if args.type_alias else []
        config = Config(image, args.system, args.port, args.graph_store, args.testsuite_dir, alias,
                        args.binaries_directory, args.exclude)
        print("Running testsuite...")
        tests, test_count = extract_tests(config)
        test_suite = TestSuite(name=args.name, tests=tests, test_count=test_count, config=config,
                               engine_manager=self.get_engine_manager(args.engine))
        test_suite.run()
        test_suite.generate_json_file()
        print("Finished!")
        return True
