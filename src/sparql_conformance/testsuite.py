import bz2
import json
import os
from typing import List, Dict, Tuple

import sparql_conformance.util as util
from qlever.log import log
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.engines.qlever import QLeverManager
from sparql_conformance.json_tools import compare_json
from sparql_conformance.protocol_tools import run_protocol_test
from sparql_conformance.rdf_tools import compare_ttl
from sparql_conformance.test_object import TestObject, Status, ErrorMessage
from sparql_conformance.tsv_csv_tools import compare_sv
from sparql_conformance.xml_tools import compare_xml


class TestSuite:
    """
    A class to represent a test suite for SPARQL using QLever.
    """

    def __init__(self, name: str, tests: Dict[str, Dict[Tuple[Tuple[str, str], ...], List[TestObject]]], test_count, config: Config, engine_manager: EngineManager):
        """
        Constructs all the necessary attributes for the TestSuite object.

        Parameters:
            name (str): Name of the current run.
        """
        self.name = name
        self.config = config
        self.tests = tests
        self.test_count = test_count
        self.passed = 0
        self.failed = 0
        self.passed_failed = 0
        self.engine_manager = engine_manager

    def evaluate_query(
            self,
            expected_string: str,
            query_result: str,
            test: TestObject,
            result_format: str):
        """
        Evaluates a query result based on the expected output and the format.
        """
        status = Status.FAILED
        error_type = ErrorMessage.RESULTS_NOT_THE_SAME
        if result_format == "srx":
            status, error_type, expected_html, test_html, expected_red, test_red = compare_xml(
                expected_string, query_result, self.config.alias, self.config.number_types)
        elif result_format == "srj":
            status, error_type, expected_html, test_html, expected_red, test_red = compare_json(
                expected_string, query_result, self.config.alias, self.config.number_types)
        elif result_format == "csv" or result_format == "tsv":
            status, error_type, expected_html, test_html, expected_red, test_red = compare_sv(
                expected_string, query_result, result_format, self.config.alias)
        elif result_format == "ttl":
            status, error_type, expected_html, test_html, expected_red, test_red = compare_ttl(
                expected_string, query_result)
        else:
            expected_html = ""
            test_html = ""
            expected_red = ""
            test_red = ""

        self.update_test_status(test, status, error_type)
        setattr(test, "got_html", test_html)
        setattr(test, "expected_html", expected_html)
        setattr(test, "got_html_red", test_red)
        setattr(test, "expected_html_red", expected_red)

    def evaluate_update(
                self,
                expected_graphs,
                graphs,
                test: TestObject):
        """
        Evaluates the graphs after running the update.

        Parameters:
            test (TestObject): Object containing the test being run.
            expected_graphs ([str]]): The expected state of each graph.
            graphs ([str]): The actual state of our graphs.
        """
        status = [Status.FAILED for _ in range(len(expected_graphs))]
        error_type = [ErrorMessage.RESULTS_NOT_THE_SAME for _ in range(len(expected_graphs))]
        expected_html = ["" for _ in range(len(expected_graphs))]
        test_html = ["" for _ in range(len(expected_graphs))]
        expected_red = ["" for _ in range(len(expected_graphs))]
        test_red = ["" for _ in range(len(expected_graphs))]
        assert(len(expected_graphs) == len(graphs))
        for i in range(len(expected_graphs)):
            status[i], error_type[i], expected_html[i], test_html[i], expected_red[i], test_red[i] = compare_ttl(
                    expected_graphs[i], graphs[i])
            
        for s, e in zip(status, error_type):
            if s != Status.PASSED:
                status[0] = s
                error_type[0] = e
                break
        
        self.update_test_status(test, status[0], error_type[0])
        t_html = f"<b>default:</b><br>{test_html[0]}"
        e_html = f"<b>default:</b><br>{expected_html[0]}"
        t_red = f"<b>default:</b><br>{test_red[0]}"
        e_red = f"<b>default:</b><br>{expected_red[0]}"
        i = 1
        for key, value in test.result_files.items():
            t_html += f"<br><br><b>{key}:</b><br>{test_html[i]}"
            e_html += f"<br><br><b>{key}:</b><br>{expected_html[i]}"
            t_red += f"<br><br><b>{key}:</b><br>{test_red[i]}"
            e_red += f"<br><br><b>{key}:</b><br>{expected_red[i]}"
            i += 1

        setattr(test, "got_html", t_html)
        setattr(test, "expected_html", e_html)
        setattr(test, "got_html_red", t_red)
        setattr(test, "expected_html_red", e_red)

    def log_for_all_tests(self, list_of_tests: list, attribute: str, log_message: str):
        """
        Logs information for all tests of a given graph.
        """
        for test in list_of_tests:
            setattr(test, attribute, log_message)

    def update_test_status(
            self,
            test: TestObject,
            status: str,
            error_type: str):
        """
        Updates the status of a test in the test data.
        """
        self.log_for_all_tests([test], "status", status)
        self.log_for_all_tests([test], "error_type", error_type)

    def update_graph_status(
            self,
            list_of_tests: list,
            status: str,
            error_type: str):
        """
        Updates the status for all test of a graph.
        """
        for test in list_of_tests:
            self.update_test_status(test, status, error_type)

    def prepare_test_environment(
            self,
            graph_paths: Tuple[Tuple[str, str], ...],
            list_of_tests: List[TestObject]) -> bool:
        """
        Prepares the test environment for a given graph.

        Args:
            graph_paths: ex. default graph + named graph (('graph_path', '-'), ('graph_path2', 'graph_name2'))
            list_of_tests: [Test1, Test2, ...]

        Returns:
            True if the environment is successfully prepared, False otherwise.
        """
        self.engine_manager.cleanup(self.config)
        index_success, server_success, index_log, server_log = self.engine_manager.setup(self.config, graph_paths)
        if not index_success:
            self.engine_manager.cleanup(self.config)
            self.update_graph_status(list_of_tests, Status.FAILED, ErrorMessage.INDEX_BUILD_ERROR)
        if not server_success:
            self.engine_manager.cleanup(self.config)
            self.update_graph_status(list_of_tests, Status.FAILED, ErrorMessage.SERVER_ERROR)
        if isinstance(self.engine_manager, QLeverManager) and index_success and server_success and "Syntax" in list_of_tests[0].type_name:
            self.engine_manager.activate_syntax_test_mode(self.config.server_address, self.config.port)
        self.log_for_all_tests(list_of_tests, "index_log", index_log)
        self.log_for_all_tests(list_of_tests, "server_log", server_log)
        return index_success and server_success

    def process_failed_response(self, test, query_response: tuple):
        if "exception" in query_response[1]:
            query_log = json.loads(
                query_response[1])["exception"].replace(
                ";", ";\n")
            error_type = ErrorMessage.QUERY_EXCEPTION
        elif "HTTP Request" in query_response[1]:
            error_type = ErrorMessage.REQUEST_ERROR
            query_log = query_response[1]
        elif "not supported" in query_response[1]:
            error_type = ErrorMessage.NOT_SUPPORTED
            if "content type" in query_response[1]:
                error_type = ErrorMessage.CONTENT_TYPE_NOT_SUPPORTED
            query_log = query_response[1]
        else:
            error_type = ErrorMessage.QUERY_ERROR
            query_log = query_response[1]
        setattr(test, "query_log", query_log)
        self.update_test_status(test, Status.FAILED, error_type)

    def run_query_tests(self, graphs_list_of_tests):
        """
        Executes query tests for each graph in the test suite.
        """
        for graph in graphs_list_of_tests:
            log.info(f"Running query tests for graph / graphs: {graph}")
            if not self.prepare_test_environment(
                    graph, graphs_list_of_tests[graph]):
                continue

            for test in graphs_list_of_tests[graph]:
                log.info(f"Running: {test.name}")
                query_result = self.engine_manager.query(
                    self.config,
                    test.query_file,
                    test.result_format)
                if query_result[0] == 200:
                    self.evaluate_query(
                        test.result_file, query_result[1], test, test.result_format)
                else:
                    self.process_failed_response(test, query_result)

            if os.path.exists("./TestSuite.server-log.txt"):
                server_log = util.read_file("./TestSuite.server-log.txt")
                self.log_for_all_tests(
                    graphs_list_of_tests[graph],
                    "server_log",
                    util.remove_date_time_parts(server_log))
            self.engine_manager.cleanup(self.config)

    def run_update_tests(self, graphs_list_of_tests):
        """
        Executes update tests for each graph in the test suite.
        """
        for graph in graphs_list_of_tests:
            log.info(f"Running update tests for graph / graphs: {graph}")
            for test in graphs_list_of_tests[graph]:
                log.info(f"Running: {test.name}")
                if not self.prepare_test_environment(
                        graph, graphs_list_of_tests[graph]):
                    # If the environment is not prepared, skip all tests for this graph.
                    break
                # Execute the update query.
                query_update_result = self.engine_manager.update(self.config, test.query_file)
                
                # If the update query was successful, retrieve the current state of all graphs
                # and check if the results match the expected results.
                if query_update_result[0] == 200:
                    actual_state_of_graphs = []
                    expected_state_of_graphs = []
                    # Handle default graph that has no uri 
                    construct_graph = self.engine_manager.query(
                        self.config,
                        "CONSTRUCT {?s ?p ?o} WHERE { GRAPH ql:default-graph {?s ?p ?o}}",
                        "ttl")
                    actual_state_of_graphs.append(construct_graph[1])
                    expected_state_of_graphs.append(test.result_file)
                    
                    # Handle named graphs.
                    if test.result_files:
                        for graph_label, expected_graph in test.result_files.items():
                            construct_graph = self.engine_manager.query(
                                self.config,
                                f"CONSTRUCT {{?s ?p ?o}} WHERE {{ GRAPH <{graph_label}> {{?s ?p ?o}}}}",
                                "ttl")
                            actual_state_of_graphs.append(construct_graph[1])
                            expected_state_of_graphs.append(expected_graph)

                    # Evaluate state of graphs.
                    self.evaluate_update(expected_state_of_graphs, actual_state_of_graphs, test)
                else:
                    self.process_failed_response(test, query_update_result)

                if os.path.exists("./TestSuite.server-log.txt"):
                    server_log = util.read_file("./TestSuite.server-log.txt")
                    self.log_for_all_tests(
                        graphs_list_of_tests[graph],
                        "server_log",
                        util.remove_date_time_parts(server_log))
                self.engine_manager.cleanup(self.config)

    def run_syntax_tests(self, graphs_list_of_tests: Dict[Tuple[Tuple[str, str], ...], List[TestObject]]):
        """
        Executes query tests for each graph in the test suite.
        """
        for graph_path in graphs_list_of_tests:
            log.info(f"Running syntax tests for graph: {graph_path}")
            if not self.prepare_test_environment(
                    graph_path, graphs_list_of_tests[graph_path]):
                continue

            for test in graphs_list_of_tests[graph_path]:
                log.info(f"Running: {test.name}")
                result_format = "srx"
                if "construct" in test.name:
                    result_format = "ttl"
                if "Update" in test.type_name:
                    query_result = self.engine_manager.update(
                        self.config,
                        test.query_file)
                else:
                    query_result = self.engine_manager.query(
                        self.config,
                        test.query_file,
                        result_format)

                if query_result[0] != 200:
                    self.process_failed_response(test, query_result)
                else:
                    setattr(test, "query_log", query_result[1])
                    self.update_test_status(test, Status.PASSED, "")
                if test.type_name == "NegativeSyntaxTest11" or test.type_name == "NegativeUpdateSyntaxTest11":
                    if ErrorMessage.is_query_error(test.error_type):
                        status = Status.PASSED
                        error_type = ""
                    else:
                        status = Status.FAILED
                        error_type = ErrorMessage.EXPECTED_EXCEPTION
                    self.update_test_status(test, status, error_type)

            if os.path.exists("./TestSuite.server-log.txt"):
                server_log = util.read_file("./TestSuite.server-log.txt")
                self.log_for_all_tests(
                    graphs_list_of_tests[graph_path],
                    "server_log",
                    util.remove_date_time_parts(server_log))
            self.engine_manager.cleanup(self.config)

    def run_protocol_tests(self, graphs_list_of_tests: Dict[Tuple[Tuple[str, str], ...], List[TestObject]]):
        """
        Executes protocol tests for each graph in the test suite.
        """
        for graph_path in graphs_list_of_tests:
            log.info(f"Running protocol tests for graph: {graph_path}")
            # Work around for issue #25, missing data for protocol tests
            path_to_data = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
            graph_paths = graph_path
            for i in range(4):
                path_to_graph = os.path.join(path_to_data, f"data{i}.rdf")
                name_of_graph = f"http://kasei.us/2009/09/sparql/data/data{i}.rdf"
                new_path: Tuple[str, str] = (path_to_graph, name_of_graph)
                graph_paths = graph_paths + (new_path,)
            for test in graphs_list_of_tests[graph_path]:
                log.info(f"Running: {test.name}")
                if not self.prepare_test_environment(
                        graph_paths, graphs_list_of_tests[graph_path]):
                    break
                if test.comment:
                    status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, newpath = run_protocol_test(
                        self.engine_manager, test, test.comment, '')

                    if os.path.exists("./TestSuite.server-log.txt"):
                        server_log = util.read_file(
                            "./TestSuite.server-log.txt")
                        self.log_for_all_tests(
                            graphs_list_of_tests[graph_path],
                            "server_log",
                            util.remove_date_time_parts(server_log))
                    self.engine_manager.cleanup(self.config)
                    self.update_test_status(test, status, error_type)
                else:
                    extracted_sent_requests = ''
                    extracted_expected_responses = ''
                    got_responses = ''
                setattr(test, "protocol", test.comment)
                setattr(test, "protocol_sent", extracted_sent_requests)
                setattr(
                    test,
                    "response_extracted",
                    extracted_expected_responses)
                setattr(test, "response", got_responses)

    def run_graphstore_protocol_tests(self, graphs_list_of_tests: Dict[Tuple[Tuple[str, str], ...], List[TestObject]]):
        """
        Executes graphstore protocol tests for each graph in the test suite.
        """
        for graph_path in graphs_list_of_tests:
            log.info(f'Running graphstore protocol tests for graph: {graph_path}')
            if not self.prepare_test_environment(
                    graph_path, graphs_list_of_tests[graph_path]):
                break
            newpath = '/newpath-not-set'
            for test in graphs_list_of_tests[graph_path]:
                log.info(f"Running: {test.name}")
                if test.comment:
                    status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, new_newpath = run_protocol_test(
                        self.engine_manager, test, test.comment, newpath)
                    if new_newpath != '':
                        newpath = new_newpath
                    self.update_test_status(test, status, error_type)
                else:
                    extracted_sent_requests = ''
                    extracted_expected_responses = ''
                    got_responses = ''
                setattr(test, 'protocol', test.comment)
                setattr(test, 'protocol_sent', extracted_sent_requests)
                setattr(
                    test,
                    'response_extracted',
                    extracted_expected_responses)
                setattr(test, 'response', got_responses)
            if os.path.exists('./TestSuite.server-log.txt'):
                server_log = util.read_file(
                    './TestSuite.server-log.txt')
                self.log_for_all_tests(
                    graphs_list_of_tests[graph_path],
                    'server_log',
                    util.remove_date_time_parts(server_log))
            self.engine_manager.cleanup(self.config)

    def analyze(self):
        """
        Method to index and start the server for a specific test.
        """
        graphs_list_of_tests = {k: v for d in self.tests.values() for k, v in d.items()}
        for graph_path in graphs_list_of_tests:
            log.info(f"Running server for graph: {graph_path}")
            if not self.prepare_test_environment(
                    graph_path, graphs_list_of_tests[graph_path]):
                break
            print(f"Listening on: {self.config.server_address}:{self.config.port} ...")
            print("\n" * 3)
            input("Press Enter to shutdown the server and continue...")
            self.engine_manager.cleanup(self.config)

    def run(self):
        """
        Main method to run all tests.
        """
        try:
            self.run_query_tests(self.tests["query"])
            self.run_query_tests(self.tests["format"])
            self.run_update_tests(self.tests["update"])
            self.run_syntax_tests(self.tests["syntax"])
            self.run_protocol_tests(self.tests["protocol"])
            self.run_graphstore_protocol_tests(self.tests["graphstoreprotocol"])
        except KeyboardInterrupt:
            log.warning("Interrupted by user.")
            self.engine_manager.cleanup(self.config)

    def compress_json_bz2(self, input_data, output_filename):
        with bz2.open(output_filename, "wt") as zipfile:
            json.dump(input_data, zipfile, indent=4)
        log.info("Done writing result file: " + output_filename)

    def generate_json_file(self):
        """
        Generates a JSON file with the test results.
        """
        os.makedirs("./results", exist_ok=True)
        file_path = f"./results/{self.name}.json.bz2"
        data = {}

        for test_format in self.tests:
            for graph in self.tests[test_format]:
                for test in self.tests[test_format][graph]:
                    match test.status:
                        case Status.PASSED:
                            self.passed += 1
                        case Status.FAILED:
                            self.failed += 1
                        case Status.INTENDED:
                            self.passed_failed += 1
                    # This will add a number behind the name if the name is not
                    # unique
                    if test.name in data:
                        i = 1
                        while True:
                            i += 1
                            new_name = f"{test.name} {i}"
                            if new_name in data:
                                continue
                            else:
                                test.name = new_name
                                data[new_name] = test.to_dict()
                                break
                    else:
                        data[test.name] = test.to_dict()
        data["info"] = {
            "name": "info",
            "passed": self.passed,
            "tests": self.test_count,
            "failed": self.failed,
            "passedFailed": self.passed_failed,
            "notTested": (
                self.test_count -
                self.passed -
                self.failed -
                self.passed_failed)}
        log.info("Writing file...")
        self.compress_json_bz2(data, file_path)
