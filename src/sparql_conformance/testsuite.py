import bz2
import io
import json
import os
import re
from typing import List, Dict, Tuple

import rdflib

import sparql_conformance.util as util
try:
    from qlever.log import log
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    log = logging.getLogger(__name__)
from sparql_conformance.config import Config
try:
    from sparql_conformance.engines.graphdb_manager import GraphdbManager
except ImportError:
    GraphdbManager = None
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.mock_sparql_server import MockSPARQLServer
from sparql_conformance.json_tools import compare_json
from sparql_conformance.protocol_tools import (
    run_graphstore_protocol_test_from_action,
    run_protocol_test,
    run_protocol_test_from_action,
)
from sparql_conformance.rdf_tools import compare_ttl
from sparql_conformance.test_object import TestObject, Status, ErrorMessage
from sparql_conformance.tsv_csv_tools import compare_sv
from sparql_conformance.xml_tools import compare_xml


def _augment_with_protocol_data(
        graph_path: Tuple[Tuple[str, str], ...],
) -> Tuple[Tuple[str, str], ...]:
    """Add the standard W3C protocol test data files (data0–data3.rdf)."""
    path_to_data = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    result = graph_path
    for i in range(4):
        result = result + ((
            os.path.join(path_to_data, f"data{i}.rdf"),
            f"http://kasei.us/2009/09/sparql/data/data{i}.rdf",
        ),)
    return result


def _get_mock_host(config) -> str:
    """Return the host address that containerized engines can use to reach the mock server."""
    import platform
    import subprocess
    import socket as _socket
    system = getattr(config, 'system', 'native')
    if system not in ('docker', 'podman'):
        return '127.0.0.1'
    if platform.system() == 'Darwin':
        return 'host.docker.internal'
    cmd = 'docker' if system == 'docker' else 'podman'
    network = 'bridge' if system == 'docker' else 'podman'
    try:
        result = subprocess.run(
            [cmd, 'network', 'inspect', network,
             '--format', '{{range .IPAM.Config}}{{.Gateway}}{{end}}'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        if not ip.startswith('127.'):
            return ip
    except Exception:
        pass
    return '172.17.0.1'


class TestSuite:
    """
    A class to represent a test suite for SPARQL using QLever.
    """

    def __init__(self, name: str, tests: Dict[str, Dict[Tuple[Tuple[str, str], ...], List[TestObject]]], test_count, config: Config, engine_manager: EngineManager, results_dir: str = "./results"):
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
        self.results_dir = results_dir

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
        expected_html = ""
        test_html = ""
        expected_red = ""
        test_red = ""
        try:
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
        except Exception as e:
            status = Status.FAILED
            error_type = ErrorMessage.FORMAT_ERROR
            setattr(
                test,
                "query_log",
                f"Format error: {e}\nResponse:\n{query_result}",
            )

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
        if GraphdbManager and isinstance(self.engine_manager, GraphdbManager):
            union_graph = rdflib.Graph()
            for expected_graph in expected_graphs:
                union_graph.parse(data=expected_graph, format="turtle")
            expected_graphs = list(expected_graphs)
            expected_graphs[0] = union_graph.serialize(format="turtle")

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
        if index_success and server_success and "Syntax" in list_of_tests[0].type_name:
            self.engine_manager.activate_syntax_test_mode(self.config.server_address, self.config.port)
        self.log_for_all_tests(list_of_tests, "index_log", index_log)
        self.log_for_all_tests(list_of_tests, "server_log", server_log)
        return index_success and server_success

    def process_failed_response(self, test, query_response: tuple):
        body = query_response[1]
        if "exception" in body:
            try:
                query_log = json.loads(body)["exception"].replace(";", ";\n")
            except Exception:
                query_log = body
            error_type = ErrorMessage.QUERY_EXCEPTION
        elif "HTTP Request" in body:
            error_type = ErrorMessage.REQUEST_ERROR
            query_log = body
        elif "not supported" in body:
            error_type = ErrorMessage.NOT_SUPPORTED
            if "content type" in body:
                error_type = ErrorMessage.CONTENT_TYPE_NOT_SUPPORTED
            query_log = body
        elif re.search(r'arqinternalerrorexception|peek\s+iterator\s+is\s+already\s+empty', body, re.IGNORECASE):
            error_type = ErrorMessage.ENGINE_INTERNAL_ERROR
            query_log = body
        elif re.search(r'\b404\b|\bnot\s+found\b', body, re.IGNORECASE):
            error_type = ErrorMessage.HTTP_NOT_FOUND
            query_log = body
        elif re.search(r'undefined\s+procedure|unknown\s+function', body, re.IGNORECASE):
            error_type = ErrorMessage.UNDEFINED_FUNCTION
            query_log = body
        elif re.search(r'required\s+argument.*not\s+supplied', body, re.IGNORECASE):
            error_type = ErrorMessage.FUNCTION_ARGUMENT_ERROR
            query_log = body
        elif re.search(r'non\s+numeric\s+argument|needs\s+a\s+(string|datetime)|cannot\s+convert.*to\s+datetime', body, re.IGNORECASE):
            error_type = ErrorMessage.TYPE_ERROR
            query_log = body
        elif re.search(r'sparql\s+compiler|transitive\s+start\s+not\s+given|no\s+column\s+', body, re.IGNORECASE):
            error_type = ErrorMessage.PARSE_ERROR
            query_log = body
        else:
            error_type = ErrorMessage.UNDEFINED_ERROR
            query_log = body
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
                        test.result_file,
                        query_result[1],
                        test,
                        test.result_format)
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
            if not self.prepare_test_environment(graph, graphs_list_of_tests[graph]):
                continue
            for i, test in enumerate(graphs_list_of_tests[graph]):
                log.info(f"Running: {test.name}")
                if i > 0:
                    if not self.engine_manager.reset_graphs(self.config, graph):
                        self.update_graph_status(
                            graphs_list_of_tests[graph][i:],
                            Status.FAILED, ErrorMessage.SERVER_ERROR)
                        break
                # Execute the update query.
                query_update_result = self.engine_manager.update(self.config, test.query_file)

                # If the update query was successful, retrieve the current state of all graphs
                # and check if the results match the expected results.
                if 200 <= query_update_result[0] < 300:
                    actual_state_of_graphs = []
                    expected_state_of_graphs = []
                    # Handle default graph that has no uri
                    default_graph_query = self.engine_manager.default_graph_construct_query()
                    construct_graph = self.engine_manager.query(
                        self.config,
                        default_graph_query,
                        "ttl",
                    )
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

                if not (200 <= query_result[0] < 400):
                    self.process_failed_response(test, query_result)
                else:
                    setattr(test, "query_log", query_result[1])
                    self.update_test_status(test, Status.PASSED, "")
                if test.type_name in ("NegativeSyntaxTest11", "NegativeUpdateSyntaxTest11",
                                     "NegativeSyntaxTest", "NegativeUpdateSyntaxTest"):
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
            # Work around for issue #25: add standard protocol test data files
            graph_paths = _augment_with_protocol_data(graph_path)
            if not self.prepare_test_environment(
                    graph_paths, graphs_list_of_tests[graph_path]):
                continue
            for i, test in enumerate(graphs_list_of_tests[graph_path]):
                log.info(f"Running: {test.name}")
                if i > 0:
                    if not self.engine_manager.reset_graphs(self.config, graph_paths):
                        self.update_graph_status(
                            graphs_list_of_tests[graph_path][i:],
                            Status.FAILED, ErrorMessage.SERVER_ERROR)
                        break
                extracted_sent_requests = ''
                extracted_expected_responses = ''
                got_responses = ''
                if test.protocol_requests:
                    status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, newpath = run_protocol_test_from_action(
                        self.engine_manager, test, test.protocol_requests, '')
                    self.update_test_status(test, status, error_type)
                elif test.comment:
                    status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, newpath = run_protocol_test(
                        self.engine_manager, test, test.comment, '')
                    self.update_test_status(test, status, error_type)
                setattr(test, "protocol", test.comment)
                setattr(test, "protocol_sent", extracted_sent_requests)
                setattr(test, "response_extracted", extracted_expected_responses)
                setattr(test, "response", got_responses)
            if os.path.exists("./TestSuite.server-log.txt"):
                server_log = util.read_file("./TestSuite.server-log.txt")
                self.log_for_all_tests(
                    graphs_list_of_tests[graph_path],
                    "server_log",
                    util.remove_date_time_parts(server_log))
            self.engine_manager.cleanup(self.config)

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
                if test.protocol_requests:
                    status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, new_newpath = run_protocol_test_from_action(
                        self.engine_manager, test, test.protocol_requests, newpath)
                    if new_newpath != '':
                        newpath = new_newpath
                    self.update_test_status(test, status, error_type)
                elif test.comment:
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

    def run_structured_graphstore_protocol_tests(self, graphs_list_of_tests: Dict[Tuple[Tuple[str, str], ...], List[TestObject]]):
        """
        Executes structured graph store protocol tests from ht:Connection actions.

        Tests whose mf:requires features are not supported by the engine (see
        EngineManager.supported_graphstore_features) are skipped and reported as
        an intended deviation rather than being run and failed.
        """
        for graph_path in graphs_list_of_tests:
            log.info(f'Running structured graphstore protocol tests for graph: {graph_path}')
            if not self.prepare_test_environment(
                    graph_path, graphs_list_of_tests[graph_path]):
                continue
            supported_features = self.engine_manager.supported_graphstore_features()
            for i, test in enumerate(graphs_list_of_tests[graph_path]):
                log.info(f"Running: {test.name}")
                missing_features = {
                    util.local_name(req) for req in test.requires
                } - supported_features
                if missing_features:
                    log.info(
                        f"Skipping {test.name}: engine does not support "
                        f"{sorted(missing_features)}")
                    self.update_test_status(
                        test, Status.INTENDED, ErrorMessage.NOT_SUPPORTED)
                    continue
                if i > 0:
                    if not self.engine_manager.reset_graphs(self.config, graph_path):
                        self.update_graph_status(
                            graphs_list_of_tests[graph_path][i:],
                            Status.FAILED, ErrorMessage.SERVER_ERROR)
                        break
                status, error_type, extracted_expected_responses, extracted_sent_requests, got_responses, newpath = run_graphstore_protocol_test_from_action(
                    test,
                    test.protocol_requests or [])
                self.update_test_status(test, status, error_type)
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

    def run_federation_tests(self, graphs_list_of_tests: Dict[Tuple[Tuple[str, str], ...], List[TestObject]]):
        """
        Executes SERVICE federation tests.

        For each test a local mock SPARQL server is started, populated with the
        data from qt:serviceData in the manifest, and the SERVICE endpoint URLs
        in the query are rewritten in memory to point at the mock before the
        query is sent to the main engine.  Test files on disk are not modified.
        """
        for graph_key in graphs_list_of_tests:
            for test in graphs_list_of_tests[graph_key]:
                log.info(f"Running federation test: {test.name}")

                service_data = test.action_node.get('serviceData', []) if isinstance(test.action_node, dict) else []
                if isinstance(service_data, dict):
                    service_data = [service_data]

                mock = MockSPARQLServer()
                url_map: dict = {}
                for sd in service_data:
                    endpoint_url = sd.get('endpoint') if isinstance(sd, dict) else None
                    data_path = sd.get('data') if isinstance(sd, dict) else None
                    if endpoint_url and data_path:
                        mock.add_endpoint(endpoint_url, util.read_file(data_path))
                        url_map[endpoint_url] = None
                mock.start()
                mock_host = _get_mock_host(self.config)
                for orig in url_map:
                    url_map[orig] = mock.local_url_for(orig, host=mock_host)

                query_text = test.query_file
                for orig, local in url_map.items():
                    query_text = query_text.replace(orig, local)

                if not self.prepare_test_environment(graph_key, [test]):
                    mock.stop()
                    continue

                query_result = self.engine_manager.query(self.config, query_text, test.result_format)

                if os.path.exists('./TestSuite.server-log.txt'):
                    setattr(test, 'server_log', util.remove_date_time_parts(
                        util.read_file('./TestSuite.server-log.txt')))

                mock.stop()
                self.engine_manager.cleanup(self.config)

                if query_result[0] == 200:
                    self.evaluate_query(test.result_file, query_result[1], test, test.result_format)
                else:
                    self.process_failed_response(test, query_result)

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
            self.run_structured_graphstore_protocol_tests(
                self.tests.get("graphstoreprotocol_structured", {}))
            self.run_federation_tests(self.tests.get("federation", {}))
        except KeyboardInterrupt:
            log.warning("Interrupted by user.")
            self.engine_manager.cleanup(self.config)

    def compress_json_bz2(self, input_data, output_filename):
        with bz2.BZ2File(output_filename, "w") as raw_file:
            with io.TextIOWrapper(raw_file, encoding="utf-8") as zipfile:
                json.dump(input_data, zipfile, indent=4)
        log.info("Done writing result file: " + output_filename)

    def build_results_dict(self) -> tuple[dict, dict]:
        """Returns (tests_data, info) for the suite without writing to disk."""
        data = {}
        passed = failed = passed_failed = 0

        for test_format in self.tests:
            for graph in self.tests[test_format]:
                for test in self.tests[test_format][graph]:
                    match test.status:
                        case Status.PASSED:
                            passed += 1
                        case Status.FAILED:
                            failed += 1
                        case Status.INTENDED:
                            passed_failed += 1
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

        info = {
            "passed": passed,
            "tests": self.test_count,
            "failed": failed,
            "passedFailed": passed_failed,
            "notTested": self.test_count - passed - failed - passed_failed,
        }
        return data, info

    def generate_json_file(self):
        """Generates a JSON file with the test results (single-suite v1 format)."""
        os.makedirs(self.results_dir, exist_ok=True)
        data, info = self.build_results_dict()
        data["info"] = {"name": "info", **info}
        log.info("Writing file...")
        self.compress_json_bz2(data, os.path.join(self.results_dir, f"{self.name}.json.bz2"))
