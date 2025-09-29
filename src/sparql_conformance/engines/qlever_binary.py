import os
import time
from typing import Tuple

import requests
import subprocess

from sparql_conformance import util
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.test_object import Config
from sparql_conformance.rdf_tools import write_ttl_file, delete_ttl_file, rdf_xml_to_turtle


class QLeverBinaryManager(EngineManager):
    """Manager for QLever using binary execution"""

    @staticmethod
    def _query(headers: dict[str, str], query: str, url: str) -> tuple[int, str]:
        try:
            response = requests.post(url, headers=headers, data=query.encode("utf-8"))
            return response.status_code, response.content.decode("utf-8")
        except requests.exceptions.RequestException as e:
            return 500, f"Query execution error: {str(e)}"

    def protocol_endpoint(self) -> str:
        return "sparql"

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        url = f"{config.server_address}:{config.port}?access-token=abc"
        headers = {"Content-type": "application/sparql-update; charset=utf-8"}
        return self._query(headers, query, url)

    def cleanup(self, config: Config):
        path_to_server_main = os.path.join(config.path_to_binaries, "ServerMain")
        command_stop_server = f"pkill -f '{path_to_server_main} -i [^ ]*TestSuite'"
        command_remove_index = "rm -f TestSuite.index.* TestSuite.vocabulary.* TestSuite.prefixes TestSuite.meta-data.json TestSuite.index-log.txt"
        self._stop_server(command_stop_server)
        self._remove_index(command_remove_index)

    def setup(self, config: Config, graph_paths: Tuple[Tuple[str, str], ...]) -> Tuple[bool, bool, str, str]:
        path_to_server_main = os.path.join(config.path_to_binaries, "ServerMain")
        path_to_index_builder = os.path.join(config.path_to_binaries, "IndexBuilderMain")
        command_index = f"{path_to_index_builder} -s TestSuite.settings.json -i TestSuite -p false"
        command_start_server = f"{path_to_server_main} -i TestSuite -j 8 -p {config.port} > TestSuite.server-log.txt -a abc"
        server_success = False
        index_success, index_log = self._index(command_index, graph_paths)
        if not index_success:
            return index_success, server_success, index_log, ''
        else:
            server_success, server_log = self._start_server(
                command_start_server,
                config.server_address,
                config.port)
            if not server_success:
                return index_success, server_success, index_log, server_log

        return index_success, server_success, index_log, server_log

    def query(self, config: Config, query: str, result_format: str) -> Tuple[int, str]:
        accept = util.get_accept_header(result_format)
        content_type = "application/sparql-query; charset=utf-8"
        url = f"{config.server_address}:{config.port}"
        headers = {"Accept": accept, "Content-type": content_type}
        return self._query(headers, query, url)

    def _index(self, command_index: str, graph_paths: Tuple[Tuple[str, str], ...]) -> Tuple[bool, str]:
        remove_paths = []
        graphs = ""
        for graph in graph_paths:
            graph_path = graph[0]
            graph_name = graph[1]
            if graph_path.endswith(".rdf"):
                graph_path_new = graph_path.replace(".rdf", ".ttl")
                remove_paths.append(graph_path_new)
                write_ttl_file(graph_path_new, rdf_xml_to_turtle(graph_path, graph_name))
                graph_path = graph_path_new
            graphs += f" -f {graph_path} -F ttl -g {graph_name}"

        status = False
        try:
            cmd = command_index + graphs
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            output, error = process.communicate()
            if process.returncode != 0:
                return status, f"Indexing error: {error.decode('utf-8')} \n \n {output.decode('utf-8')}"
            index_log = output.decode("utf-8")
            if "Index build completed" in index_log:
                status = True
            for path in remove_paths:
                delete_ttl_file(path)
            return status, index_log
        except Exception as e:
            return status, f"Exception executing index command: {str(e)}"

    def _remove_index(self, command_remove_index: str) -> Tuple[bool, str]:
        try:
            subprocess.check_call(command_remove_index, shell=True)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, f"Error removing index files: {e}"

    def _start_server(self, command_start_server: str, server_address: str, port: str) -> Tuple[bool, str]:
        try:
            subprocess.Popen(command_start_server, shell=True)
            return self._wait_for_server_startup(server_address, port)
        except Exception as e:
            return False, f"Exception executing server command: {str(e)}"

    def _stop_server(self, command_stop_server: str) -> str:
        try:
            subprocess.check_call(command_stop_server, shell=True)
            return ""
        except subprocess.CalledProcessError as e:
            return f"Error stopping server: {e}"

    def _wait_for_server_startup(self, server_address: str, port: str) -> Tuple[bool, str]:
        max_retries = 8
        retry_interval = 0.25
        url = f"{server_address}:{port}"
        headers = {"Content-type": "application/sparql-query"}
        test_query = "SELECT ?s ?p ?o { ?s ?p ?o } LIMIT 1"

        for i in range(max_retries):
            try:
                response = requests.post(url, headers=headers, data=test_query)
                if response.status_code == 200:
                    return False, "Server ready!"
            except requests.exceptions.RequestException:
                pass
            time.sleep(retry_interval)

        return False, "Server failed to start within expected time"

    def activate_syntax_test_mode(self, server_address, port):
        url = f'{server_address}:{port}'
        params = {
            "access-token": "abc",
            "syntax-test-mode": "true"
        }
        requests.get(url, params)
