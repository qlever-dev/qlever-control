import json
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from argparse import Namespace
from typing import Tuple, List
import requests
import rdflib

from qlever.commands.query import QueryCommand
from qlever.log import mute_log
from qlever.util import run_command
from qlever.commands.start import StartCommand
from qlever.commands.stop import StopCommand
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance import util
from qlever.commands.index import IndexCommand
from sparql_conformance.rdf_tools import write_ttl_file, delete_ttl_file, rdf_xml_to_turtle

_SPARQL_RESULTS_NS = "http://www.w3.org/2005/sparql-results#"
_RS = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")


def _sparql_xml_to_result_set_ttl(xml_str: str) -> str:
    """Convert SPARQL results XML to SPARQL result-set Turtle vocabulary.

    QLever returns SPARQL Results XML when Accept: application/sparql-results+xml
    is sent for SELECT/ASK queries.  We fetch XML instead and convert here so
    that compare_ttl() can compare against .ttl expected-result files from the
    SPARQL 1.0 test suite.
    """
    g = rdflib.Graph()
    g.bind("rs", _RS)

    root = ET.fromstring(xml_str)
    ns = _SPARQL_RESULTS_NS

    result_set = rdflib.BNode()
    g.add((result_set, rdflib.RDF.type, _RS.ResultSet))

    boolean_elem = root.find(f"{{{ns}}}boolean")
    if boolean_elem is not None:
        val = (boolean_elem.text or "").strip().lower() == "true"
        g.add((result_set, _RS.boolean,
               rdflib.Literal(val, datatype=rdflib.XSD.boolean)))
        return g.serialize(format="turtle")

    head = root.find(f"{{{ns}}}head")
    if head is not None:
        for var in head.findall(f"{{{ns}}}variable"):
            g.add((result_set, _RS.resultVariable,
                   rdflib.Literal(var.get("name"))))

    results_elem = root.find(f"{{{ns}}}results")
    if results_elem is not None:
        for result_elem in results_elem.findall(f"{{{ns}}}result"):
            solution = rdflib.BNode()
            g.add((result_set, _RS.solution, solution))
            for binding_elem in result_elem.findall(f"{{{ns}}}binding"):
                name = binding_elem.get("name")
                binding_node = rdflib.BNode()
                g.add((solution, _RS.binding, binding_node))
                g.add((binding_node, _RS.variable, rdflib.Literal(name)))

                uri_elem = binding_elem.find(f"{{{ns}}}uri")
                lit_elem = binding_elem.find(f"{{{ns}}}literal")
                bnode_elem = binding_elem.find(f"{{{ns}}}bnode")

                if uri_elem is not None:
                    text = (uri_elem.text or "").strip()
                    g.add((binding_node, _RS.value, rdflib.URIRef(text)))
                elif lit_elem is not None:
                    text = lit_elem.text or ""
                    datatype = lit_elem.get("datatype")
                    lang = lit_elem.get(
                        "{http://www.w3.org/XML/1998/namespace}lang")
                    if datatype:
                        g.add((binding_node, _RS.value,
                               rdflib.Literal(text,
                                              datatype=rdflib.URIRef(datatype))))
                    elif lang:
                        g.add((binding_node, _RS.value,
                               rdflib.Literal(text, lang=lang)))
                    else:
                        g.add((binding_node, _RS.value, rdflib.Literal(text)))
                elif bnode_elem is not None:
                    text = (bnode_elem.text or "").strip()
                    g.add((binding_node, _RS.value, rdflib.BNode(text)))

    return g.serialize(format="turtle")


def _is_select_or_ask(query: str) -> bool:
    """Return True for SELECT/ASK queries; CONSTRUCT/DESCRIBE return RDF so False."""
    no_comments = re.sub(r'#[^\n]*', '', query)
    m = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', no_comments, re.IGNORECASE)
    return m is not None and m.group(1).upper() in ('SELECT', 'ASK')


class QLeverManager(EngineManager):
    """Manager for QLever using docker execution"""

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        return self._query(config, query, "ru", "json")

    def protocol_endpoint(self) -> str:
        return "sparql"

    def cleanup(self, config: Config):
        self._stop_server(config)
        with mute_log():
            run_command('rm -f qlever-sparql-conformance*')

    def query(self, config: Config, query: str, result_format: str) -> Tuple[int, str]:
        if result_format == "ttl" and _is_select_or_ask(query):
            # QLever ignores Accept: text/turtle for SELECT/ASK and returns JSON.
            # Fetch as SPARQL results XML and convert to rs: Turtle vocabulary.
            status, body = self._query(config, query, "rq", "srx")
            if status != 200:
                return status, body
            try:
                return 200, _sparql_xml_to_result_set_ttl(body)
            except Exception as e:
                return 1, str(e)
        return self._query(config, query, "rq", result_format)

    def _query(self, config: Config, query: str, query_type: str, result_format: str) -> Tuple[int, str]:
        content_type = "query=" if query_type == "rq" else "update="
        args = util.make_args(
            config,
            accept=util.get_accept_header(result_format),
            query=query,
            content_type=content_type,
        )

        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, True)
                body, _, status_line = qc.query_output.rpartition("HTTP_STATUS:")
                status = int(status_line.strip())
            return status, body
        except Exception as e:
            return 1, str(e)

    def setup(self, config: Config, graph_paths: Tuple[Tuple[str, str], ...]) -> Tuple[bool, bool, str, str]:
        server_success = False
        graphs = []
        for graph_path, graph_name in graph_paths:
            # Handle rdf files by turning them into turtle format.
            if graph_path.endswith(".rdf"):
                graph_path_new = Path(graph_path).name
                graph_path_new = graph_path_new.replace(".rdf", ".ttl")
                write_ttl_file(graph_path_new, rdf_xml_to_turtle(graph_path, graph_name))
                graph_path = graph_path_new
            else:
                graph_path = util.copy_graph_to_workdir(graph_path, os.getcwd())
            graphs.append((graph_path, graph_name))

        index_success, index_log = self._index(config, graphs)
        for path, name in graphs:
            delete_ttl_file(path)
        if not index_success:
            return index_success, server_success, index_log, ''
        else:
            server_success, server_log = self._start_server(config)
            print(server_log)
            print(server_success)

            if not server_success:
                return index_success, server_success, index_log, server_log
        return index_success, server_success, index_log, server_log

    def _stop_server(self, config: Config) -> Tuple[bool, str]:
        args = Namespace(
            name='qlever-sparql-conformance',
            port=config.port,
            server_container='qlever-sparql-conformance-server-container',
            no_containers=config.system == 'native',
            show=False,
            cmdline_regex='ServerMain.* -i [^ ]*%%NAME%%'
        )
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            error_output = str(e)
            return False, error_output
        return result, 'Success'

    def _start_server(self, config: Config) -> Tuple[bool, str]:
        binary = 'qlever-server'
        binary = binary if config.system != 'native' else Path(config.path_to_binaries, binary)
        args = util.make_args(
            config,
            server_binary=binary,
        )
        try:
            with mute_log():
                result = StartCommand().execute(args, called_from_conformance_test=True)
        except Exception as e:
            error_output = str(e)
            return False, error_output

        server_log = ''
        if os.path.exists('./qlever-sparql-conformance.server-log.txt'):
            server_log = util.read_file('./qlever-sparql-conformance.server-log.txt')
        return result, server_log

    def _index(self, config: Config, graph_paths: List[Tuple[str, str]]) -> Tuple[bool, str]:
        binary = 'qlever-index'
        index_binary = binary if config.system != 'native' else Path(config.path_to_binaries, binary)
        args = util.make_args(
            config,
            multi_input_json=self._generate_multi_input_json(graph_paths),
            index_binary=index_binary
        )
        try:
            with mute_log():
                result = IndexCommand().execute(args=args, called_from_conformance_test=True)
        except Exception as e:
            error_output = str(e)
            return False, error_output

        index_log = ''
        if os.path.exists("./qlever-sparql-conformance.index-log.txt"):
            index_log = util.read_file("./qlever-sparql-conformance.index-log.txt")
        # Docker tee/pipefail workaround: verify the index was actually completed.
        # When QLever index builder fails inside docker, the tee exit code masks
        # the failure and IndexCommand returns True.  meta-data.json is only
        # written on successful completion.
        if result and not os.path.exists("qlever-sparql-conformance.meta-data.json"):
            result = False
        return result, index_log

    _FORMAT_BY_EXTENSION = {
        '.ttl': 'ttl',
        '.trig': 'trig',
        '.nt': 'nt',
        '.nq': 'nq',
        '.rdf': 'rdf',
        '.xml': 'rdf',
    }

    def _format_for_file(self, path: str) -> str:
        ext = Path(path).suffix.lower()
        return self._FORMAT_BY_EXTENSION.get(ext, 'ttl')

    def _generate_multi_input_json(self, graph_paths: List[Tuple[str, str]]) -> str:
        """Generate the JSON input for multi_input_json in IndexCommand.execute()"""
        input_list = []
        for graph_path, graph_name in graph_paths:
            entry = {
                'cmd': f'cat {graph_path}',
                'graph': graph_name if graph_name else '-',
                'format': self._format_for_file(graph_path)
            }
            input_list.append(entry)
        return json.dumps(input_list)

    def default_graph_construct_query(self) -> str:
        return "CONSTRUCT {?s ?p ?o} WHERE { GRAPH ql:default-graph {?s ?p ?o}}"

    def activate_syntax_test_mode(self, server_address, port):
        url = f'http://{server_address}:{port}'
        params = {
            "access-token": "abc",
            "syntax-test-mode": "true"
        }
        requests.get(url, params)
