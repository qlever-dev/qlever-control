import os
import re
from pathlib import Path
from rdflib import Graph, Namespace, RDF, URIRef
from typing import Union, Dict, Any, List, Tuple, Optional, Set

from .config import Config
from .util import uri_to_path, local_name
from .test_object import TestObject
from .protocol_request import ProtocolHeader, ProtocolRequest, ProtocolResponse

# Namespaces
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
DAWGT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
HT = Namespace("http://www.w3.org/2011/http#")
CNT = Namespace("http://www.w3.org/2011/content#")


def extract_protocol_requests(graph: Graph, action_node: Any) -> Optional[List[ProtocolRequest]]:
    """
    Extract structured protocol requests from a ht:Connection RDF node.
    Returns None when the action node is not a ht:Connection (old-style manifest).
    """
    if action_node is None:
        return None
    connection_type = graph.value(action_node, RDF.type)
    if str(connection_type) != str(HT.Connection):
        return None

    authority = str(graph.value(action_node, HT.connectionAuthority) or "")
    requests_head = graph.value(action_node, HT.requests)
    if requests_head is None:
        return None

    protocol_requests: List[ProtocolRequest] = []
    for req_node in graph.items(requests_head):
        method = str(graph.value(req_node, HT.methodName) or "GET")
        absolute_path = str(graph.value(req_node, HT.absolutePath) or "/")
        http_version = str(graph.value(req_node, HT.httpVersion) or "1.1")

        body: Optional[str] = None
        character_encoding = "UTF-8"
        body_node = graph.value(req_node, HT.body)
        if body_node is not None:
            chars = graph.value(body_node, CNT.chars)
            if chars is not None:
                body = str(chars)
            enc = graph.value(body_node, CNT.characterEncoding)
            if enc is not None:
                character_encoding = str(enc)

        headers: List[ProtocolHeader] = []
        headers_head = graph.value(req_node, HT.headers)
        if headers_head is not None:
            for header_node in graph.items(headers_head):
                name = graph.value(header_node, HT.fieldName)
                value = graph.value(header_node, HT.fieldValue)
                if name is not None and value is not None:
                    headers.append(ProtocolHeader(name=str(name), value=str(value)))

        status_codes: List[str] = []
        expected_boolean: Optional[bool] = None
        expected_format: Optional[str] = None
        expectation: Optional[str] = None
        resp_node = graph.value(req_node, HT.resp)
        if resp_node is not None:
            for status_uri in graph.objects(resp_node, MF.expectedStatus):
                lname = local_name(str(status_uri))
                m = re.match(r'StatusCode(\w+)', lname)
                if m:
                    status_codes.append(m.group(1))
            bool_val = graph.value(resp_node, MF.expectedBoolean)
            if bool_val is not None:
                expected_boolean = bool(bool_val)
            fmt = graph.value(resp_node, MF.expectedFormat)
            if fmt is not None:
                expected_format = str(fmt)
            exp = graph.value(resp_node, MF.expectation)
            if exp is not None:
                expectation = str(exp)

        protocol_requests.append(ProtocolRequest(
            method=method,
            absolute_path=absolute_path,
            http_version=http_version,
            connection_authority=authority,
            headers=headers,
            body=body,
            character_encoding=character_encoding,
            expected_response=ProtocolResponse(
                status_codes=status_codes,
                expected_boolean=expected_boolean,
                expected_format=expected_format,
                expectation=expectation,
            ),
        ))

    return protocol_requests if protocol_requests else None


def collect_tests_by_graph(tests: List[TestObject]) -> Dict[str, Dict[Tuple[Tuple[str, str], ...], List[TestObject]]]:
    """
    Groups tests by their graph references and categories.
    The resulting dictionary has the following structure:
    {'query': { (('graph_path', 'graph_name'), ...): [Test1, Test2, ...], ...}, ...}
    """
    if len(tests) == 0:
        return {}
    type_to_category: Dict[str, str] = {
        'QueryEvaluationTest': 'query',
        'CSVResultFormatTest': 'format',
        'UpdateEvaluationTest': 'update',
        'PositiveSyntaxTest': 'syntax',
        'NegativeSyntaxTest': 'syntax',
        'PositiveSyntaxTest11': 'syntax',
        'NegativeSyntaxTest11': 'syntax',
        'PositiveUpdateSyntaxTest11': 'syntax',
        'NegativeUpdateSyntaxTest11': 'syntax',
        'ProtocolTest': 'protocol',
        'GraphStoreProtocolTest': 'graphstoreprotocol',
        'ServiceDescriptionTest': 'service',
    }

    graph_index: Dict[str, Dict[Tuple[Tuple[str, str], ...], List[TestObject]]] = {
        'query': dict(),
        'format': dict(),
        'update': dict(),
        'syntax': dict(),
        'protocol': dict(),
        'graphstoreprotocol': dict(),
        'service': dict(),
    }

    fallback_graph = (str(Path(__file__).parent / 'data' / 'empty.ttl'), '-')

    for test in tests:
        if isinstance(test.action_node, dict):
            graph_refs: List[Tuple[str, str]] = []

            if "data" in test.action_node:
                graph_refs.append((test.action_node["data"], "-"))
            else:
                graph_refs.append(fallback_graph)

            graph_data = test.action_node.get("graphData")
            if isinstance(graph_data, list):
                for entry in graph_data:
                    if isinstance(entry, dict):
                        graph_file = entry.get("graph")
                        label = entry.get("label")
                        if graph_file:
                            graph_refs.append((graph_file, label))
                    else:
                        graph_refs.append((entry, entry.split('/')[-1]))
            elif isinstance(graph_data, dict):
                graph_file = graph_data.get("graph")
                label = graph_data.get("label")
                if graph_file:
                    graph_refs.append((graph_file, label))
            elif isinstance(graph_data, str):
                graph_refs.append((graph_data, graph_data.split('/')[-1]))
        else:
            graph_refs = [fallback_graph]

        key = tuple(sorted(set(graph_refs)))
        category = type_to_category.get(test.type_name)
        if category:
            if key in graph_index[category]:
                graph_index[category][key].append(test)
            else:
                graph_index[category][key] = [test]

    return graph_index


def parse_node(graph: Graph, node: Any) -> Union[str, Dict[str, Any], None]:
    """
    Parse a RDF-node and convert it into an object.
    """
    if isinstance(node, URIRef):
        return str(node)
    if node is None:
        return None
    if node.__class__.__name__ == "Literal":
        return str(node)

    value_dict: Dict[str, Union[str, List[str]]] = {}
    for p, o in graph.predicate_objects(node):
        key = local_name(str(p))
        if key == 'request':
            key = 'query'
        value = uri_to_path(parse_node(graph, o))

        if key in value_dict:
            if isinstance(value_dict[key], list):
                value_dict[key].append(value)
            else:
                value_dict[key] = [value_dict[key], value]
        else:
            value_dict[key] = value

    return value_dict


def load_tests_from_manifest(
        manifest_path: str,
        config: Config,
        visited: Optional[Set[str]] = None
) -> List[TestObject]:
    """
    Load tests from a manifest file and all included sub-manifests.
    """
    if visited is None:
        visited = set()

    manifest_abs_path = os.path.abspath(manifest_path)
    if manifest_abs_path in visited:
        return []
    visited.add(manifest_abs_path)

    g = Graph()
    g.parse(manifest_abs_path, format="turtle")
    tests: List[TestObject] = []
    sub_manifest_paths: List[str] = []

    for collection in g.objects(None, MF.entries):
        for test_uri in g.items(collection):
            test_type = g.value(test_uri, RDF.type)
            if not isinstance(test_type, URIRef):
                continue

            test_type = str(local_name(test_type))
            name = g.value(test_uri, MF.name)
            action_node = g.value(test_uri, MF.action)
            result_node = g.value(test_uri, MF.result)

            action = parse_node(g, action_node)
            if isinstance(action, str):
                action = {"query": action}
            result = parse_node(g, result_node)
            if isinstance(result, str):
                result = {"data": result}

            approval = g.value(test_uri, DAWGT.approval)
            approved_by = g.value(test_uri, DAWGT.approvedBy)
            comment = g.value(test_uri, RDFS.comment)

            feature = [str(f) for f in g.objects(test_uri, MF.feature) if isinstance(f, URIRef)]
            path = manifest_abs_path.split("manifest.ttl")[0]
            entailment_regime = g.value(test_uri, SD.entailmentRegime)
            entailment_profile = g.value(test_uri, SD.entailmentProfile)
            group = os.path.basename(os.path.normpath(path))
            if str(name) in config.exclude or group in config.exclude:
                continue
            if config.include and str(name) not in config.include and group not in config.include:
                continue

            protocol_requests = None
            if test_type in ('ProtocolTest', 'GraphStoreProtocolTest'):
                protocol_requests = extract_protocol_requests(g, action_node)

            tests.append(TestObject(
                test=str(test_uri),
                name=str(name),
                type_name=test_type,
                group=group,
                path=path,
                action_node=action,
                result_node=result,
                approval=str(approval) if approval else None,
                approved_by=str(approved_by) if approved_by else None,
                comment=str(comment) if comment else None,
                entailment_regime=str(entailment_regime) if entailment_regime else None,
                entailment_profile=str(entailment_profile) if entailment_profile else None,
                feature=feature,
                config=config,
                protocol_requests=protocol_requests,
            ))

    for include_list in g.objects(None, MF.include):
        for sub_manifest_uri in g.items(include_list):
            sub_manifest_path = uri_to_path(sub_manifest_uri)
            sub_manifest_path = os.path.normpath(sub_manifest_path)

            if os.path.exists(sub_manifest_path):
                sub_manifest_paths.append(sub_manifest_path)
                tests.extend(load_tests_from_manifest(
                    sub_manifest_path,
                    config,
                    visited=visited
                ))

    return tests


def extract_tests(config: Config) -> Tuple[Dict[str, Dict[Tuple[Tuple[str, str], ...], List[TestObject]]], int]:
    """
    Extract tests from the SPARQL testsuite manifest file.

    Returns:
        Tuple:
        - A dictionary grouped by categories
        - Number of tests
    """
    manifest_all = os.path.join(config.path_to_test_suite, 'manifest-all.ttl')
    path_to_manifest = manifest_all if os.path.exists(manifest_all) else os.path.join(config.path_to_test_suite, 'manifest.ttl')
    tests = load_tests_from_manifest(path_to_manifest, config)
    return collect_tests_by_graph(tests), len(tests)