from enum import Enum
from typing import Optional, List, Union, Dict, Any, TYPE_CHECKING

from sparql_conformance.config import Config
from sparql_conformance.util import local_name, read_file, escape
import os
import json

if TYPE_CHECKING:
    from sparql_conformance.protocol_request import ProtocolRequest

class Status(str, Enum):
    PASSED = "Passed"
    INTENDED = "Intended deviation"
    FAILED = "Failed"
    NOT_TESTED = "Not tested"

class ErrorMessage(str, Enum):
    QUERY_EXCEPTION = 'Query error'
    REQUEST_ERROR = 'Request error'
    UNDEFINED_ERROR = 'Undefined error'
    INDEX_BUILD_ERROR = 'Indexing error'
    SERVER_ERROR = 'Server error'
    NOT_TESTED = 'Not tested'
    RESULTS_NOT_THE_SAME = 'Results differ'
    INTENDED_MSG = 'Intended deviation from SPARQL standard'
    EXPECTED_EXCEPTION = 'Expected error response from query'
    FORMAT_ERROR = 'Result format error'
    NOT_SUPPORTED = 'Not supported'
    CONTENT_TYPE_NOT_SUPPORTED = "Content type not supported"
    PARSE_ERROR = 'Parse error'
    TYPE_ERROR = 'Type error'
    ENGINE_INTERNAL_ERROR = 'Engine internal error'
    HTTP_NOT_FOUND = 'HTTP not found'
    UNDEFINED_FUNCTION = 'Undefined function'
    FUNCTION_ARGUMENT_ERROR = 'Function argument error'

    @classmethod
    def is_query_error(cls, error: str) -> bool:
        """Subset of query-related errors."""
        return error in [
            cls.QUERY_EXCEPTION,
            cls.REQUEST_ERROR,
            cls.NOT_SUPPORTED,
            cls.UNDEFINED_ERROR,
            cls.CONTENT_TYPE_NOT_SUPPORTED,
            cls.ENGINE_INTERNAL_ERROR,
            cls.HTTP_NOT_FOUND,
            cls.PARSE_ERROR,
            cls.TYPE_ERROR,
            cls.FUNCTION_ARGUMENT_ERROR,
            cls.UNDEFINED_FUNCTION,
        ]

def process_graph_data(graph_data: Union[None, str, Dict, List], target_dict: Dict[str, str]) -> None:
    """
    Process graph data and store results in the target dictionary.
    Result: {'label': 'graph', ...}
    """
    if graph_data is None:
        return

    if isinstance(graph_data, str):
        label = graph_data.split('/')[-1]
        target_dict[label] = read_file(graph_data)
        return

    if not isinstance(graph_data, List):
        graph_data = [graph_data]

    for graph_entry in graph_data:
        if isinstance(graph_entry, dict):
            graph_path = graph_entry.get('graph')
            if graph_path:
                label = graph_entry.get('label', graph_path.split('/')[-1])
                target_dict[label] = read_file(graph_path)
        elif isinstance(graph_entry, str):
            label = graph_entry.split('/')[-1]
            target_dict[label] = read_file(graph_entry)


class TestObject:
    """Represents a single SPARQL test case with its configuration and results."""

    def __init__(
            self,
            test: str,
            name: str,
            type_name: str,
            group: str,
            path: str,
            action_node: Optional[Dict[str, Any]],
            result_node: Optional[Dict[str, Any]],
            approval: Optional[str],
            approved_by: Optional[str],
            comment: Optional[str],
            entailment_regime: Optional[str],
            entailment_profile: Optional[str],
            feature: List[str],
            config: Config,
            protocol_requests: Optional[List['ProtocolRequest']] = None,
            requires: Optional[List[str]] = None,
    ):
        """
        Initialize a test object with all its properties.

        Args:
            test: Test URI
            name: Test name
            type_name: Type of the test
            group: Test group identifier
            path: Path to test files
            action_node: Node containing test actions
            result_node: Node containing expected results
            approval: Test approval status
            approved_by: Approver identifier
            comment: Test description/comment
            entailment_regime: SPARQL entailment regime
            entailment_profile: Entailment profile
            feature: List of test features
            config: Test configuration
        """
        self.test = test
        self.name = name
        self.type_name = type_name
        self.group = group
        self.path = path
        self.action_node = action_node
        self.result_node = result_node
        self.approval = approval
        self.approved_by = approved_by
        self.comment = comment
        self.entailment_regime = entailment_regime
        self.entailment_profile = entailment_profile
        self.feature = feature
        self.config = config
        self.protocol_requests = protocol_requests
        self.requires = requires or []

        self.status = Status.NOT_TESTED
        self.index_files: Dict[str, str] = {}
        self.result_files: Dict[str, str] = {}

        # Process action node
        if isinstance(action_node, dict):
            self.query = local_name(action_node.get('query', 'no query'))
            self.graph = local_name(action_node.get('data', 'no query'))
            self.query_file = read_file(os.path.join(self.path, self.query))
            self.graph_file = read_file(os.path.join(self.path, self.graph))
            process_graph_data(action_node.get('graphData'), self.index_files)
        else:
            self.query = self.graph = self.query_file = self.graph_file = ''

        # Process result node
        if isinstance(result_node, dict):
            self.result = local_name(result_node.get('data', 'no query'))
            self.result_format = self.result[self.result.rfind('.') + 1:]
            self.result_file = read_file(os.path.join(self.path, self.result))
            process_graph_data(result_node.get('graphData'), self.result_files)
        else:
            self.result = self.result_file = self.result_format = ''

        # Initialize test execution results
        self.error_type = ''
        self.expected_html = ''
        self.got_html = ''
        self.expected_html_red = ''
        self.got_html_red = ''
        self.index_log = ''
        self.server_log = ''
        self.server_status = ''
        self.query_result = ''
        self.query_answer = ''
        self.query_log = ''
        self.query_sent = ''
        self.protocol = ''
        self.protocol_sent = ''
        self.response_extracted = ''
        self.response = ''
        self.response_not_matching = ''

    def __repr__(self) -> str:
        """Return string representation of the test object."""
        return f'<TestObject name={self.name}, type={self.type_name}, uri={self.test}>'

    def to_dict(self) -> Dict[str, str]:
        """Convert test object to dictionary format for serialization."""
        graph_html = '<b>default:</b> <br> <pre>' + escape(self.graph_file) + '</pre>'
        for name, graph in self.index_files.items():
            graph_html += f'<br><b>{name}:</b> <br> <pre>{escape(graph)}</pre>'

        return {
            'test': escape(self.test),
            'typeName': escape(self.type_name),
            'name': escape(self.name),
            'group': escape(self.group),
            'feature': escape(';'.join(self.feature)),
            'requires': escape(';'.join(self.requires)),
            'comment': escape(self.comment),
            'approval': escape(self.approval),
            'approvedBy': escape(self.approved_by),
            'query': escape(self.query),
            'graph': escape(self.graph),
            'queryFile': escape(self.query_file),
            'graphFile': graph_html,
            'resultFile': escape(self.result_file),
            'status': escape(self.status),
            'errorType': escape(self.error_type),
            'expectedHtml': self.expected_html,
            'gotHtml': self.got_html,
            'expectedHtmlRed': self.expected_html_red,
            'gotHtmlRed': self.got_html_red,
            'indexLog': escape(self.index_log),
            'serverLog': escape(self.server_log),
            'serverStatus': escape(self.server_status),
            'queryResult': escape(self.query_result),
            'queryAnswer': escape(self.query_answer),
            'queryLog': escape(self.query_log),
            'querySent': escape(self.query_sent),
            'regime': escape(self.entailment_regime),
            'protocol': escape(self.protocol),
            'protocolSent': escape(self.protocol_sent),
            'responseExtracted': escape(self.response_extracted),
            'response': escape(self.response),
            'notMatching': escape(self.response_not_matching),
            'config': escape(json.dumps(self.config.to_dict(), indent=4)),
            'indexFiles': escape(json.dumps(self.index_files, indent=4)),
            'resultFiles': escape(json.dumps(self.result_files, indent=4))
        }