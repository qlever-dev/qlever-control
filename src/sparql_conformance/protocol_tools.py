import telnetlib as telnet
import re
import json
from typing import Tuple

from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.test_object import TestObject, Status, ErrorMessage
from sparql_conformance.rdf_tools import compare_ttl


def prepare_request(engine_manager: EngineManager, test: TestObject, request_with_reponse: str, newpath: str) -> Tuple[str, str]:
    request = request_with_reponse.split('#### Response')[0]
    # Quick fix: change the x-www-url-form-urlencoded content type to x-www-form-urlencoded
    request = request.replace('application/x-www-url-form-urlencoded', 'application/x-www-form-urlencoded')
    if test.type_name == 'GraphStoreProtocolTest':
        request = request.replace(
            '$HOST$', 'localhost')
        request = request.replace(
            '$NEWPATH$', newpath)
    before_header = True
    request_lines = request.splitlines()
    index_header = 0
    index_line_between = 0
    for index, line in enumerate(request_lines):
        line = line.strip()
        request_lines[index] = line
        if not line and not before_header and index_line_between == 0:
            index_line_between = index
        if line.startswith('POST') or line.startswith('GET') or line.startswith(
                'PUT') or line.startswith('DELETE') or line.startswith('HEAD'):
            before_header = False
            index_header = index
            line = line.replace('sparql', engine_manager.protocol_endpoint())
        if line.startswith('GET') and not line.endswith('HTTP/1.1'):
            request_lines[index] = line + ' HTTP/1.1'
    request_header_lines = request_lines[index_header:index_line_between]
    if len([l for l in request_header_lines if "Content-Length" in l]) == 0:
        request_header_lines.append("Content-Length: XXX")
    request_body_lines = [
        x for x in request_lines[index_line_between + 1:] if x]
    request_header = '\r\n'.join(request_header_lines)
    request_body = '\r\n'.join(request_body_lines)
    request_header = request_header + '\r\n' + 'Authorization: Bearer abc'
    if test.type_name == 'GraphStoreProtocolTest':
        request_header = request_header.replace(
            '$GRAPHSTORE$', '/' + test.config.GRAPHSTORE)
        request_body = request_body.replace(
            '$GRAPHSTORE$', test.config.GRAPHSTORE)
    request_header = request_header.replace('XXX', str(len(request_body)))
    return request_header + '\r\n\r\n', request_body + '\r\n'


def prepare_response(test: TestObject, request_with_reponse: str, newpath: str) -> dict[str, str | list[str]]:
    response: dict[str, str | list[str]] = {'status_codes': [], 'content_types': []}
    response_string = request_with_reponse.split('#### Response')[1]
    if test.type_name == 'GraphStoreProtocolTest':
        response_string = response_string.replace(
            '$HOST$', 'localhost')
        response_string = response_string.replace(
            '$GRAPHSTORE$', test.config.GRAPHSTORE)
        response_string = response_string.replace(
            '$NEWPATH$', newpath)
    response_lines = [x.strip() for x in response_string.splitlines() if x]
    for line in response_lines:
        if line.endswith('response') or re.search(r'\dxx', line) is not None:
            line = line.replace('response', '')
            status_codes = line.strip().split('or')
            for status_code in status_codes:
                response['status_codes'].append(status_code.strip())
        if re.search(r'^\d\d\d ', line) is not None:
            response['status_codes'].append(
                re.search(r'^\d\d\d ', line).group(0))
        if line.startswith('Content-Type:'):
            line = line.replace('Content-Type:', '')
            content_types = line.strip().split('or')
            for content_type in content_types:
                # Split on ',' to handle multiple content types
                cts = content_type.split(',')
                for ct in cts:
                    if ct != '':
                        response['content_types'].append(ct.strip().split(';')[0])
        if line.startswith('true'):
            response['result'] = 'true'
        if line.startswith('false'):
            response['result'] = 'false'
        if line.startswith('Location: $NEWPATH$'):
            response['newpath'] = 'Location: $NEWPATH$'
    if 'text/turtle' in response['content_types'] and response.get(
            'result') is None:
        response['result'] = '\n\n'.join(response_string.split('\n\n')[2:])
    return response


def parse_chunked_response(response: str) -> str:
    """
    Extract the body of a http response that uses chunked transfer encoding.
    Important: This function assumes that the input still consists of the
    headers + chunked body.
    """
    # only extract the chunked body, and then parse it.
    headers, body = response.split('\r\n\r\n', 1)
    return parse_chunked_body(body)

def parse_chunked_body(response_body: str) -> str:
    """
    Parses a chunked transfer encoded HTTP response body and returns the complete decoded string.

    Parameters:
    - response_body: The raw body as a string (with chunk sizes and data).

    Returns:
    - A string with the fully concatenated body content.
    """
    result = []
    i = 0
    length = len(response_body)

    while i < length:
        # Find the next \r\n to extract the chunk size
        rn_index = response_body.find("\r\n", i)
        if rn_index == -1:
            break  # Malformed chunk

        # Parse chunk size (hexadecimal)
        chunk_size_str = response_body[i:rn_index]
        try:
            chunk_size = int(chunk_size_str, 16)
        except ValueError:
            raise ValueError(f"Invalid chunk size: {chunk_size_str}")

        if chunk_size == 0:
            break

        # Move pointer past chunk size line
        i = rn_index + 2

        chunk_data = response_body[i:i + chunk_size]
        result.append(chunk_data)

        # Move pointer past chunk data and the following \r\n
        i += chunk_size + 2

    return ''.join(result)


def compare_response(expected_response: dict[str, str | list[str]], got_response: str, is_select: bool) -> Tuple[bool, str]:
    status_code_match = False
    content_type_match = False
    result_match = False

    for status_code in expected_response['status_codes']:
        pattern = r'HTTP/1\.1 '
        for digit in status_code:
            if digit == 'x':
                pattern += '\\d'
            else:
                pattern += digit
        found_status_code = re.search(pattern, got_response)
        if found_status_code is not None:
            status_code_match = True

    if len(expected_response['content_types']) == 0:
        content_type_match = True

    for content_type in expected_response['content_types']:
        if got_response.find(content_type) != -1:
            content_type_match = True

    if expected_response.get('result') is None or got_response.find(
            expected_response['result']) != -1:
        result_match = True
    # Handle SELECT queries with the expected result true
    if expected_response.get('result', False) and is_select:
        try:
            json_body = parse_chunked_response(got_response)
            parsed = json.loads(json_body)
            result_match = bool(parsed.get('results', {}).get('bindings'))
        except:
            pass
    if 'text/turtle' in expected_response.get(
            'content_types') and status_code_match and content_type_match:
        response_ttl = parse_chunked_response(got_response)
        status, error_type, expected_string, query_string, expected_string_red, query_string_red = compare_ttl(
            expected_response['result'], response_ttl)
        if status == 'Passed':
            result_match = True
    newpath = ''
    if 'newpath' in expected_response:
        match = re.search(r'^Location:\s*(.*)', got_response, re.MULTILINE)
        if match:
            newpath = match.group(1)
    return status_code_match and content_type_match and result_match, newpath


def run_protocol_test(
        engine_manager: EngineManager,
        test: TestObject,
        test_protocol: str,
        newpath: str) -> tuple:
    server_address = 'localhost'
    port = test.config.port
    result = Status.FAILED
    error_type = ErrorMessage.RESULTS_NOT_THE_SAME
    status = []
    if 'followed by' in test_protocol:
        test_request_split = test_protocol.split('followed by')
    elif test_protocol.count('#### Request') > 1:
        test_request_split = [line for line in test_protocol.split(
            '#### Request') if len(line) > 2]
    else:
        test_request_split = [test_protocol]
    requests = []
    responses = []
    got_responses = []
    for request_with_reponse in test_request_split:
        request_head, request_body = prepare_request(engine_manager, test, request_with_reponse, newpath)
        requests.append(request_head + request_body)
        response = prepare_response(test, request_with_reponse, newpath)
        responses.append(response)
        tn = telnet.Telnet(server_address, int(port))
        if 'charset=UTF-16' in request_head:
            encoding = 'utf-16'
        else:
            encoding = 'utf-8'
        tn.write(request_head.encode('utf-8') + request_body.encode(encoding))
        tn_response = tn.read_until(b"\r\n\r\n", timeout=1.7).decode('utf-8')
        got_responses.append(tn_response)
        matching, newpath = compare_response(response, tn_response, 'SELECT' in request_with_reponse)
        status.append(matching)
        tn.close()
    if all(status):
        result = Status.PASSED
        error_type = ''
    extracted_expected_responses = ''
    for response in responses:
        extracted_expected_responses += str(response) + '\n'
    extracted_sent_requests = ''
    for request in requests:
        extracted_sent_requests += request + '\n'
    got_responses_string = ''
    for response in got_responses:
        got_responses_string += response + '\n'
    return result, error_type, extracted_expected_responses, extracted_sent_requests, got_responses_string, newpath
