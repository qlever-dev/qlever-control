import json
import re
import socket
import time
from typing import List, Tuple
from urllib.parse import urlsplit, urlunsplit

from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.protocol_request import ProtocolRequest
from sparql_conformance.test_object import TestObject, Status, ErrorMessage
from sparql_conformance.rdf_tools import compare_ttl


def prepare_request(engine_manager: EngineManager, test: TestObject, request_with_reponse: str, newpath: str) -> Tuple[str, str]:
    request = request_with_reponse.split('#### Response')[0]
    request_lower = request.lower()
    is_update_request = (
        'update=' in request_lower
        or 'application/sparql-update' in request_lower
    )
    protocol_endpoint = engine_manager.protocol_endpoint()
    if is_update_request:
        protocol_endpoint = engine_manager.protocol_update_endpoint()
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
            line = _replace_endpoint_in_request_line(
                line,
                protocol_endpoint,
            )
            request_lines[index] = line
        if line.startswith('GET') and not line.endswith('HTTP/1.1'):
            request_lines[index] = line + ' HTTP/1.1'
    request_header_lines = [
        header_line
        for header_line in request_lines[index_header:index_line_between]
        if not header_line.lower().startswith("content-length:")
    ]
    request_body_lines = [
        x for x in request_lines[index_line_between + 1:] if x]
    request_body = '\r\n'.join(request_body_lines)
    request_header = '\r\n'.join(request_header_lines)
    if test.type_name == 'GraphStoreProtocolTest':
        # Replace only the first occurrence with leading slash (in the request header),
        # then replace remaining occurrences in header and body without the slash.
        request_header = request_header.replace(
            '$GRAPHSTORE$', '/' + test.config.GRAPHSTORE, 1)
        request_header = request_header.replace(
            '$GRAPHSTORE$', test.config.GRAPHSTORE)
        request_body = request_body.replace(
            '$GRAPHSTORE$', test.config.GRAPHSTORE)
    if 'authorization:' not in request_header.lower():
        request_header = request_header + '\r\n' + 'Authorization: Bearer abc'
    body_encoding = 'utf-8'
    if 'charset=utf-16' in request_header.lower():
        body_encoding = 'utf-16'
    content_length = len(request_body.encode(body_encoding))
    request_header = request_header + '\r\n' + f'Content-Length: {content_length}'
    return request_header + '\r\n\r\n', request_body


def _replace_endpoint_in_request_line(
        request_line: str,
        endpoint: str,
) -> str:
    parts = request_line.split(' ', 2)
    if len(parts) < 2:
        return request_line
    if len(parts) == 2:
        method, target = parts
        version = ''
    else:
        method, target, version = parts
    parsed = urlsplit(target)
    path_parts = parsed.path.split('/')
    endpoint_parts = endpoint.strip('/').split('/')
    if endpoint_parts == ['']:
        return request_line
    changed = False
    replaced_at_end = False
    for i, path_part in enumerate(path_parts):
        if path_part == 'sparql':
            replaced_at_end = i == len(path_parts) - 2 and path_parts[-1] == ''
            path_parts = path_parts[:i] + endpoint_parts + path_parts[i + 1:]
            changed = True
            break
    if not changed:
        return request_line
    if replaced_at_end and path_parts and path_parts[-1] == '':
        path_parts = path_parts[:-1]
    new_path = '/'.join(path_parts)
    new_target = urlunsplit((
        parsed.scheme,
        parsed.netloc,
        new_path,
        parsed.query,
        parsed.fragment,
    ))
    if not version:
        return f'{method} {new_target}'
    return f'{method} {new_target} {version}'


def prepare_response(test: TestObject, request_with_reponse: str, newpath: str) -> dict[str, str | list[str]]:
    response: dict[str, str | list[str]] = {'status_codes': [], 'content_types': []}
    request_string = request_with_reponse.split('#### Response')[0]
    response_string = request_with_reponse.split('#### Response')[1]
    if test.type_name == 'GraphStoreProtocolTest':
        response_string = response_string.replace(
            '$HOST$', 'localhost')
        response_string = response_string.replace(
            '$GRAPHSTORE$', test.config.GRAPHSTORE)
        response_string = response_string.replace(
            '$NEWPATH$', newpath)
    response_lines = [x.strip() for x in response_string.splitlines() if x]
    is_put_request = 'PUT ' in request_string
    for line in response_lines:
        if line.endswith('response') or re.search(r'\dxx', line) is not None:
            line = line.replace('response', '')
            status_codes = line.strip().split('or')
            for status_code in status_codes:
                response['status_codes'].append(status_code.strip())
        if re.search(r'^\d\d\d ', line) is not None:
            status_code = re.search(r'^\d\d\d ', line).group(0)
            response['status_codes'].append(status_code)
            # PUT modifying existing content can return 200 or 204
            # https://www.w3.org/TR/sparql11-http-rdf-update/#http-put
            if is_put_request and status_code.strip() == '204':
                response['status_codes'].append('200')
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


def send_raw_http(
        server_address: str,
        port: int,
        request_head: str,
        request_body: str,
        encoding: str,
        connect_timeout: float = 5.0,
        idle_timeout: float = 10.0,
        total_timeout: float = 30.0) -> str:
    body_bytes = request_body.encode(encoding)
    request_head = _set_content_length(request_head, len(body_bytes))
    request_bytes = request_head.encode('utf-8') + body_bytes
    try:
        with socket.create_connection(
                (server_address, port), timeout=connect_timeout) as sock:
            sock.settimeout(idle_timeout)
            sock.sendall(request_bytes)
            response_chunks = []
            start_time = time.monotonic()
            while True:
                if time.monotonic() - start_time > total_timeout:
                    break
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                response_chunks.append(chunk)
            if not response_chunks:
                return 'timed out waiting for response'
            return b''.join(response_chunks).decode('utf-8')
    except Exception as e:
        return str(e)


def _set_content_length(request_head: str, content_length: int) -> str:
    stripped_head = request_head
    if stripped_head.endswith('\r\n\r\n'):
        stripped_head = stripped_head[:-4]
    lines = [line for line in stripped_head.split('\r\n') if line != '']
    lines = [
        line
        for line in lines
        if not line.lower().startswith('content-length:')
    ]
    lines.append(f'Content-Length: {content_length}')
    return '\r\n'.join(lines) + '\r\n\r\n'

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
        if 'charset=utf-16' in request_head.lower():
            encoding = 'utf-16'
        else:
            encoding = 'utf-8'
        tn_response = send_raw_http(
            server_address,
            int(port),
            request_head,
            request_body,
            encoding)
        got_responses.append(tn_response)
        matching, newpath = compare_response(response, tn_response, 'SELECT' in request_with_reponse)
        status.append(matching)
    print(status)
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
    print(result)
    return result, error_type, extracted_expected_responses, extracted_sent_requests, got_responses_string, newpath


def prepare_request_from_action(
        engine_manager: EngineManager,
        test: TestObject,
        req: ProtocolRequest,
        newpath: str) -> Tuple[str, str]:
    is_update_request = any(
        h.name.lower() == 'content-type' and 'sparql-update' in h.value.lower()
        for h in req.headers
    )
    protocol_endpoint = (
        engine_manager.protocol_update_endpoint()
        if is_update_request
        else engine_manager.protocol_endpoint()
    )

    first_line = _replace_endpoint_in_request_line(
        f'{req.method} {req.absolute_path} HTTP/{req.http_version}',
        protocol_endpoint,
    )

    header_lines = [first_line, f'Host: {req.connection_authority}']
    for h in req.headers:
        if h.name.lower() != 'content-length':
            header_lines.append(f'{h.name}: {h.value}')

    if 'authorization:' not in '\n'.join(header_lines).lower():
        header_lines.append('Authorization: Bearer abc')

    request_body = req.body or ''
    body_encoding = 'utf-16' if req.character_encoding.lower() == 'utf-16' else 'utf-8'
    content_length = len(request_body.encode(body_encoding))
    header_lines.append(f'Content-Length: {content_length}')

    return '\r\n'.join(header_lines) + '\r\n\r\n', request_body


def prepare_response_from_action(req: ProtocolRequest) -> dict:
    response: dict = {'status_codes': list(req.expected_response.status_codes), 'content_types': []}
    if req.expected_response.expected_boolean is not None:
        response['result'] = 'true' if req.expected_response.expected_boolean else 'false'
    return response


def run_protocol_test_from_action(
        engine_manager: EngineManager,
        test: TestObject,
        protocol_requests: List[ProtocolRequest],
        newpath: str) -> tuple:
    server_address = 'localhost'
    port = test.config.port
    result = Status.FAILED
    error_type = ErrorMessage.RESULTS_NOT_THE_SAME
    status = []
    requests = []
    responses = []
    got_responses = []
    for req in protocol_requests:
        request_head, request_body = prepare_request_from_action(engine_manager, test, req, newpath)
        requests.append(request_head + request_body)
        response = prepare_response_from_action(req)
        responses.append(response)
        body_encoding = 'utf-16' if req.character_encoding.lower() == 'utf-16' else 'utf-8'
        tn_response = send_raw_http(
            server_address,
            int(port),
            request_head,
            request_body,
            body_encoding)
        got_responses.append(tn_response)
        is_select = req.expected_response.expected_format == 'tabular'
        matching, newpath = compare_response(response, tn_response, is_select)
        status.append(matching)
    print(status)
    if all(status):
        result = Status.PASSED
        error_type = ''
    extracted_expected_responses = ''.join(str(r) + '\n' for r in responses)
    extracted_sent_requests = ''.join(r + '\n' for r in requests)
    got_responses_string = ''.join(r + '\n' for r in got_responses)
    print(result)
    return result, error_type, extracted_expected_responses, extracted_sent_requests, got_responses_string, newpath
