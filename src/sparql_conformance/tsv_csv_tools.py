from typing import List, Tuple

from sparql_conformance.util import escape, is_number
from io import StringIO
import csv
from sparql_conformance.test_object import Status, ErrorMessage

def _build_column_mapping(expected_header: list, actual_header: list):
    """
    Return a list L which aligns actual[row][L[i]] with expected[row][i].
    Example: actual: s p o expected: o p s -> L[0] = 2, L[1] = 1, L[2] = 0
    If no perfect mapping exists, return None.
    """
    if len(expected_header) != len(actual_header):
        return None

    wanted = expected_header
    have = actual_header

    used = set()
    mapping = []
    for name in wanted:
        idx = None
        for j, col in enumerate(have):
            if j in used:
                continue
            if col.strip() == name.strip():
                idx = j
                break
        if idx is None:
            return None
        used.add(idx)
        mapping.append(idx)
    return mapping


def _reorder_columns_to_expected(expected_array: list, actual_array: list):
    """
    If the first rows (headers) of expected/actual are a permutation of each other,
    reorder every row of the actual array to match the expected header order.
    Otherwise, just return actual_array.
    """
    if not expected_array or not actual_array:
        return actual_array

    expected_header = expected_array[0]
    actual_header = actual_array[0]

    if sorted(expected_header) != sorted(actual_header):
        return actual_array

    mapping = _build_column_mapping(expected_header, actual_header)
    if mapping is None:
        return actual_array

    def reorder_row(row):
        return [row[i] if i < len(row) else "" for i in mapping]

    return [reorder_row(r) for r in actual_array]


def write_csv_file(file_path: str, csv_rows: list):
    with open(file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(csv_rows)


def row_to_string(row: list, separator: str) -> str:
    """
    Converts a row (list of values) to a string representation separated by a specified delimiter.

    Parameters:
        row (list): The row to be converted to a string.
        separator (str): The separator used to separate the values in the row "," or "\t"

    Returns:
        str: A string representation of the row.
    """
    result = ""
    index = 0
    row_length = len(row) - 1
    for element in row:
        if index == row_length:
            delimiter = ""
        else:
            delimiter = separator
        element = str(element)
        if separator in element:
            element = "\"" + element + "\""
        result += element + delimiter
        index += 1
    return result


def generate_highlighted_string_sv(
        array: list,
        remaining: list,
        mark_red: list,
        result_type: str) -> str:
    """
    Generates a string representation of an array, with specific rows highlighted.

    Parameters:
        array (list): The array to be converted to a string.
        mark_red (list): The rows to be highlighted in red.
        remaining (list): The rows to be highlighted.
        result_type (str): The type of result (csv or tsv) to determine the separator.

    Returns:
        str: A string representation of the array with highlighted rows.
    """
    separator = "," if result_type == "csv" else "\t"

    result_string = ""
    for row in array:
        if row in remaining:
            if row in mark_red:
                result_string += '<label class="red">'
            else:
                result_string += '<label class="yellow">'
            result_string += escape(row_to_string(row, separator))
            result_string += '</label>\n'
        else:
            result_string += escape(row_to_string(row, separator)) + "\n"
    return result_string


def compare_values(
        value1: str,
        value2: str,
        use_config: bool,
        alias: List[Tuple[str, str]],
        map_bnodes: dict) -> bool:
    """
    Compares two values for equality accounting for numeric differences and aliases.

    Parameters:
        value1 (str): The first value to compare.
        value2 (str): The second value to compare.
        use_config (bool): Flag to use configuration for additional comparison logic.
        alias (dict): Dictionary with aliases for datatypes ex. int = integer .
        map_bnodes (dict): Dictionary mapping the used bnodes.

    Returns:
        bool: True if the values are considered equal.
    """
    if value1 is None or value2 is None:
        return False
    # Blank nodes
    if len(value1) > 1 and len(
            value2) > 1 and value1[0] == "_" and value2[0] == "_":
        if value1 not in map_bnodes and value2 not in map_bnodes:
            map_bnodes[value1] = value2
            map_bnodes[value2] = value1
            return True
        if map_bnodes.get(value1) == value2 and map_bnodes.get(
                value2) == value1:
            return True
        return False
    # In most cases the values are in the same representation
    if value1 == value2:
        return True
    # Handle exceptions ex. 30000 == 3E4
    if is_number(value1) and is_number(value2):
        if float(value1) == float(value2):
            return True
    else:  # Handle exceptions integer = int
        if use_config and ((value1, value2) in alias or (value2, value1) in alias):
            return True
    return False


def compare_rows(
        row1: list,
        row2: list,
        use_config: bool,
        alias: List[Tuple[str, str]],
        map_bnodes: dict) -> bool:
    """
    Compares two rows for equality.

    Parameters:
        row1 (list): The first row to compare.
        row2 (list): The second row to compare.
        use_config (bool): Flag to use configuration for additional comparison logic.
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .
        map_bnodes (dict): Dictionary mapping the used bnodes.

    Returns:
        bool: True if the rows are considered equal otherwise False
    """
    if len(row1) != len(row2):
        return False

    for element1, element2 in zip(row1, row2):
        if not compare_values(
                element1.split("^")[0],
                element2.split("^")[0],
                use_config,
                alias,
                map_bnodes):
            return False
    return True


def compare_array(
        expected_result: list,
        result: list,
        result_copy: list,
        expected_result_copy: list,
        use_config: bool,
        alias: List[Tuple[str, str]],
        map_bnodes: dict):
    """
    Compares two arrays and removes equal rows from both arrays.

    Parameters:
        expected_result (list): The expected result array.
        result (list): The actual result array.
        result_copy (list): A copy of the actual result array for modification.
        expected_result_copy (list): A copy of the expected result array for modification.
        use_config (bool): Flag to use configuration for additional comparison logic.
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .
        map_bnodes (dict): Dictionary mapping the used bnodes.
    """
    for row1 in result:
        equal = False
        row2_delete = None
        for row2 in expected_result:
            if compare_rows(row1, row2, use_config, alias, map_bnodes):
                equal = True
                row2_delete = row2
                break
        if equal:
            result_copy.remove(row1)
            expected_result_copy.remove(row2_delete)


def convert_csv_tsv_to_array(input_string: str, input_type: str):
    """
    Converts a CSV/TSV string to an array of rows.

    Parameters:
        input_string (str): The CSV/TSV formatted string.
        input_type (str): The type of the input ('csv' or 'tsv').

    Returns:
        An array representation of the input string.
    """
    rows = []
    delimiter = "," if input_type == "csv" else "\t"
    with StringIO(input_string) as io:
        reader = csv.reader(io, delimiter=delimiter)
        for row in reader:
            # Drop empty rows
            if not row or not any(cell.strip() for cell in row):
                continue
            rows.append(row)
    return rows


def compare_sv(
        expected_string: str,
        query_result: str,
        result_format: str,
        alias: List[Tuple[str, str]]):
    """
    Compares CSV/TSV formatted query result with the expected output.

    Parameters:
        expected_string (str): Expected CSV/TSV formatted string.
        query_result (str): Actual CSV/TSV formatted string from the query.
        result_format (str): Format of the output ('csv' or 'tsv').
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .

    Returns:
        tuple(int, str, str, str, str, str): A tuple of test status and error message and expected html, query html, expected red, query red
    """
    map_bnodes = {}
    status = Status.FAILED
    error_type = ErrorMessage.RESULTS_NOT_THE_SAME

    expected_array = convert_csv_tsv_to_array(expected_string, result_format)
    actual_array = convert_csv_tsv_to_array(query_result, result_format)

    # NEW: normalize actual column order to match expected header
    actual_array = _reorder_columns_to_expected(expected_array, actual_array)

    actual_array_copy = actual_array.copy()
    expected_array_copy = expected_array.copy()
    actual_array_mark_red = []
    expected_array_mark_red = []

    compare_array(
        expected_array,
        actual_array,
        actual_array_copy,
        expected_array_copy,
        False,
        alias,
        map_bnodes)

    if len(actual_array_copy) == 0 and len(expected_array_copy) == 0:
        status = Status.PASSED
        error_type = ""
    else:
        actual_array_mark_red = actual_array_copy.copy()
        expected_array_mark_red = expected_array_copy.copy()
        compare_array(
            expected_array_copy,
            actual_array_copy,
            actual_array_mark_red,
            expected_array_mark_red,
            True,
            alias,
            map_bnodes)
        if len(actual_array_mark_red) == 0 and len(
                expected_array_mark_red) == 0:
            status = Status.INTENDED
            error_type = ErrorMessage.INTENDED_MSG

    expected_html = generate_highlighted_string_sv(
        expected_array,
        expected_array_copy,
        expected_array_mark_red,
        result_format)
    actual_html = generate_highlighted_string_sv(
        actual_array, actual_array_copy, actual_array_mark_red, result_format)
    expected_html_red = generate_highlighted_string_sv(
        expected_array_copy,
        expected_array_copy,
        expected_array_mark_red,
        result_format)
    actual_html_red = generate_highlighted_string_sv(
        actual_array_copy, actual_array_copy, actual_array_mark_red, result_format)

    return status, error_type, expected_html, actual_html, expected_html_red, actual_html_red
