import json
from typing import List, Tuple

from sparql_conformance.test_object import Status, ErrorMessage


def handle_bindings(
        indent: int,
        level: int,
        bindings: list,
        remaining_bindings: list,
        mark_red: list) -> str:
    """
    Formats the "bindings" list with HTML labels as needed for highlighting.

    This method iterates over a list of bindings and applies HTML labels to those
    that match any in the reference bindings list. The method handles indentation
    and formatting to create a readable HTML-formatted string.

    Parameters:
        indent (int): Number of spaces used for indentation.
        level (int): Current nesting level for correct indentation.
        bindings (list): List of binding items to format.
        remaining_bindings (list): List of binding items used for comparison.
        mark_red (list): List containing the elements that must be highlighted red.

    Returns:
        str: An HTML-formatted string representing the bindings list with highlighted items.
    """
    mark_red_copy = list(mark_red)
    parts = ["["]
    for i, binding in enumerate(bindings):
        if i > 0:
            parts.append(", ")
        parts.append("\n" + " " * (indent * (level + 1)))

        # Apply label if the binding matches any in the reference bindings
        if binding in remaining_bindings:
            if binding in mark_red_copy:
                label = '<label class="red">'
            else:
                label = '<label class="yellow">'
            end_label = '</label>'
        else:
            label = ""
            end_label = ""
        parts.append(
            f"{label}{json_to_string(binding, {},mark_red, level + 1)}{end_label}")
    parts.append("\n" + " " * (indent * level) + "]")
    return "".join(parts)


def json_dict(
        indent: int,
        level: int,
        json_dictionary: dict,
        remaining_dict: dict,
        mark_red: list) -> str:
    """
    Formats a dictionary with HTML labels as needed for highlighting.

    Iterates through the dictionary and formats each key-value pair. Special handling is
    applied for lists under specific keys "vars" and "bindings". The method manages
    indentation and applies HTML labels for highlighting as needed.

    Parameters:
        indent (int): Number of spaces used for indentation.
        level (int): Current nesting level for correct indentation.
        json_dictionary (dict): Dictionary to format.
        remaining_dict (dict): Dictionary used for comparison to determine highlighting.
        mark_red (list): List containing the elements that must be highlighted red.

    Returns:
        str: An HTML-formatted string representing the dictionary with highlighted elements.
    """
    parts = ["{"]
    for i, (key, value) in enumerate(json_dictionary.items()):
        if i > 0:
            parts.append(", ")
        parts.append("\n" + " " * (indent * (level + 1)))
        if isinstance(value, list) and key == "vars":
            # Special handling for "vars" in "head"
            parts.append(
                f"\"{key}\": {json_to_string(value, remaining_dict.get(key, []),mark_red, level + 1)}")
        elif isinstance(value, list) and key == "bindings":
            # Special handling for "bindings" in "results"
            formatted_bindings = handle_bindings(
                indent, level, value, remaining_dict.get(
                    key, []), mark_red)
            parts.append(f"\"{key}\": {formatted_bindings}")
        elif key == "boolean":
            # Special handling for "boolean" in "results"
            label = ""
            end_label = ""
            if remaining_dict.get("boolean") is not None:
                label = '<label class="red">'
                end_label = '</label>'
            parts.append(f"\"{key}\": {label}{str(value).lower()}{end_label}")
        else:
            parts.append(
                f"\"{key}\": {json_to_string(value, remaining_dict.get(key, {}), mark_red, level + 1)}")

    parts.append("\n" + " " * (indent * level) + "}")
    return "".join(parts)


def json_list(
        indent: int,
        level: int,
        json_list_items: list,
        remaining_list: list) -> str:
    """
    Formats a list with HTML labels as needed for highlighting.

    Iterates through the list and applies HTML labels to items that match
    any in the list. Manages indentation for a readable format.

    Parameters:
        indent (int): Number of spaces used for indentation.
        level (int): Current nesting level for correct indentation.
        json_list_items (list): List of items to format.
        remaining_list (list): List used for comparison to determine highlighting.

    Returns:
        str: An HTML-formatted string representing the list with highlighted elements.
    """
    parts = ["["]
    for i, item in enumerate(json_list_items):
        if i > 0:
            parts.append(", ")
        parts.append("\n" + " " * (indent * (level + 1)))
        # Apply label if the item is in the list
        if item in remaining_list:
            label = '<label class="red">'
            end_label = '</label>'
        else:
            label = ""
            end_label = ""
        parts.append(f"{label}\"{item}\"{end_label}")
    parts.append("\n" + " " * (indent * level) + "]")
    return "".join(parts)


def json_to_string(json_obj, remaining_json, mark_red: list, level=0) -> str:
    """
    Converts a JSON object to a readable string and highlights elements found in the reference JSON with <"></">.

    Parameters:
    json_obj (dict or list): The JSON object to be converted.
    remaining_json (dict or list): SON object to check for matching elements.
    mark_red (list): List containing the elements that must be highlighted red.
    level (int): Current recursion level to calculate indentation.

    Returns:
    str: A readable string representation of the JSON object with highlighted elements.
    """
    indent = 4
    if isinstance(json_obj, dict) and json_obj:
        return json_dict(indent, level, json_obj, remaining_json, mark_red)
    elif isinstance(json_obj, list):
        return json_list(indent, level, json_obj, remaining_json)
    elif isinstance(json_obj, str):
        return f"\"{json_obj}\""
    else:
        return str(json_obj)


def generate_highlighted_string_json(
        json_obj: dict,
        remaining_json: dict,
        mark_red: list) -> str:
    """
    Generates an HTML-formatted and highlighted string representation of a JSON object.

    Parameters:
        json_obj: The JSON object to be formatted and highlighted.
        remaining_json: The JSON object used as a reference for highlighting elements in the json_obj.
        mark_red (list): List containing the elements that must be highlighted red.

    Returns:
        str: An HTML string representing the formatted and highlighted JSON object.
    """
    return json_to_string(json_obj, remaining_json, mark_red)


def json_elements_equal(
        element1: dict,
        element2: dict,
        compare_with_intended_behaviour: bool,
        alias: List[Tuple[str, str]],
        number_types: list,
        map_bnodes: dict) -> bool:
    """
    Compares two JSON elements for equality.

    This method compares two JSON elements for equality. It checks for matching
    keys and compares their values. It also accounts for datatype differences by comparing numerical values.
    The comparison can include intended behavior based on the compare_with_intended_behaviour Bool.

    Parameters:
        element1 (dict): The first JSON element to compare.
        element2 (dict): The second JSON element to compare.
        compare_with_intended_behaviour (bool): Bool to determine whether to use intended behavior aliases in comparison.
        alias (dict): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.
        map_bnodes (dict): Dictionary mapping the used bnodes.

    Returns:
        bool: True if considered equal otherwise False.
    """
    if set(element1.keys()) != set(element2.keys()):
        return False
    for key in element1:
        field1 = element1[key]
        field2 = element2[key]

        if isinstance(field1, dict) and isinstance(field2, dict):
            for sub_key in set(field1.keys()) | set(field2.keys()):
                if field1.get(sub_key) != field2.get(sub_key):
                    if str(
                            field1.get("type")) == "bnode" and str(
                            field2.get("type")) == "bnode" and str(sub_key) == "value":
                        if field1.get("value") not in map_bnodes and field2.get(
                                "value") not in map_bnodes:
                            map_bnodes[field1.get(
                                "value")] = field2.get("value")
                            map_bnodes[field2.get(
                                "value")] = field1.get("value")
                            continue
                        if map_bnodes.get(
                                field1.get("value")) == field2.get("value") and map_bnodes.get(
                                field2.get("value")) == field1.get("value"):
                            continue
                    if str(field1.get("datatype")) in number_types and str(
                            field2.get("datatype")) in number_types and str(sub_key) == "value":
                        if float(
                                field1.get(sub_key)) == float(
                                field2.get(sub_key)):
                            continue
                    if compare_with_intended_behaviour and ((field1.get(sub_key), field2.get(sub_key)) in alias or (field2.get(sub_key), field1.get(sub_key)) in alias):
                        continue
                    return False
        else:
            if field1 != field2:
                return False
    return True


def remove_once_found(
        list1: list,
        list2: list,
        compare_with_intended_behaviour: bool,
        alias: List[Tuple[str, str]],
        number_types: list,
        map_bnodes: dict) -> list:
    """
    Compares two lists and returns the first list will all elements remove that are also in the second list.

    Parameters:
        list1 (list): The first list to compare.
        list2 (list): The second list to compare.
        compare_with_intended_behaviour (bool): Bool to determine whether to use intended behavior aliases in comparison.
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.
        map_bnodes (dict): Dictionary mapping the used bnodes.

    Returns:
        list: Retuns list1 with removed elements.
    """
    temp_list1 = list1[:]

    for item2 in list2:
        for i, item1 in enumerate(temp_list1):
            if json_elements_equal(
                    item1,
                    item2,
                    compare_with_intended_behaviour,
                    alias,
                    number_types,
                    map_bnodes):
                # Remove the first found match and break the loop to move to
                # the next b2
                temp_list1.pop(i)
                break

    return temp_list1


def compare_json(
        expected_json: str,
        query_json: str,
        alias: List[Tuple[str, str]],
        number_types: list) -> tuple:
    """
    Compares two JSON objects and identifies differences in their "head" and "results" sections.

    This method parses two JSON strings representing expected and query results. It compares
    these JSON objects, particularly focusing on the "head" and "results" sections.
    Differences are highlighted, and a status of comparison along with any error type is returned.

    Parameters:
        expected_json (str): The expected JSON content as a string.
        query_json (str): The query JSON content as a string.
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.

    Returns:
        tuple: A tuple containing the status and error type.
    """
    map_bnodes = {}
    status = Status.FAILED
    error_type = ErrorMessage.RESULTS_NOT_THE_SAME
    expected = json.loads(expected_json)
    query = json.loads(query_json)

    vars1 = []
    vars2 = []

    # Compare and remove similar parts in "head" section
    if expected.get("head") is not None and expected.get(
            "head").get("vars") is not None:
        vars1 = expected.get("head").get("vars")

    if query.get("head") is not None and query.get(
            "head").get("vars") is not None:
        vars2 = query.get("head").get("vars")

    if expected.get("head") is not None and expected.get(
            "head").get("vars") is not None:
        unique_vars1 = [v for v in vars1 if v not in vars2]
        expected["head"]["vars"] = unique_vars1

    if query.get("head") is not None and query.get(
            "head").get("vars") is not None:
        unique_vars2 = [v for v in vars2 if v not in vars1]
        query["head"]["vars"] = unique_vars2

    # Check if its a boolean result or variable binding results
    if query.get("results") is not None and expected.get(
            "results") is not None:
        # Compare and remove similar parts in "bindings" section using the
        # custom comparison function
        bindings1 = expected["results"]["bindings"]
        bindings2 = query["results"]["bindings"]

        unique_bindings1 = remove_once_found(
            bindings1, bindings2, False, alias, number_types, map_bnodes)
        unique_bindings2 = remove_once_found(
            bindings2, bindings1, False, alias, number_types, map_bnodes)

        expected["results"]["bindings"] = unique_bindings1
        query["results"]["bindings"] = unique_bindings2

        if len(
            expected["results"]["bindings"]) == 0 and len(
            query["results"]["bindings"]) == 0 and len(
            expected["head"]["vars"]) == 0 and len(
                query["head"]["vars"]) == 0:
            status = Status.PASSED
            error_type = ""
        else:
            unique_bindings1 = remove_once_found(
                bindings1, bindings2, True, alias, number_types, map_bnodes)
            unique_bindings2 = remove_once_found(
                bindings2, bindings1, True, alias, number_types, map_bnodes)
            if len(unique_bindings1) == 0 and len(unique_bindings2) == 0:
                status = Status.INTENDED
                error_type = ErrorMessage.INTENDED_MSG
        expected_string = generate_highlighted_string_json(
            json.loads(expected_json), expected, unique_bindings1)
        query_string = generate_highlighted_string_json(
            json.loads(query_json), query, unique_bindings2)
        expected_string_red = generate_highlighted_string_json(
            expected, expected, unique_bindings1)
        query_string_red = generate_highlighted_string_json(
            query, query, unique_bindings2)
    else:
        bool1 = expected["boolean"]
        bool2 = query["boolean"]
        if str(bool1) == str(bool2):
            del expected["boolean"]
            del query["boolean"]
            status = Status.PASSED
            error_type = ""
        expected_string = generate_highlighted_string_json(
            json.loads(expected_json), expected, [])
        query_string = generate_highlighted_string_json(
            json.loads(query_json), query, [])
        expected_string_red = generate_highlighted_string_json(
            expected, expected, [])
        query_string_red = generate_highlighted_string_json(query, query, [])

    return status, error_type, expected_string, query_string, expected_string_red, query_string_red
