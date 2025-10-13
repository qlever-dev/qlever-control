import re
import xml.etree.ElementTree as ET
import xml.dom.minidom as md
from typing import List, Tuple

from sparql_conformance.test_object import Status, ErrorMessage
from sparql_conformance.util import escape


def replace_self_closing_tag(xml: str) -> str:
    """
    Takes any xml string and replaces all self-closing xml tags (<abc/>) with open and close tags (<abc></abc>).

    The regular expression \\w+ matches one or more word characters which is then used in the replacement pattern
    where \1 refers to the content of the first capture group.

    Parameters:
        xml (str): The  string containing self-closing xml tags.

    Returns:
        str: xml string without self-closing xml tags
    """
    pattern = r"<(\w+)/>"
    replacement = r"<\1></\1>"
    return re.sub(pattern, replacement, xml)


def highlight_first_occurrence(original: str, string_part: str, label: str) -> str:
    """
    Highlights the first occurrence of string_part in original by wrapping it with a <label class="red"> tag.

    Ensures that if string_part occurs multiple times it does not get double-wrapped.

    Parameters:
        original (str): Any string
        string_part (str): A string which might be a part of original
        label (str): css class of the label

    Returns:
        str: The original string with the string_part highlighted if found
    """
    string_part_escaped = re.escape(string_part)
    # This stops double-wrapping look for first occurrence without a label
    pattern = rf"{string_part_escaped}(?!</label>)"

    def replace_first_match(match):
        return f'<label class="{label}">{match.group()}</label>'
    original_highlighted = re.sub(
        pattern, replace_first_match, original, count=1)

    return original_highlighted


def element_to_string(element: ET.Element, escaped_xml: str, label: str):
    """
    This function takes an element turns in into a string and if the string is part of the escaped_xml string it will be enclosed with a HTML label

    Returns:
        str: An HTML-escaped XML string with specific elements highlighted.
    """
    element_str = ET.tostring(
        element, encoding="utf-8").decode("utf-8").replace(" />", "/>")
    element_str = element_str.replace("ns0:", "")
    escaped_element_str = escape(element_str).rstrip()
    if escaped_element_str in escaped_xml:
        return highlight_first_occurrence(
            escaped_xml, escaped_element_str, label)
    elif escaped_element_str.replace('&quot;', "&apos;") in escaped_xml:
        return highlight_first_occurrence(
            escaped_xml, escaped_element_str.replace(
                '&quot;', "&apos;"), label)
    else:
        element_str = replace_self_closing_tag(element_str)
        escaped_element_str = escape(element_str).rstrip()
        if escaped_element_str in escaped_xml:
            return highlight_first_occurrence(
                escaped_xml, escaped_element_str, label)
        return escaped_xml


def generate_highlighted_string_xml(
        original_xml: str,
        remaining_tree: ET.ElementTree,
        red_tree: ET.ElementTree,
        number_types: list) -> str:
    """
    This method takes an XML string and an ElementTree object representing a subset of the XML.
    It escapes the XML string for HTML display and then highlights the elements from the
    ElementTree within the escaped XML string. Elements to be highlighted are wrapped in a
    <label> tag.

    Returns:
        str: An HTML-escaped XML string with specific elements highlighted.
    """
    escaped_xml = escape(original_xml)

    for element in remaining_tree.getroot().findall('.//head/variable'):
        escaped_xml = element_to_string(element, escaped_xml, "red")

    bool_element = remaining_tree.getroot().find(".//boolean")
    if bool_element is not None:
        escaped_xml = element_to_string(bool_element, escaped_xml, "red")

    for element in remaining_tree.getroot().findall('.//result'):
        label = "yellow"
        for elem in red_tree.getroot().findall('.//result'):
            if xml_elements_equal(element, elem, False, [], number_types, {}):
                label = "red"
        escaped_xml = element_to_string(element, escaped_xml, label)

    return escaped_xml


def strip_namespace(tree: ET.ElementTree) -> ET.ElementTree:
    """
    Removes the namespace from the tags in an XML ElementTree.

    Parameters:
        tree (ET.ElementTree): An XML ElementTree with namespace in the tags.

    Returns:
        ET.ElementTree: The modified XML ElementTree with namespace removed from tags.
    """
    for elem in tree.iter():
        elem.tag = elem.tag.partition("}")[-1]
    return tree


def generate_html_for_xml(
        xml1: str,
        xml2: str,
        remaining_tree1: ET.ElementTree,
        remaining_tree2: ET.ElementTree,
        red_tree1: ET.ElementTree,
        red_tree2: ET.ElementTree,
        number_types: list) -> tuple:
    """
    Generates HTML representations for two XML strings with specific elements highlighted.

    Returns:
        tuple (str, str, str, str): A tuple containing four HTML-escaped and highlighted XML strings. (XML1, XML2, XML1 RED, XML2 RED)
    """
    strip_namespace(red_tree1)
    strip_namespace(red_tree2)
    strip_namespace(remaining_tree1)
    strip_namespace(remaining_tree2)
    remaining_tree1_string = ET.tostring(remaining_tree1.getroot(
    ), encoding='utf-8').decode("utf-8").replace(" />", "/>").replace("ns0:", "")
    remaining_tree2_string = ET.tostring(remaining_tree2.getroot(
    ), encoding='utf-8').decode("utf-8").replace(" />", "/>").replace("ns0:", "")
    highlighted_xml1 = generate_highlighted_string_xml(
        xml1, remaining_tree1, red_tree1, number_types)
    highlighted_xml2 = generate_highlighted_string_xml(
        xml2, remaining_tree2, red_tree2, number_types)
    highlighted_xml1_only_red = generate_highlighted_string_xml(
        remaining_tree1_string, remaining_tree1, red_tree1, number_types)
    highlighted_xml2_only_red = generate_highlighted_string_xml(
        remaining_tree2_string, remaining_tree2, red_tree2, number_types)

    return highlighted_xml1, highlighted_xml2, highlighted_xml1_only_red, highlighted_xml2_only_red


def xml_elements_equal(
        element1: ET.Element,
        element2: ET.Element,
        compare_with_intended_behaviour: bool,
        alias: List[Tuple[str, str]],
        number_types: list,
        map_bnodes: dict) -> bool:
    """
    Compares two XML elements for equality in tags, attributes and text.

    Parameters:
        element1 (ET.Element): The first XML element
        element2 (ET.Element): The second XML element
        compare_with_intended_behaviour (bool): Bool to determine whether to use intended behaviour aliases in comparison.
        alias (List[Tuple[str, str]]): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.
        map_bnodes (dict): Dictionary mapping the used bnodes.

    Returns:
        bool: True if elements are considered equal and if not False.
    """
    if len(list(element1)) != len(list(element2)):
        return False

    is_number = False
    if element1.tag != element2.tag:
        if not compare_with_intended_behaviour or not (element1.tag, element2.tag) in alias and not (element2.tag, element1.tag) in alias:
            return False

    if element1.attrib != element2.attrib:
        if isinstance(
                element1.attrib,
                dict) != isinstance(
                element2.attrib,
                dict):
            return False
        if ((element1.attrib.get("datatype") is not None or element2.attrib.get("datatype") != "http://www.w3.org/2001/XMLSchema#string") and
            (element2.attrib.get("datatype") is not None or element1.attrib.get("datatype") != "http://www.w3.org/2001/XMLSchema#string")):
            if not isinstance(element1.attrib, dict):
                if not compare_with_intended_behaviour or not (element1.attrib, element2.attrib) in alias and not (element2.attrib, element1.attrib) in alias:
                    return False
            if isinstance(element1.attrib, dict):
                if element1.attrib.get("datatype") is None and element2.attrib.get(
                        "datatype") is None:
                    # Check if language tags are equal, treat them as case-insensitive ex. en-US = en-us
                    xml_lang_key = '{http://www.w3.org/XML/1998/namespace}lang'
                    if xml_lang_key in element1.attrib and xml_lang_key in element2.attrib:
                        if not element1.attrib[xml_lang_key].lower() == element2.attrib[xml_lang_key].lower():
                            return False
                    else:
                        return False
                else:
                    if not compare_with_intended_behaviour or not (element1.attrib.get("datatype"),
                                                                   element2.attrib.get("datatype")) in alias and not (element2.attrib.get("datatype"),
                                                                                                      element1.attrib.get("datatype")) in alias:
                        return False

    if (element1.attrib.get("datatype") in number_types) != (
            element2.attrib.get("datatype") in number_types):
        return False

    if element1.attrib.get("datatype") in number_types and element2.attrib.get(
            "datatype") in number_types:
        is_number = True

    if element1.tail != element2.tail:
        if (
            (
                isinstance(
                    element1.tail,
                    str) and element2.tail is None and not element1.tail.strip() == "") and (
                isinstance(
                    element2.tail,
                    str) and element1.tail is None and not element2.tail.strip() == "")) or (
                        isinstance(
                            element1.tail,
                            str) and isinstance(
                                element2.tail,
                            str) and element1.tail.strip() != element2.tail.strip()):
            return False

    if element1.text != element2.text:
        if element1.tag == "{http://www.w3.org/2005/sparql-results#}bnode":
            if element1.text not in map_bnodes and element2.text not in map_bnodes:
                map_bnodes[element1.text] = element2.text
                map_bnodes[element2.text] = element1.text
                return all(any(xml_elements_equal(
                    c1,
                    c2,
                    compare_with_intended_behaviour,
                    alias,
                    number_types,
                    map_bnodes) for c2 in element2) for c1 in element1)
            elif map_bnodes.get(element1.text) == element2.text and map_bnodes.get(element2.text) == element1.text:
                return all(any(xml_elements_equal(
                    c1,
                    c2,
                    compare_with_intended_behaviour,
                    alias,
                    number_types,
                    map_bnodes) for c2 in element2) for c1 in element1)
            return False
        if (element1.text is None and element2.text.strip() == "") or (
                element2.text is None and element1.text.strip() == ""):
            return all(any(xml_elements_equal(
                    c1,
                    c2,
                    compare_with_intended_behaviour,
                    alias,
                    number_types,
                    map_bnodes) for c2 in element2) for c1 in element1)
        if element1.text is None or element2.text is None:
            return False
        if element1.text.strip() == element2.text.strip():
            return all(any(xml_elements_equal(
                c1,
                c2,
                compare_with_intended_behaviour,
                alias,
                number_types,
                map_bnodes) for c2 in element2) for c1 in element1)
        if is_number:
            if float(element1.text) == float(element2.text):
                return all(any(xml_elements_equal(
                    c1,
                    c2,
                    compare_with_intended_behaviour,
                    alias,
                    number_types,
                    map_bnodes) for c2 in element2) for c1 in element1)
        if not compare_with_intended_behaviour or not (element1.text, element2.text) in alias and not (element2.text, element1.text) in alias:
            return False
    return all(any(xml_elements_equal(
            c1,
            c2,
            compare_with_intended_behaviour,
            alias,
            number_types,
            map_bnodes) for c2 in element2) for c1 in element1)


def xml_remove_equal_elements(
        parent1: ET.Element,
        parent2: ET.Element,
        use_config: bool,
        alias: List[Tuple[str, str]],
        number_types: list,
        map_bnodes: dict):
    """
    Compares and removes equal child elements from two parent XML elements.

    This method iterates over the children of two given parent XML elements and removes
    matching children from both parents.

    Parameters:
        parent1 (ET.Element): The first parent XML element.
        parent2 (ET.Element): The second parent XML element.
        use_config (bool): Configuration Bool to control comparison behavior.
        alias (dict): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.
        map_bnodes (dict): Dictionary mapping the used bnodes.
    """
    for child1 in list(parent1):
        for child2 in list(parent2):
            if xml_elements_equal(
                    child1,
                    child2,
                    use_config,
                    alias,
                    number_types,
                    map_bnodes):
                parent1.remove(child1)
                parent2.remove(child2)
                break


def compare_xml(
        expected_xml: str,
        query_xml: str,
        alias: List[Tuple[str, str]],
        number_types: list) -> tuple:
    """
    Compares two XML documents, identifies differences and generates HTML representations.

    This method compares two XML documents and identifies differences.
    It removes equal elements in both documents and generates HTML representations highlighting the remaining differences.

    Parameters:
        expected_xml (str): The expected XML content as a string.
        query_xml (str): The query XML content as a string.
        alias (dict): Dictionary with aliases for datatypes ex. int = integer .
        number_types (list): List containing all datatypes that should be used as numbers.

    Returns:
        tuple (str,str,str,str,str,str): A tuple containing the status, error type and the strings XML1, XML2, XML1 RED, XML2 RED
    """
    query_xml = md.parseString(query_xml).toxml()
    query_xml = md.parseString(query_xml).toprettyxml(indent="  ")
    map_bnodes = {}
    status = Status.FAILED
    error_type = ErrorMessage.RESULTS_NOT_THE_SAME
    expected_tree = ET.ElementTree(ET.fromstring(expected_xml))
    query_tree = ET.ElementTree(ET.fromstring(query_xml))

    # Compare and remove equal elements in <head>
    head1 = expected_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}head")
    head2 = query_tree.find(".//{http://www.w3.org/2005/sparql-results#}head")
    if head1 is not None and head2 is not None:
        xml_remove_equal_elements(
            head1,
            head2,
            False,
            alias,
            number_types,
            map_bnodes)

    # Compare and remove equal <boolean>
    expected_bool = expected_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}boolean")
    query_bool = query_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}boolean")
    if expected_bool is not None and query_bool is not None:
        if str(expected_bool.text) == str(query_bool.text):
            expected_tree.getroot().remove(expected_bool)
            query_tree.getroot().remove(query_bool)

    expected_bool = expected_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}boolean")
    query_bool = query_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}boolean")
    
    # Compare and remove equal <result> elements in <results>
    results1 = expected_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}results")
    results2 = query_tree.find(
        ".//{http://www.w3.org/2005/sparql-results#}results")

    if results1 is not None and results2 is not None:
        xml_remove_equal_elements(
            results1,
            results2,
            False,
            alias,
            number_types,
            map_bnodes)
    # Copy expected_tree
    expected_tree_string = ET.tostring(expected_tree.getroot())
    copied_expected_tree = ET.ElementTree(ET.fromstring(expected_tree_string))

    # Copy query_tree
    query_tree_string = ET.tostring(query_tree.getroot())
    copied_query_tree = ET.ElementTree(ET.fromstring(query_tree_string))
    if (
        results1 is not None and results2 is not None and len(
            list(results1)) == 0 and len(
            list(results2)) == 0 and len(
                list(head1)) == 0 and len(
                    list(head2)) == 0) or (
                        results1 is None and results2 is None and head1 is None and head2 is None and expected_bool is None and query_bool is None):
        status = Status.PASSED
        error_type = ""
    else:
        if results1 is not None and results2 is not None:
            xml_remove_equal_elements(
                results1,
                results2,
                True,
                alias,
                number_types,
                map_bnodes)

            if len(list(results1)) == 0 and len(list(results2)) == 0:
                status = Status.INTENDED
                error_type = ErrorMessage.INTENDED_MSG
        elif expected_bool is None and query_bool is None:
            status = Status.PASSED
            error_type = ""

    expected_string, query_string, expected_string_red, query_string_red = generate_html_for_xml(
        expected_xml, query_xml, copied_expected_tree, copied_query_tree, expected_tree, query_tree, number_types)
    return status, error_type, expected_string, query_string, expected_string_red, query_string_red
