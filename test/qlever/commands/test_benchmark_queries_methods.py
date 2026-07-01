import pytest

from qlever.commands.benchmark_queries import (
    filter_queries,
    get_result_size,
    get_single_int_result,
    parse_queries_tsv,
    parse_queries_yml,
    resolve_benchmark_metadata,
    sparql_query_type,
)

MODULE = "qlever.commands.benchmark_queries"

JSON_ACCEPT_HEADERS_AND_RESULT_FILES = [
    ("application/sparql-results+json", "result.json"),
    ("application/qlever-results+json", "result.json"),
]

ALL_ACCEPT_HEADERS_AND_RESULT_FILES = [
    ("text/csv", "result.csv"),
    ("text/tab-separated-values", "result.tsv"),
    *JSON_ACCEPT_HEADERS_AND_RESULT_FILES,
]


@pytest.mark.parametrize("download_or_count", ["count", "download"])
@pytest.mark.parametrize(
    "accept_header, result_file", ALL_ACCEPT_HEADERS_AND_RESULT_FILES
)
def test_empty_result_non_construct_describe(
    mock_command,
    download_or_count,
    accept_header,
    result_file,
):
    mock_path_stat = mock_command(MODULE, "Path.stat")
    mock_path_stat.return_value.st_size = 0
    run_cmd_mock = mock_command(MODULE, "run_command")

    size, err = get_result_size(
        count_only=download_or_count == "count",
        query_type="SELECT",
        accept_header=accept_header,
        result_file=result_file,
    )

    assert size == 0
    assert err["short"] == "Empty result"
    assert (
        err["long"] == "curl returned with code 200, but the result is empty"
    )
    run_cmd_mock.assert_not_called()


@pytest.mark.parametrize("download_or_count", ["count", "download"])
@pytest.mark.parametrize(
    "accept_header, result_file", ALL_ACCEPT_HEADERS_AND_RESULT_FILES
)
@pytest.mark.parametrize("query_type", ["CONSTRUCT", "DESCRIBE"])
def test_empty_result_construct_describe(
    mock_command,
    download_or_count,
    query_type,
    accept_header,
    result_file,
):
    mock_path_stat = mock_command(MODULE, "Path.stat")
    mock_path_stat.return_value.st_size = 0
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "42"

    size, err = get_result_size(
        count_only=download_or_count == "count",
        query_type=query_type,
        accept_header=accept_header,
        result_file=result_file,
    )

    assert size == 42
    assert err is None


@pytest.mark.parametrize("download_or_count", ["count", "download"])
@pytest.mark.parametrize(
    "accept_header, result_file", ALL_ACCEPT_HEADERS_AND_RESULT_FILES
)
def test_count_and_download_success(
    mock_command,
    download_or_count,
    accept_header,
    result_file,
):
    mock_path_stat = mock_command(MODULE, "Path.stat")
    mock_path_stat.return_value.st_size = 100

    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "42"

    size, err = get_result_size(
        count_only=download_or_count == "count",
        query_type="SELECT",
        accept_header=accept_header,
        result_file=result_file,
    )

    run_cmd_mock.assert_called_once()
    assert size == 42
    assert err is None


def test_download_turtle_success(mock_command):
    mock_path_stat = mock_command(MODULE, "Path.stat")
    mock_path_stat.return_value.st_size = 100
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "42"

    size, err = get_result_size(
        count_only=False,
        query_type="SELECT",
        accept_header="text/turtle",
        result_file="result.ttl",
    )

    run_cmd_mock.assert_called_once()
    assert size == 42
    assert err is None


@pytest.mark.parametrize("download_or_count", ["count", "download"])
@pytest.mark.parametrize(
    "accept_header, result_file", JSON_ACCEPT_HEADERS_AND_RESULT_FILES
)
def test_download_and_count_json_malformed(
    mock_command,
    download_or_count,
    accept_header,
    result_file,
):
    mock_path_stat = mock_command(MODULE, "Path.stat")
    mock_path_stat.return_value.st_size = 100

    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.side_effect = Exception("jq failed")

    size, err = get_result_size(
        count_only=download_or_count == "count",
        query_type="SELECT",
        accept_header=accept_header,
        result_file=result_file,
    )

    run_cmd_mock.assert_called_once()
    assert size == 0
    assert err["short"] == "Malformed JSON"
    assert (
        "curl returned with code 200, but the JSON is malformed: "
        in err["long"]
    )
    assert "jq failed" in err["long"]


def test_single_int_result_success(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "123"

    single_int_result = get_single_int_result("result.json")

    run_cmd_mock.assert_called_once()
    assert single_int_result == 123


def test_single_int_result_non_int_fail(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "abc"

    single_int_result = get_single_int_result("result.json")

    run_cmd_mock.assert_called_once()
    assert single_int_result is None


def test_single_int_result_failure(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.side_effect = Exception("jq failed")

    single_int_result = get_single_int_result("result.json")

    run_cmd_mock.assert_called_once()
    assert single_int_result is None


@pytest.mark.parametrize(
    "query, expected",
    [
        # Basic types
        ("SELECT ?x WHERE { ?x ?y ?z }", "SELECT"),
        ("ASK { ?x ?y ?z }", "ASK"),
        ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "CONSTRUCT"),
        ("DESCRIBE <http://example.org>", "DESCRIBE"),
        # Case insensitivity
        ("Select ?x WHERE { ?x ?y ?z }", "SELECT"),
        ("ask { ?x ?y ?z }", "ASK"),
        ("construct { ?s ?p ?o } WHERE { ?s ?p ?o }", "CONSTRUCT"),
        ("Describe <http://example.org>", "DESCRIBE"),
        # With prefixes (first match wins)
        (
            "PREFIX ex: <http://example.org/> SELECT ?x WHERE { ?x ex:p ?y }",
            "SELECT",
        ),
        # First keyword wins when multiple present
        ("SELECT ?x WHERE { ?x ?y ?z } CONSTRUCT { ?a ?b ?c }", "SELECT"),
        # Unknown types
        ("DELETE WHERE { ?x ?y ?z }", "UNKNOWN"),
        ("", "UNKNOWN"),
        ("SELECTED ?x WHERE { ?x ?y ?z }", "UNKNOWN"),
    ],
)
def test_sparql_query_type(query, expected):
    assert sparql_query_type(query) == expected


SAMPLE_QUERIES = [
    ("q1", "cities query", "SELECT ?x WHERE { ?x a :City }"),
    ("q2", "countries", "SELECT ?c WHERE { ?c a :Country }"),
    ("q3", "people", "SELECT ?p WHERE { ?p a :Person }"),
    ("q4", "rivers", "CONSTRUCT { ?r ?p ?o } WHERE { ?r a :River }"),
    ("q5", "mountains", "ASK { ?m a :Mountain }"),
]


@pytest.mark.parametrize(
    "query_ids, expected_names",
    [
        # Single ID
        ("2", ["q2"]),
        # Range
        ("1-3", ["q1", "q2", "q3"]),
        # $ as end of range
        ("3-$", ["q3", "q4", "q5"]),
        # $ as single value (last query)
        ("$", ["q5"]),
        # Comma-separated mixed
        ("1,3,5", ["q1", "q3", "q5"]),
        ("1-2,4-5", ["q1", "q2", "q4", "q5"]),
        # All queries
        ("1-$", ["q1", "q2", "q3", "q4", "q5"]),
        # Out-of-range indices skipped
        ("99", []),
        ("4-7", ["q4", "q5"]),
        # Leading/trailing commas (empty parts skipped)
        (",1,2", ["q1", "q2"]),
        ("1,2,", ["q1", "q2"]),
        # Whitespace around parts
        (" 1 , 2 ", ["q1", "q2"]),
    ],
)
def test_filter_queries_by_ids(query_ids, expected_names):
    result = filter_queries(SAMPLE_QUERIES, query_ids, None)
    assert [name for name, _, _ in result] == expected_names


@pytest.mark.parametrize(
    "query_ids",
    [
        # Negative range start → int("") raises ValueError
        "-2",
        # Non-numeric
        "abc",
        # Duplicate via single IDs
        "1,1,2",
        "1-3,2,4,$",
        # Duplicate via overlapping ranges
        "1-3,2-4",
    ],
)
def test_filter_queries_invalid_ids(query_ids):
    assert filter_queries(SAMPLE_QUERIES, query_ids, None) == []


def test_filter_queries_empty_input():
    assert filter_queries([], "1-$", None) == []


@pytest.mark.parametrize(
    "query_regex, expected_names",
    [
        # Match on name
        ("q1", ["q1"]),
        # Match on description
        ("cities", ["q1"]),
        # Match on query body
        ("CONSTRUCT", ["q4"]),
        # Case-insensitive
        ("CITIES", ["q1"]),
        # Regex matching multiple queries
        ("Country|Person", ["q2", "q3"]),
        # No match
        ("abcd", []),
    ],
)
def test_filter_queries_by_regex(query_regex, expected_names):
    result = filter_queries(SAMPLE_QUERIES, "1-$", query_regex)
    assert [name for name, _, _ in result] == expected_names


@pytest.mark.parametrize(
    "query_ids, query_regex, expected_names",
    [
        ("1-3", "Country", ["q2"]),
        ("2,3", "cities", []),
    ],
)
def test_filter_queries_ids_and_regex_combined(
    query_ids, query_regex, expected_names
):
    result = filter_queries(SAMPLE_QUERIES, query_ids, query_regex)
    assert [name for name, _, _ in result] == expected_names


def test_filter_queries_invalid_regex():
    assert filter_queries(SAMPLE_QUERIES, "1-$", "[invalid") == []


VALID_YML = """\
name: My Benchmark
description: A test benchmark
queries:
  - name: q1
    description: first query
    query: SELECT ?x WHERE { ?x ?y ?z }
  - name: q2
    query: ASK { ?x ?y ?z }
"""


def test_parse_queries_yml_valid(tmp_path):
    yml_file = tmp_path / "test.yml"
    yml_file.write_text(VALID_YML)
    name, description, queries = parse_queries_yml(str(yml_file))
    assert name == "My Benchmark"
    assert description == "A test benchmark"
    assert queries == [
        ("q1", "first query", "SELECT ?x WHERE { ?x ?y ?z }"),
        ("q2", "", "ASK { ?x ?y ?z }"),
    ]


def test_parse_queries_yml_no_top_level_name(tmp_path):
    yml_file = tmp_path / "test.yml"
    yml_file.write_text("queries:\n  - name: q1\n    query: SELECT 1\n")
    name, description, queries = parse_queries_yml(str(yml_file))
    assert name is None
    assert description is None
    assert queries == [("q1", "", "SELECT 1")]


@pytest.mark.parametrize(
    "yml_content",
    [
        # Missing top-level 'queries' key
        "name: test\n",
        # 'queries' is not a list
        "queries: not_a_list\n",
        # Query item missing 'name'
        "queries:\n  - query: SELECT 1\n",
        # Query item missing 'query'
        "queries:\n  - name: q1\n",
        # Query item is not a dict
        "queries:\n  - just a string\n",
        # Not a dict at top level
        "- item1\n- item2\n",
        # Invalid YAML syntax
        ":\n  bad: [yaml\n",
    ],
)
def test_parse_queries_yml_invalid(tmp_path, yml_content):
    yml_file = tmp_path / "test.yml"
    yml_file.write_text(yml_content)
    assert parse_queries_yml(str(yml_file)) == (None, None, [])


def test_parse_queries_tsv_valid(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = (
        "q1\tSELECT ?x WHERE { ?x ?y ?z }\nq2\tASK { ?x ?y ?z }\n"
    )
    result = parse_queries_tsv("cat queries.tsv")
    assert result == [
        ("q1", "", "SELECT ?x WHERE { ?x ?y ?z }"),
        ("q2", "", "ASK { ?x ?y ?z }"),
    ]


def test_parse_queries_tsv_tab_in_query(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "q1\tSELECT ?x\tWHERE { ?x ?y ?z }\n"
    result = parse_queries_tsv("cat queries.tsv")
    assert result == [("q1", "", "SELECT ?x\tWHERE { ?x ?y ?z }")]


def test_parse_queries_tsv_empty_output(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = ""
    assert parse_queries_tsv("cat queries.tsv") == []


def test_parse_queries_tsv_command_failure(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.side_effect = Exception("command failed")
    assert parse_queries_tsv("cat queries.tsv") == []


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            dict(
                cli=("CLI", "CLI Desc"),
                yml=("YML", "YML Desc"),
                dataset="wikidata",
                expected=("CLI", "CLI Desc"),
            ),
            id="cli-takes-priority",
        ),
        pytest.param(
            dict(
                cli=(None, None),
                yml=("YML", "YML Desc"),
                dataset="wikidata",
                expected=("YML", "YML Desc"),
            ),
            id="yml-over-default",
        ),
        pytest.param(
            dict(
                cli=(None, None),
                yml=(None, None),
                dataset="wikidata",
                expected=("Wikidata", "auto"),
            ),
            id="default-from-dataset",
        ),
        pytest.param(
            dict(
                cli=(None, None),
                yml=(None, None),
                dataset=None,
                expected=(None, None),
            ),
            id="all-none",
        ),
        pytest.param(
            dict(
                cli=("CLI", None),
                yml=(None, "YML Desc"),
                dataset="wikidata",
                expected=("CLI", "YML Desc"),
            ),
            id="cli-name-yml-desc",
        ),
    ],
)
def test_resolve_benchmark_metadata(case):
    name, desc = resolve_benchmark_metadata(
        *case["cli"], *case["yml"], case["dataset"]
    )
    exp_name, exp_desc = case["expected"]
    assert name == exp_name
    if exp_desc == "auto":
        assert case["dataset"].capitalize() in desc
        assert "benchmark-queries" in desc
    else:
        assert desc == exp_desc
