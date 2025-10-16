import pytest

from qlever.commands.benchmark_queries import BenchmarkQueriesCommand

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

    size, err = BenchmarkQueriesCommand().get_result_size(
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

    size, err = BenchmarkQueriesCommand().get_result_size(
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

    size, err = BenchmarkQueriesCommand().get_result_size(
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

    size, err = BenchmarkQueriesCommand().get_result_size(
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

    size, err = BenchmarkQueriesCommand().get_result_size(
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

    single_int_result = BenchmarkQueriesCommand().get_single_int_result(
        "result.json"
    )

    run_cmd_mock.assert_called_once()
    assert single_int_result == 123


def test_single_int_result_non_int_fail(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "abc"

    single_int_result = BenchmarkQueriesCommand().get_single_int_result(
        "result.json"
    )

    run_cmd_mock.assert_called_once()
    assert single_int_result is None


def test_single_int_result_failure(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.side_effect = Exception("jq failed")

    single_int_result = BenchmarkQueriesCommand().get_single_int_result(
        "result.json"
    )

    run_cmd_mock.assert_called_once()
    assert single_int_result is None
